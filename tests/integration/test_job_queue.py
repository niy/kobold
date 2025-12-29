from datetime import UTC, datetime, timedelta

import pytest
from sqlmodel import Session, SQLModel, create_engine

from kobold.models import Task as TaskModel
from kobold.models import TaskStatus
from kobold.task_queue import TaskQueue
from kobold.tasks.convert import ConvertTask
from kobold.tasks.ingest import IngestTask
from kobold.tasks.metadata import MetadataTask


class TestTaskQueue:
    @pytest.fixture
    def test_engine(self, tmp_path):
        db_path = tmp_path / "test.db"
        engine = create_engine(
            f"sqlite:///{db_path}",
            connect_args={"check_same_thread": False},
        )
        SQLModel.metadata.create_all(engine)
        return engine

    @pytest.fixture
    def mock_settings(self):
        from unittest.mock import Mock

        settings = Mock()
        settings.JOB_STALE_MINUTES = 30
        settings.JOB_MAX_RETRIES = 3
        return settings

    @pytest.fixture
    def task_queue(self, test_engine, mock_settings) -> TaskQueue:
        return TaskQueue(mock_settings, test_engine)

    def test_add_task(self, task_queue: TaskQueue) -> None:
        result = task_queue.add_task(
            IngestTask.TASK_TYPE,
            payload={"path": "/test/file.epub"},
        )

        assert result is not None
        assert result.type == IngestTask.TASK_TYPE
        assert result.status == TaskStatus.PENDING
        assert result.payload["path"] == "/test/file.epub"

    def test_fetch_next_task_empty_queue(
        self,
        task_queue: TaskQueue,
    ) -> None:
        result = task_queue.fetch_next_task()
        assert result is None

    def test_fetch_next_task_fifo_order(
        self,
        task_queue: TaskQueue,
    ) -> None:
        task_queue.add_task(IngestTask.TASK_TYPE, payload={"order": 1})
        task_queue.add_task(IngestTask.TASK_TYPE, payload={"order": 2})
        task_queue.add_task(IngestTask.TASK_TYPE, payload={"order": 3})

        first = task_queue.fetch_next_task()
        second = task_queue.fetch_next_task()

        assert first is not None
        assert second is not None

        assert first.payload["order"] == 1
        assert second.payload["order"] == 2

    def test_fetch_next_task_marks_as_processing(
        self,
        task_queue: TaskQueue,
    ) -> None:
        task_queue.add_task(MetadataTask.TASK_TYPE, payload={})

        fetched = task_queue.fetch_next_task()

        assert fetched is not None
        assert fetched.status == TaskStatus.PROCESSING
        assert fetched.started_at is not None

    def test_complete_task_success(
        self,
        task_queue: TaskQueue,
        test_engine,
    ) -> None:
        task_queue.add_task(ConvertTask.TASK_TYPE, payload={})
        fetched = task_queue.fetch_next_task()
        assert fetched is not None

        task_queue.complete_task(fetched.id)

        with Session(test_engine) as session:
            completed = session.get(TaskModel, fetched.id)
            assert completed is not None
            assert completed.status == TaskStatus.COMPLETED
            assert completed.completed_at is not None

    def test_complete_task_with_error(
        self,
        task_queue: TaskQueue,
        test_engine,
    ) -> None:
        task_queue.add_task(IngestTask.TASK_TYPE, payload={})
        fetched = task_queue.fetch_next_task()
        assert fetched is not None

        task_queue.complete_task(fetched.id, error="Something went wrong")

        with Session(test_engine) as session:
            failed = session.get(TaskModel, fetched.id)
            assert failed is not None
            assert failed.status == TaskStatus.FAILED
            assert failed.error_message == "Something went wrong"

    def test_retry_task(
        self,
        task_queue: TaskQueue,
        test_engine,
    ) -> None:
        task_queue.add_task(MetadataTask.TASK_TYPE, payload={})
        fetched = task_queue.fetch_next_task()
        assert fetched is not None

        task_queue.retry_task(fetched.id, "Temporary error")

        with Session(test_engine) as session:
            retried = session.get(TaskModel, fetched.id)
            assert retried is not None
            assert retried.status == TaskStatus.PENDING
            assert retried.retry_count == 1
            assert retried.next_retry_at is not None
            assert retried.error_message == "Temporary error"

    def test_recover_stale_tasks(
        self,
        task_queue: TaskQueue,
        test_engine,
        mock_settings,
    ) -> None:
        with Session(test_engine) as session:
            stale_job = TaskModel(
                type=IngestTask.TASK_TYPE,
                payload={"path": "/stale"},
                status=TaskStatus.PROCESSING,
                started_at=datetime.now(UTC) - timedelta(hours=2),
            )
            session.add(stale_job)
            session.commit()
            stale_id = stale_job.id

        mock_settings.JOB_STALE_MINUTES = 30
        mock_settings.JOB_MAX_RETRIES = 3

        recovered = task_queue.recover_stale_tasks()

        assert recovered == 1

        with Session(test_engine) as session:
            job = session.get(TaskModel, stale_id)
            assert job is not None
            assert job.status == TaskStatus.PENDING

    def test_complete_unknown_job_handles_gracefully(
        self,
        task_queue: TaskQueue,
    ) -> None:
        from uuid import uuid4

        unknown_id = uuid4()
        # Should not raise
        task_queue.complete_task(unknown_id)

    def test_retry_unknown_job_handles_gracefully(
        self,
        task_queue: TaskQueue,
    ) -> None:
        from uuid import uuid4

        unknown_id = uuid4()
        # Should not raise
        task_queue.retry_task(unknown_id, "Some error")

    def test_get_queue_stats(
        self,
        task_queue: TaskQueue,
    ) -> None:
        task_queue.add_task(IngestTask.TASK_TYPE, payload={"order": 1})
        task_queue.add_task(IngestTask.TASK_TYPE, payload={"order": 2})
        task_queue.add_task(MetadataTask.TASK_TYPE, payload={})

        job = task_queue.fetch_next_task()
        assert job is not None
        task_queue.complete_task(job.id)

        stats = task_queue.get_queue_stats()

        assert stats["PENDING"] == 2
        assert stats["COMPLETED"] == 1
        assert stats["PROCESSING"] == 0

    def test_task_event_is_set_when_job_added(
        self,
        task_queue: TaskQueue,
    ) -> None:
        """Adding a job signals the event for real-time processing."""
        assert not task_queue.task_event.is_set()

        task_queue.add_task(IngestTask.TASK_TYPE, payload={"path": "/test.epub"})

        assert task_queue.task_event.is_set()

    def test_task_event_can_be_cleared_and_reused(
        self,
        task_queue: TaskQueue,
    ) -> None:
        """Event can be cleared and re-triggered for multiple jobs."""
        task_queue.add_task(IngestTask.TASK_TYPE, payload={"path": "/first.epub"})
        assert task_queue.task_event.is_set()

        task_queue.task_event.clear()
        assert not task_queue.task_event.is_set()

        task_queue.add_task(MetadataTask.TASK_TYPE, payload={"book_id": "123"})
        assert task_queue.task_event.is_set()
