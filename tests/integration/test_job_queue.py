from datetime import UTC, datetime, timedelta

import pytest
from sqlmodel import Session, SQLModel, create_engine

from kobold.job_queue import JobQueue
from kobold.models import Job, JobStatus, JobType


class TestJobQueue:
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
    def job_queue(self, test_engine, mock_settings) -> JobQueue:
        return JobQueue(mock_settings, test_engine)

    def test_add_job(self, job_queue: JobQueue) -> None:
        result = job_queue.add_job(
            JobType.INGEST,
            payload={"path": "/test/file.epub"},
        )

        assert result is not None
        assert result.type == JobType.INGEST
        assert result.status == JobStatus.PENDING
        assert result.payload["path"] == "/test/file.epub"

    def test_add_job_with_deduplication(
        self,
        job_queue: JobQueue,
    ) -> None:
        job1 = job_queue.add_job(
            JobType.INGEST,
            payload={"path": "/test/file.epub"},
            deduplicate_key="/test/file.epub",
        )

        job2 = job_queue.add_job(
            JobType.INGEST,
            payload={"path": "/test/file.epub"},
            deduplicate_key="/test/file.epub",
        )

        assert job1 is not None
        assert job2 is None  # Should be deduplicated

    def test_fetch_next_job_empty_queue(
        self,
        job_queue: JobQueue,
    ) -> None:
        result = job_queue.fetch_next_job()
        assert result is None

    def test_fetch_next_job_fifo_order(
        self,
        job_queue: JobQueue,
    ) -> None:
        job_queue.add_job(JobType.INGEST, payload={"order": 1})
        job_queue.add_job(JobType.INGEST, payload={"order": 2})
        job_queue.add_job(JobType.INGEST, payload={"order": 3})

        first = job_queue.fetch_next_job()
        second = job_queue.fetch_next_job()

        assert first is not None
        assert second is not None

        assert first.payload["order"] == 1
        assert second.payload["order"] == 2

    def test_fetch_next_job_marks_as_processing(
        self,
        job_queue: JobQueue,
    ) -> None:
        job_queue.add_job(JobType.METADATA, payload={})

        fetched = job_queue.fetch_next_job()

        assert fetched is not None
        assert fetched.status == JobStatus.PROCESSING
        assert fetched.started_at is not None

    def test_complete_job_success(
        self,
        job_queue: JobQueue,
        test_engine,
    ) -> None:
        job_queue.add_job(JobType.CONVERT, payload={})
        fetched = job_queue.fetch_next_job()
        assert fetched is not None

        job_queue.complete_job(fetched.id)

        with Session(test_engine) as session:
            completed = session.get(Job, fetched.id)
            assert completed is not None
            assert completed.status == JobStatus.COMPLETED
            assert completed.completed_at is not None

    def test_complete_job_with_error(
        self,
        job_queue: JobQueue,
        test_engine,
    ) -> None:
        job_queue.add_job(JobType.INGEST, payload={})
        fetched = job_queue.fetch_next_job()
        assert fetched is not None

        job_queue.complete_job(fetched.id, error="Something went wrong")

        with Session(test_engine) as session:
            failed = session.get(Job, fetched.id)
            assert failed is not None
            assert failed.status == JobStatus.FAILED
            assert failed.error_message == "Something went wrong"

    def test_retry_job(
        self,
        job_queue: JobQueue,
        test_engine,
    ) -> None:
        job_queue.add_job(JobType.METADATA, payload={})
        fetched = job_queue.fetch_next_job()
        assert fetched is not None

        job_queue.retry_job(fetched.id, "Temporary error")

        with Session(test_engine) as session:
            retried = session.get(Job, fetched.id)
            assert retried is not None
            assert retried.status == JobStatus.PENDING
            assert retried.retry_count == 1
            assert retried.next_retry_at is not None
            assert retried.error_message == "Temporary error"

    def test_recover_stale_jobs(
        self,
        job_queue: JobQueue,
        test_engine,
        mock_settings,
    ) -> None:
        with Session(test_engine) as session:
            stale_job = Job(
                type=JobType.INGEST,
                payload={"path": "/stale"},
                status=JobStatus.PROCESSING,
                started_at=datetime.now(UTC) - timedelta(hours=2),
            )
            session.add(stale_job)
            session.commit()
            stale_id = stale_job.id

        mock_settings.JOB_STALE_MINUTES = 30
        mock_settings.JOB_MAX_RETRIES = 3

        recovered = job_queue.recover_stale_jobs()

        assert recovered == 1

        with Session(test_engine) as session:
            job = session.get(Job, stale_id)
            assert job is not None
            assert job.status == JobStatus.PENDING

    def test_complete_unknown_job_handles_gracefully(
        self,
        job_queue: JobQueue,
    ) -> None:
        from uuid import uuid4

        unknown_id = uuid4()
        # Should not raise
        job_queue.complete_job(unknown_id)

    def test_retry_unknown_job_handles_gracefully(
        self,
        job_queue: JobQueue,
    ) -> None:
        from uuid import uuid4

        unknown_id = uuid4()
        # Should not raise
        job_queue.retry_job(unknown_id, "Some error")

    def test_get_queue_stats(
        self,
        job_queue: JobQueue,
    ) -> None:
        job_queue.add_job(JobType.INGEST, payload={"order": 1})
        job_queue.add_job(JobType.INGEST, payload={"order": 2})
        job_queue.add_job(JobType.METADATA, payload={})

        job = job_queue.fetch_next_job()
        assert job is not None
        job_queue.complete_job(job.id)

        stats = job_queue.get_queue_stats()

        assert stats["PENDING"] == 2
        assert stats["COMPLETED"] == 1
        assert stats["PROCESSING"] == 0
