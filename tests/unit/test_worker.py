import asyncio
import contextlib
from unittest.mock import AsyncMock, MagicMock

import pytest

from kobold.models import Task as TaskModel
from kobold.models import TaskStatus
from kobold.task_queue import TaskQueue
from kobold.tasks.base import Task
from kobold.tasks.convert import ConvertTask
from kobold.tasks.ingest import IngestTask
from kobold.tasks.metadata import MetadataTask
from kobold.tasks.organize import OrganizeTask
from kobold.worker import worker


@pytest.fixture
def mock_tasks():
    """Create mock tasks for each job type."""
    return {
        IngestTask.TASK_TYPE: MagicMock(spec=Task, process=AsyncMock()),
        MetadataTask.TASK_TYPE: MagicMock(spec=Task, process=AsyncMock()),
        ConvertTask.TASK_TYPE: MagicMock(spec=Task, process=AsyncMock()),
        OrganizeTask.TASK_TYPE: MagicMock(spec=Task, process=AsyncMock()),
    }


@pytest.fixture
def mock_queue():
    queue = MagicMock(spec=TaskQueue)
    queue.recover_stale_tasks.return_value = 0
    queue.task_event = asyncio.Event()
    return queue


async def run_worker_with_jobs(
    mock_queue: MagicMock,
    mock_tasks: dict[str, Task],
    jobs: list[TaskModel],
    poll_interval: float = 0.01,
) -> None:
    """Run the worker with jobs, then cancel it cleanly."""
    job_iter = iter(jobs)

    def fetch_next():
        try:
            return next(job_iter)
        except StopIteration:
            return None

    mock_queue.fetch_next_task.side_effect = fetch_next

    worker_task = asyncio.create_task(worker(mock_queue, mock_tasks, poll_interval))

    await asyncio.sleep(0.05)
    worker_task.cancel()

    with contextlib.suppress(asyncio.CancelledError):
        await worker_task


@pytest.mark.asyncio
async def test_worker_startup_and_shutdown(mock_tasks, mock_queue):
    await run_worker_with_jobs(mock_queue, mock_tasks, [])

    mock_queue.recover_stale_tasks.assert_called_once()


@pytest.mark.asyncio
async def test_worker_processes_ingest_job(mock_tasks, mock_queue):
    job = TaskModel(
        id=1,
        type=IngestTask.TASK_TYPE,
        payload={"path": "/tmp/book.epub"},
        status=TaskStatus.PENDING,
    )

    await run_worker_with_jobs(mock_queue, mock_tasks, [job])

    mock_tasks[IngestTask.TASK_TYPE].process.assert_awaited_once_with(job.payload)
    mock_queue.complete_task.assert_called_with(1)


@pytest.mark.asyncio
async def test_worker_handles_unknown_job_type(mock_tasks, mock_queue):
    job = MagicMock()
    job.id = 1
    job.type = "UNKNOWN_TYPE"  # String since TaskModel.type is now str
    job.payload = {}
    job.retry_count = 0
    job.max_retries = 3

    # Use empty tasks to make it "unknown"
    tasks = {}

    await run_worker_with_jobs(mock_queue, tasks, [job])

    mock_queue.complete_task.assert_called_with(
        1, error="Unknown task type: UNKNOWN_TYPE", status=TaskStatus.FAILED
    )


@pytest.mark.asyncio
async def test_worker_retries_failed_job(mock_tasks, mock_queue):
    job = TaskModel(
        id=1,
        type=MetadataTask.TASK_TYPE,
        payload={},
        status=TaskStatus.PENDING,
        retry_count=0,
        max_retries=3,
    )

    mock_tasks[MetadataTask.TASK_TYPE].process.side_effect = Exception("API error")

    await run_worker_with_jobs(mock_queue, mock_tasks, [job])

    mock_queue.retry_task.assert_called_once()
    assert "API error" in mock_queue.retry_task.call_args[0][1]


@pytest.mark.asyncio
async def test_worker_moves_to_dead_letter_after_max_retries(mock_tasks, mock_queue):
    job = TaskModel(
        id=1,
        type=MetadataTask.TASK_TYPE,
        payload={},
        status=TaskStatus.PENDING,
        retry_count=3,
        max_retries=3,
    )

    mock_tasks[MetadataTask.TASK_TYPE].process.side_effect = Exception("API error")

    await run_worker_with_jobs(mock_queue, mock_tasks, [job])

    mock_queue.complete_task.assert_called_with(
        1, error="Exception: API error", status=TaskStatus.DEAD_LETTER
    )


@pytest.mark.asyncio
async def test_worker_handles_critical_loop_error(mock_tasks, mock_queue):
    """Test that worker backs off after a critical error."""
    call_count = 0

    def fetch_with_error():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise Exception("Database is gone")
        return None

    mock_queue.fetch_next_task.side_effect = fetch_with_error

    worker_task = asyncio.create_task(worker(mock_queue, mock_tasks, 0.01))

    await asyncio.sleep(0.1)
    worker_task.cancel()

    with contextlib.suppress(asyncio.CancelledError):
        await worker_task

    assert call_count >= 1


@pytest.mark.asyncio
async def test_worker_processes_convert_job(mock_tasks, mock_queue):
    job = TaskModel(
        id=2,
        type=ConvertTask.TASK_TYPE,
        payload={"book_id": "123e4567-e89b-12d3-a456-426614174000"},
        status=TaskStatus.PENDING,
    )

    await run_worker_with_jobs(mock_queue, mock_tasks, [job])

    mock_tasks[ConvertTask.TASK_TYPE].process.assert_awaited_once_with(job.payload)
    mock_queue.complete_task.assert_called_with(2)
