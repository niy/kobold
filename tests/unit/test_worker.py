from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kobosync.job_queue import JobQueue
from kobosync.models import Job, JobStatus, JobType
from kobosync.worker import stop_event, worker


@pytest.fixture
def mock_dependencies():
    with (
        patch("kobosync.worker.IngestService", autospec=True) as mock_ingest_cls,
        patch("kobosync.worker.MetadataJobService", autospec=True) as mock_meta_cls,
        patch("kobosync.worker.ConversionJobService", autospec=True) as mock_conv_cls,
        patch("kobosync.worker.MetadataManager", autospec=True),
    ):
        mock_ingest_svc = mock_ingest_cls.return_value
        mock_meta_svc = mock_meta_cls.return_value
        mock_conv_svc = mock_conv_cls.return_value

        mock_ingest_svc.process_job = AsyncMock()
        mock_meta_svc.process_job = AsyncMock()
        mock_conv_svc.process_job = AsyncMock()

        yield {
            "ingest": mock_ingest_svc,
            "metadata": mock_meta_svc,
            "conversion": mock_conv_svc,
        }


@pytest.fixture
def mock_queue():
    queue = MagicMock(spec=JobQueue)
    queue.recover_stale_jobs.return_value = 0
    return queue


@pytest.fixture(autouse=True)
def reset_stop_event():
    stop_event.clear()
    yield
    stop_event.clear()


@pytest.mark.asyncio
async def test_worker_startup_and_shutdown(mock_dependencies, mock_queue):
    mock_queue.fetch_next_job.side_effect = [None]

    async def side_effect_sleep(*args):
        stop_event.set()

    with patch("asyncio.sleep", side_effect=side_effect_sleep):
        await worker(MagicMock(), MagicMock(), mock_queue)

    mock_queue.recover_stale_jobs.assert_called_once()


@pytest.mark.asyncio
async def test_worker_processes_ingest_job(mock_dependencies, mock_queue):
    job = Job(
        id=1,
        type=JobType.INGEST,
        payload={"path": "/tmp/book.epub"},
        status=JobStatus.PENDING,
    )

    mock_queue.fetch_next_job.side_effect = [job, None]

    async def side_effect_sleep(*args):
        stop_event.set()

    with patch("asyncio.sleep", side_effect=side_effect_sleep):
        await worker(MagicMock(), MagicMock(), mock_queue)

    mock_dependencies["ingest"].process_job.assert_awaited_once_with(job.payload)
    mock_queue.complete_job.assert_called_with(1)


@pytest.mark.asyncio
async def test_worker_handles_unknown_job_type(mock_dependencies, mock_queue):
    job = MagicMock()
    job.id = 1
    job.type = MagicMock()
    job.type.value = "UNKNOWN_TYPE"
    job.type.__str__.return_value = "UNKNOWN_TYPE"

    job.payload = {}
    job.retry_count = 0
    job.max_retries = 3

    mock_queue.fetch_next_job.side_effect = [job, None]

    async def side_effect_sleep(*args):
        stop_event.set()

    with patch("asyncio.sleep", side_effect=side_effect_sleep):
        await worker(MagicMock(), MagicMock(), mock_queue)

    mock_queue.complete_job.assert_called_with(
        1, error="Unknown job type: UNKNOWN_TYPE", status=JobStatus.FAILED
    )


@pytest.mark.asyncio
async def test_worker_retries_failed_job(mock_dependencies, mock_queue):
    job = Job(
        id=1,
        type=JobType.METADATA,
        payload={},
        status=JobStatus.PENDING,
        retry_count=0,
        max_retries=3,
    )

    mock_dependencies["metadata"].process_job.side_effect = Exception("API error")
    mock_queue.fetch_next_job.side_effect = [job, None]

    async def side_effect_sleep(*args):
        stop_event.set()

    with patch("asyncio.sleep", side_effect=side_effect_sleep):
        await worker(MagicMock(), MagicMock(), mock_queue)

    mock_queue.retry_job.assert_called_once()
    assert "API error" in mock_queue.retry_job.call_args[0][1]


@pytest.mark.asyncio
async def test_worker_moves_to_dead_letter_after_max_retries(
    mock_dependencies, mock_queue
):
    job = Job(
        id=1,
        type=JobType.METADATA,
        payload={},
        status=JobStatus.PENDING,
        retry_count=3,
        max_retries=3,
    )

    mock_dependencies["metadata"].process_job.side_effect = Exception("API error")
    mock_queue.fetch_next_job.side_effect = [job, None]

    async def side_effect_sleep(*args):
        stop_event.set()

    with patch("asyncio.sleep", side_effect=side_effect_sleep):
        await worker(MagicMock(), MagicMock(), mock_queue)

    mock_queue.complete_job.assert_called_with(
        1, error="Exception: API error", status=JobStatus.DEAD_LETTER
    )


@pytest.mark.asyncio
async def test_worker_handles_critical_loop_error(mock_dependencies, mock_queue):
    mock_queue.fetch_next_job.side_effect = Exception("Database is gone")

    sleep_mock = AsyncMock()

    async def sleep_side_effect(*args):
        stop_event.set()

    sleep_mock.side_effect = sleep_side_effect

    with patch("asyncio.sleep", sleep_mock):
        await worker(MagicMock(), MagicMock(), mock_queue)

    sleep_mock.assert_awaited()


@pytest.mark.asyncio
async def test_worker_processes_convert_job(mock_dependencies, mock_queue):
    job = Job(
        id=2,
        type=JobType.CONVERT,
        payload={"book_id": "123e4567-e89b-12d3-a456-426614174000"},
        status=JobStatus.PENDING,
    )

    mock_queue.fetch_next_job.side_effect = [job, None]

    async def side_effect_sleep(*args):
        stop_event.set()

    with patch("asyncio.sleep", side_effect=side_effect_sleep):
        await worker(MagicMock(), MagicMock(), mock_queue)

    mock_dependencies["conversion"].process_job.assert_awaited_once_with(job.payload)
    mock_queue.complete_job.assert_called_with(2)
