from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
from sqlmodel import Session

from kobosync.models import Book
from kobosync.services.ingest import IngestService


@pytest.fixture
def mock_session():
    return MagicMock(spec=Session)


@pytest.fixture
def mock_settings():
    return Mock()


@pytest.fixture
def mock_job_queue():
    return Mock()


@pytest.fixture
def mock_engine():
    return Mock()


@pytest.fixture
def ingest_service(mock_settings, mock_engine, mock_job_queue):
    return IngestService(mock_settings, mock_engine, mock_job_queue)


@pytest.mark.asyncio
async def test_process_job_dispatch(ingest_service):
    """Test that process_job dispatches to correct handler."""
    with (
        patch.object(ingest_service, "_handle_add", new_callable=AsyncMock) as mock_add,
        patch.object(
            ingest_service, "_handle_delete", new_callable=AsyncMock
        ) as mock_del,
    ):
        # Test ADD
        await ingest_service.process_job({"event": "ADD", "path": "/path/to/file.epub"})
        mock_add.assert_called_once()
        mock_del.assert_not_called()

        mock_add.reset_mock()
        mock_del.reset_mock()

        # Test DELETE
        await ingest_service.process_job(
            {"event": "DELETE", "path": "/path/to/file.epub"}
        )
        mock_del.assert_called_once()
        mock_add.assert_not_called()


@pytest.mark.asyncio
async def test_handle_add_new_file(
    ingest_service, mock_session, mock_job_queue, mock_engine
):
    path = Path("/books/new_book.epub")

    mock_session_instance = MagicMock(spec=Session)
    mock_session.return_value.__enter__.return_value = mock_session_instance
    mock_session_instance.exec.return_value.first.side_effect = [None, None]

    with (
        patch("kobosync.services.ingest.Session", mock_session),
        patch("pathlib.Path.exists", return_value=True),
        patch("pathlib.Path.stat", return_value=Mock(st_size=1024)),
        patch("kobosync.services.ingest.get_file_hash", return_value="hash123"),
    ):
        await ingest_service._handle_add(path, Mock())

        # Verify book created on the session instance
        mock_session_instance.add.assert_called()
        args = mock_session_instance.add.call_args[0]
        assert isinstance(args[0], Book)
        assert args[0].title == "new_book"
        assert args[0].file_hash == "hash123"

        # Verify jobs queued
        assert mock_job_queue.add_job.call_count >= 1


@pytest.mark.asyncio
async def test_handle_delete(ingest_service, mock_session, mock_engine):
    path = Path("/books/deleted.epub")

    mock_book = Mock(spec=Book)
    mock_book.id = "123"
    mock_book.title = "Deleted Book"

    mock_session_instance = MagicMock(spec=Session)
    mock_session.return_value.__enter__.return_value = mock_session_instance
    mock_session_instance.exec.return_value.first.return_value = mock_book

    with patch("kobosync.services.ingest.Session", mock_session):
        await ingest_service._handle_delete(path, Mock())

        mock_book.mark_deleted.assert_called_once()
        mock_session_instance.add.assert_called_with(mock_book)
        mock_session_instance.commit.assert_called_once()


@pytest.mark.asyncio
async def test_handle_add_restores_soft_deleted_book(
    ingest_service, mock_session, mock_job_queue, mock_engine
):
    path = Path("/books/restored_book.epub")

    mock_deleted_book = Mock(spec=Book)
    mock_deleted_book.id = "456"
    mock_deleted_book.title = "Restored Book"
    mock_deleted_book.is_deleted = True
    mock_deleted_book.file_path = str(path)

    mock_session_instance = MagicMock(spec=Session)
    mock_session.return_value.__enter__.return_value = mock_session_instance
    # First query (by hash): no match
    # Second query (by path): returns soft-deleted book
    mock_session_instance.exec.return_value.first.side_effect = [
        None,
        mock_deleted_book,
    ]

    with (
        patch("kobosync.services.ingest.Session", mock_session),
        patch("pathlib.Path.exists", return_value=True),
        patch("pathlib.Path.stat", return_value=Mock(st_size=1024)),
        patch("kobosync.services.ingest.get_file_hash", return_value="hash456"),
    ):
        await ingest_service._handle_add(path, Mock())

        # Verify book was restored
        assert mock_deleted_book.is_deleted is False
        assert mock_deleted_book.deleted_at is None
        mock_deleted_book.mark_updated.assert_called_once()
        mock_session_instance.add.assert_called_with(mock_deleted_book)
        mock_session_instance.commit.assert_called_once()


@pytest.mark.asyncio
async def test_process_job_missing_path(ingest_service):
    with (
        patch.object(ingest_service, "_handle_add", new_callable=AsyncMock) as mock_add,
        patch.object(
            ingest_service, "_handle_delete", new_callable=AsyncMock
        ) as mock_del,
    ):
        await ingest_service.process_job({"event": "ADD"})  # Missing path

        mock_add.assert_not_called()
        mock_del.assert_not_called()


@pytest.mark.asyncio
async def test_process_job_unknown_event(ingest_service):
    with (
        patch.object(ingest_service, "_handle_add", new_callable=AsyncMock) as mock_add,
        patch.object(
            ingest_service, "_handle_delete", new_callable=AsyncMock
        ) as mock_del,
    ):
        await ingest_service.process_job(
            {"event": "UNKNOWN", "path": "/path/to/file.epub"}
        )

        mock_add.assert_not_called()
        mock_del.assert_not_called()


@pytest.mark.asyncio
async def test_handle_add_non_existent_file(ingest_service):
    """Test _handle_add with non-existent file."""
    path = Path("/books/missing.epub")

    with patch("pathlib.Path.exists", return_value=False):
        await ingest_service._handle_add(path, Mock())

        # Should return early without error
        pass


@pytest.mark.asyncio
async def test_handle_add_unsupported_extension(ingest_service):
    path = Path("/books/not_a_book.txt")

    with (
        patch("pathlib.Path.exists", return_value=True),
        patch("kobosync.services.ingest.SUPPORTED_EXTENSIONS", {".epub", ".kepub"}),
    ):
        await ingest_service._handle_add(path, Mock())

        # Should return early without error and not call database
        pass


@pytest.mark.asyncio
async def test_handle_add_hashing_failure(ingest_service):
    path = Path("/books/error.epub")

    with (
        patch("pathlib.Path.exists", return_value=True),
        patch("pathlib.Path.stat", return_value=Mock(st_size=1024)),
        patch(
            "kobosync.services.ingest.get_file_hash",
            side_effect=Exception("Disk error"),
        ),
        pytest.raises(Exception, match="Disk error"),
    ):
        await ingest_service._handle_add(path, Mock())


@pytest.mark.asyncio
async def test_handle_add_idempotency(ingest_service, mock_session):
    """Test _handle_add when book already exists with same path and hash."""
    path = Path("/books/existing.epub")

    mock_book = Mock(spec=Book)
    mock_book.id = "123"
    mock_book.file_path = str(path)

    mock_session_instance = MagicMock(spec=Session)
    mock_session.return_value.__enter__.return_value = mock_session_instance
    # existing_book found by hash and size
    mock_session_instance.exec.return_value.first.return_value = mock_book

    with (
        patch("kobosync.services.ingest.Session", mock_session),
        patch("pathlib.Path.exists", return_value=True),
        patch("pathlib.Path.stat", return_value=Mock(st_size=1024)),
        patch("kobosync.services.ingest.get_file_hash", return_value="hash123"),
    ):
        await ingest_service._handle_add(path, Mock())

        # Should catch "Book already exists" path
        mock_session_instance.commit.assert_not_called()
