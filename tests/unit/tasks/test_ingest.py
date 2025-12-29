from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
from sqlmodel import Session

from kobold.models import Book
from kobold.tasks.ingest import IngestTask
from kobold.tasks.organize import OrganizeTask


@pytest.fixture
def mock_session():
    return MagicMock(spec=Session)


@pytest.fixture
def mock_settings():
    return Mock()


@pytest.fixture
def mock_task_queue():
    return Mock()


@pytest.fixture
def mock_engine():
    return Mock()


@pytest.fixture
def ingest_service(mock_settings, mock_engine, mock_task_queue):
    return IngestTask(mock_settings, mock_engine, mock_task_queue)


@pytest.mark.asyncio
async def test_process_dispatch(ingest_service):
    """Test that process dispatches to correct handler."""
    with (
        patch.object(ingest_service, "_handle_add", new_callable=AsyncMock) as mock_add,
        patch.object(
            ingest_service, "_handle_delete", new_callable=AsyncMock
        ) as mock_del,
    ):
        # Test ADD
        await ingest_service.process({"event": "ADD", "path": "/path/to/file.epub"})
        mock_add.assert_called_once()
        mock_del.assert_not_called()

        mock_add.reset_mock()
        mock_del.reset_mock()

        await ingest_service.process({"event": "DELETE", "path": "/path/to/file.epub"})
        mock_del.assert_called_once()
        mock_add.assert_not_called()


@pytest.mark.asyncio
async def test_handle_add_new_file(
    ingest_service, mock_session, mock_task_queue, mock_engine
):
    path = Path("/books/new_book.epub")

    mock_session_instance = MagicMock(spec=Session)
    mock_session.return_value.__enter__.return_value = mock_session_instance
    mock_session_instance.exec.return_value.first.side_effect = [None, None]

    with (
        patch("kobold.tasks.ingest.Session", mock_session),
        patch("pathlib.Path.exists", return_value=True),
        patch("pathlib.Path.stat", return_value=Mock(st_size=1024)),
        patch("kobold.tasks.ingest.get_file_hash", return_value="hash123"),
    ):
        await ingest_service._handle_add(path, Mock())

        mock_session_instance.add.assert_called()
        args = mock_session_instance.add.call_args[0]
        assert isinstance(args[0], Book)
        assert args[0].title == "new_book"
        assert args[0].file_hash == "hash123"

        assert mock_task_queue.add_task.call_count >= 1


@pytest.mark.asyncio
async def test_handle_delete(ingest_service, mock_session, mock_engine):
    path = Path("/books/deleted.epub")

    mock_book = Mock(spec=Book)
    mock_book.id = "123"
    mock_book.title = "Deleted Book"

    mock_session_instance = MagicMock(spec=Session)
    mock_session.return_value.__enter__.return_value = mock_session_instance
    mock_session_instance.exec.return_value.first.return_value = mock_book

    with patch("kobold.tasks.ingest.Session", mock_session):
        await ingest_service._handle_delete(path, Mock())

        mock_book.mark_deleted.assert_called_once()
        mock_session_instance.add.assert_called_with(mock_book)
        mock_session_instance.commit.assert_called_once()


@pytest.mark.asyncio
async def test_handle_add_restores_soft_deleted_book(
    ingest_service, mock_session, mock_task_queue, mock_engine
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
        patch("kobold.tasks.ingest.Session", mock_session),
        patch("pathlib.Path.exists", return_value=True),
        patch("pathlib.Path.stat", return_value=Mock(st_size=1024)),
        patch("kobold.tasks.ingest.get_file_hash", return_value="hash456"),
    ):
        await ingest_service._handle_add(path, Mock())

        assert mock_deleted_book.is_deleted is False
        assert mock_deleted_book.deleted_at is None
        mock_deleted_book.mark_updated.assert_called_once()
        mock_session_instance.add.assert_called_with(mock_deleted_book)
        mock_session_instance.commit.assert_called_once()


@pytest.mark.asyncio
async def test_process_missing_path(ingest_service):
    with (
        patch.object(ingest_service, "_handle_add", new_callable=AsyncMock) as mock_add,
        patch.object(
            ingest_service, "_handle_delete", new_callable=AsyncMock
        ) as mock_del,
    ):
        await ingest_service.process({"event": "ADD"})  # Missing path

        mock_add.assert_not_called()
        mock_del.assert_not_called()


@pytest.mark.asyncio
async def test_process_unknown_event(ingest_service):
    with (
        patch.object(ingest_service, "_handle_add", new_callable=AsyncMock) as mock_add,
        patch.object(
            ingest_service, "_handle_delete", new_callable=AsyncMock
        ) as mock_del,
    ):
        await ingest_service.process({"event": "UNKNOWN", "path": "/path/to/file.epub"})

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
        patch("kobold.tasks.ingest.SUPPORTED_EXTENSIONS", {".epub", ".kepub"}),
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
            "kobold.tasks.ingest.get_file_hash",
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
        patch("kobold.tasks.ingest.Session", mock_session),
        patch("pathlib.Path.exists", return_value=True),
        patch("pathlib.Path.stat", return_value=Mock(st_size=1024)),
        patch("kobold.tasks.ingest.get_file_hash", return_value="hash123"),
    ):
        await ingest_service._handle_add(path, Mock())

        mock_session_instance.commit.assert_not_called()


@pytest.mark.asyncio
async def test_handle_add_duplicate_file_deleted(ingest_service, mock_session):
    """Test Scenario A: Duplicate file deleted if original exists."""
    new_path = Path("/books/duplicate.epub")
    original_str = "/books/original.epub"

    mock_book = Mock(spec=Book)
    mock_book.id = "123"
    mock_book.file_path = original_str
    mock_book.title = "Original Book"

    mock_session_instance = MagicMock(spec=Session)
    mock_session.return_value.__enter__.return_value = mock_session_instance
    mock_session_instance.exec.return_value.first.return_value = mock_book

    def exists_side_effect(self):
        # Original file exists
        if str(self) == original_str:
            return True
        # New file exists (initially)
        return str(self) == str(new_path)

    with (
        patch("kobold.tasks.ingest.Session", mock_session),
        patch(
            "pathlib.Path.exists",
            start_new_session=True,
            autospec=True,
            side_effect=exists_side_effect,
        ),
        patch("pathlib.Path.stat", return_value=Mock(st_size=1024)),
        patch("pathlib.Path.unlink") as mock_unlink,
        patch("kobold.tasks.ingest.get_file_hash", return_value="hash123"),
    ):
        await ingest_service._handle_add(new_path, Mock())

        mock_unlink.assert_called_once()
        mock_session_instance.commit.assert_not_called()


@pytest.mark.asyncio
async def test_handle_add_self_healing(ingest_service, mock_session, mock_task_queue):
    """Test Scenario B: Self-healing if original file is missing."""
    new_path = Path("/books/restored.epub")
    original_str = "/books/missing_original.epub"

    mock_book = Mock(spec=Book)
    mock_book.id = "123"
    mock_book.file_path = original_str
    mock_book.title = "Broken Link Book"
    mock_book.is_deleted = False

    mock_session_instance = MagicMock(spec=Session)
    mock_session.return_value.__enter__.return_value = mock_session_instance
    mock_session_instance.exec.return_value.first.return_value = mock_book

    def exists_side_effect(self):
        # Original file MISSING
        if str(self) == original_str:
            return False
        # New file exists
        # New file exists
        return str(self) == str(new_path)

    with (
        patch("kobold.tasks.ingest.Session", mock_session),
        patch(
            "pathlib.Path.exists",
            start_new_session=True,
            autospec=True,
            side_effect=exists_side_effect,
        ),
        patch("pathlib.Path.stat", return_value=Mock(st_size=1024)),
        patch("pathlib.Path.unlink") as mock_unlink,
        patch("kobold.tasks.ingest.get_file_hash", return_value="hash123"),
    ):
        await ingest_service._handle_add(new_path, Mock())

        mock_unlink.assert_not_called()

        assert mock_book.file_path == str(new_path)
        mock_session_instance.add.assert_called_with(mock_book)
        mock_session_instance.commit.assert_called_once()

        mock_task_queue.add_task.assert_called_with(
            OrganizeTask.TASK_TYPE,
            payload={"book_id": str(mock_book.id)},
        )
