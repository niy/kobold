from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
from sqlmodel import Session

from kobosync.models import Book
from kobosync.services.conversion_service import ConversionJobService


@pytest.fixture
def mock_session():
    return MagicMock(spec=Session)


@pytest.fixture
def mock_engine():
    return Mock()


@pytest.fixture
def mock_settings():
    return Mock()


@pytest.fixture
def mock_converter():
    return Mock()


@pytest.fixture
def conversion_service(mock_settings, mock_engine, mock_converter):
    return ConversionJobService(mock_settings, mock_engine, mock_converter)


@pytest.mark.asyncio
async def test_process_job_converts_book(
    conversion_service, mock_session, mock_converter, mock_engine
):
    book_id = "123e4567-e89b-12d3-a456-426614174000"

    mock_book = Mock(spec=Book)
    mock_book.id = book_id
    mock_book.file_path = "/books/test.epub"
    mock_book.is_converted = False
    mock_book.title = "Test"

    mock_session_instance = MagicMock(spec=Session)
    mock_session.return_value.__enter__.return_value = mock_session_instance
    mock_session_instance.get.return_value = mock_book

    with (
        patch("kobosync.services.conversion_service.Session", mock_session),
        patch("pathlib.Path.exists", return_value=True),
        patch("pathlib.Path.parent", new_callable=lambda: Path("/books")),
    ):
        mock_converter.convert = AsyncMock(return_value=Path("/books/test.kepub.epub"))

        await conversion_service.process_job({"book_id": book_id})

        mock_converter.convert.assert_called_once()

        assert mock_book.kepub_path == "/books/test.kepub.epub"
        assert mock_book.is_converted is True
        mock_session_instance.add.assert_called_with(mock_book)
        mock_session_instance.commit.assert_called_once()


@pytest.mark.asyncio
async def test_process_job_deletes_original(
    conversion_service, mock_session, mock_converter, mock_engine
):
    book_id = "123e4567-e89b-12d3-a456-426614174000"

    conversion_service.settings.DELETE_ORIGINAL_AFTER_CONVERSION = True

    mock_book = Mock(spec=Book)
    mock_book.file_path = "/books/test.epub"
    mock_book.is_converted = False

    mock_session_instance = MagicMock(spec=Session)
    mock_session.return_value.__enter__.return_value = mock_session_instance
    mock_session_instance.get.return_value = mock_book

    mock_path = MagicMock(spec=Path)
    mock_path.exists.return_value = True
    mock_path.parent = Path("/books")

    mock_converter.convert = AsyncMock(return_value=Path("/books/test.kepub.epub"))

    with (
        patch("kobosync.services.conversion_service.Session", mock_session),
        patch("kobosync.services.conversion_service.Path", return_value=mock_path),
    ):
        await conversion_service.process_job({"book_id": book_id})

        mock_path.unlink.assert_called_once()


@pytest.mark.asyncio
async def test_process_job_missing_book_id(conversion_service):
    """When book_id is missing from payload, job returns early without error."""
    await conversion_service.process_job({})
    # No exception raised, function returns early


@pytest.mark.asyncio
async def test_process_job_nonexistent_book(
    conversion_service, mock_session, mock_engine
):
    """When book doesn't exist in database, job returns early without error."""
    mock_session_instance = MagicMock(spec=Session)
    mock_session.return_value.__enter__.return_value = mock_session_instance
    mock_session_instance.get.return_value = None

    with patch("kobosync.services.conversion_service.Session", mock_session):
        await conversion_service.process_job(
            {"book_id": "123e4567-e89b-12d3-a456-426614174000"}
        )


@pytest.mark.asyncio
async def test_process_job_already_converted(
    conversion_service, mock_session, mock_converter, mock_engine
):
    mock_book = Mock(spec=Book)
    mock_book.is_converted = True
    mock_book.title = "Test"

    mock_session_instance = MagicMock(spec=Session)
    mock_session.return_value.__enter__.return_value = mock_session_instance
    mock_session_instance.get.return_value = mock_book

    with patch("kobosync.services.conversion_service.Session", mock_session):
        await conversion_service.process_job(
            {"book_id": "123e4567-e89b-12d3-a456-426614174000"}
        )

    mock_converter.convert.assert_not_called()


@pytest.mark.asyncio
async def test_process_job_source_file_not_found(
    conversion_service, mock_session, mock_engine
):
    mock_book = Mock(spec=Book)
    mock_book.file_path = "/books/missing.epub"
    mock_book.is_converted = False
    mock_book.title = "Test"

    mock_session_instance = MagicMock(spec=Session)
    mock_session.return_value.__enter__.return_value = mock_session_instance
    mock_session_instance.get.return_value = mock_book

    with (
        patch("kobosync.services.conversion_service.Session", mock_session),
        patch("pathlib.Path.exists", return_value=False),
        pytest.raises(FileNotFoundError),
    ):
        await conversion_service.process_job(
            {"book_id": "123e4567-e89b-12d3-a456-426614174000"}
        )


@pytest.mark.asyncio
async def test_process_job_conversion_fails(
    conversion_service, mock_session, mock_converter, mock_engine
):
    mock_book = Mock(spec=Book)
    mock_book.file_path = "/books/test.epub"
    mock_book.is_converted = False
    mock_book.title = "Test"

    mock_session_instance = MagicMock(spec=Session)
    mock_session.return_value.__enter__.return_value = mock_session_instance
    mock_session_instance.get.return_value = mock_book

    mock_converter.convert = AsyncMock(return_value=None)

    with (
        patch("kobosync.services.conversion_service.Session", mock_session),
        patch("pathlib.Path.exists", return_value=True),
        pytest.raises(RuntimeError, match="Conversion returned no output path"),
    ):
        await conversion_service.process_job(
            {"book_id": "123e4567-e89b-12d3-a456-426614174000"}
        )


@pytest.mark.asyncio
async def test_process_job_delete_fails_gracefully(
    conversion_service, mock_session, mock_converter, mock_engine
):
    book_id = "123e4567-e89b-12d3-a456-426614174000"
    conversion_service.settings.DELETE_ORIGINAL_AFTER_CONVERSION = True

    mock_book = Mock(spec=Book)
    mock_book.file_path = "/books/test.epub"
    mock_book.is_converted = False
    mock_book.title = "Test"

    mock_session_instance = MagicMock(spec=Session)
    mock_session.return_value.__enter__.return_value = mock_session_instance
    mock_session_instance.get.return_value = mock_book

    mock_path = MagicMock(spec=Path)
    mock_path.exists.return_value = True
    mock_path.parent = Path("/books")
    mock_path.unlink.side_effect = PermissionError("Cannot delete")

    mock_converter.convert = AsyncMock(return_value=Path("/books/test.kepub.epub"))

    with (
        patch("kobosync.services.conversion_service.Session", mock_session),
        patch("kobosync.services.conversion_service.Path", return_value=mock_path),
    ):
        await conversion_service.process_job({"book_id": book_id})

    assert mock_book.is_converted is True
