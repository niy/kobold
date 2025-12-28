from unittest.mock import AsyncMock, MagicMock, Mock, patch
from uuid import uuid4

import pytest
from sqlmodel import Session

from kobold.models import Book, JobType
from kobold.services.metadata_service import MetadataJobService


@pytest.fixture
def mock_session():
    return MagicMock(spec=Session)


@pytest.fixture
def mock_engine():
    return MagicMock()


@pytest.fixture
def mock_settings():
    settings = Mock()
    settings.EMBED_METADATA = False
    settings.ORGANIZE_LIBRARY = False
    return settings


@pytest.fixture
def mock_metadata_manager():
    manager = Mock()
    manager.get_metadata = AsyncMock()
    return manager


@pytest.fixture
def mock_queue():
    return Mock()


@pytest.fixture
def service(mock_settings, mock_engine, mock_metadata_manager, mock_queue):
    return MetadataJobService(
        mock_settings, mock_engine, mock_metadata_manager, mock_queue
    )


@pytest.mark.asyncio
async def test_process_job_successful(
    service, mock_engine, mock_metadata_manager, mock_queue, mock_settings
):
    mock_settings.ORGANIZE_LIBRARY = True
    book_id = uuid4()
    payload = {"book_id": str(book_id)}

    real_book = Book(
        id=book_id,
        title="Old Title",
        file_path="/path/to/book.epub",
        file_hash="dummy_hash",
    )
    real_book.isbn = None
    real_book.author = None

    mock_session = Mock(spec=Session)
    mock_session.get.return_value = real_book
    mock_session.__enter__ = Mock(return_value=mock_session)
    mock_session.__exit__ = Mock(return_value=None)

    mock_engine.connect.return_value.__enter__.return_value = mock_session

    mock_metadata_manager.get_metadata.return_value = {
        "title": "New Title",
        "author": "New Author",
    }

    with patch("kobold.services.metadata_service.Session", return_value=mock_session):
        await service.process_job(payload)

    mock_session.get.assert_called_with(Book, book_id)
    assert real_book.title == "New Title"
    assert real_book.author == "New Author"
    mock_session.add.assert_called_with(real_book)
    mock_session.commit.assert_called()

    mock_queue.add_job.assert_called_once()
    call_args = mock_queue.add_job.call_args
    assert call_args[0][0] == JobType.ORGANIZE
    assert call_args[1]["payload"] == {"book_id": str(book_id)}


@pytest.mark.asyncio
async def test_process_job_ignores_non_existent_book(service, mock_session):
    book_id = "123e4567-e89b-12d3-a456-426614174000"
    mock_session_instance = MagicMock(spec=Session)
    mock_session.return_value.__enter__.return_value = mock_session_instance
    mock_session.return_value.__enter__.return_value = mock_session_instance
    mock_session_instance.get.return_value = None

    with patch("kobold.services.metadata_service.Session", mock_session):
        await service.process_job({"book_id": book_id})

    service.metadata_manager.get_metadata.assert_not_called()


@pytest.mark.asyncio
async def test_process_job_handles_no_metadata_found(
    service, mock_session, mock_metadata_manager
):
    book_id = "123e4567-e89b-12d3-a456-426614174000"

    mock_book = Mock(spec=Book)
    mock_book.id = book_id
    mock_book.title = "Title"
    mock_book.file_path = "/path/book.epub"

    mock_session_instance = MagicMock(spec=Session)
    mock_session.return_value.__enter__.return_value = mock_session_instance
    mock_session_instance.get.return_value = mock_book

    mock_metadata_manager.get_metadata.return_value = None

    with patch("kobold.services.metadata_service.Session", mock_session):
        await service.process_job({"book_id": book_id})

    mock_session_instance.add.assert_not_called()
    mock_session_instance.commit.assert_not_called()


@pytest.mark.asyncio
async def test_process_job_handles_no_updated_fields(
    service, mock_session, mock_metadata_manager
):
    book_id = "123e4567-e89b-12d3-a456-426614174000"

    mock_book = Mock(spec=Book)
    mock_book.id = book_id
    mock_book.title = "Current Title"
    mock_book.author = "Current Author"
    mock_book.file_path = "/path/book.epub"

    mock_session_instance = MagicMock(spec=Session)
    mock_session.return_value.__enter__.return_value = mock_session_instance
    mock_session_instance.get.return_value = mock_book

    mock_metadata_manager.get_metadata.return_value = {
        "title": "Current Title",
        "author": "Current Author",
    }

    with patch("kobold.services.metadata_service.Session", mock_session):
        await service.process_job({"book_id": book_id})

    mock_session_instance.add.assert_not_called()
    mock_session_instance.commit.assert_not_called()


@pytest.mark.asyncio
async def test_process_job_ignores_unknown_fields(
    service, mock_session, mock_metadata_manager
):
    book_id = "123e4567-e89b-12d3-a456-426614174000"

    mock_book = Mock(spec=Book)
    mock_book.id = book_id
    mock_book.file_path = "/path/book.epub"

    mock_session_instance = MagicMock(spec=Session)
    mock_session.return_value.__enter__.return_value = mock_session_instance
    mock_session_instance.get.return_value = mock_book

    mock_metadata_manager.get_metadata = AsyncMock(
        return_value={"unknown_field": "some value"}
    )

    with patch("kobold.services.metadata_service.Session", mock_session):
        await service.process_job({"book_id": book_id})

    mock_session_instance.add.assert_not_called()
    mock_session_instance.commit.assert_not_called()


@pytest.mark.asyncio
async def test_process_job_handles_cover_download_failure(
    service, mock_session, mock_metadata_manager, mock_engine
):
    book_id = "123e4567-e89b-12d3-a456-426614174000"

    service.settings.EMBED_METADATA = True

    mock_book = Mock(spec=Book)
    mock_book.file_path = "/path/book.epub"
    mock_book.isbn = None

    mock_session_instance = MagicMock(spec=Session)
    mock_session.return_value.__enter__.return_value = mock_session_instance
    mock_session_instance.get.return_value = mock_book

    mock_metadata_manager.get_metadata = AsyncMock(
        return_value={"title": "Title", "cover_path": "http://example.com/cover.jpg"}
    )

    mock_client = AsyncMock()
    mock_client.get.return_value.status_code = 404

    with (
        patch("kobold.services.metadata_service.Session", mock_session),
        patch(
            "kobold.services.metadata_service.HttpClientManager.get_client",
            AsyncMock(return_value=mock_client),
        ),
    ):
        await service.process_job({"book_id": book_id})

        expected_metadata = {
            "title": "Title",
            "cover_path": "http://example.com/cover.jpg",
        }
        mock_metadata_manager.embed_metadata.assert_called_once_with(
            "/path/book.epub", expected_metadata
        )


@pytest.mark.asyncio
async def test_process_job_handles_cover_download_exception(
    service, mock_session, mock_metadata_manager, mock_engine
):
    book_id = "123e4567-e89b-12d3-a456-426614174000"

    service.settings.EMBED_METADATA = True

    mock_book = Mock(spec=Book)
    mock_book.file_path = "/path/book.epub"
    mock_book.isbn = None

    mock_session_instance = MagicMock(spec=Session)
    mock_session.return_value.__enter__.return_value = mock_session_instance
    mock_session_instance.get.return_value = mock_book

    mock_metadata_manager.get_metadata = AsyncMock(
        return_value={"title": "Title", "cover_path": "http://example.com/cover.jpg"}
    )

    mock_client = AsyncMock()
    mock_client.get.side_effect = Exception("Network error")

    with (
        patch("kobold.services.metadata_service.Session", mock_session),
        patch(
            "kobold.services.metadata_service.HttpClientManager.get_client",
            AsyncMock(return_value=mock_client),
        ),
    ):
        await service.process_job({"book_id": book_id})

        expected_metadata = {
            "title": "Title",
            "cover_path": "http://example.com/cover.jpg",
        }
        mock_metadata_manager.embed_metadata.assert_called_once_with(
            "/path/book.epub", expected_metadata
        )


@pytest.mark.asyncio
async def test_process_job_embeds_metadata(
    service, mock_session, mock_metadata_manager, mock_engine
):
    book_id = "123e4567-e89b-12d3-a456-426614174000"

    service.settings.EMBED_METADATA = True

    mock_book = Mock(spec=Book)
    mock_book.file_path = "/path/book.epub"
    mock_book.isbn = None

    mock_session_instance = MagicMock(spec=Session)
    mock_session.return_value.__enter__.return_value = mock_session_instance
    mock_session_instance.get.return_value = mock_book

    mock_metadata_manager.get_metadata = AsyncMock(
        return_value={"title": "Title", "cover_path": "http://example.com/cover.jpg"}
    )

    mock_client = AsyncMock()
    mock_client.get.return_value.status_code = 200
    mock_client.get.return_value.content = b"cover_data"

    with (
        patch("kobold.services.metadata_service.Session", mock_session),
        patch(
            "kobold.services.metadata_service.HttpClientManager.get_client",
            AsyncMock(return_value=mock_client),
        ),
    ):
        await service.process_job({"book_id": book_id})

        expected_metadata = {
            "title": "Title",
            "cover_path": "http://example.com/cover.jpg",
            "cover_data": b"cover_data",
        }
        mock_metadata_manager.embed_metadata.assert_called_once_with(
            "/path/book.epub", expected_metadata
        )
