from pathlib import Path
from unittest.mock import MagicMock, Mock, patch
from uuid import uuid4

import pytest
from sqlmodel import Session

from kobold.models import Book
from kobold.organizer import LibraryOrganizer
from kobold.tasks.organize import OrganizeTask


@pytest.fixture
def mock_settings():
    settings = Mock()
    settings.ORGANIZE_LIBRARY = True
    settings.ORGANIZE_TEMPLATE = "{author}/{series}/{title}"
    settings.watch_dirs_list = [Path("/books")]
    return settings


@pytest.fixture
def mock_engine():
    return MagicMock()


@pytest.fixture
def mock_organizer():
    return Mock(spec=LibraryOrganizer)


@pytest.fixture
def service(mock_settings, mock_engine, mock_organizer):
    return OrganizeTask(mock_settings, mock_engine, mock_organizer)


@pytest.mark.asyncio
async def test_process_organizes_book(service, mock_engine):
    book_id = uuid4()
    payload = {"book_id": str(book_id)}

    real_book = Book(
        id=book_id,
        title="My Title",
        author="My Author",
        file_path="/books/incoming/my_book.epub",
    )

    mock_session = Mock(spec=Session)
    mock_session.get.return_value = real_book
    mock_session.__enter__ = Mock(return_value=mock_session)
    mock_session.__exit__ = Mock(return_value=None)

    mock_current_path = MagicMock()
    mock_current_path.__str__.return_value = "/books/incoming/my_book.epub"
    mock_current_path.exists.return_value = True

    mock_expected_path = MagicMock()
    mock_expected_path.__str__.return_value = "/books/organized/new_path.epub"

    mock_organizer = service.organizer
    mock_organizer.get_organize_path.return_value = (
        mock_current_path,
        mock_expected_path,
    )
    mock_organizer.organize_book.return_value = str(mock_expected_path)

    with patch("kobold.tasks.organize.Session", return_value=mock_session):
        await service.process(payload)

    mock_organizer.organize_book.assert_called_once()
    assert real_book.file_path == str(mock_expected_path)
    mock_session.add.assert_called_with(real_book)
    mock_session.commit.assert_called()


@pytest.mark.asyncio
async def test_process_recovers_from_zombie_state(service, mock_engine):
    book_id = uuid4()
    payload = {"book_id": str(book_id)}

    real_book = Book(
        id=book_id,
        title="My Title",
        author="My Author",
        file_path="/books/incoming/my_book.epub",
        file_hash="dummy_hash",
    )

    mock_session = Mock(spec=Session)
    mock_session.get.return_value = real_book
    mock_session.__enter__ = Mock(return_value=mock_session)
    mock_session.__exit__ = Mock(return_value=None)

    mock_current_path = MagicMock()
    mock_current_path.__str__.return_value = "/books/incoming/my_book.epub"
    mock_current_path.exists.return_value = False

    mock_expected_path = MagicMock()
    mock_expected_path.__str__.return_value = "/books/organized/new_path.epub"
    mock_expected_path.exists.return_value = True

    service.organizer.get_organize_path.return_value = (
        mock_current_path,
        mock_expected_path,
    )

    with (
        patch("kobold.tasks.organize.Session", return_value=mock_session),
        patch(
            "kobold.tasks.organize.get_file_hash",
            return_value="dummy_hash",
        ) as mock_hash,
    ):
        await service.process(payload)

    mock_hash.assert_called_once_with(mock_expected_path)
    service.organizer.organize_book.assert_not_called()
    assert real_book.file_path == str(mock_expected_path)
    mock_session.commit.assert_called()


@pytest.mark.asyncio
async def test_process_fails_recovery_on_hash_mismatch(service, mock_engine):
    book_id = uuid4()
    payload = {"book_id": str(book_id)}

    real_book = Book(
        id=book_id,
        title="My Title",
        author="My Author",
        file_path="/books/incoming/my_book.epub",
        file_hash="correct_hash",
    )

    mock_session = Mock(spec=Session)
    mock_session.get.return_value = real_book
    mock_session.__enter__ = Mock(return_value=mock_session)
    mock_session.__exit__ = Mock(return_value=None)

    mock_current_path = MagicMock()
    mock_current_path.exists.return_value = False

    mock_expected_path = MagicMock()
    mock_expected_path.exists.return_value = True

    service.organizer.get_organize_path.return_value = (
        mock_current_path,
        mock_expected_path,
    )

    with (
        patch("kobold.tasks.organize.Session", return_value=mock_session),
        patch(
            "kobold.tasks.organize.get_file_hash",
            return_value="wrong_hash",
        ),
        pytest.raises(FileNotFoundError, match=r"Source file .* not found"),
    ):
        await service.process(payload)

    mock_session.commit.assert_not_called()


@pytest.mark.asyncio
async def test_process_fails_if_source_and_target_missing(service, mock_engine):
    book_id = uuid4()
    payload = {"book_id": str(book_id)}

    real_book = Book(
        id=book_id,
        title="My Title",
        author="My Author",
        file_path="/books/incoming/my_book.epub",
    )

    mock_session = Mock(spec=Session)
    mock_session.get.return_value = real_book
    mock_session.__enter__ = Mock(return_value=mock_session)
    mock_session.__exit__ = Mock(return_value=None)

    mock_current_path = MagicMock()
    mock_current_path.exists.return_value = False

    mock_expected_path = MagicMock()
    mock_expected_path.exists.return_value = False

    service.organizer.get_organize_path.return_value = (
        mock_current_path,
        mock_expected_path,
    )

    with (
        patch("kobold.tasks.organize.Session", return_value=mock_session),
        pytest.raises(FileNotFoundError, match=r"Source file .* not found"),
    ):
        await service.process(payload)

    service.organizer.organize_book.assert_not_called()
    mock_session.commit.assert_not_called()


@pytest.mark.asyncio
async def test_process_skips_when_disabled(mock_engine, mock_organizer):
    settings = Mock()
    settings.ORGANIZE_LIBRARY = False
    settings.ORGANIZE_TEMPLATE = "{author}/{title}"
    settings.watch_dirs_list = [Path("/books")]

    service = OrganizeTask(settings, mock_engine, mock_organizer)

    await service.process({"book_id": str(uuid4())})

    mock_organizer.organize_book.assert_not_called()


@pytest.mark.asyncio
async def test_process_missing_book_id(service):
    await service.process({})
    service.organizer.organize_book.assert_not_called()


@pytest.mark.asyncio
async def test_process_nonexistent_book(service):
    book_id = uuid4()
    payload = {"book_id": str(book_id)}

    mock_session = Mock(spec=Session)
    mock_session.get.return_value = None
    mock_session.__enter__ = Mock(return_value=mock_session)
    mock_session.__exit__ = Mock(return_value=None)

    with patch("kobold.tasks.organize.Session", return_value=mock_session):
        await service.process(payload)

    service.organizer.organize_book.assert_not_called()


@pytest.mark.asyncio
async def test_process_hash_verification_error(service):
    book_id = uuid4()
    payload = {"book_id": str(book_id)}

    real_book = Book(
        id=book_id,
        title="My Title",
        author="My Author",
        file_path="/books/incoming/my_book.epub",
        file_hash="dummy_hash",
    )

    mock_session = Mock(spec=Session)
    mock_session.get.return_value = real_book
    mock_session.__enter__ = Mock(return_value=mock_session)
    mock_session.__exit__ = Mock(return_value=None)

    mock_current_path = MagicMock()
    mock_current_path.exists.return_value = False

    mock_expected_path = MagicMock()
    mock_expected_path.exists.return_value = True

    service.organizer.get_organize_path.return_value = (
        mock_current_path,
        mock_expected_path,
    )

    with (
        patch("kobold.tasks.organize.Session", return_value=mock_session),
        patch(
            "kobold.tasks.organize.get_file_hash",
            side_effect=Exception("I/O error"),
        ),
        pytest.raises(FileNotFoundError),
    ):
        await service.process(payload)


@pytest.mark.asyncio
async def test_process_already_organized(service):
    book_id = uuid4()
    payload = {"book_id": str(book_id)}

    real_book = Book(
        id=book_id,
        title="My Title",
        author="My Author",
        file_path="/books/organized/my_book.epub",
    )

    mock_session = Mock(spec=Session)
    mock_session.get.return_value = real_book
    mock_session.__enter__ = Mock(return_value=mock_session)
    mock_session.__exit__ = Mock(return_value=None)

    mock_current_path = MagicMock()
    mock_current_path.exists.return_value = True

    mock_expected_path = MagicMock()

    service.organizer.get_organize_path.return_value = (
        mock_current_path,
        mock_expected_path,
    )
    service.organizer.organize_book.return_value = None

    with patch("kobold.tasks.organize.Session", return_value=mock_session):
        await service.process(payload)

    service.organizer.organize_book.assert_called_once()
    mock_session.commit.assert_not_called()


@pytest.mark.asyncio
async def test_process_generic_exception(service):
    book_id = uuid4()
    payload = {"book_id": str(book_id)}

    real_book = Book(
        id=book_id,
        title="My Title",
        author="My Author",
        file_path="/books/incoming/my_book.epub",
    )

    mock_session = Mock(spec=Session)
    mock_session.get.return_value = real_book
    mock_session.__enter__ = Mock(return_value=mock_session)
    mock_session.__exit__ = Mock(return_value=None)

    mock_current_path = MagicMock()
    mock_current_path.exists.return_value = True

    mock_expected_path = MagicMock()

    service.organizer.get_organize_path.return_value = (
        mock_current_path,
        mock_expected_path,
    )
    service.organizer.organize_book.side_effect = RuntimeError("Unexpected error")

    with (
        patch("kobold.tasks.organize.Session", return_value=mock_session),
        pytest.raises(RuntimeError, match="Unexpected error"),
    ):
        await service.process(payload)
