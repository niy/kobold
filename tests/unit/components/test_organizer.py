from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from kobold.models import Book
from kobold.organizer import LibraryOrganizer


class TestLibraryOrganizer:
    @pytest.fixture
    def mock_settings(self):
        settings = Mock()
        settings.ORGANIZE_LIBRARY = True
        settings.ORGANIZE_TEMPLATE = "{author}/{title}"
        settings.watch_dirs_list = [Path("/books")]
        return settings

    @pytest.fixture
    def organizer(self, mock_settings):
        return LibraryOrganizer(mock_settings)

    @pytest.fixture
    def mock_book(self):
        book = Mock(spec=Book)
        book.id = "123"
        book.title = "Test Book"
        book.author = "Test Author"
        book.series = None
        book.series_index = None
        book.language = "en"
        book.genre = "Fiction"
        book.publication_date = None
        book.file_path = "/books/incoming/test.epub"
        book.kepub_path = None
        book.file_hash = "test_hash"
        return book

    def test_organize_disabled(self, mock_book, mock_settings, organizer):
        mock_settings.ORGANIZE_LIBRARY = False
        assert organizer.organize_book(mock_book) is None

    @patch("kobold.organizer.shutil.move")
    @patch("pathlib.Path.mkdir")
    @patch("pathlib.Path.exists")
    def test_organize_moves_file(
        self, mock_exists, mock_mkdir, mock_move, mock_book, organizer
    ):
        mock_exists.return_value = False

        new_path = organizer.organize_book(mock_book)

        expected_path = "/books/Test Author/Test Book/test.epub"
        assert new_path == str(Path(expected_path))
        mock_mkdir.assert_called()
        mock_move.assert_called()

    @patch("kobold.organizer.shutil.move")
    @patch("pathlib.Path.mkdir")
    @patch("pathlib.Path.exists")
    @patch("kobold.organizer.get_file_hash")
    def test_organize_handles_collision_with_rename(
        self, mock_hash, mock_exists, mock_mkdir, mock_move, mock_book, organizer
    ):
        mock_exists.side_effect = [True, False]
        mock_hash.return_value = "different_hash"
        mock_book.file_hash = "original_hash"

        new_path = organizer.organize_book(mock_book)

        assert "_1" in new_path
        assert new_path.endswith(".epub")
        mock_move.assert_called()

    @patch("pathlib.Path.unlink")
    @patch("kobold.organizer.shutil.move")
    @patch("pathlib.Path.mkdir")
    @patch("pathlib.Path.exists")
    @patch("kobold.organizer.get_file_hash")
    def test_organize_deduplicates_identical_file(
        self,
        mock_hash,
        mock_exists,
        mock_mkdir,
        mock_move,
        mock_unlink,
        mock_book,
        organizer,
    ):
        mock_exists.return_value = True
        mock_hash.return_value = "same_hash"
        mock_book.file_hash = "same_hash"

        new_path = organizer.organize_book(mock_book)

        assert "_1" not in new_path
        assert new_path == "/books/Test Author/Test Book/test.epub"
        mock_unlink.assert_called()
        mock_move.assert_not_called()

    @patch("kobold.organizer.shutil.move")
    @patch("pathlib.Path.mkdir")
    @patch("pathlib.Path.exists")
    def test_organize_skips_if_already_in_place(
        self, mock_exists, mock_mkdir, mock_move, mock_book, organizer
    ):
        mock_book.file_path = "/books/Test Author/Test Book/test.epub"
        mock_exists.return_value = False

        new_path = organizer.organize_book(mock_book)
        assert new_path is None
        mock_move.assert_not_called()

    @patch("pathlib.Path.unlink")
    @patch("kobold.organizer.shutil.move")
    @patch("pathlib.Path.mkdir")
    @patch("pathlib.Path.exists")
    @patch("kobold.organizer.get_file_hash")
    def test_deduplication_delete_fails_falls_back_to_rename(
        self,
        mock_hash,
        mock_exists,
        mock_mkdir,
        mock_move,
        mock_unlink,
        mock_book,
        organizer,
    ):
        # First call: target exists, second call: unique path doesn't exist
        mock_exists.side_effect = [True, False]
        mock_hash.return_value = "same_hash"
        mock_book.file_hash = "same_hash"
        mock_unlink.side_effect = OSError("Permission denied")

        new_path = organizer.organize_book(mock_book)

        assert "_1" in new_path
        mock_move.assert_called()

    @patch("kobold.organizer.shutil.move")
    @patch("pathlib.Path.mkdir")
    @patch("pathlib.Path.exists")
    def test_organize_mkdir_fails(
        self, mock_exists, mock_mkdir, mock_move, mock_book, organizer
    ):
        mock_exists.return_value = False
        mock_mkdir.side_effect = OSError("Cannot create directory")

        with pytest.raises(OSError, match="Cannot create directory"):
            organizer.organize_book(mock_book)

    @patch("kobold.organizer.shutil.move")
    @patch("pathlib.Path.mkdir")
    @patch("pathlib.Path.exists")
    def test_organize_move_fails(
        self, mock_exists, mock_mkdir, mock_move, mock_book, organizer
    ):
        mock_exists.return_value = False
        mock_move.side_effect = OSError("Cannot move file")

        with pytest.raises(OSError, match="Cannot move file"):
            organizer.organize_book(mock_book)
