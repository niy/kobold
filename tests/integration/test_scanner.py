from pathlib import Path
from unittest.mock import MagicMock, Mock

import pytest

from kobold.scanner import ScannerService


class TestScanner:
    @pytest.fixture
    def scanner_service(self) -> ScannerService:
        settings = Mock()
        settings.SUPPORTED_EXTENSIONS = {"epub", "pdf", "cbz", "cbr"}
        queue = Mock()
        return ScannerService(settings, queue)

    @pytest.mark.asyncio
    async def test_scan_discovers_epub_files(self, tmp_path: Path) -> None:
        (tmp_path / "book1.epub").touch()
        (tmp_path / "book2.epub").touch()

        mock_queue = MagicMock()
        mock_settings = Mock()
        mock_settings.watch_dirs_list = [tmp_path]
        mock_settings.SUPPORTED_EXTENSIONS = {"epub", "pdf", "cbz", "cbr"}
        mock_settings.SUPPORTED_EXTENSIONS = {"epub", "pdf", "cbz", "cbr"}

        service = ScannerService(mock_settings, mock_queue)
        await service.scan_directories()

        assert mock_queue.add_task.call_count == 2

    @pytest.mark.asyncio
    async def test_scan_discovers_all_supported_extensions(
        self, tmp_path: Path
    ) -> None:
        (tmp_path / "book.epub").touch()
        (tmp_path / "book.kepub.epub").touch()
        (tmp_path / "document.pdf").touch()
        (tmp_path / "comic.cbz").touch()
        (tmp_path / "comic.cbr").touch()

        mock_queue = MagicMock()
        mock_settings = Mock()
        mock_settings.watch_dirs_list = [tmp_path]
        mock_settings.SUPPORTED_EXTENSIONS = {"epub", "pdf", "cbz", "cbr"}
        mock_settings.SUPPORTED_EXTENSIONS = {"epub", "pdf", "cbz", "cbr"}

        service = ScannerService(mock_settings, mock_queue)
        await service.scan_directories()

        assert mock_queue.add_task.call_count == 5

    @pytest.mark.asyncio
    async def test_scan_ignores_unsupported_files(self, tmp_path: Path) -> None:
        (tmp_path / "notes.txt").touch()
        (tmp_path / "image.jpg").touch()
        (tmp_path / "readme.md").touch()

        mock_queue = MagicMock()
        mock_settings = Mock()
        mock_settings.watch_dirs_list = [tmp_path]
        mock_settings.SUPPORTED_EXTENSIONS = {"epub", "pdf", "cbz", "cbr"}

        service = ScannerService(mock_settings, mock_queue)
        await service.scan_directories()

        mock_queue.add_task.assert_not_called()

    @pytest.mark.asyncio
    async def test_scan_recursive_subdirectories(self, tmp_path: Path) -> None:
        subdir = tmp_path / "subdir" / "nested"
        subdir.mkdir(parents=True)
        (subdir / "deep_book.epub").touch()
        (tmp_path / "top_book.epub").touch()

        mock_queue = MagicMock()
        mock_settings = Mock()
        mock_settings.watch_dirs_list = [tmp_path]
        mock_settings.SUPPORTED_EXTENSIONS = {"epub", "pdf", "cbz", "cbr"}

        service = ScannerService(mock_settings, mock_queue)
        await service.scan_directories()

        assert mock_queue.add_task.call_count == 2

    @pytest.mark.asyncio
    async def test_scan_handles_missing_directory(self, tmp_path: Path) -> None:
        missing_dir = tmp_path / "does_not_exist"

        mock_queue = MagicMock()
        mock_settings = Mock()
        mock_settings.watch_dirs_list = [missing_dir]
        mock_settings.SUPPORTED_EXTENSIONS = {"epub", "pdf", "cbz", "cbr"}

        service = ScannerService(mock_settings, mock_queue)
        await service.scan_directories()

        mock_queue.add_task.assert_not_called()

    @pytest.mark.asyncio
    async def test_scan_multiple_directories(self, tmp_path: Path) -> None:
        dir1 = tmp_path / "dir1"
        dir2 = tmp_path / "dir2"
        dir1.mkdir()
        dir2.mkdir()

        (dir1 / "book1.epub").touch()
        (dir2 / "book2.epub").touch()

        mock_queue = MagicMock()
        mock_settings = Mock()
        mock_settings.watch_dirs_list = [dir1, dir2]
        mock_settings.SUPPORTED_EXTENSIONS = {"epub", "pdf", "cbz", "cbr"}

        service = ScannerService(mock_settings, mock_queue)
        await service.scan_directories()

        assert mock_queue.add_task.call_count == 2
