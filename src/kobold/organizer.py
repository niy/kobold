from __future__ import annotations

import shutil
from pathlib import Path
from typing import TYPE_CHECKING

from .logging_config import get_logger
from .utils.hashing import get_file_hash
from .utils.paths import (
    PathTemplate,
    sanitize_filename,
)

if TYPE_CHECKING:
    from .config import Settings
    from .models import Book

logger = get_logger(__name__)


class LibraryOrganizer:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.template = PathTemplate(settings.ORGANIZE_TEMPLATE)

    def get_organize_path(self, book: Book) -> tuple[Path, Path]:
        metadata: dict[str, str | None] = {
            "author": book.author or "Unknown Author",
            "title": book.title,
            "series": book.series,
            "series_index": f"{int(book.series_index):02d}"
            if book.series_index
            else None,
            "language": book.language,
            "genre": book.genre,
            "year": str(book.publication_date.year) if book.publication_date else None,
        }

        new_dir = self.template.render(metadata)

        original_path = Path(book.file_path)
        filename = sanitize_filename(original_path.name)

        base_dir = (
            self.settings.watch_dirs_list[0]
            if self.settings.watch_dirs_list
            else Path("/books")
        )

        new_path = base_dir / new_dir / filename

        return original_path, new_path

    def organize_book(self, book: Book) -> str | None:
        """
        Move a book to its organized location.

        Args:
            book: The book model to organize.

        Returns:
            New path as string if moved, None if skipped.
        """
        if not self.settings.ORGANIZE_LIBRARY:
            return None

        log = logger.bind(
            book_id=str(book.id)[:8],
            title=book.title,
            current_path=book.file_path,
        )

        original_path, new_path = self.get_organize_path(book)

        if new_path == original_path:
            log.debug("Book already in correct location")
            return None

        if new_path.exists():
            try:
                target_hash = get_file_hash(new_path)
                if target_hash == book.file_hash:
                    log.info(
                        "Target file has identical content, overwriting (deduplication)"
                    )
                    try:
                        original_path.unlink()
                        log.info(
                            "Deleted redundant source file", source=str(original_path)
                        )
                        return str(new_path)
                    except OSError as e:
                        log.error(
                            "Failed to delete redundant source file", error=str(e)
                        )
                        raise
            except Exception as e:
                log.warning(
                    "Failed to verify target hash, falling back to rename", error=str(e)
                )

            new_path = _generate_unique_path(new_path)
            log.info("Duplicate filename, using unique path", unique_path=str(new_path))

        try:
            new_path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            log.error("Failed to create directory", error=str(e))
            raise

        try:
            shutil.move(str(original_path), str(new_path))
            log.info("Moved book", new_path=str(new_path))
        except OSError as e:
            log.error("Failed to move book", error=str(e))
            raise

        if book.kepub_path:
            kepub_original = Path(book.kepub_path)
            if kepub_original.exists():
                kepub_filename = sanitize_filename(kepub_original.name)
                kepub_new_path = new_path.parent / kepub_filename

                if kepub_new_path.exists():
                    kepub_new_path = _generate_unique_path(kepub_new_path)

                try:
                    shutil.move(str(kepub_original), str(kepub_new_path))
                    book.kepub_path = str(kepub_new_path)
                    log.debug("Moved kepub", kepub_path=str(kepub_new_path))
                except OSError as e:
                    log.warning("Failed to move kepub", error=str(e))

        return str(new_path)


def _generate_unique_path(path: Path) -> Path:
    counter = 1
    stem = path.stem
    suffix = path.suffix
    parent = path.parent

    while True:
        new_path = parent / f"{stem}_{counter}{suffix}"
        if not new_path.exists():
            return new_path
        counter += 1
        if counter > 1000:
            raise OSError(f"Could not generate unique path for {path}")
