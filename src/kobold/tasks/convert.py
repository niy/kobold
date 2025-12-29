"""Task for CONVERT jobs."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import UUID

from sqlmodel import Session

from ..logging_config import get_logger
from ..models import Book
from .base import Task

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

    from ..config import Settings
    from ..conversion import KepubConverter

logger = get_logger(__name__)


class ConvertTask(Task):
    TASK_TYPE = "CONVERT"

    def __init__(
        self,
        settings: Settings,
        engine: Engine,
        converter: KepubConverter,
    ):
        self.settings = settings
        self.engine = engine
        self.converter = converter

    async def process(self, payload: dict[str, Any]) -> None:
        book_id_str = payload.get("book_id")
        if not book_id_str:
            logger.warning("Convert task missing book_id", payload=payload)
            return

        book_id = UUID(book_id_str)

        with Session(self.engine) as session:
            book = session.get(Book, book_id)
            if not book:
                logger.warning(
                    "Convert task for non-existent book", book_id=book_id_str
                )
                return

            log = logger.bind(
                book_id=book_id_str,
                title=book.title,
            )

            if book.is_converted:
                log.debug("Book already converted")
                return

            original_path = Path(book.file_path)
            if not original_path.exists():
                raise FileNotFoundError(f"Source file not found: {original_path}")

            output_path = original_path.with_suffix(".kepub.epub")
            log.info("Starting conversion", output_file=str(output_path))

            kepub_path = await self.converter.convert(original_path, output_path)

            if kepub_path:
                book.kepub_path = str(kepub_path)
                book.is_converted = True
                book.mark_updated()
                session.add(book)
                session.commit()
                log.info("Conversion successful", kepub_path=str(kepub_path))

                if self.settings.DELETE_ORIGINAL_AFTER_CONVERSION:
                    try:
                        original_path.unlink()
                        log.info(
                            "Deleted original file after conversion",
                            path=str(original_path),
                        )
                    except Exception as e:
                        log.error("Failed to delete original file", error=str(e))

            else:
                raise RuntimeError("Conversion returned no output path")
