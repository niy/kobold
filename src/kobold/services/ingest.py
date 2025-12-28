from __future__ import annotations

import asyncio
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sqlmodel import Session, select

from ..constants import SUPPORTED_EXTENSIONS
from ..logging_config import get_logger
from ..models import Book, JobType
from ..utils.hashing import get_file_hash

if TYPE_CHECKING:
    import structlog
    from sqlalchemy.engine import Engine

    from ..config import Settings
    from ..job_queue import JobQueue

logger = get_logger(__name__)


class IngestService:
    def __init__(
        self,
        settings_obj: Settings,
        db_engine: Engine,
        queue: JobQueue,
    ):
        self.settings = settings_obj
        self.engine = db_engine
        self.job_queue = queue

    async def process_job(self, payload: dict[str, Any]) -> None:
        event_type = payload.get("event")
        filepath_str = payload.get("path")

        if not filepath_str:
            logger.warning("Ingest job missing path", payload=payload)
            return

        filepath = Path(filepath_str)
        log = logger.bind(filepath=str(filepath), event=event_type)

        match event_type:
            case "DELETE":
                await self._handle_delete(filepath, log)
            case "ADD" | "MODIFIED":
                await self._handle_add(filepath, log)
            case _:
                log.warning("Unknown ingest event type")

    async def _handle_delete(
        self, filepath: Path, log: structlog.stdlib.BoundLogger
    ) -> None:
        with Session(self.engine) as session:
            book = session.exec(
                select(Book).where(Book.file_path == str(filepath))
            ).first()

            if book:
                book.mark_deleted()
                session.add(book)
                session.commit()
                log.info(
                    "Book marked as deleted", book_id=str(book.id), title=book.title
                )
            else:
                log.debug("No book found for deleted file")

    async def _handle_add(
        self, filepath: Path, log: structlog.stdlib.BoundLogger
    ) -> None:
        if not filepath.exists():
            log.debug("File no longer exists, skipping")
            return

        if filepath.suffix.lower() not in SUPPORTED_EXTENSIONS:
            log.debug("Unsupported file extension", extension=filepath.suffix)
            return

        file_size = filepath.stat().st_size

        try:
            file_hash = await asyncio.to_thread(get_file_hash, filepath)
        except Exception as e:
            log.error("Failed to hash file", error=str(e))
            raise

        with Session(self.engine) as session:
            existing_book = session.exec(
                select(Book).where(
                    Book.file_size == file_size,
                    Book.file_hash == file_hash,
                )
            ).first()

            if existing_book:
                if existing_book.file_path != str(filepath):
                    original_path = Path(existing_book.file_path)

                    if original_path.exists():
                        # Delete new file if it's a duplicate
                        try:
                            filepath.unlink()
                            log.info(
                                "Deleted duplicate file",
                                original_path=str(original_path),
                                duplicate_path=str(filepath),
                            )
                        except OSError as e:
                            log.error("Failed to delete duplicate file", error=str(e))
                        return

                    else:
                        # Heal broken link
                        log.info(
                            "Original file missing, healing link",
                            old_path=str(original_path),
                            new_path=str(filepath),
                        )
                        existing_book.file_path = str(filepath)
                        existing_book.is_deleted = False
                        existing_book.deleted_at = None
                        existing_book.mark_updated()
                        session.add(existing_book)
                        session.commit()

                        # Trigger Organization to move it to correct location
                        self.job_queue.add_job(
                            JobType.ORGANIZE,
                            payload={"book_id": str(existing_book.id)},
                        )
                else:
                    log.debug("Book already exists with same path")
                return

            existing_book_by_path = session.exec(
                select(Book).where(Book.file_path == str(filepath))
            ).first()

            if existing_book_by_path:
                existing_book_by_path.file_hash = file_hash
                existing_book_by_path.file_size = filepath.stat().st_size
                existing_book_by_path.is_deleted = False
                existing_book_by_path.deleted_at = None
                existing_book_by_path.mark_updated()
                session.add(existing_book_by_path)
                session.commit()

                log.info(
                    "Updated existing book content",
                    book_id=str(existing_book_by_path.id),
                    title=existing_book_by_path.title,
                    new_hash=file_hash[:12],
                )
                return

            file_stat = filepath.stat()
            new_book = Book(
                title=filepath.stem,
                file_path=str(filepath),
                file_hash=file_hash,
                file_size=file_stat.st_size,
                file_format=filepath.suffix.lower().lstrip("."),
                is_converted=False,
            )
            session.add(new_book)
            session.commit()
            session.refresh(new_book)

            log.info(
                "Ingested new book",
                book_id=str(new_book.id),
                title=new_book.title,
                file_hash=file_hash[:12],
            )

            self.job_queue.add_job(
                JobType.METADATA,
                payload={"book_id": str(new_book.id)},
            )

            is_epub = filepath.suffix.lower() == ".epub"
            is_already_kepub = filepath.stem.lower().endswith(".kepub")
            if self.settings.CONVERT_EPUB and is_epub and not is_already_kepub:
                self.job_queue.add_job(
                    JobType.CONVERT,
                    payload={"book_id": str(new_book.id)},
                )
