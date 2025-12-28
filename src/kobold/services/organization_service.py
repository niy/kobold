from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any
from uuid import UUID

from sqlmodel import Session

from ..logging_config import get_logger
from ..models import Book
from ..utils.hashing import get_file_hash

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

    from ..config import Settings
    from ..organizer import LibraryOrganizer

logger = get_logger(__name__)


class OrganizationJobService:
    def __init__(
        self,
        settings_obj: Settings,
        db_engine: Engine,
        organizer: LibraryOrganizer,
    ):
        self.settings = settings_obj
        self.engine = db_engine
        self.organizer = organizer

    async def process_job(self, payload: dict[str, Any]) -> None:
        """
        Process a library organization job.

        This method is designed to be idempotent and robust against partial failures.
        """
        if not self.settings.ORGANIZE_LIBRARY:
            return

        book_id_str = payload.get("book_id")
        if not book_id_str:
            logger.warning("Organization job missing book_id", payload=payload)
            return

        book_id = UUID(book_id_str)

        with Session(self.engine) as session:
            book = session.get(Book, book_id)
            if not book:
                logger.warning(
                    "Organization job for non-existent book", book_id=book_id_str
                )
                return

            log = logger.bind(
                book_id=book_id_str,
                title=book.title,
                current_path=book.file_path,
            )
            log.info("Processing organization job")

            try:
                current_path, expected_path = self.organizer.get_organize_path(book)

                if not current_path.exists():
                    log.warning(
                        "Source file missing, attempting recovery",
                        expected_path=str(expected_path),
                    )

                    if expected_path.exists():
                        try:
                            target_hash = await asyncio.to_thread(
                                get_file_hash, expected_path
                            )
                            if target_hash == book.file_hash:
                                log.info(
                                    "Recovery successful: Found valid file at target path",
                                    path=str(expected_path),
                                )
                                book.file_path = str(expected_path)
                                book.mark_updated()
                                session.add(book)
                                session.commit()
                                return
                            else:
                                log.error(
                                    "Recovery failed: Hash mismatch at target path",
                                    expected_hash=book.file_hash[:12],
                                    found_hash=target_hash[:12],
                                )
                        except Exception as e:
                            log.error(
                                "Recovery failed: Error verifying target file",
                                error=str(e),
                            )

                    raise FileNotFoundError(f"Source file {current_path} not found")

                new_path = self.organizer.organize_book(book)

                if new_path:
                    book.file_path = new_path
                    book.mark_updated()
                    session.add(book)
                    session.commit()
                    log.info("Organization completed", new_path=new_path)
                else:
                    log.debug("Book already organized")

            except FileNotFoundError:
                log.error("Organization failed: Source file missing", exc_info=True)
                raise

            except Exception as e:
                log.error("Organization failed", error=str(e), exc_info=True)
                raise
