from __future__ import annotations

from typing import TYPE_CHECKING, Any
from uuid import UUID

from sqlmodel import Session

from ..http_client import HttpClientManager
from ..logging_config import get_logger
from ..models import Book

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

    from ..config import Settings
    from ..metadata.manager import MetadataManager

logger = get_logger(__name__)


class MetadataJobService:
    def __init__(
        self,
        settings_obj: Settings,
        db_engine: Engine,
        manager: MetadataManager,
    ):
        self.settings = settings_obj
        self.engine = db_engine
        self.metadata_manager = manager

    async def process_job(self, payload: dict[str, Any]) -> None:
        book_id_str = payload.get("book_id")
        if not book_id_str:
            logger.warning("Metadata job missing book_id", payload=payload)
            return

        book_id = UUID(book_id_str)

        with Session(self.engine) as session:
            book = session.get(Book, book_id)
            if not book:
                logger.warning(
                    "Metadata job for non-existent book", book_id=book_id_str
                )
                return

            log = logger.bind(
                book_id=book_id_str,
                title=book.title,
                author=book.author,
            )
            log.info("Fetching metadata")

            metadata = await self.metadata_manager.get_metadata(
                title=book.title,
                author=book.author,
                isbn=book.isbn13 or book.isbn10 or book.isbn,
                filepath=book.file_path,
            )

            if not metadata:
                log.info("No metadata found")
                return

            updated_fields = []
            for field, value in metadata.items():
                if value is not None and hasattr(book, field):
                    current_value = getattr(book, field)
                    if current_value != value:
                        setattr(book, field, value)
                        updated_fields.append(field)

            if updated_fields:
                book.mark_updated()
                session.add(book)
                session.commit()
                log.info(
                    "Metadata updated",
                    updated_fields=updated_fields,
                    new_title=book.title,
                )

                if self.settings.EMBED_METADATA:
                    cover_path = metadata.get("cover_path")
                    if cover_path and cover_path.startswith("http"):
                        try:
                            client = await HttpClientManager.get_client()
                            response = await client.get(cover_path)
                            if response.status_code == 200:
                                metadata["cover_data"] = response.content
                                log.info(
                                    "Downloaded cover image",
                                    url=cover_path,
                                    size=len(response.content),
                                )
                            else:
                                log.warning(
                                    "Failed to download cover image",
                                    status=response.status_code,
                                )
                        except Exception as e:
                            log.warning("Error downloading cover image", error=str(e))

                    self.metadata_manager.embed_metadata(book.file_path, metadata)

            else:
                log.debug("No new metadata to update")
