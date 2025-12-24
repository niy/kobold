from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

from ..config import Settings
from ..logging_config import get_logger
from .amazon import AmazonProvider
from .epub import EpubMetadataExtractor
from .goodreads import GoodreadsProvider
from .pdf import PdfMetadataExtractor
from .types import BookMetadata

logger = get_logger(__name__)


class MetadataManager:
    def __init__(self, settings: Settings) -> None:
        self._epub_extractor = EpubMetadataExtractor()
        self._pdf_extractor = PdfMetadataExtractor()

        self._amazon = AmazonProvider(settings)
        self._goodreads = GoodreadsProvider()

    async def get_metadata(
        self,
        title: str,
        author: str | None = None,
        isbn: str | None = None,
        filepath: str | None = None,
    ) -> BookMetadata:
        log = logger.bind(initial_title=title, author=author, isbn=isbn)

        metadata: BookMetadata = {}
        internal_isbn: str | None = None

        if filepath:
            internal_meta = self._extract_internal_metadata(filepath)

            if internal_meta:
                if internal_meta.get("isbn"):
                    internal_isbn = internal_meta["isbn"]
                    log.info("Found internal ISBN", isbn=internal_isbn)

                if internal_meta.get("title") and title == Path(filepath).stem:
                    title = internal_meta["title"]
                    log.debug(
                        "Using internal title",
                        internal_title=title,
                    )

                if internal_meta.get("author"):
                    author = internal_meta["author"]

                metadata = self._merge_metadata(metadata, internal_meta)

        search_isbn = isbn or internal_isbn

        if search_isbn:
            log.info("Strategy 1: Amazon by ISBN", isbn=search_isbn)
            result = await self._amazon.fetch_metadata(search_isbn)
            if result:
                return self._merge_metadata(metadata, result)

        query = f"{title} {author}".strip() if author else title
        log.info("Strategy 2: Amazon by query", query=query)
        result = await self._amazon.fetch_metadata(query)
        if result:
            return self._merge_metadata(metadata, result)

        log.info("Strategy 3: Goodreads fallback", query=query)
        result = await self._goodreads.fetch_metadata(query)
        if result:
            return self._merge_metadata(metadata, result)

        if filepath:
            log.info("Strategy 4: Filename parsing")
            filename_meta = self._parse_filename(filepath)
            if filename_meta:
                return self._merge_metadata(metadata, filename_meta)

        log.debug("No external metadata found, using internal only")
        fallback: BookMetadata = cast("BookMetadata", {"title": title, **metadata})
        if author:
            fallback["author"] = author
        return fallback

    def _extract_internal_metadata(self, filepath: str) -> BookMetadata:
        lower_path = filepath.lower()

        if lower_path.endswith(".epub") or lower_path.endswith(".kepub.epub"):
            return self._epub_extractor.extract(filepath)
        elif lower_path.endswith(".pdf"):
            return self._pdf_extractor.extract(filepath)

        return {}

    def _parse_filename(self, filepath: str) -> BookMetadata | None:
        stem = Path(filepath).stem

        if " - " in stem:
            parts = stem.split(" - ", maxsplit=1)
            if len(parts) == 2:
                return {"author": parts[0].strip(), "title": parts[1].strip()}

        if "_" in stem and " " not in stem:
            parts = stem.split("_", maxsplit=1)
            if len(parts) == 2:
                return {
                    "title": parts[0].replace("_", " ").strip(),
                    "author": parts[1].replace("_", " ").strip(),
                }

        return None

    def embed_metadata(
        self,
        filepath: str,
        metadata: BookMetadata,
    ) -> bool:
        log = logger.bind(filepath=filepath)
        lower_path = filepath.lower()

        try:
            if lower_path.endswith(".epub"):
                self._epub_extractor.write_metadata(filepath, metadata)
                log.info("Embedded metadata into EPUB")
                return True
            elif lower_path.endswith(".pdf"):
                self._pdf_extractor.write_metadata(filepath, metadata)
                log.info("Embedded metadata into PDF")
                return True
            else:
                log.debug("Metadata embedding not supported for this format")
                return False

        except Exception as e:
            log.error("Failed to embed metadata", error=str(e))
            return False

    def _merge_metadata(
        self,
        base: BookMetadata,
        overlay: Mapping[str, Any],
    ) -> BookMetadata:
        result = dict(base)

        for key, value in overlay.items():
            if value is not None:
                result[key] = value

        return cast("BookMetadata", result)
