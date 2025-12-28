import contextlib
import urllib.parse
from typing import TYPE_CHECKING

from selectolax.parser import HTMLParser

from ..logging_config import get_logger
from .base import RateLimitedProvider

if TYPE_CHECKING:
    from .types import BookMetadata

logger = get_logger(__name__)


class GoodreadsProvider(RateLimitedProvider):
    BASE_URL = "https://www.goodreads.com"

    async def fetch_metadata(self, query: str) -> BookMetadata | None:
        client = await self._get_client()

        search_url = f"{self.BASE_URL}/search?q={urllib.parse.quote_plus(query)}"
        log = logger.bind(query=query, url=search_url)
        log.debug("Searching Goodreads")

        try:
            response = await client.get(search_url)

            if response.status_code in (429, 503):
                log.warning(
                    "Goodreads rate limit or service unavailable",
                    status_code=response.status_code,
                )
                raise Exception(
                    f"Goodreads rate limit/unavailable: {response.status_code}"
                )

            if response.status_code != 200:
                log.warning(
                    "Goodreads search failed",
                    status_code=response.status_code,
                )
                return None

            tree = HTMLParser(response.text)
            book_url = self._extract_book_url(tree)

            if not book_url:
                log.debug("No book found in Goodreads search")
                return None

            if not book_url.startswith("http"):
                book_url = self.BASE_URL + book_url

            log.debug("Fetching book details", book_url=book_url)

            response = await client.get(book_url)

            if response.status_code in (429, 503):
                log.warning(
                    "Goodreads detail page rate limit or service unavailable",
                    status_code=response.status_code,
                )
                raise Exception(
                    f"Goodreads detail page rate limit/unavailable: {response.status_code}"
                )

            if response.status_code != 200:
                log.warning(
                    "Goodreads detail page failed",
                    status_code=response.status_code,
                )
                return None

            detail_tree = HTMLParser(response.text)
            metadata = self._parse_details(detail_tree)

            if metadata.get("title"):
                log.info(
                    "Metadata extracted from Goodreads",
                    title=metadata.get("title"),
                    author=metadata.get("author"),
                )

            return metadata if metadata else None

        except Exception as e:
            if "rate limit/unavailable" in str(e):
                raise e
            log.error("Goodreads scraping error", error=str(e), exc_info=True)
            return None

    def _extract_book_url(self, tree: HTMLParser) -> str | None:
        link = tree.css_first("table.tableList tr a.bookTitle")
        if link:
            href = link.attributes.get("href")
            return str(href) if href else None
        return None

    def _parse_details(self, tree: HTMLParser) -> BookMetadata:
        metadata: BookMetadata = {}

        title_element = tree.css_first('h1[data-testid="bookTitle"]')
        if not title_element:
            title_element = tree.css_first("#bookTitle")

        if title_element:
            text = title_element.text()
            if text:
                metadata["title"] = text.strip()

        author_element = tree.css_first(".authorName")
        if not author_element:
            author_element = tree.css_first('span[data-testid="name"]')

        if author_element:
            text = author_element.text()
            if text:
                metadata["author"] = text.strip()

        desc_element = tree.css_first("#description span")
        if not desc_element:
            desc_element = tree.css_first('div[data-testid="description"]')

        if desc_element:
            metadata["description"] = desc_element.html or ""

        rating_element = tree.css_first("[itemprop=ratingValue]")
        if rating_element:
            text = rating_element.text()
            if text:
                with contextlib.suppress(ValueError):
                    metadata["rating"] = float(text.strip())

        img_element = tree.css_first("#coverImage")
        if not img_element:
            img_element = tree.css_first("img.ResponsiveImage")

        if img_element:
            src = img_element.attributes.get("src")
            if src:
                metadata["cover_path"] = src

        genre_element = tree.css_first('div[data-testid="genresList"] a')
        if not genre_element:
            genre_element = tree.css_first(".bookPageGenreLink")

        if genre_element:
            text = genre_element.text()
            if text:
                metadata["genre"] = text.strip()

        return metadata
