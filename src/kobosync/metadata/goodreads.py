import contextlib
import urllib.parse

from bs4 import BeautifulSoup

from ..logging_config import get_logger
from .base import RateLimitedProvider
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

            soup = BeautifulSoup(response.text, "html.parser")
            book_url = self._extract_book_url(soup)

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

            detail_soup = BeautifulSoup(response.text, "html.parser")
            metadata = self._parse_details(detail_soup)

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

    def _extract_book_url(self, soup: BeautifulSoup) -> str | None:
        link = soup.select_one("table.tableList tr a.bookTitle")
        if link:
            href = link.get("href")
            return str(href) if href else None
        return None

    def _parse_details(self, soup: BeautifulSoup) -> BookMetadata:
        metadata: BookMetadata = {}

        title_element = soup.select_one('h1[data-testid="bookTitle"]')
        if not title_element:
            title_element = soup.select_one("#bookTitle")

        if title_element:
            metadata["title"] = title_element.get_text().strip()

        author_element = soup.select_one(".authorName")
        if not author_element:
            author_element = soup.select_one('span[data-testid="name"]')

        if author_element:
            metadata["author"] = author_element.get_text().strip()

        desc_element = soup.select_one("#description span")
        if not desc_element:
            desc_element = soup.select_one('div[data-testid="description"]')

        if desc_element:
            metadata["description"] = desc_element.decode_contents()

        rating_element = soup.select_one("[itemprop=ratingValue]")
        if rating_element:
            with contextlib.suppress(ValueError):
                metadata["rating"] = float(rating_element.get_text().strip())

        img_element = soup.select_one("#coverImage")
        if not img_element:
            img_element = soup.select_one("img.ResponsiveImage")

        if img_element:
            src = img_element.get("src")
            if isinstance(src, str):
                metadata["cover_path"] = src

        return metadata
