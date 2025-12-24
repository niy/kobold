import asyncio
import random
import time
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

import httpx

from ..logging_config import get_logger

if TYPE_CHECKING:
    from .types import BookMetadata

logger = get_logger(__name__)


SCRAPER_MIN_DELAY = 2.0  # seconds
SCRAPER_JITTER_MAX = 1.0  # seconds

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "sec-ch-ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "none",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
}


class RateLimitedTransport(httpx.AsyncBaseTransport):
    def __init__(
        self,
        min_delay: float = SCRAPER_MIN_DELAY,
        jitter_max: float = SCRAPER_JITTER_MAX,
    ) -> None:
        self._transport = httpx.AsyncHTTPTransport()
        self._min_delay = min_delay
        self._jitter_max = jitter_max
        self._last_request_time: float = 0.0
        self._lock = asyncio.Lock()

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_request_time

            jitter = random.uniform(0, self._jitter_max)
            required_delay = self._min_delay + jitter

            if elapsed < required_delay:
                wait_time = required_delay - elapsed
                logger.debug(
                    "Rate limiting",
                    wait_seconds=round(wait_time, 2),
                    jitter=round(jitter, 2),
                )
                await asyncio.sleep(wait_time)

            self._last_request_time = time.monotonic()

        return await self._transport.handle_async_request(request)

    async def aclose(self) -> None:
        await self._transport.aclose()


class MetadataProvider(ABC):
    @abstractmethod
    async def fetch_metadata(self, query: str) -> BookMetadata | None:
        """
        Fetch metadata for a book.

        Args:
            query: Search query (ISBN, title, author, or combination)

        Returns:
            BookMetadata dict with available fields, or None if not found.
        """
        ...


class RateLimitedProvider(MetadataProvider):
    def __init__(self) -> None:
        self._transport = RateLimitedTransport()
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                transport=self._transport,
                timeout=httpx.Timeout(30.0),
                follow_redirects=True,
                headers=BROWSER_HEADERS,
            )
        return self._client
