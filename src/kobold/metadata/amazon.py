from __future__ import annotations

import re
import urllib.parse
from typing import TYPE_CHECKING

from selectolax.parser import HTMLParser

from ..logging_config import get_logger
from .base import RateLimitedProvider

if TYPE_CHECKING:
    from ..config import Settings
    from .types import BookMetadata

logger = get_logger(__name__)

DOMAIN_LOCALE_MAP: dict[str, str] = {
    "com": "en-US,en;q=0.9",
    "co.uk": "en-GB,en;q=0.9",
    "de": "en-GB,en;q=0.9,de;q=0.8",
    "fr": "en-GB,en;q=0.9,fr;q=0.8",
    "it": "en-GB,en;q=0.9,it;q=0.8",
    "es": "en-GB,en;q=0.9,es;q=0.8",
    "ca": "en-US,en;q=0.9",
    "com.au": "en-GB,en;q=0.9",
    "co.jp": "en-GB,en;q=0.9,ja;q=0.8",
    "in": "en-GB,en;q=0.9",
    "com.br": "en-GB,en;q=0.9,pt;q=0.8",
    "com.mx": "en-US,en;q=0.9,es;q=0.8",
    "nl": "en-GB,en;q=0.9,nl;q=0.8",
    "se": "en-GB,en;q=0.9,sv;q=0.8",
    "pl": "en-GB,en;q=0.9,pl;q=0.8",
    "ae": "en-US,en;q=0.9,ar;q=0.8",
    "sa": "en-US,en;q=0.9,ar;q=0.8",
    "cn": "zh-CN,zh;q=0.9",
    "sg": "en-GB,en;q=0.9",
    "tr": "en-GB,en;q=0.9,tr;q=0.8",
    "eg": "en-US,en;q=0.9,ar;q=0.8",
    "com.be": "en-GB,en;q=0.9,fr;q=0.8,nl;q=0.8",
}


class AmazonProvider(RateLimitedProvider):
    def __init__(self, settings: Settings) -> None:
        super().__init__()
        self._settings = settings
        self._base_url = f"https://www.amazon.{self._settings.AMAZON_DOMAIN}"
        self._headers = self._build_headers()
        self._cookie_configured = bool(self._settings.AMAZON_COOKIE)

        log = logger.bind(
            domain=self._settings.AMAZON_DOMAIN,
            base_url=self._base_url,
            cookie_configured=self._cookie_configured,
        )
        log.info("Amazon provider initialized")

    def _build_headers(self) -> dict[str, str]:
        domain = self._settings.AMAZON_DOMAIN
        headers: dict[str, str] = {
            "accept-language": DOMAIN_LOCALE_MAP.get(domain, "en-US,en;q=0.9"),
            "origin": self._base_url,
        }

        if self._settings.AMAZON_COOKIE:
            headers["cookie"] = self._settings.AMAZON_COOKIE

        return headers

    def _is_auth_error(self, response_text: str) -> bool:
        auth_indicators = [
            "ap_email",  # Login form field
            "signin-form",  # Login form ID
            "captcha",  # CAPTCHA challenge
            "robot check",  # Bot detection
            "enter the characters",  # CAPTCHA prompt
            "type the characters",  # CAPTCHA prompt
            "validate your identity",  # Auth challenge
        ]
        lower_text = response_text.lower()
        return any(indicator in lower_text for indicator in auth_indicators)

    async def fetch_metadata(self, query: str) -> BookMetadata | None:
        client = await self._get_client()

        search_url = f"{self._base_url}/s?k={urllib.parse.quote_plus(query)}"
        log = logger.bind(
            query=query,
            url=search_url,
            domain=self._settings.AMAZON_DOMAIN,
        )
        log.debug("Searching Amazon")

        try:
            response = await client.get(search_url, headers=self._headers)

            if response.status_code in (429, 503):
                log.warning(
                    "Amazon rate limit or service unavailable",
                    status_code=response.status_code,
                )
                raise Exception(
                    f"Amazon rate limit/unavailable: {response.status_code}"
                )

            if response.status_code != 200:
                log.warning(
                    "Amazon search failed",
                    status_code=response.status_code,
                )
                return None

            if self._is_auth_error(response.text):
                if self._cookie_configured:
                    log.error(
                        "Amazon authentication failed - cookie may be invalid or expired. "
                        "Please update KB_AMAZON_COOKIE with a fresh session cookie.",
                        domain=self._settings.AMAZON_DOMAIN,
                    )
                else:
                    log.warning(
                        "Amazon returned a login/captcha page. Consider providing "
                        "KB_AMAZON_COOKIE to authenticate requests.",
                    )
                return None

            tree = HTMLParser(response.text)
            book_url = self._extract_book_url(tree)

            if not book_url:
                log.debug("No book found in search results")
                return None

            if not book_url.startswith("http"):
                book_url = self._base_url + book_url

            log.debug("Fetching book details", book_url=book_url)

            response = await client.get(book_url, headers=self._headers)

            if response.status_code in (429, 503):
                log.warning(
                    "Amazon detail page rate limit or service unavailable",
                    status_code=response.status_code,
                )
                raise Exception(
                    f"Amazon detail page rate limit/unavailable: {response.status_code}"
                )

            if response.status_code != 200:
                log.warning(
                    "Amazon detail page failed",
                    status_code=response.status_code,
                )
                return None

            if self._is_auth_error(response.text):
                if self._cookie_configured:
                    log.error(
                        "Amazon authentication failed on detail page - cookie may be "
                        "invalid or expired. Please update KB_AMAZON_COOKIE.",
                        domain=self._settings.AMAZON_DOMAIN,
                    )
                else:
                    log.warning(
                        "Amazon detail page requires authentication. Consider "
                        "providing KB_AMAZON_COOKIE.",
                    )
                return None

            detail_tree = HTMLParser(response.text)
            metadata = self._parse_details(detail_tree)

            if metadata.get("title"):
                log.info(
                    "Metadata extracted from Amazon",
                    title=metadata.get("title"),
                    author=metadata.get("author"),
                )

            return metadata if metadata else None

        except Exception as e:
            if "rate limit/unavailable" in str(e):
                raise e
            log.error("Amazon scraping error", error=str(e), exc_info=True)
            return None

    def _extract_book_url(self, tree: HTMLParser) -> str | None:
        results = tree.css('div[data-component-type="s-search-result"]')

        for result in results:
            link = result.css_first("h2 a")
            if link:
                href = link.attributes.get("href")
                if href:
                    return str(href)

        return None

    def _parse_details(self, tree: HTMLParser) -> BookMetadata:
        metadata: BookMetadata = {}

        def get_text(*selectors: str) -> str | None:
            for selector in selectors:
                element = tree.css_first(selector)
                if element:
                    text = element.text()
                    if text:
                        return text.strip()
            return None

        title = get_text(
            "#productTitle",
            "#ebooksProductTitle",
            "h1#title",
            "span#productTitle",
        )
        if title:
            metadata["title"] = title

        author_elements = tree.css("#bylineInfo_feature_div .author a")
        if not author_elements:
            author_elements = tree.css("#bylineInfo .author a")

        if author_elements:
            text = author_elements[0].text()
            if text:
                metadata["author"] = text.strip()

        desc_element = tree.css_first(
            '[data-a-expander-name="book_description_expander"] .a-expander-content'
        )
        if desc_element:
            metadata["description"] = desc_element.html or ""

        series_element = tree.css_first(
            "#rpi-attribute-book_details-series .rpi-attribute-value a span"
        )
        if series_element:
            series_text = series_element.text()
            if series_text:
                series_text = series_text.strip()
                metadata["series"] = series_text

                match = re.search(r"Book (\d+)", series_text)
                if match:
                    metadata["series_index"] = float(match.group(1))

        rating_element = tree.css_first(
            "#averageCustomerReviews_feature_div span#acrPopover"
        )
        if rating_element:
            rating_text = rating_element.text()
            if rating_text:
                match = re.search(r"([\d.]+)", rating_text.strip())
                if match:
                    metadata["rating"] = float(match.group(1))

        img_element = tree.css_first("#landingImage")
        if img_element:
            high_res = img_element.attributes.get("data-old-hires")
            if high_res:
                metadata["cover_path"] = high_res
            else:
                src = img_element.attributes.get("src")
                if src:
                    metadata["cover_path"] = src

        breadcrumbs = tree.css("#wayfinding-breadcrumbs_feature_div .a-list-item a")
        if breadcrumbs:
            genres = [
                b.text().strip()
                for b in breadcrumbs
                if b.text() and b.text().strip() not in ("Books", "Kindle Store")
            ]
            if genres:
                metadata["genre"] = genres[0]

        return metadata
