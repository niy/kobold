from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kobosync.metadata.goodreads import GoodreadsProvider


class TestGoodreadsProvider:
    @pytest.fixture
    def provider(self) -> GoodreadsProvider:
        return GoodreadsProvider()

    @pytest.mark.asyncio
    async def test_fetch_metadata_success(self, provider: GoodreadsProvider) -> None:
        search_html = """
        <html>
        <table class="tableList">
            <tr><a class="bookTitle" href="/book/show/12345">Test Book</a></tr>
        </table>
        </html>
        """

        book_html = """
        <html>
        <h1 data-testid="bookTitle">Goodreads Book Title</h1>
        <span class="authorName">GR Author</span>
        <div id="description">
            <span>Book description here</span>
        </div>
        </html>
        """

        mock_search_response = MagicMock()
        mock_search_response.text = search_html
        mock_search_response.status_code = 200

        mock_book_response = MagicMock()
        mock_book_response.text = book_html
        mock_book_response.status_code = 200

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(
            side_effect=[mock_search_response, mock_book_response]
        )

        with patch.object(provider, "_get_client", return_value=mock_client):
            result = await provider.fetch_metadata("Test Query")

        assert result is not None
        assert result["title"] == "Goodreads Book Title"
        assert result["author"] == "GR Author"

    @pytest.mark.asyncio
    async def test_fetch_metadata_no_results(self, provider: GoodreadsProvider) -> None:
        mock_response = MagicMock()
        mock_response.text = "<html><body>No results found</body></html>"
        mock_response.status_code = 200

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_response)

        with patch.object(provider, "_get_client", return_value=mock_client):
            result = await provider.fetch_metadata("nonexistent")

        assert result is None

    @pytest.mark.asyncio
    async def test_network_error_returns_none(
        self, provider: GoodreadsProvider
    ) -> None:
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=Exception("Connection failed"))

        with patch.object(provider, "_get_client", return_value=mock_client):
            result = await provider.fetch_metadata("any query")

        assert result is None

    @pytest.mark.asyncio
    async def test_rate_limit_error_raises_exception(
        self, provider: GoodreadsProvider
    ) -> None:
        mock_response = MagicMock()
        mock_response.status_code = 429

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_response)

        with (
            patch.object(provider, "_get_client", return_value=mock_client),
            pytest.raises(Exception, match="Goodreads rate limit/unavailable"),
        ):
            await provider.fetch_metadata("any query")

    @pytest.mark.asyncio
    async def test_http_error_returns_none(self, provider: GoodreadsProvider) -> None:
        mock_response = MagicMock()
        mock_response.status_code = 403

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_response)

        with patch.object(provider, "_get_client", return_value=mock_client):
            result = await provider.fetch_metadata("any query")

        assert result is None
