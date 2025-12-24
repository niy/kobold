from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kobosync.metadata.amazon import AmazonProvider


class TestAmazonProvider:
    @pytest.fixture
    def provider(self) -> AmazonProvider:
        mock_settings = MagicMock()
        mock_settings.AMAZON_DOMAIN = "com"
        mock_settings.AMAZON_COOKIE = None
        return AmazonProvider(settings=mock_settings)

    @pytest.mark.asyncio
    async def test_fetch_metadata_success(self, provider: AmazonProvider) -> None:
        mock_search_html = """
        <html>
        <div data-component-type="s-search-result">
            <h2><a href="/dp/B001234567">Book Result</a></h2>
        </div>
        </html>
        """

        mock_detail_html = """
        <html>
        <div id="productTitle">Test Book Title</div>
        <div id="bylineInfo_feature_div">
            <span class="author"><a>Test Author</a></span>
        </div>
        <img id="landingImage" src="https://example.com/cover.jpg" />
        </html>
        """

        mock_search_response = MagicMock()
        mock_search_response.text = mock_search_html
        mock_search_response.status_code = 200

        mock_detail_response = MagicMock()
        mock_detail_response.text = mock_detail_html
        mock_detail_response.status_code = 200

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(
            side_effect=[mock_search_response, mock_detail_response]
        )

        with patch.object(provider, "_get_client", return_value=mock_client):
            result = await provider.fetch_metadata("9781234567890")

        assert result is not None
        assert result["title"] == "Test Book Title"

    @pytest.mark.asyncio
    async def test_fetch_metadata_not_found(self, provider: AmazonProvider) -> None:
        mock_response = MagicMock()
        mock_response.text = "<html><body>No results</body></html>"
        mock_response.status_code = 200

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_response)

        with patch.object(provider, "_get_client", return_value=mock_client):
            result = await provider.fetch_metadata("nonexistent")

        assert result is None

    @pytest.mark.asyncio
    async def test_network_error_returns_none(self, provider: AmazonProvider) -> None:
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=Exception("Network error"))

        with patch.object(provider, "_get_client", return_value=mock_client):
            result = await provider.fetch_metadata("9781234567890")

        assert result is None

    @pytest.mark.asyncio
    async def test_rate_limit_error_raises_exception(
        self, provider: AmazonProvider
    ) -> None:
        mock_response = MagicMock()
        mock_response.status_code = 503

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_response)

        with (
            patch.object(provider, "_get_client", return_value=mock_client),
            pytest.raises(Exception, match="Amazon rate limit/unavailable"),
        ):
            await provider.fetch_metadata("any query")

    @pytest.mark.asyncio
    async def test_generic_http_error_returns_none(
        self, provider: AmazonProvider
    ) -> None:
        mock_response = MagicMock()
        mock_response.status_code = 500

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_response)

        with patch.object(provider, "_get_client", return_value=mock_client):
            result = await provider.fetch_metadata("any query")

        assert result is None
