from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kobosync.metadata.manager import MetadataManager

if TYPE_CHECKING:
    from pathlib import Path


class TestMetadataManager:
    @pytest.fixture
    def manager(self) -> MetadataManager:
        mock_settings = MagicMock()
        mock_settings.AMAZON_DOMAIN = "com"
        mock_settings.AMAZON_COOKIE = None
        return MetadataManager(mock_settings)

    @pytest.mark.asyncio
    async def test_get_metadata_returns_internal_when_providers_fail(
        self,
        manager: MetadataManager,
        synthetic_epub: Path,
    ) -> None:
        with (
            patch.object(
                manager._amazon,
                "fetch_metadata",
                new_callable=AsyncMock,
                return_value=None,
            ),
            patch.object(
                manager._goodreads,
                "fetch_metadata",
                new_callable=AsyncMock,
                return_value=None,
            ),
        ):
            result = await manager.get_metadata(
                title=synthetic_epub.stem,
                author=None,
                filepath=str(synthetic_epub),
            )

        assert result["isbn"] == "9780123456789"
        assert result is not None

    @pytest.mark.asyncio
    async def test_external_metadata_merged_with_internal(
        self,
        manager: MetadataManager,
        synthetic_epub: Path,
    ) -> None:
        amazon_metadata = {
            "title": "Enriched Title",
            "cover_path": "https://amazon.com/cover.jpg",
            "description": "Great book!",
        }

        with (
            patch.object(
                manager._amazon,
                "fetch_metadata",
                new_callable=AsyncMock,
                return_value=amazon_metadata,
            ),
        ):
            result = await manager.get_metadata(
                title="SomeBook",
                filepath=str(synthetic_epub),
            )

        assert result["cover_path"] == "https://amazon.com/cover.jpg"
        assert result["description"] == "Great book!"

    @pytest.mark.asyncio
    async def test_goodreads_fallback_when_amazon_fails(
        self,
        manager: MetadataManager,
    ) -> None:
        goodreads_metadata = {
            "title": "GR Title",
            "author": "GR Author",
        }

        with (
            patch.object(
                manager._amazon,
                "fetch_metadata",
                new_callable=AsyncMock,
                return_value=None,
            ),
            patch.object(
                manager._goodreads,
                "fetch_metadata",
                new_callable=AsyncMock,
                return_value=goodreads_metadata,
            ),
        ):
            result = await manager.get_metadata(
                title="Search Title",
                author="Search Author",
            )

        assert result["title"] == "GR Title"
        assert result["author"] == "GR Author"

    @pytest.mark.asyncio
    async def test_filename_parsing_fallback_when_all_providers_fail(
        self,
        manager: MetadataManager,
    ) -> None:
        with (
            patch.object(
                manager._amazon,
                "fetch_metadata",
                new_callable=AsyncMock,
                return_value=None,
            ),
            patch.object(
                manager._goodreads,
                "fetch_metadata",
                new_callable=AsyncMock,
                return_value=None,
            ),
        ):
            result = await manager.get_metadata(
                title="ignored",
                filepath="/books/Jane Doe - Great Novel.epub",
            )

        assert result["author"] == "Jane Doe"
        assert result["title"] == "Great Novel"

        assert result["author"] == "Jane Doe"
        assert result["title"] == "Great Novel"

    @pytest.mark.asyncio
    async def test_partial_external_metadata_preserves_internal_fields(
        self,
        manager: MetadataManager,
        synthetic_epub: Path,
    ) -> None:
        internal_meta = {
            "title": "Internal Title",
            "publisher": "O'Reilly",
        }

        amazon_metadata = {"title": "External Title"}

        with (
            patch.object(
                manager, "_extract_internal_metadata", return_value=internal_meta
            ),
            patch.object(
                manager._amazon,
                "fetch_metadata",
                new_callable=AsyncMock,
                return_value=amazon_metadata,
            ),
            patch.object(
                manager._goodreads,
                "fetch_metadata",
                new_callable=AsyncMock,
                return_value=None,
            ),
        ):
            result = await manager.get_metadata(
                title="Ignored",
                filepath=str(synthetic_epub),
            )

        assert result["title"] == "External Title"
        assert result["publisher"] == "O'Reilly"

    @pytest.mark.asyncio
    async def test_handles_unexpected_provider_types_gracefully(
        self,
        manager: MetadataManager,
        synthetic_epub: Path,
    ) -> None:
        bad_metadata = {"title": 12345}

        with (
            patch.object(manager, "_extract_internal_metadata", return_value={}),
            patch.object(
                manager._amazon,
                "fetch_metadata",
                new_callable=AsyncMock,
                return_value=bad_metadata,
            ),
        ):
            result = await manager.get_metadata(
                title="Test", filepath=str(synthetic_epub)
            )

        assert result["title"] == 12345  # type: ignore[comparison-overlap]
