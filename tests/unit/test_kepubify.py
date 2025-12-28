from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from kobold.kepubify import KepubifyBinary


class TestKepubifyBinaryUnit:
    @pytest.fixture
    def kepubify_binary(self) -> KepubifyBinary:
        return KepubifyBinary(bin_dir=Path("/fake/bin/dir"))

    @pytest.mark.asyncio
    async def test_ensure_returns_system_binary_when_available(
        self, kepubify_binary: KepubifyBinary
    ) -> None:
        with patch("shutil.which", return_value="/usr/bin/kepubify"):
            result = await kepubify_binary.ensure()

        assert result == "/usr/bin/kepubify"

    @pytest.mark.asyncio
    async def test_ensure_raises_on_download_failure(
        self, kepubify_binary: KepubifyBinary
    ) -> None:
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=Exception("Network error"))

        with (
            patch("shutil.which", return_value=None),
            patch("pathlib.Path.mkdir"),
            patch(
                "kobold.kepubify.HttpClientManager.get_client",
                return_value=mock_client,
            ),
            pytest.raises(RuntimeError, match="Cannot download kepubify"),
        ):
            await kepubify_binary.ensure()

    @pytest.mark.asyncio
    async def test_ensure_returns_cached_path_on_subsequent_calls(
        self, kepubify_binary: KepubifyBinary
    ) -> None:
        with patch("shutil.which", return_value="/usr/bin/kepubify"):
            first_result = await kepubify_binary.ensure()

        with (
            patch("shutil.which", return_value=None),
            patch("pathlib.Path.exists", return_value=True),
        ):
            second_result = await kepubify_binary.ensure()

        assert first_result == second_result
