from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kobold.kepubify import KepubifyBinary


@pytest.fixture
def kepubify_binary(tmp_path: Path) -> KepubifyBinary:
    return KepubifyBinary(bin_dir=tmp_path / "bin")


class TestKepubifyBinaryFilesystem:
    @pytest.mark.asyncio
    async def test_ensure_returns_local_binary_when_already_downloaded(
        self, kepubify_binary: KepubifyBinary
    ) -> None:
        kepubify_binary.bin_dir.mkdir(parents=True)
        local_path = (
            kepubify_binary.bin_dir / kepubify_binary._get_platform_binary_name()
        )
        local_path.touch()

        with patch("shutil.which", return_value=None):
            result = await kepubify_binary.ensure()

        assert result == str(local_path)

    @pytest.mark.asyncio
    async def test_ensure_downloads_and_returns_path_when_not_found(
        self, kepubify_binary: KepubifyBinary
    ) -> None:
        mock_response = MagicMock()
        mock_response.content = b"binary content"
        mock_response.raise_for_status = MagicMock()

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_response)

        with (
            patch("shutil.which", return_value=None),
            patch(
                "kobold.kepubify.HttpClientManager.get_client",
                return_value=mock_client,
            ),
        ):
            result = await kepubify_binary.ensure()

        assert result is not None
        assert Path(result).exists()
        mock_client.get.assert_called_once()
