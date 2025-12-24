from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kobosync.kepubify import KepubifyBinary


@pytest.fixture
def kepubify_binary(tmp_path: Path) -> KepubifyBinary:
    return KepubifyBinary(bin_dir=tmp_path / "bin")


class TestKepubifyBinary:
    def test_resolve_returns_system_binary_when_available(
        self, kepubify_binary: KepubifyBinary
    ) -> None:
        with patch("shutil.which", return_value="/usr/bin/kepubify"):
            result = kepubify_binary.resolve()

        assert result == "/usr/bin/kepubify"

    def test_resolve_returns_local_binary_when_downloaded(
        self, kepubify_binary: KepubifyBinary
    ) -> None:
        kepubify_binary.bin_dir.mkdir(parents=True)
        local_path = (
            kepubify_binary.bin_dir / kepubify_binary._get_platform_binary_name()
        )
        local_path.touch()

        with patch("shutil.which", return_value=None):
            result = kepubify_binary.resolve()

        assert result == str(local_path)

    @pytest.mark.parametrize(
        "system, machine",
        [
            ("darwin", "arm64"),
            ("darwin", "x86_64"),
            ("linux", "aarch64"),
            ("linux", "armv7l"),
            ("linux", "x86_64"),
            ("windows", "amd64"),
        ],
    )
    def test_resolve_correctly_identifies_platform_binary(
        self,
        kepubify_binary: KepubifyBinary,
        system: str,
        machine: str,
    ) -> None:
        with (
            patch("platform.system", return_value=system),
            patch("platform.machine", return_value=machine),
            patch("shutil.which", return_value=None),
            patch("pathlib.Path.exists", return_value=True),
            patch("pathlib.Path.chmod"),
        ):
            result = kepubify_binary.resolve()

            assert result is not None
            if system == "windows":
                assert result.endswith(".exe")
            elif system == "darwin":
                assert "darwin" in result
            elif system == "linux":
                assert "linux" in result

    def test_resolve_returns_none_when_not_found(
        self, kepubify_binary: KepubifyBinary
    ) -> None:
        with patch("shutil.which", return_value=None):
            result = kepubify_binary.resolve()

        assert result is None

    @pytest.mark.asyncio
    async def test_ensure_reuses_resolved_path(
        self, kepubify_binary: KepubifyBinary
    ) -> None:
        with patch.object(
            kepubify_binary, "resolve", return_value="/usr/bin/kepubify"
        ) as mock_resolve:
            result = await kepubify_binary.ensure()

            assert result == "/usr/bin/kepubify"
            mock_resolve.assert_called_once()

    @pytest.mark.asyncio
    async def test_ensure_returns_cached_path(
        self, kepubify_binary: KepubifyBinary, tmp_path: Path
    ) -> None:
        cached_path = tmp_path / "cached_kepubify"
        cached_path.touch()
        kepubify_binary._cached_path = str(cached_path)

        result = await kepubify_binary.ensure()

        assert result == str(cached_path)

    @pytest.mark.asyncio
    async def test_ensure_downloads_when_not_found(
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
                "kobosync.kepubify.HttpClientManager.get_client",
                return_value=mock_client,
            ),
        ):
            result = await kepubify_binary.ensure()

        assert result is not None
        assert Path(result).exists()
        mock_client.get.assert_called_once()

    @pytest.mark.asyncio
    async def test_ensure_raises_on_download_failure(
        self, kepubify_binary: KepubifyBinary
    ) -> None:
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=Exception("Network error"))

        with (
            patch("shutil.which", return_value=None),
            patch(
                "kobosync.kepubify.HttpClientManager.get_client",
                return_value=mock_client,
            ),
            pytest.raises(RuntimeError, match="Cannot download kepubify"),
        ):
            await kepubify_binary.ensure()
