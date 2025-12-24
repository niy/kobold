import subprocess
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kobosync.conversion import KepubConverter
from kobosync.kepubify import KepubifyBinary


@pytest.fixture
def mock_kepubify(tmp_path: Path) -> MagicMock:
    binary = MagicMock(spec=KepubifyBinary)
    binary.ensure = AsyncMock(return_value=str(tmp_path / "bin" / "kepubify"))
    return binary


@pytest.fixture
def converter(mock_kepubify: MagicMock) -> KepubConverter:
    return KepubConverter(binary=mock_kepubify)


@pytest.fixture
def mock_subprocess_success():
    async def mock_to_thread(func, *args, **kwargs):
        if func == subprocess.run:
            cmd_list = args[0]
            try:
                out_idx = cmd_list.index("-o")
                output_path = Path(cmd_list[out_idx + 1])
                output_path.parent.mkdir(parents=True, exist_ok=True)
                output_path.touch()
            except (ValueError, IndexError):
                pass

            result = MagicMock()
            result.returncode = 0
            result.stdout = ""
            result.stderr = ""
            return result

        return func(*args, **kwargs)

    with patch("asyncio.to_thread", side_effect=mock_to_thread):
        yield


@pytest.fixture
def mock_subprocess_failure():
    async def mock_to_thread(func, *args, **kwargs):
        if func == subprocess.run:
            raise subprocess.CalledProcessError(1, "kepubify", stderr="Error")
        return func(*args, **kwargs)

    with patch("asyncio.to_thread", side_effect=mock_to_thread):
        yield


class TestKepubConverter:
    @pytest.mark.asyncio
    async def test_convert_missing_input_returns_none(
        self, converter: KepubConverter, tmp_path: Path
    ) -> None:
        nonexistent = tmp_path / "missing.epub"
        result = await converter.convert(nonexistent, tmp_path / "out.kepub.epub")
        assert result is None

    @pytest.mark.asyncio
    async def test_convert_same_input_output_returns_none(
        self, converter: KepubConverter, synthetic_epub: Path
    ) -> None:
        result = await converter.convert(synthetic_epub, synthetic_epub)
        assert result is None

    @pytest.mark.asyncio
    @pytest.mark.usefixtures("mock_subprocess_success")
    async def test_convert_success(
        self, converter: KepubConverter, synthetic_epub: Path, tmp_path: Path
    ) -> None:
        output_path = tmp_path / "output" / "book.kepub.epub"
        output_path.parent.mkdir(parents=True, exist_ok=True)

        result = await converter.convert(synthetic_epub, output_path)

        assert result == output_path
        assert output_path.exists()

    @pytest.mark.asyncio
    @pytest.mark.usefixtures("mock_subprocess_failure")
    async def test_convert_subprocess_error_returns_none(
        self, converter: KepubConverter, synthetic_epub: Path, tmp_path: Path
    ) -> None:
        output_path = tmp_path / "output" / "failed.kepub.epub"
        output_path.parent.mkdir(parents=True, exist_ok=True)

        result = await converter.convert(synthetic_epub, output_path)

        assert result is None

    @pytest.mark.asyncio
    async def test_convert_binary_unavailable_returns_none(
        self, synthetic_epub: Path, tmp_path: Path
    ) -> None:
        mock_binary = MagicMock(spec=KepubifyBinary)
        mock_binary.ensure = AsyncMock(side_effect=RuntimeError("Download failed"))

        converter = KepubConverter(binary=mock_binary)
        output_path = tmp_path / "output" / "book.kepub.epub"
        output_path.parent.mkdir(parents=True, exist_ok=True)

        result = await converter.convert(synthetic_epub, output_path)

        assert result is None

    @pytest.mark.asyncio
    async def test_convert_unexpected_error_returns_none(
        self, synthetic_epub: Path, tmp_path: Path
    ) -> None:
        mock_binary = MagicMock(spec=KepubifyBinary)
        mock_binary.ensure = AsyncMock(return_value="/bin/kepubify")

        converter = KepubConverter(binary=mock_binary)
        output_path = tmp_path / "output" / "book.kepub.epub"
        output_path.parent.mkdir(parents=True, exist_ok=True)

        async def mock_to_thread(func, *args, **kwargs):
            if func == subprocess.run:
                raise OSError("Unexpected system error")
            return func(*args, **kwargs)

        with patch("asyncio.to_thread", side_effect=mock_to_thread):
            result = await converter.convert(synthetic_epub, output_path)

        assert result is None
        assert not output_path.exists()

    @pytest.mark.asyncio
    async def test_convert_output_not_created_returns_none(
        self, synthetic_epub: Path, tmp_path: Path
    ) -> None:
        mock_binary = MagicMock(spec=KepubifyBinary)
        mock_binary.ensure = AsyncMock(return_value="/bin/kepubify")

        converter = KepubConverter(binary=mock_binary)
        output_path = tmp_path / "output" / "book.kepub.epub"
        output_path.parent.mkdir(parents=True, exist_ok=True)

        async def mock_to_thread(func, *args, **kwargs):
            if func == subprocess.run:
                result = MagicMock()
                result.returncode = 0
                result.stdout = ""
                result.stderr = ""
                return result
            return func(*args, **kwargs)

        with patch("asyncio.to_thread", side_effect=mock_to_thread):
            result = await converter.convert(synthetic_epub, output_path)

        assert result is None
