import asyncio
import platform
import shutil
from pathlib import Path

from .config import get_settings
from .http_client import HttpClientManager
from .logging_config import get_logger

logger = get_logger(__name__)

KEPUBIFY_VERSION = "v4.0.4"
KEPUBIFY_DOWNLOAD_BASE = (
    f"https://github.com/pgaskin/kepubify/releases/download/{KEPUBIFY_VERSION}"
)


class KepubifyBinary:
    def __init__(self, bin_dir: Path | None = None) -> None:
        self.bin_dir = bin_dir or get_settings().tools_path
        self._cached_path: str | None = None

    def _get_platform_binary_name(self) -> str:
        system = platform.system().lower()
        machine = platform.machine().lower()

        match (system, machine):
            case ("darwin", m) if "arm" in m or "aarch64" in m:
                return "kepubify-darwin-arm64"
            case ("darwin", _):
                return "kepubify-darwin-64bit"
            case ("linux", m) if "aarch64" in m or "arm64" in m:
                return "kepubify-linux-arm64"
            case ("linux", m) if "arm" in m:
                return "kepubify-linux-arm"
            case ("linux", _):
                return "kepubify-linux-64bit"
            case ("windows", _):
                return "kepubify-windows-64bit.exe"
            case _:
                logger.warning(
                    "Unknown platform, defaulting to linux-64bit",
                    system=system,
                    machine=machine,
                )
                return "kepubify-linux-64bit"

    def _find_existing(self) -> str | None:
        system_binary = shutil.which("kepubify")
        if system_binary:
            logger.debug("Using system kepubify", path=system_binary)
            return system_binary

        local_binary = self.bin_dir / self._get_platform_binary_name()

        if local_binary.exists():
            logger.debug("Using local kepubify", path=str(local_binary))
            return str(local_binary)

        return None

    async def ensure(self) -> str:
        """
        Ensure kepubify binary is available, downloading if necessary.

        Returns:
            Path to the binary

        Raises:
            RuntimeError: If binary cannot be found or downloaded
        """
        if self._cached_path and Path(self._cached_path).exists():
            return self._cached_path

        resolved = self._find_existing()
        if resolved:
            self._cached_path = resolved
            return resolved

        binary_name = self._get_platform_binary_name()
        download_url = f"{KEPUBIFY_DOWNLOAD_BASE}/{binary_name}"

        log = logger.bind(url=download_url, binary_name=binary_name)
        log.info("Downloading kepubify binary")

        try:
            client = await HttpClientManager.get_client()
            response = await client.get(download_url)
            response.raise_for_status()

            self.bin_dir.mkdir(parents=True, exist_ok=True)
            local_path = self.bin_dir / binary_name

            await asyncio.to_thread(local_path.write_bytes, response.content)
            local_path.chmod(0o755)

            self._cached_path = str(local_path)
            log.info("Kepubify downloaded successfully", path=str(local_path))

            return self._cached_path

        except Exception as e:
            log.error("Failed to download kepubify", error=str(e), exc_info=True)
            raise RuntimeError(f"Cannot download kepubify: {e}") from e
