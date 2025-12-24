import asyncio
import contextlib
import subprocess
from pathlib import Path

from .kepubify import KepubifyBinary
from .logging_config import get_logger

logger = get_logger(__name__)


class KepubConverter:
    def __init__(self, binary: KepubifyBinary | None = None) -> None:
        self._binary = binary or KepubifyBinary()

    async def convert(
        self,
        input_path: Path,
        output_path: Path,
    ) -> Path | None:
        log = logger.bind(
            input_file=str(input_path),
            output_file=str(output_path),
        )

        if not input_path.exists():
            log.error("Input file not found")
            return None

        if input_path.resolve() == output_path.resolve():
            log.error("Input and output paths are the same", path=str(input_path))
            return None

        try:
            binary = await self._binary.ensure()
        except RuntimeError as e:
            log.error("Cannot ensure kepubify binary", error=str(e))
            return None

        if output_path.exists():
            with contextlib.suppress(OSError):
                output_path.unlink()

        cmd = [binary, "-o", str(output_path), str(input_path)]
        log.info("Running kepubify", command=" ".join(cmd))

        try:
            result = await asyncio.to_thread(
                subprocess.run,
                cmd,
                capture_output=True,
                text=True,
                check=True,
            )

            if output_path.exists():
                log.info(
                    "Conversion successful",
                    output_file=str(output_path),
                    stdout=result.stdout[:200] if result.stdout else None,
                )
                return output_path

            log.error(
                "Conversion completed but output file not found",
                expected=str(output_path),
            )
            return None

        except subprocess.CalledProcessError as e:
            log.error(
                "Conversion failed",
                error=str(e),
                stderr=e.stderr[:500] if e.stderr else None,
                stdout=e.stdout[:500] if e.stdout else None,
            )
            with contextlib.suppress(OSError):
                if output_path.exists():
                    output_path.unlink()
            return None

        except Exception:
            log.exception("Unexpected error during conversion")
            with contextlib.suppress(OSError):
                if output_path.exists():
                    output_path.unlink()
            return None
