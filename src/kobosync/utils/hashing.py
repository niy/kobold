from pathlib import Path

import xxhash

from ..logging_config import get_logger

logger = get_logger(__name__)

CHUNK_SIZE = 65536


def get_file_hash(filepath: Path) -> str:
    if not filepath.exists():
        raise FileNotFoundError(f"File not found: {filepath}")

    h = xxhash.xxh3_64()
    with filepath.open("rb") as f:
        for chunk in iter(lambda: f.read(CHUNK_SIZE), b""):
            h.update(chunk)

    return h.hexdigest()
