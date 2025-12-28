from pathlib import Path

import pytest

from kobold.utils.hashing import get_file_hash


class TestFileHash:
    def test_hash_returns_string(self, sample_text_file: Path) -> None:
        result = get_file_hash(sample_text_file)

        assert result
        assert isinstance(result, str)
        assert result.isalnum()

    def test_hash_is_deterministic(self, sample_text_file: Path) -> None:
        hash1 = get_file_hash(sample_text_file)
        hash2 = get_file_hash(sample_text_file)

        assert hash1 == hash2

    def test_different_content_produces_different_hash(self, tmp_path: Path) -> None:
        file1 = tmp_path / "file1.txt"
        file2 = tmp_path / "file2.txt"

        file1.write_bytes(b"Content A" * 1000)
        file2.write_bytes(b"Content B" * 1000)

        hash1 = get_file_hash(file1)
        hash2 = get_file_hash(file2)

        assert hash1 != hash2

    def test_nonexistent_file_raises_error(self, tmp_path: Path) -> None:
        nonexistent = tmp_path / "does_not_exist.txt"

        with pytest.raises(FileNotFoundError):
            get_file_hash(nonexistent)

    def test_small_file_works(self, tmp_path: Path) -> None:
        small_file = tmp_path / "small.txt"
        small_file.write_bytes(b"tiny")

        result = get_file_hash(small_file)
        assert result

    def test_empty_file_works(self, tmp_path: Path) -> None:
        empty_file = tmp_path / "empty.txt"
        empty_file.write_bytes(b"")

        result = get_file_hash(empty_file)
        assert result
