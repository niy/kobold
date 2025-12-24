from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from kobosync.metadata.pdf import PdfMetadataExtractor


class TestPdfMetadataExtractor:
    @pytest.fixture
    def extractor(self) -> PdfMetadataExtractor:
        return PdfMetadataExtractor()

    def test_extract_from_nonexistent_file(
        self,
        extractor: PdfMetadataExtractor,
        tmp_path: Path,
    ) -> None:
        nonexistent = tmp_path / "does_not_exist.pdf"

        result = extractor.extract(str(nonexistent))

        assert result.get("title") is None
        assert result.get("author") is None
        assert result.get("isbn") is None

    def test_extract_from_invalid_pdf(
        self,
        extractor: PdfMetadataExtractor,
        tmp_path: Path,
    ) -> None:
        invalid = tmp_path / "invalid.pdf"
        invalid.write_bytes(b"not a pdf file")

        result = extractor.extract(str(invalid))

        assert result.get("title") is None

    def test_extract_info_dict_metadata(
        self,
        extractor: PdfMetadataExtractor,
        tmp_path: Path,
    ) -> None:
        mock_info = MagicMock()
        mock_info.title = "Test PDF Title"
        mock_info.author = "Test Author"
        mock_info.subject = "Test Description"

        mock_reader = MagicMock()
        mock_reader.metadata = mock_info
        mock_reader.xmp_metadata = None

        pdf_path = tmp_path / "test.pdf"
        pdf_path.write_bytes(b"%PDF-1.4")

        with patch("kobosync.metadata.pdf.PdfReader", return_value=mock_reader):
            result = extractor.extract(str(pdf_path))

        assert result["title"] == "Test PDF Title"
        assert result["author"] == "Test Author"
        assert result["description"] == "Test Description"

    def test_extract_xmp_metadata(
        self,
        extractor: PdfMetadataExtractor,
        tmp_path: Path,
    ) -> None:
        mock_xmp = MagicMock()
        mock_xmp.dc_title = {"x-default": "XMP Title"}
        mock_xmp.dc_creator = ["Author One", "Author Two"]
        mock_xmp.dc_description = {"x-default": "XMP Description"}
        mock_xmp.dc_language = ["en"]
        mock_xmp.dc_identifier = ["urn:isbn:9781234567890"]

        mock_reader = MagicMock()
        mock_reader.metadata = None
        mock_reader.xmp_metadata = mock_xmp

        pdf_path = tmp_path / "test.pdf"
        pdf_path.write_bytes(b"%PDF-1.4")

        with patch("kobosync.metadata.pdf.PdfReader", return_value=mock_reader):
            result = extractor.extract(str(pdf_path))

        assert result["title"] == "XMP Title"
        assert result["author"] == "Author One, Author Two"
        assert result["description"] == "XMP Description"
        assert result["language"] == "en"
        assert result["isbn"] == "9781234567890"


class TestIsbnParsing:
    @pytest.fixture
    def extractor(self) -> PdfMetadataExtractor:
        return PdfMetadataExtractor()

    def test_parse_isbn_urn_format(self, extractor: PdfMetadataExtractor) -> None:
        result = extractor._parse_isbn("urn:isbn:9781234567890")
        assert result == "9781234567890"

    def test_parse_isbn_13_digit(self, extractor: PdfMetadataExtractor) -> None:
        result = extractor._parse_isbn("978-1-234-56789-0")
        assert result == "9781234567890"

    def test_parse_isbn_10_digit(self, extractor: PdfMetadataExtractor) -> None:
        result = extractor._parse_isbn("1-234-56789-X")
        assert result == "123456789X"

    def test_parse_isbn_invalid(self, extractor: PdfMetadataExtractor) -> None:
        result = extractor._parse_isbn("not-an-isbn")
        assert result is None

    def test_parse_isbn_empty(self, extractor: PdfMetadataExtractor) -> None:
        result = extractor._parse_isbn("")
        assert result is None
