import pytest

from kobold.metadata.pdf import PdfMetadataExtractor


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
