from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from kobold.metadata.pdf import PdfMetadataExtractor


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

    def test_extract_metadata(
        self,
        extractor: PdfMetadataExtractor,
        tmp_path: Path,
    ) -> None:
        mock_doc = MagicMock()
        mock_doc.metadata = {
            "title": "Test PDF Title",
            "author": "Test Author",
            "subject": "Test Description",
        }
        mock_doc.xref_get_key.return_value = (None, None)
        mock_doc.__enter__ = MagicMock(return_value=mock_doc)
        mock_doc.__exit__ = MagicMock(return_value=None)

        pdf_path = tmp_path / "test.pdf"
        pdf_path.write_bytes(b"%PDF-1.4")

        with patch("kobold.metadata.pdf.pymupdf.open", return_value=mock_doc):
            result = extractor.extract(str(pdf_path))

        assert result["title"] == "Test PDF Title"
        assert result["author"] == "Test Author"
        assert result["description"] == "Test Description"

    def test_extract_xmp_metadata(
        self,
        extractor: PdfMetadataExtractor,
        tmp_path: Path,
    ) -> None:
        xmp_xml = b"""<?xml version="1.0"?>
        <x:xmpmeta xmlns:x="adobe:ns:meta/">
            <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
                <rdf:Description xmlns:dc="http://purl.org/dc/elements/1.1/">
                    <dc:title>
                        <rdf:Alt>
                            <rdf:li xml:lang="x-default">XMP Title</rdf:li>
                        </rdf:Alt>
                    </dc:title>
                    <dc:creator>
                        <rdf:Seq>
                            <rdf:li>Author One</rdf:li>
                            <rdf:li>Author Two</rdf:li>
                        </rdf:Seq>
                    </dc:creator>
                    <dc:description>
                        <rdf:Alt>
                            <rdf:li xml:lang="x-default">XMP Description</rdf:li>
                        </rdf:Alt>
                    </dc:description>
                    <dc:language>
                        <rdf:Bag>
                            <rdf:li>en</rdf:li>
                        </rdf:Bag>
                    </dc:language>
                    <dc:identifier>
                        <rdf:Bag>
                            <rdf:li>urn:isbn:9781234567890</rdf:li>
                        </rdf:Bag>
                    </dc:identifier>
                </rdf:Description>
            </rdf:RDF>
        </x:xmpmeta>"""

        mock_doc = MagicMock()
        mock_doc.metadata = {}
        mock_doc.xref_get_key.return_value = ("stream", None)
        mock_doc.xref_stream.return_value = xmp_xml
        mock_doc.__enter__ = MagicMock(return_value=mock_doc)
        mock_doc.__exit__ = MagicMock(return_value=None)

        pdf_path = tmp_path / "test.pdf"
        pdf_path.write_bytes(b"%PDF-1.4")

        with patch("kobold.metadata.pdf.pymupdf.open", return_value=mock_doc):
            result = extractor.extract(str(pdf_path))

        assert result["title"] == "XMP Title"
        assert result["author"] == "Author One, Author Two"
        assert result["description"] == "XMP Description"
        assert result["language"] == "en"
        assert result["isbn"] == "9781234567890"
