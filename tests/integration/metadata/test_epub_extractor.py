from pathlib import Path

import pytest

from kobold.metadata.epub import EpubMetadataExtractor


class TestEpubMetadataExtractor:
    @pytest.fixture
    def extractor(self) -> EpubMetadataExtractor:
        return EpubMetadataExtractor()

    def test_extract_from_valid_epub(
        self,
        extractor: EpubMetadataExtractor,
        synthetic_epub: Path,
    ) -> None:
        result = extractor.extract(str(synthetic_epub))

        assert result["title"] == "Test Book Title"
        assert result["author"] == "Test Author"
        assert result["isbn"] == "9780123456789"
        assert result["series"] == "Test Series"
        assert result["series_index"] == 1.0

    def test_extract_from_nonexistent_file(
        self,
        extractor: EpubMetadataExtractor,
        tmp_path: Path,
    ) -> None:
        nonexistent = tmp_path / "does_not_exist.epub"

        result = extractor.extract(str(nonexistent))

        assert result.get("title") is None
        assert result.get("author") is None
        assert result.get("isbn") is None

    def test_extract_from_invalid_zip(
        self,
        extractor: EpubMetadataExtractor,
        tmp_path: Path,
    ) -> None:
        invalid_file = tmp_path / "invalid.epub"
        invalid_file.write_bytes(b"not a zip file")

        result = extractor.extract(str(invalid_file))

        assert result.get("title") is None

    def test_extract_with_missing_opf(
        self,
        extractor: EpubMetadataExtractor,
        tmp_path: Path,
    ) -> None:
        import zipfile

        epub_path = tmp_path / "no_opf.epub"

        container_xml = """<?xml version="1.0" encoding="UTF-8"?>
<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">
    <rootfiles>
    </rootfiles>
</container>"""

        with zipfile.ZipFile(epub_path, "w") as zf:
            zf.writestr("mimetype", "application/epub+zip")
            zf.writestr("META-INF/container.xml", container_xml)

        result = extractor.extract(str(epub_path))

        assert result.get("title") is None


class TestIsbnExtraction:
    @pytest.fixture
    def extractor(self) -> EpubMetadataExtractor:
        return EpubMetadataExtractor()

    def _create_epub_with_identifier(
        self,
        tmp_path: Path,
        identifier_xml: str,
    ) -> Path:
        import zipfile

        epub_path = tmp_path / "isbn_test.epub"

        container_xml = """<?xml version="1.0" encoding="UTF-8"?>
<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">
    <rootfiles>
        <rootfile full-path="content.opf" media-type="application/oebps-package+xml"/>
    </rootfiles>
</container>"""

        content_opf = f"""<?xml version="1.0" encoding="UTF-8"?>
<package xmlns="http://www.idpf.org/2007/opf" xmlns:opf="http://www.idpf.org/2007/opf" version="3.0">
    <metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
        <dc:title>Test</dc:title>
        {identifier_xml}
    </metadata>
    <manifest/>
    <spine/>
</package>"""

        with zipfile.ZipFile(epub_path, "w") as zf:
            zf.writestr("mimetype", "application/epub+zip")
            zf.writestr("META-INF/container.xml", container_xml)
            zf.writestr("content.opf", content_opf)

        return epub_path

    def test_isbn_from_urn_format(
        self,
        extractor: EpubMetadataExtractor,
        tmp_path: Path,
    ) -> None:
        epub = self._create_epub_with_identifier(
            tmp_path,
            "<dc:identifier>urn:isbn:9781234567890</dc:identifier>",
        )

        result = extractor.extract(str(epub))

        assert result["isbn"] == "9781234567890"

    def test_isbn_from_scheme_attribute(
        self,
        extractor: EpubMetadataExtractor,
        tmp_path: Path,
    ) -> None:
        epub = self._create_epub_with_identifier(
            tmp_path,
            '<dc:identifier opf:scheme="ISBN">978-0-123-45678-9</dc:identifier>',
        )

        result = extractor.extract(str(epub))

        assert result["isbn"] == "9780123456789"

    def test_isbn_from_numeric_pattern(
        self,
        extractor: EpubMetadataExtractor,
        tmp_path: Path,
    ) -> None:
        epub = self._create_epub_with_identifier(
            tmp_path,
            "<dc:identifier>9789876543210</dc:identifier>",
        )

        result = extractor.extract(str(epub))

        assert result["isbn"] == "9789876543210"
