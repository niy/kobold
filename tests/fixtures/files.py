import zipfile
from pathlib import Path

import pytest


@pytest.fixture
def test_data_dir() -> Path:
    return Path(__file__).parents[1] / "data"


@pytest.fixture
def synthetic_epub(tmp_path: Path) -> Path:
    epub_path = tmp_path / "test_book.epub"

    container_xml = """<?xml version="1.0" encoding="UTF-8"?>
<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">
    <rootfiles>
        <rootfile full-path="OEBPS/content.opf" media-type="application/oebps-package+xml"/>
    </rootfiles>
</container>"""

    content_opf = """<?xml version="1.0" encoding="UTF-8"?>
<package xmlns="http://www.idpf.org/2007/opf" version="3.0" unique-identifier="uid">
    <metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
        <dc:title>Test Book Title</dc:title>
        <dc:creator>Test Author</dc:creator>
        <dc:identifier id="uid">urn:isbn:9780123456789</dc:identifier>
        <dc:language>en</dc:language>
        <dc:subject>Fiction</dc:subject>
        <dc:subject>Drama</dc:subject>
        <meta name="calibre:series" content="Test Series"/>
        <meta name="calibre:series_index" content="1"/>
    </metadata>
    <manifest>
        <item id="nav" href="nav.xhtml" media-type="application/xhtml+xml" properties="nav"/>
    </manifest>
    <spine>
        <itemref idref="nav"/>
    </spine>
</package>"""

    nav_xhtml = """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml" xmlns:epub="http://www.idpf.org/2007/ops">
<head><title>Navigation</title></head>
<body>
<nav epub:type="toc"><ol><li><a href="#">Chapter 1</a></li></ol></nav>
</body>
</html>"""

    with zipfile.ZipFile(epub_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("mimetype", "application/epub+zip")
        zf.writestr("META-INF/container.xml", container_xml)
        zf.writestr("OEBPS/content.opf", content_opf)
        zf.writestr("OEBPS/nav.xhtml", nav_xhtml)

    return epub_path


@pytest.fixture
def sample_text_file(tmp_path: Path) -> Path:
    file_path = tmp_path / "sample.txt"
    content = b"Hello, Kobold! " * 1000
    file_path.write_bytes(content)
    return file_path
