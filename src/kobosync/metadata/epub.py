import contextlib
import os
import re
import shutil
import tempfile
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar

from ..logging_config import get_logger

if TYPE_CHECKING:
    from .types import BookMetadata

logger = get_logger(__name__)


class EpubMetadataExtractor:
    NAMESPACES: ClassVar[dict[str, str]] = {
        "n": "urn:oasis:names:tc:opendocument:xmlns:container",
        "pkg": "http://www.idpf.org/2007/opf",
        "dc": "http://purl.org/dc/elements/1.1/",
    }

    def extract(self, filepath: str) -> BookMetadata:
        metadata: BookMetadata = {}

        path = Path(filepath)
        if not path.exists():
            logger.debug("EPUB file not found", path=filepath)
            return metadata

        try:
            with zipfile.ZipFile(path, "r") as zf:
                opf_path = self._find_opf_path(zf)
                if not opf_path:
                    logger.debug("No OPF file found", path=filepath)
                    return metadata

                metadata = self._parse_opf(zf, opf_path, metadata)

        except zipfile.BadZipFile as e:
            logger.warning("Invalid EPUB file", path=filepath, error=str(e))
        except Exception as e:
            logger.warning(
                "Error extracting EPUB metadata",
                path=filepath,
                error=str(e),
            )

        return metadata

    def write_metadata(self, filepath: str, metadata: BookMetadata) -> None:
        path = Path(filepath)
        if not path.exists():
            raise FileNotFoundError(f"EPUB file not found: {filepath}")

        fd, temp_path_str = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
        os.close(fd)
        temp_path = Path(temp_path_str)

        try:
            with (
                zipfile.ZipFile(path, "r") as zf_in,
                zipfile.ZipFile(
                    temp_path, "w", compression=zipfile.ZIP_DEFLATED
                ) as zf_out,
            ):
                opf_path = self._find_opf_path(zf_in)
                if not opf_path:
                    raise ValueError("Could not find OPF file in EPUB")

                cover_href = None
                if metadata.get("cover_data"):
                    cover_href = self._find_cover_href(zf_in, opf_path)

                for item in zf_in.infolist():
                    if item.filename == opf_path:
                        xml_data = zf_in.read(item.filename)
                        new_xml = self._update_opf_xml(xml_data, metadata)
                        zf_out.writestr(item, new_xml)
                    elif cover_href and item.filename == cover_href:
                        logger.info("Replacing cover image", path=cover_href)
                        zf_out.writestr(item, metadata["cover_data"])
                    else:
                        zf_out.writestr(item, zf_in.read(item.filename))

            shutil.move(str(temp_path), str(path))
            logger.info("Updated EPUB metadata", path=filepath)

        except Exception:
            if temp_path.exists():
                temp_path.unlink()
            raise

    def _find_cover_href(self, zf: zipfile.ZipFile, opf_path: str) -> str | None:
        try:
            with zf.open(opf_path) as f:
                tree = ET.parse(f)
                root = tree.getroot()

                manifest = root.find("pkg:manifest", self.NAMESPACES)
                if manifest is None:
                    return None

                for item in manifest.findall("pkg:item", self.NAMESPACES):
                    props = item.get("properties", "")
                    if "cover-image" in props:
                        href = item.get("href")
                        if href:
                            return self._resolve_path(opf_path, href)

                metadata = root.find("pkg:metadata", self.NAMESPACES)
                if metadata is not None:
                    for meta in metadata.findall("pkg:meta", self.NAMESPACES):
                        if meta.get("name") == "cover":
                            cover_id = meta.get("content")

                            for item in manifest.findall("pkg:item", self.NAMESPACES):
                                if item.get("id") == cover_id:
                                    href = item.get("href")
                                    if href:
                                        return self._resolve_path(opf_path, href)

                for item in manifest.findall("pkg:item", self.NAMESPACES):
                    item_id = item.get("id", "").lower()
                    if item_id in ("cover", "cover-image", "coverimg"):
                        href = item.get("href")
                        if href:
                            return self._resolve_path(opf_path, href)

        except Exception as e:
            logger.warning("Failed to find cover href", error=str(e))

        return None

    def _resolve_path(self, opf_path: str, href: str) -> str:
        if not href:
            return ""
        opf_dir = Path(opf_path).parent
        if opf_dir and str(opf_dir) != ".":
            return f"{opf_dir}/{href}"
        return href

    def _update_opf_xml(self, xml_data: bytes, metadata: BookMetadata) -> bytes:
        for prefix, uri in self.NAMESPACES.items():
            ET.register_namespace(prefix, uri)

        root = ET.fromstring(xml_data)

        metadata_elem = root.find("pkg:metadata", self.NAMESPACES)
        if metadata_elem is None:
            return xml_data

        def update_dc(tag_name: str, value: str | None) -> None:
            if not value:
                return

            found = False
            for child in metadata_elem:
                if child.tag == f"{{{self.NAMESPACES['dc']}}}{tag_name}":
                    child.text = value
                    found = True
                    break

            if not found:
                new_elem = ET.SubElement(
                    metadata_elem, f"{{{self.NAMESPACES['dc']}}}{tag_name}"
                )
                new_elem.text = value

        update_dc("title", metadata.get("title"))
        update_dc("creator", metadata.get("author"))
        update_dc("description", metadata.get("description"))
        update_dc("language", metadata.get("language"))

        new_isbn = metadata.get("isbn")
        if new_isbn:
            for child in list(metadata_elem.findall("dc:identifier", self.NAMESPACES)):
                scheme = child.get(f"{{{self.NAMESPACES['pkg']}}}scheme")
                if scheme and scheme.upper() == "ISBN":
                    metadata_elem.remove(child)

            isbn_elem = ET.SubElement(
                metadata_elem, f"{{{self.NAMESPACES['dc']}}}identifier"
            )
            isbn_elem.set(f"{{{self.NAMESPACES['pkg']}}}scheme", "ISBN")
            isbn_elem.text = new_isbn

        return ET.tostring(root, encoding="utf-8", xml_declaration=True)  # type: ignore[no-any-return]

    def _find_opf_path(self, zf: zipfile.ZipFile) -> str | None:
        try:
            with zf.open("META-INF/container.xml") as f:
                tree = ET.parse(f)
                root = tree.getroot()

                rootfile = root.find(".//n:rootfile", self.NAMESPACES)
                if rootfile is not None:
                    return rootfile.get("full-path")
        except KeyError:
            logger.debug("No container.xml found")

        return None

    def _parse_opf(
        self,
        zf: zipfile.ZipFile,
        opf_path: str,
        metadata: BookMetadata,
    ) -> BookMetadata:
        with zf.open(opf_path) as f:
            tree = ET.parse(f)
            package = tree.getroot()

            metadata_elem = package.find("pkg:metadata", self.NAMESPACES)
            if metadata_elem is None:
                return metadata

            title = metadata_elem.find("dc:title", self.NAMESPACES)
            if title is not None and title.text:
                metadata["title"] = title.text.strip()

            author = metadata_elem.find("dc:creator", self.NAMESPACES)
            if author is not None and author.text:
                metadata["author"] = author.text.strip()

            description = metadata_elem.find("dc:description", self.NAMESPACES)
            if description is not None and description.text:
                metadata["description"] = description.text.strip()

            language = metadata_elem.find("dc:language", self.NAMESPACES)
            if language is not None and language.text:
                metadata["language"] = language.text.strip()

            isbn = self._extract_isbn(metadata_elem)
            if isbn:
                metadata["isbn"] = isbn

            self._extract_series(metadata_elem, metadata)

        return metadata

    def _extract_isbn(self, metadata_elem: ET.Element) -> str | None:
        for identifier in metadata_elem.findall("dc:identifier", self.NAMESPACES):
            text = identifier.text
            if not text:
                continue

            scheme = None
            for attr_name, attr_value in identifier.attrib.items():
                if "scheme" in attr_name.lower():
                    scheme = attr_value

            cleaned = re.sub(r"[^0-9X]", "", text.upper())

            if scheme and "ISBN" in scheme.upper() and len(cleaned) in (10, 13):
                return cleaned

            if text.lower().startswith("urn:isbn:"):
                isbn_part = text.split(":")[-1]
                cleaned = re.sub(r"[^0-9X]", "", isbn_part.upper())
                if len(cleaned) in (10, 13):
                    return cleaned

            if len(cleaned) == 13 and cleaned.startswith(("978", "979")):
                return cleaned

        return None

    def _extract_series(
        self,
        metadata_elem: ET.Element,
        metadata: BookMetadata,
    ) -> None:
        for meta in metadata_elem.findall("pkg:meta", self.NAMESPACES):
            name = meta.get("name", "")
            content = meta.get("content", "")

            match name:
                case "calibre:series":
                    metadata["series"] = content
                case "calibre:series_index":
                    with contextlib.suppress(ValueError):
                        metadata["series_index"] = float(content)
