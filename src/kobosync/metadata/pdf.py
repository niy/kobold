import os
import re
import shutil
import tempfile
from datetime import UTC
from pathlib import Path
from typing import Any

from pypdf import PdfReader, PdfWriter

from ..logging_config import get_logger
from .types import BookMetadata

logger = get_logger(__name__)


class PdfMetadataExtractor:
    def extract(self, filepath: str) -> BookMetadata:
        metadata: BookMetadata = {}

        path = Path(filepath)
        if not path.exists():
            logger.debug("PDF file not found", path=filepath)
            return metadata

        try:
            reader = PdfReader(filepath)

            self._extract_info(reader, metadata)

            self._extract_xmp(reader, metadata)

            if metadata.get("isbn"):
                metadata["isbn"] = re.sub(
                    r"[^0-9X]",
                    "",
                    metadata["isbn"].upper(),
                )

        except Exception as e:
            logger.warning(
                "Error extracting PDF metadata",
                path=filepath,
                error=str(e),
            )

        return metadata

    def write_metadata(self, filepath: str, metadata: BookMetadata) -> None:
        path = Path(filepath)
        if not path.exists():
            raise FileNotFoundError(f"PDF file not found: {filepath}")

        pdf_metadata = {}
        if metadata.get("title"):
            pdf_metadata["/Title"] = metadata["title"]
        if metadata.get("author"):
            pdf_metadata["/Author"] = metadata["author"]
        if metadata.get("description"):
            pdf_metadata["/Subject"] = metadata["description"]

        keywords = []
        if metadata.get("isbn"):
            keywords.append(f"ISBN:{metadata['isbn']}")
        if metadata.get("language"):
            keywords.append(f"Lang:{metadata['language']}")
        if keywords:
            pdf_metadata["/Keywords"] = ", ".join(keywords)

        try:
            xmp_bytes = self._generate_xmp(metadata)
        except Exception as e:
            logger.warning("Failed to generate XMP", error=str(e))
            xmp_bytes = None

        if not pdf_metadata and not xmp_bytes:
            return

        fd, temp_path_str = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
        os.close(fd)
        temp_path = Path(temp_path_str)

        try:
            writer = PdfWriter(clone_from=path)

            if pdf_metadata:
                writer.add_metadata(pdf_metadata)

            if xmp_bytes:
                writer.xmp_metadata = xmp_bytes

            with temp_path.open("wb") as f_out:
                writer.write(f_out)

            shutil.move(str(temp_path), str(path))
            logger.info("Updated PDF metadata (Info+XMP)", path=filepath)

        except Exception:
            if temp_path.exists():
                temp_path.unlink()
            raise

    def _generate_xmp(self, metadata: BookMetadata) -> bytes:
        import xml.etree.ElementTree as ET
        from datetime import datetime

        NS_RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
        NS_XMP = "http://ns.adobe.com/xap/1.0/"
        NS_DC = "http://purl.org/dc/elements/1.1/"
        NS_XMPIDQ = "http://ns.adobe.com/xmp/Identifier/qual/1.0/"
        NS_CALIBRE = "http://calibre-ebook.com/xmp-namespace"
        NS_CALIBRE_SI = "http://calibre-ebook.com/xmp-namespace-series-index"
        NS_XML = "http://www.w3.org/XML/1998/namespace"

        ET.register_namespace("rdf", NS_RDF)
        ET.register_namespace("xmp", NS_XMP)
        ET.register_namespace("dc", NS_DC)
        ET.register_namespace("xmpidq", NS_XMPIDQ)
        ET.register_namespace("calibre", NS_CALIBRE)
        ET.register_namespace("calibreSI", NS_CALIBRE_SI)
        ET.register_namespace("xml", NS_XML)

        xmpmeta = ET.Element("{adobe:ns:meta/}xmpmeta")
        xmpmeta.set("{adobe:ns:meta/}xmptk", "KoboSync via pypdf")

        rdf = ET.SubElement(xmpmeta, f"{{{NS_RDF}}}RDF")

        desc_main = ET.SubElement(rdf, f"{{{NS_RDF}}}Description")
        desc_main.set(f"{{{NS_RDF}}}about", "")
        desc_main.set(f"{{{NS_XMP}}}CreateDate", datetime.now(UTC).isoformat())
        desc_main.set(f"{{{NS_XMP}}}CreatorTool", "KoboSync")

        if metadata.get("title"):
            title_elem = ET.SubElement(desc_main, f"{{{NS_DC}}}title")
            alt_bag = ET.SubElement(title_elem, f"{{{NS_RDF}}}Alt")
            li = ET.SubElement(alt_bag, f"{{{NS_RDF}}}li")
            li.set(f"{{{NS_XML}}}lang", "x-default")
            li.text = metadata["title"]

        if metadata.get("author"):
            creator_elem = ET.SubElement(desc_main, f"{{{NS_DC}}}creator")
            seq = ET.SubElement(creator_elem, f"{{{NS_RDF}}}Seq")
            li = ET.SubElement(seq, f"{{{NS_RDF}}}li")
            li.text = metadata["author"]

        if metadata.get("description"):
            desc_elem = ET.SubElement(desc_main, f"{{{NS_DC}}}description")
            alt_bag = ET.SubElement(desc_elem, f"{{{NS_RDF}}}Alt")
            li = ET.SubElement(alt_bag, f"{{{NS_RDF}}}li")
            li.set(f"{{{NS_XML}}}lang", "x-default")
            li.text = metadata["description"]

        if metadata.get("language"):
            lang_elem = ET.SubElement(desc_main, f"{{{NS_DC}}}language")
            bag = ET.SubElement(lang_elem, f"{{{NS_RDF}}}Bag")
            li = ET.SubElement(bag, f"{{{NS_RDF}}}li")
            li.text = metadata["language"]

        identifiers = []
        if metadata.get("isbn"):
            identifiers.append(("isbn", metadata["isbn"]))
        if metadata.get("amazon_id"):
            identifiers.append(("amazon", metadata["amazon_id"]))
        if metadata.get("goodreads_id"):
            identifiers.append(("goodreads", metadata["goodreads_id"]))

        xmp_id_elem = ET.SubElement(desc_main, f"{{{NS_XMP}}}Identifier")
        bag_id = ET.SubElement(xmp_id_elem, f"{{{NS_RDF}}}Bag")

        for scheme, value in identifiers:
            li = ET.SubElement(bag_id, f"{{{NS_RDF}}}li")
            li.set(f"{{{NS_RDF}}}parseType", "Resource")

            sch = ET.SubElement(li, f"{{{NS_XMPIDQ}}}Scheme")
            sch.text = scheme

            val = ET.SubElement(li, f"{{{NS_RDF}}}value")
            val.text = value

        if metadata.get("series"):
            desc_cal = ET.SubElement(rdf, f"{{{NS_RDF}}}Description")
            desc_cal.set(f"{{{NS_RDF}}}about", "")

            # Calibre series is a structured value
            series_elem = ET.SubElement(desc_cal, f"{{{NS_CALIBRE}}}series")
            series_elem.set(f"{{{NS_RDF}}}parseType", "Resource")

            val_elem = ET.SubElement(series_elem, f"{{{NS_RDF}}}value")
            val_elem.text = metadata["series"]

            if metadata.get("series_index"):
                idx_elem = ET.SubElement(
                    series_elem, f"{{{NS_CALIBRE_SI}}}series_index"
                )
                idx_elem.text = f"{metadata['series_index']:.2f}"

        xml_str = ET.tostring(xmpmeta, encoding="utf-8")

        start = b'<?xpacket begin="\xef\xbb\xbf" id="W5M0MpCehiHzreSzNTczkc9d"?>'
        end = b'<?xpacket end="w"?>'

        return start + xml_str + end  # type: ignore[no-any-return]

    def _extract_info(
        self,
        reader: PdfReader,
        metadata: BookMetadata,
    ) -> None:
        info = reader.metadata
        if not info:
            return

        if info.title:
            metadata["title"] = info.title

        if info.author:
            metadata["author"] = info.author

        if info.subject:
            metadata["description"] = info.subject

    def _extract_xmp(
        self,
        reader: PdfReader,
        metadata: BookMetadata,
    ) -> None:
        try:
            xmp = reader.xmp_metadata
            if not xmp:
                return

            if xmp.dc_title:
                title = self._get_localized_value(xmp.dc_title)
                if title:
                    metadata["title"] = title

            if xmp.dc_creator and isinstance(xmp.dc_creator, list):
                metadata["author"] = ", ".join(xmp.dc_creator)

            if xmp.dc_description:
                description = self._get_localized_value(xmp.dc_description)
                if description:
                    metadata["description"] = description

            if xmp.dc_language and isinstance(xmp.dc_language, list):
                metadata["language"] = xmp.dc_language[0]

            # ISBN from identifiers
            if xmp.dc_identifier:
                for identifier in xmp.dc_identifier:
                    isbn = self._parse_isbn(identifier)
                    if isbn:
                        metadata["isbn"] = isbn
                        break
        except Exception as e:
            logger.warning("Error reading XMP", error=str(e))

    def _get_localized_value(self, value: Any) -> str | None:
        if isinstance(value, dict):
            return value.get("x-default") or (
                next(iter(value.values())) if value else None
            )

        return str(value) if value else None

    def _parse_isbn(self, text: str) -> str | None:
        if not text:
            return None

        if "urn:isbn:" in text.lower():
            cleaned = re.sub(r"[^0-9X]", "", text.upper())
            if len(cleaned) in (10, 13):
                return cleaned

        cleaned = re.sub(r"[^0-9X]", "", text.upper())
        if len(cleaned) in (10, 13):
            if len(cleaned) == 13 and cleaned.startswith(("978", "979")):
                return cleaned
            if len(cleaned) == 10:
                return cleaned

        return None
