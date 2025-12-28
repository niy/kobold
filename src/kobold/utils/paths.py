from __future__ import annotations

import re
from pathlib import Path

# Characters that are invalid in file/directory names across platforms
INVALID_CHARS_PATTERN = re.compile(r'[<>:"/\\|?*\x00-\x1f]')

# Maximum length for a single path segment (directory or filename)
MAX_SEGMENT_LENGTH = 200

# Template variable pattern: {variable_name}
TEMPLATE_VAR_PATTERN = re.compile(r"\{(\w+)\}")


class PathTemplate:
    """
    Simple template engine for generating organized file paths.

    Template variables are enclosed in curly braces: {author}, {title}, etc.
    Missing optional fields are simply omitted from the path.
    """

    def __init__(self, pattern: str) -> None:
        """
        Initialize with a template pattern.

        Args:
            pattern: Template string like "{author}/{series}/{title}"
        """
        self.pattern = pattern

    def render(self, metadata: dict[str, str | None]) -> Path:
        """
        Render the template with metadata values.

        Args:
            metadata: Dictionary of field names to values.
            None values are treated as missing and omitted.

        Returns:
            Path object with the rendered template.
        """
        result = self.pattern

        # Replace each {variable} with its sanitized value or empty string
        for match in TEMPLATE_VAR_PATTERN.finditer(self.pattern):
            var_name = match.group(1)
            value = metadata.get(var_name)

            sanitized = sanitize_filename(str(value)) if value is not None else ""

            result = result.replace(match.group(0), sanitized)

        segments = [s.strip() for s in result.split("/") if s.strip()]

        if not segments:
            # Fallback if all fields were empty
            return Path()

        return Path("/".join(segments))


def sanitize_filename(filename: str) -> str:
    sanitized = INVALID_CHARS_PATTERN.sub("_", filename)
    sanitized = sanitized.strip(". \t\n\r")

    if len(sanitized) > MAX_SEGMENT_LENGTH:
        path = Path(sanitized)
        stem = path.stem[: MAX_SEGMENT_LENGTH - len(path.suffix)]
        sanitized = stem + path.suffix

    return sanitized
