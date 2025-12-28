from kobold.utils.paths import PathTemplate, sanitize_filename


class TestSanitizeFilename:
    def test_basic_sanitization(self):
        assert sanitize_filename("valid_filename.txt") == "valid_filename.txt"

    def test_replaces_invalid_chars(self):
        assert sanitize_filename("file/with/slashes.txt") == "file_with_slashes.txt"
        assert sanitize_filename("file:with:colons.txt") == "file_with_colons.txt"

    def test_truncates_long_filename(self):
        long_name = "a" * 255 + ".txt"
        sanitized = sanitize_filename(long_name)
        assert len(sanitized) == 200
        assert sanitized.endswith(".txt")


class TestPathTemplate:
    def test_basic_rendering(self):
        template = PathTemplate("{author}/{title}")
        metadata = {"author": "Author Name", "title": "Book Title"}
        assert str(template.render(metadata)) == "Author Name/Book Title"

    def test_rendering_with_missing_optional_fields(self):
        template = PathTemplate("{author}/{series}/{title}")
        metadata = {"author": "Author", "title": "Title", "series": None}
        assert str(template.render(metadata)) == "Author/Title"

    def test_rendering_missing_all_fields_fallback(self):
        template = PathTemplate("{author}/{title}")
        metadata = {"author": None, "title": None}
        assert str(template.render(metadata)) == "."

    def test_sanitization(self):
        template = PathTemplate("{title}")
        metadata = {"title": "Title/With:Invalid*Chars"}
        assert str(template.render(metadata)) == "Title_With_Invalid_Chars"

    def test_whitespace_stripping(self):
        template = PathTemplate("{author}/{title}")
        metadata = {"author": " Author ", "title": " Title. "}
        assert str(template.render(metadata)) == "Author/Title"
