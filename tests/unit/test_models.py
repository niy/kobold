from datetime import timedelta
from uuid import uuid4

import time_machine

from kobosync.models import Book, ReadingState


class TestBookModel:
    def test_mark_updated_changes_timestamp(self) -> None:
        book = Book(
            title="Test",
            file_path="/test.epub",
            file_hash="hash",
        )
        original_updated = book.updated_at

        with time_machine.travel(original_updated + timedelta(seconds=1), tick=False):
            book.mark_updated()

        assert book.updated_at > original_updated

    def test_mark_deleted_sets_deletion_state(self) -> None:
        book = Book(
            title="Test",
            file_path="/test.epub",
            file_hash="hash",
        )

        assert book.is_deleted is False
        assert book.deleted_at is None

        book.mark_deleted()

        assert book.is_deleted is True
        assert book.deleted_at is not None


class TestReadingStateModel:
    def test_mark_updated_changes_timestamp(self) -> None:
        state = ReadingState(book_id=uuid4())
        original = state.last_modified

        with time_machine.travel(original + timedelta(seconds=1), tick=False):
            state.mark_updated()

        assert state.last_modified > original
