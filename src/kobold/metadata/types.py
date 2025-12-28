from typing import TypedDict


class BookMetadata(TypedDict, total=False):
    title: str
    author: str
    description: str
    cover_path: str
    cover_data: bytes
    rating: float
    series: str
    series_index: float
    isbn: str
    language: str
    publisher: str
    genre: str
    amazon_id: str
    goodreads_id: str
