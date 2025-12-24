from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from sqlmodel import Session, select

from ..config import Settings, get_settings
from ..database import get_session_dependency
from ..http_client import HttpClientManager
from ..logging_config import get_logger
from ..models import Book, ReadingState
from ..utils.kobo_token import KoboSyncToken
from .proxy import KoboProxyService

logger = get_logger(__name__)

router = APIRouter()


def _verify_token(token: str, settings: Settings) -> None:
    if token != settings.USER_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")


def _is_local_book(book_id: str) -> bool:
    try:
        UUID(book_id)
        return True
    except ValueError:
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Initialization & Authentication
# ─────────────────────────────────────────────────────────────────────────────


@router.get("/api/kobo/{token}/v1/initialization")
async def initialization(
    token: str,
    request: Request,
    settings: Annotated[Settings, Depends(get_settings)],
) -> dict[str, Any]:
    _verify_token(token, settings)

    base_url = str(request.base_url).rstrip("/")

    return {
        "Resources": {
            "image_host": f"{base_url}/images",
            "image_url_template": (
                f"{base_url}/images/{{ImageId}}/{{Width}}/{{Height}}/False/img.jpg"
            ),
        }
    }


@router.post("/api/kobo/{token}/v1/auth/device")
async def auth_device(
    token: str,
    request: Request,
    settings: Annotated[Settings, Depends(get_settings)],
) -> JSONResponse:
    _verify_token(token, settings)

    try:
        body = await request.json()
    except Exception:
        body = {}

    user_key = body.get("UserKey", "local-user")

    from uuid import uuid4

    return JSONResponse(
        content={
            "AccessToken": "ACCESS_TOKEN",
            "RefreshToken": "REFRESH_TOKEN",
            "TrackingId": str(uuid4()),
            "UserKey": user_key,
        }
    )


# ─────────────────────────────────────────────────────────────────────────────
# Library Synchronization
# ─────────────────────────────────────────────────────────────────────────────


@router.get("/api/kobo/{token}/v1/library/sync")
async def sync_library(
    token: str,
    request: Request,
    settings: Annotated[Settings, Depends(get_settings)],
    session: Annotated[Session, Depends(get_session_dependency)],
    proxy_service: Annotated[KoboProxyService, Depends(KoboProxyService)],
) -> JSONResponse:
    """
    Hybrid library sync endpoint.

    Merges local sideloaded books with official Kobo store entitlements.
    Uses the composite KoboSyncToken to track sync state for both sources.
    """
    _verify_token(token, settings)

    log = logger.bind(endpoint="sync_library")

    # Parse incoming sync token
    sync_token_obj = KoboSyncToken.from_request(request)

    # Parse last local sync time
    last_sync_local: datetime | None = None
    if sync_token_obj.lastSuccessfulSyncPointId:
        try:
            last_sync_local = datetime.fromisoformat(
                sync_token_obj.lastSuccessfulSyncPointId
            )
        except ValueError:
            log.warning(
                "Invalid sync timestamp",
                value=sync_token_obj.lastSuccessfulSyncPointId,
            )

    # Fetch local updates
    entitlements: list[dict[str, Any]] = []

    # Get new/updated books
    query = select(Book).where(Book.is_deleted == False)  # noqa: E712
    if last_sync_local:
        query = query.where(Book.updated_at > last_sync_local)

    books = session.exec(query).all()

    base_url = str(request.base_url).rstrip("/")
    for book in books:
        entitlements.append(_book_to_entitlement(book, base_url))

    # Get deleted books since last sync
    if last_sync_local:
        deleted_query = (
            select(Book)
            .where(Book.is_deleted == True)  # noqa: E712
            .where(Book.updated_at > last_sync_local)
        )
        deleted_books = session.exec(deleted_query).all()

        for book in deleted_books:
            entitlements.append(
                {
                    "RemoveEntitlement": {
                        "EntitlementId": str(book.id),
                    }
                }
            )

    log.info(
        "Local sync complete",
        new_books=len([e for e in entitlements if "NewEntitlement" in e]),
        removed_books=len([e for e in entitlements if "RemoveEntitlement" in e]),
    )

    # Fetch Kobo store entitlements
    resp_headers: dict[str, str] = {}

    kobo_status, kobo_headers, kobo_ents = await proxy_service.fetch_kobo_sync(request)

    if kobo_status == 200:
        entitlements.extend(kobo_ents)
        log.info("Kobo store sync", store_items=len(kobo_ents))

    # Get updated token from proxy response
    if "X-Kobo-SyncToken" in kobo_headers:
        proxy_token = KoboSyncToken.from_base64(kobo_headers["X-Kobo-SyncToken"])
        sync_token_obj.rawKoboSyncToken = proxy_token.rawKoboSyncToken

    if "X-Kobo-Sync" in kobo_headers:
        resp_headers["X-Kobo-Sync"] = kobo_headers["X-Kobo-Sync"]

    sync_token_obj.lastSuccessfulSyncPointId = datetime.now(UTC).isoformat()
    sync_token_obj.ongoingSyncPointId = None

    resp_headers.update(sync_token_obj.to_headers())

    return JSONResponse(content=entitlements, headers=resp_headers)


def _book_to_entitlement(book: Book, base_url: str) -> dict[str, Any]:
    download_url = f"{base_url}/download/{book.id}"
    return {
        "NewEntitlement": {
            "Id": str(book.id),
            "Title": book.title,
            "Author": book.author or "Unknown",
            "Description": book.description or "",
            "URL": download_url,
            "Format": "EPUB",
            "DownloadUrl": download_url,
            "ProductUrl": download_url,
            "ImageId": str(book.id),
            "IsPreorder": False,
            "IsLocked": False,
            "Language": book.language or "en",
            "PublicationYear": (
                book.publication_date.year if book.publication_date else 2024
            ),
            "Series": book.series,
            "SeriesNumber": str(book.series_index) if book.series_index else None,
            "SeriesNumberFloat": float(book.series_index)
            if book.series_index
            else None,
            "AverageRating": book.rating or 0,
            "ReviewCount": book.review_count or 0,
            "MinKoboVersion": "0.0.0",
            "EntitlementId": str(book.id),
            "ContentSource": "KoboSync",
        }
    }


# ─────────────────────────────────────────────────────────────────────────────
# Cover Images
# ─────────────────────────────────────────────────────────────────────────────


@router.get("/images/{image_id}/{width}/{height}/{grey}/img.jpg", response_model=None)
async def get_cover(
    image_id: str,
    width: int,
    height: int,
    grey: str,
    session: Annotated[Session, Depends(get_session_dependency)],
) -> FileResponse | StreamingResponse:
    logger.debug(
        "Cover image request", image_id=image_id, width=width, height=height, grey=grey
    )
    if not _is_local_book(image_id):
        raise HTTPException(status_code=404, detail="Image not found")

    book = session.get(Book, UUID(image_id))

    if not book or not book.cover_path:
        raise HTTPException(status_code=404, detail="Cover not found")

    # If cover is a URL, fetch and stream it
    if book.cover_path.startswith("http"):
        try:
            client = await HttpClientManager.get_client()
            response = await client.get(book.cover_path)

            return StreamingResponse(
                response.iter_bytes(),
                media_type=response.headers.get("content-type", "image/jpeg"),
            )
        except Exception as e:
            logger.warning(
                "Failed to fetch remote cover",
                url=book.cover_path,
                error=str(e),
            )
            raise HTTPException(status_code=404, detail="Cover unavailable") from e

    # Local file path
    cover_path = Path(book.cover_path)
    if not cover_path.exists():
        raise HTTPException(status_code=404, detail="Cover file not found")

    return FileResponse(cover_path)


# ─────────────────────────────────────────────────────────────────────────────
# File Downloads
# ─────────────────────────────────────────────────────────────────────────────


@router.get("/download/{book_id}")
async def download_book(
    book_id: str, session: Annotated[Session, Depends(get_session_dependency)]
) -> FileResponse:
    """
    Download an ebook file.

    Serves the KEPUB version if available, otherwise the original file.
    """
    if not _is_local_book(book_id):
        raise HTTPException(status_code=404, detail="Book not found")

    book = session.get(Book, UUID(book_id))

    if not book or book.is_deleted:
        raise HTTPException(status_code=404, detail="Book not found")

    # Prefer KEPUB if available
    file_path = Path(book.kepub_path or book.file_path)

    if not file_path.exists():
        raise HTTPException(status_code=404, detail="File not found")

    # Determine filename for download
    filename = f"{book.title}.epub"
    if book.kepub_path:
        filename = f"{book.title}.kepub.epub"

    return FileResponse(
        file_path,
        filename=filename,
        media_type="application/epub+zip",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Reading State
# ─────────────────────────────────────────────────────────────────────────────


@router.get("/api/kobo/{token}/v1/library/{book_id}/state")
async def get_reading_state(
    token: str,
    book_id: str,
    request: Request,
    settings: Annotated[Settings, Depends(get_settings)],
    session: Annotated[Session, Depends(get_session_dependency)],
    proxy_service: Annotated[KoboProxyService, Depends(KoboProxyService)],
) -> Response:
    """
    Get reading state for a book.

    For local books: returns state from database.
    For Kobo store books: proxies to Kobo API.
    """
    _verify_token(token, settings)

    if not _is_local_book(book_id):
        path = f"/v1/library/{book_id}/state"
        return await proxy_service.proxy_request(request, path)

    state = session.exec(
        select(ReadingState).where(ReadingState.book_id == UUID(book_id))
    ).first()

    now = datetime.now(UTC).isoformat()

    # Build response with defaults
    status_info = {
        "Status": state.status if state else "Unread",
        "LastModified": state.last_modified.isoformat() if state else now,
    }

    statistics = {
        "SpentReadingMinutes": state.spent_reading_minutes if state else 0,
        "RemainingTimeMinutes": state.remaining_time_minutes if state else 0,
        "LastModified": now,
    }

    current_bookmark = {
        "ProgressPercent": state.progress_percent if state else 0,
        "Location": {
            "Value": state.location_value if state else None,
            "Type": state.location_type if state else None,
            "Source": state.location_source if state else None,
        },
        "LastModified": state.last_modified.isoformat() if state else now,
    }

    return JSONResponse(
        [
            {
                "EntitlementId": book_id,
                "StatusInfo": status_info,
                "Statistics": statistics,
                "CurrentBookmark": current_bookmark,
            }
        ]
    )


@router.put("/api/kobo/{token}/v1/library/{book_id}/state")
async def update_reading_state(
    token: str,
    book_id: str,
    request: Request,
    settings: Annotated[Settings, Depends(get_settings)],
    session: Annotated[Session, Depends(get_session_dependency)],
    proxy_service: Annotated[KoboProxyService, Depends(KoboProxyService)],
) -> Response:
    """
    Update reading state for a book.

    For local books: updates database.
    For Kobo store books: proxies to Kobo API.
    """
    _verify_token(token, settings)

    if not _is_local_book(book_id):
        path = f"/v1/library/{book_id}/state"
        return await proxy_service.proxy_request(request, path)

    try:
        body = await request.json()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {e}") from e

    states = body.get("ReadingStates", [])
    if not states:
        raise HTTPException(status_code=400, detail="No states provided")

    incoming = states[0]

    state = session.exec(
        select(ReadingState).where(ReadingState.book_id == UUID(book_id))
    ).first()

    if not state:
        state = ReadingState(book_id=UUID(book_id))

        # Update from incoming data
        if "StatusInfo" in incoming:
            state.status = incoming["StatusInfo"].get("Status", state.status)

        if "Statistics" in incoming:
            stats = incoming["Statistics"]
            state.spent_reading_minutes = stats.get(
                "SpentReadingMinutes", state.spent_reading_minutes
            )
            state.remaining_time_minutes = stats.get(
                "RemainingTimeMinutes", state.remaining_time_minutes
            )

        if "CurrentBookmark" in incoming:
            bookmark = incoming["CurrentBookmark"]
            state.progress_percent = bookmark.get(
                "ProgressPercent", state.progress_percent
            )

            location = bookmark.get("Location", {})
            state.location_value = location.get("Value", state.location_value)
            state.location_type = location.get("Type", state.location_type)
            state.location_source = location.get("Source", state.location_source)

        state.mark_updated()
        session.add(state)
        session.commit()

    logger.info(
        "Reading state updated",
        book_id=book_id,
        progress=state.progress_percent,
    )

    return JSONResponse(content=body)


# ─────────────────────────────────────────────────────────────────────────────
# Catch-All Proxy
# ─────────────────────────────────────────────────────────────────────────────


@router.api_route(
    "/api/kobo/{token}/{path:path}",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH"],
)
async def catch_all_proxy(
    token: str,
    path: str,
    request: Request,
    settings: Annotated[Settings, Depends(get_settings)],
    proxy_service: Annotated[KoboProxyService, Depends(KoboProxyService)],
) -> Response:
    _verify_token(token, settings)

    proxy_path = f"/{path}"
    return await proxy_service.proxy_request(request, proxy_path)
