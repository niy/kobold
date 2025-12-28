# Architecture

## Core Components

1.  **API Server (FastAPI)**: Handles HTTP requests from Kobo devices implementing the Kobo Sync Protocol.
2.  **File Watcher**: Monitors configured directories for changes using `watchfiles` (Rust-based file system events).
3.  **Job Worker**: Processes background tasks (hashing, ingestion, conversion) from a persistent SQLite queue.
4.  **Scheduler**:Executes periodic maintenance tasks, such as directory reconciliation.

## Data Flow

### 1. Ingestion
1.  **Detection**: The Watcher or Scanner identifies a new or modified file.
2.  **Job Creation**: An `INGEST` job is enqueued.
3.  **Processing**:
    *   **Hashing**: The file is hashed using xxHash3-64 for identification.
    *   **Deduplication**: Checks against the database to prevent duplicate entries.
    *   **Registration**: A `Book` record is created or updated.

### 2. Metadata Enrichment
1.  **Internal Extraction**: Parses OPF (EPUB) or XMP (PDF) data for ISBN, Title, and Author.
2.  **Amazon Scraping**: Queries Amazon using the ISBN or Title/Author to retrieve high-resolution covers, descriptions, and series information.
3.  **Goodreads Fallback**: If Amazon retrieval fails, queries Goodreads.
4.  **Filename Parsing**: Falls back to parsing the filename format (e.g., "Author - Title.epub").

### 3. Library Organization (Optional)
If enabled via `KB_ORGANIZE_LIBRARY`, books are automatically renamed and moved into a folder structure defined by `KB_ORGANIZE_TEMPLATE`.
1.  **Trigger**: Occurs after successful ingestion or metadata updates.
2.  **Processing**: `OrganizationJobService` calculates the target path based on metadata (e.g., `{author}/{series}/{title}`).
3.  **Conflict Resolution**:
    *   **Deduplication**: If a file exists at the target with the exact same content hash, the source file is deleted.
    *   **Renaming**: If content differs, a numeric suffix is appended (e.g., `Book_1.epub`).
4.  **Recovery**: Detects and fixes "zombie states" where the database path differs from the actual file location due to previous interruptions.

### 4. Conversion (Optional)
If configured, `.epub` files are converted to `.kepub.epub` using `kepubify`. This binary is automatically managed and invoked by the worker process. The converted file is stored alongside the original or (optionally) replaces the original.

### 5. Synchronization
When a device initiates a sync:
1.  **Authentication**: The device authenticates using the `KB_USER_TOKEN`.
2.  **Entitlement Sync**: The server compares the device's `SyncToken` with the library state.
3.  **Delivery**: New or updated books are sent to the device. Optimized KEPUB files are served if available.

## Data Persistence

*   **Database**: SQLite (`kobold.db`) stores metadata, file paths, and job states.
*   **Filesystem**: User files are treated as read-only by default. However, if **Library Organization** is enabled, files will be moved and renamed. Only converted files or temporary artifacts are written to the storage volume otherwise.
