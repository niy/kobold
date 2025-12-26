# Configuration

Kobold is configured via environment variables.

## Core Settings

| Variable | Default | Description |
| :--- | :--- | :--- |
| `KB_USER_TOKEN` | *(none)* | **Required**: Secure token for API authentication. App will fail to start if missing. |
| `KB_DATA_PATH` | `./data` | Directory for persistent application data (`kobold.db`). |
| `KB_WATCH_DIRS` | `/books` | Comma-separated list of directories to monitor. |
| `KB_WORKER_POLL_INTERVAL` | `300.0` | Interval in seconds between worker polls for new jobs (metadata, conversion). |
| `KB_LOG_LEVEL` | `INFO` | Logging verbosity (`DEBUG`, `INFO`, `WARNING`, `ERROR`). |

## Metadata Providers

| Variable | Default | Description |
| :--- | :--- | :--- |
| `KB_AMAZON_DOMAIN` | `com` | Amazon region domain (e.g., `com`, `co.uk`, `de`, `jp`). |
| `KB_AMAZON_COOKIE` | *(empty)* | Optional session cookie for authenticated requests to avoid rate limits. |

## Feature Flags

| Variable | Default | Description |
| :--- | :--- | :--- |
| `KB_CONVERT_EPUB` | `True` | Automatically convert `.epub` to `.kepub.epub`. |
| `KB_DELETE_ORIGINAL_AFTER_CONVERSION` | `False` | Delete the original `.epub` file after successful conversion. |
| `KB_EMBED_METADATA` | `False` | Write scraped metadata (cover, author, ISBN) back into the source file. |
| `KB_FETCH_EXTERNAL_METADATA` | `True` | Query external sources (Amazon, Goodreads) for metadata. Set to `False` in test environments to avoid hitting external APIs. |

## File Watcher

| Variable | Default | Description |
| :--- | :--- | :--- |
| `KB_WATCH_FORCE_POLLING` | `False` | Force polling mode. Required for some network shares (NFS/SMB). |
| `KB_WATCH_POLL_DELAY_MS` | `300` | Polling interval in milliseconds (used only if polling is active). |
