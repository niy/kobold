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

## Library Organization

Automatically organize your library into a structured folder hierarchy based on book metadata.

| Variable | Default | Description |
| :--- | :--- | :--- |
| `KB_ORGANIZE_LIBRARY` | `False` | Enable automatic file organization after metadata is fetched. files are **moved** from their import location. |
| `KB_ORGANIZE_TEMPLATE` | `{author}/{title}` | Template pattern for folder structure. |

### Template Variables

Data is sourced from embedded metadata (EPUB) and external providers (Amazon, Goodreads).

| Variable | Description |
| :--- | :--- |
| `{author}` | Author name |
| `{title}` | Book title |
| `{series}` | Series name |
| `{series_index}` | Number in series |
| `{language}` | Language code |
| `{genre}` | Primary genre |
| `{year}` | Publication year |

### Template Examples

```bash
# Simple author/title (default)
KB_ORGANIZE_TEMPLATE={author}/{title}
# → William Shakespeare/Romeo and Juliet.epub

# Group by Series
KB_ORGANIZE_TEMPLATE={author}/{series}/{series_index} - {title}
# → William Shakespeare/The Folger Shakespeare Library/Romeo and Juliet.epub
# → William Shakespeare/Hamlet.epub (no series)

# Flat Author Directories
KB_ORGANIZE_TEMPLATE={author} - {title}
# → William Shakespeare - Romeo and Juliet.epub

# Organize by Genre
KB_ORGANIZE_TEMPLATE={genre}/{author}/{title}
# → Plays/William Shakespeare/Romeo and Juliet.epub

# Organize by Language and Year
KB_ORGANIZE_TEMPLATE={language}/{year}/{author} - {title}
# → en/1597/William Shakespeare - Romeo and Juliet.epub
```

## File Watcher

| Variable | Default | Description |
| :--- | :--- | :--- |
| `KB_WATCH_FORCE_POLLING` | `False` | Force polling mode. Required for some network shares (NFS/SMB). |
| `KB_WATCH_POLL_DELAY_MS` | `300` | Polling interval in milliseconds (used only if polling is active). |
