# KoboSync

[![License](https://img.shields.io/github/license/niy/kobosync)](LICENSE)
[![Build Status](https://img.shields.io/github/actions/workflow/status/niy/kobosync/ci.yml)](https://github.com/niy/kobosync/actions/workflows/ci.yml)
[![Python Version](https://img.shields.io/badge/python-3.13-blue)](pyproject.toml)

KoboSync is a lightweight service that synchronizes a local eBook collection with Kobo eReaders. It automates file ingestion, enriches content with metadata, and serves books via the Kobo Sync API.

**[Documentation](https://niy.github.io/kobosync/)**

## Features

*   **Lightweight**: Minimal resource footprint. It has no UI, no external dependencies, and runs as a single process.
*   **Automatic Ingestion**: Detects and processes new files immediately upon addition to watched directories.
*   **Metadata Enrichment**: Retrieves high-resolution covers, series information, and ISBNs from Amazon and Goodreads.
*   **Automatic Conversion**: Optionally converts `.epub` files to optimized `.kepub.epub` format.

## Getting Started

The recommended way to deploy is via Docker Compose.

### Quick Start

1. Generate a secure token:

    ```bash
    openssl rand -hex 16
    ```

2.  Create a `docker-compose.yml`:

    ```yaml
    services:
      kobosync:
        image: ghcr.io/niy/kobosync:latest
        container_name: kobosync
        restart: unless-stopped
        ports:
          - "8000:8000"
        volumes:
          - ./data:/data
          - /path/to/my/books:/books
        environment:
          - KS_USER_TOKEN=your_generated_token
    ```

3.  Start the service:

    ```bash
    docker compose up -d
    ```

## Configuration

KoboSync is configured using environment variables.

The only required environment variable is `KS_USER_TOKEN`, which is used for authentication. You can use `openssl rand -hex 16` or any other method you prefer (e.g. a password manager) to generate a cryptographically secure random string.

For a list of available options, refer to the [Configuration Documentation](docs/configuration.md).

## Device Setup

To synchronize a Kobo device, edit the `Kobo eReader.conf` file on the device to point to the KoboSync server.

1.  Connect the Kobo device to a computer via USB.
2.  Locate `.kobo/Kobo/Kobo eReader.conf`.
3.  Update the `api_endpoint` in the `[OneStoreServices]` section:

    ```ini
    [OneStoreServices]
    api_endpoint=http://<SERVER_IP>:8000/api/kobo/<KS_USER_TOKEN>
    ```

For detailed instructions, refer to the [Device Setup Guide](docs/device_setup.md).

## Architecture

The application consists of a core Python process that handles file watching, metadata fetching, and API requests. Data is stored in a structured directory format.

For more information, see the [Architecture Overview](docs/architecture.md).

## License

Distributed under the GNU Affero General Public License v3.0. See [LICENSE](LICENSE) for more information.
