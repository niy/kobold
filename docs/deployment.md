# Deployment

The official image is available at `ghcr.io/niy/kobold`.

## Deployment Guide

### Docker Compose

#### 1. Generate Secure Token

Start by generating a strong random string. You can use any of the following commands:

*   **OpenSSL**: `openssl rand -hex 16`
*   **Python**: `python3 -c "import secrets; print(secrets.token_hex(16))"`
*   **Password Manager**: Use your password manager's built-in generator.

    !!! note "Token Generator"
        <div style="display: flex; gap: 0;">
            <input type="text" class="ks-token-input" readonly placeholder="Generate a token" style="flex-grow: 1; padding: 8px 12px; font-family: var(--md-code-font-family); min-width: 0; cursor: pointer;" title="Click to copy">
            <button type="button" class="md-button md-button--primary ks-generate-btn" title="Generate new token" style="margin: 0; border-radius: 0 4px 4px 0; min-width: 40px; padding: 0 12px;">↻</button>
        </div>

#### 2. Create Configuration

Create a `docker-compose.yml` file.

```yaml
services:
    kobold:
      image: ghcr.io/niy/kobold:latest
      container_name: kobold
      restart: unless-stopped
      ports:
        - "8000:8000"
      volumes:
        - ./data:/data
        - /path/to/books:/books
      environment:
        - KB_USER_TOKEN=__YOUR_TOKEN_HERE__
```

#### 3. Start Service

```bash
docker compose up -d
```

#### 4. Next Steps

Once the service is running, configure your Kobo device to connect:

1. See [Device Setup](device_setup.md) to configure your Kobo eReader
2. Review [Configuration](configuration.md) for additional options

## Troubleshooting

### Permission Issues

If you encounter permission errors accessing the `/books` directory, ensure the container has read access. You can set the ownership to the current user:

```bash
chown -R $(id -u):$(id -g) ./books
```

### Network Shares (NFS/SMB)

If your `/books` directory is mounted from a network share (e.g., NAS), file watcher events might not propagate. In this case, you must enable polling mode.

Update your `docker-compose.yml`:

```yaml
environment:
  - KB_WATCH_FORCE_POLLING=True
  - KB_WATCH_POLL_DELAY_MS=500
```

### Container Fails to Start

Check the logs for error details:

```bash
docker compose logs -f
```

Common issues include:

*   Port `8000` is already in use.
*   `KB_USER_TOKEN` is missing (it is required for startup).
