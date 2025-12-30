FROM python:3.14-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libffi-dev \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN pip install uv

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1

COPY pyproject.toml uv.lock README.md ./
COPY src ./src

RUN uv sync --frozen --no-dev

# Download Kepubify
ARG TARGETARCH
RUN if [ "$TARGETARCH" = "amd64" ]; then \
        KEPUBIFY_ARCH="64bit"; \
    elif [ "$TARGETARCH" = "arm64" ]; then \
        KEPUBIFY_ARCH="arm64"; \
    else \
        echo "Unsupported architecture: $TARGETARCH"; exit 1; \
    fi && \
    curl -L "https://github.com/geek1011/kepubify/releases/download/v4.0.4/kepubify-linux-${KEPUBIFY_ARCH}" -o /kepubify && \
    chmod +x /kepubify

# Runtime Stage
FROM python:3.14-slim

WORKDIR /app

COPY --from=builder /app/.venv /app/.venv

COPY --from=builder /kepubify /usr/local/bin/kepubify

COPY src /app/src
COPY pyproject.toml /app/

ENV PATH="/app/.venv/bin:$PATH"

ENV KB_DATA_PATH=/data

RUN mkdir -p /data

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')" || exit 1

CMD ["uvicorn", "kobold.main:app", "--host", "0.0.0.0", "--port", "8000"]
