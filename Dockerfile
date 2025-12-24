FROM python:3.14-alpine AS builder

RUN apk add --no-cache build-base libffi-dev curl

RUN pip install uv

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1

COPY pyproject.toml uv.lock ./

RUN uv sync --frozen --no-dev --no-install-project

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
FROM python:3.14-alpine

RUN apk add --no-cache libffi

WORKDIR /app

COPY --from=builder /app/.venv /app/.venv

COPY --from=builder /kepubify /usr/local/bin/kepubify

COPY src /app/src
COPY pyproject.toml /app/

ENV PATH="/app/.venv/bin:$PATH"

ENV KS_DATA_PATH=/data
ENV PYTHONPATH=/app/src

RUN mkdir -p /data

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD wget -q --spider http://localhost:8000/health || exit 1

CMD ["uvicorn", "kobosync.main:app", "--host", "0.0.0.0", "--port", "8000"]
