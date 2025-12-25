#!/bin/bash
set -e

TEST_DIR=".tmp/smoke_tests"
BOOKS_DIR="${TEST_DIR}/books"
DATA_DIR="${TEST_DIR}/data"
TOKEN="smoke_test_token"

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

SKIP_BUILD=false
IMAGE_TAG="latest"

while [[ "$#" -gt 0 ]]; do
    case $1 in
        --skip-build) SKIP_BUILD=true ;;
        --image-tag) IMAGE_TAG="$2"; shift ;;
        --with-metadata) WITH_METADATA=true ;;
        *) echo "Unknown parameter: $1"; exit 1 ;;
    esac
    shift
done

IMAGE_NAME="kobosync:${IMAGE_TAG}"
CONTAINER_NAME="kobosync_test"

echo -e "${GREEN}Starting Local Smoke Tests...${NC}"
echo -e "${GREEN}Target Image: ${IMAGE_NAME}${NC}"

if [ "$SKIP_BUILD" = true ]; then
    echo "Skipping Docker image build..."
else
    echo "Building Docker image ${IMAGE_NAME}..."
    docker build -t ${IMAGE_NAME} -f Dockerfile .
fi

echo "Setting up test directories in ${TEST_DIR}..."
rm -rf "${TEST_DIR}"
mkdir -p "${BOOKS_DIR}"
mkdir -p "${DATA_DIR}"

if [ "$(docker ps -aq -f name=${CONTAINER_NAME})" ]; then
    echo "Removing existing container ${CONTAINER_NAME}..."
    docker rm -f ${CONTAINER_NAME} > /dev/null
fi

echo "Starting container..."
docker run -d --name ${CONTAINER_NAME} \
  --user "$(id -u):$(id -g)" \
  -p 8000:8000 \
  -e KS_USER_TOKEN=${TOKEN} \
  -e KS_WORKER_POLL_INTERVAL=0.1 \
  -e KS_FETCH_EXTERNAL_METADATA=${WITH_METADATA:-false} \
  -v "$(pwd)/${BOOKS_DIR}:/books" \
  -v "$(pwd)/${DATA_DIR}:/data" \
  ${IMAGE_NAME}

cleanup() {
    echo -e "\n${GREEN}Cleaning up...${NC}"
    if [ "$(docker ps -aq -f name=${CONTAINER_NAME})" ]; then
        docker rm -f ${CONTAINER_NAME} > /dev/null
    fi
}
trap cleanup EXIT

echo "Waiting for service to be healthy..."
timeout=30
counter=0
until curl -s http://localhost:8000/health > /dev/null; do
    if [ $counter -ge $timeout ]; then
        echo -e "${RED}Service failed to start within ${timeout} seconds.${NC}"
        docker logs ${CONTAINER_NAME}
        exit 1
    fi
    sleep 1
    counter=$((counter+1))
done
echo -e "${GREEN}Service is up!${NC}"

echo "Installing test dependencies..."
uv sync --frozen --all-extras --dev

echo "Running pytest..."
export KS_USER_TOKEN=${TOKEN}
export KS_TEST_URL="http://localhost:8000"
export KS_TEST_BOOKS_DIR="${BOOKS_DIR}"

export KS_TEST_FETCH_METADATA=${WITH_METADATA:-false}

if uv run pytest tests/smoke/test_docker.py tests/smoke/test_metadata_verification.py -v; then
    echo -e "${GREEN}Smoke tests passed successfully!${NC}"
else
    echo -e "${RED}Smoke tests failed! Printing container logs:${NC}"
    docker logs ${CONTAINER_NAME}
    exit 1
fi
