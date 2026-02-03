#!/bin/bash

# CsvQuery Multi-Platform Builder
# Compiles Go binaries for all supported operating systems and architectures.

set -e

# Configuration
BINARY_NAME="csvquery"
GO_DIR="go"
BIN_DIR="bin"
VERSION=$(grep -E 'Version\s+=' go/main.go | cut -d'"' -f2 || echo "1.1.0")
BUILD_DATE=$(date +%Y-%m-%d)

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== CsvQuery Builder v${VERSION} ===${NC}"
echo -e "Build Date: ${BUILD_DATE}"
echo -e "Output Dir: ${BIN_DIR}\n"

# Ensure output directory exists
mkdir -p "${BIN_DIR}"

# Platforms to build for: "GOOS/GOARCH"
PLATFORMS=(
    "darwin/amd64"
    "darwin/arm64"
    "linux/amd64"
    "linux/arm64"
    "windows/amd64"
)

# Build for current platform first
echo -e "${YELLOW}Building for host platform...${NC}"
cd "${GO_DIR}"
go build -ldflags="-s -w -X main.Version=${VERSION} -X main.BuildDate=${BUILD_DATE}" -o "../${BIN_DIR}/${BINARY_NAME}"
cd ..
echo -e "${GREEN}✓ Native binary built: ${BIN_DIR}/${BINARY_NAME}${NC}\n"

# Cross-compilation
for PLATFORM in "${PLATFORMS[@]}"; do
    OS=$(echo $PLATFORM | cut -d'/' -f1)
    ARCH=$(echo $PLATFORM | cut -d'/' -f2)
    
    OUTPUT_NAME="${BINARY_NAME}_${OS}_${ARCH}"
    if [ "$OS" = "windows" ]; then
        OUTPUT_NAME="${OUTPUT_NAME}.exe"
    fi
    
    echo -e "Building for ${YELLOW}${OS}/${ARCH}${NC}..."
    
    cd "${GO_DIR}"
    GOOS=$OS GOARCH=$ARCH go build \
        -ldflags="-s -w -X main.Version=${VERSION} -X main.BuildDate=${BUILD_DATE}" \
        -o "../${BIN_DIR}/${OUTPUT_NAME}" .
    cd ..
    
    echo -e "${GREEN}✓ Done: ${BIN_DIR}/${OUTPUT_NAME}${NC}"
done

echo -e "\n${BLUE}=== All Builds Complete ===${NC}"
ls -lh "${BIN_DIR}"
