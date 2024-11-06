#!/bin/bash

VERSION="0.1.0"

# Detect OS and architecture
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)

# Map architecture to Go naming convention
case $ARCH in
  x86_64|amd64)
    ARCH="amd64"
    ;;
  aarch64|arm64)
    ARCH="arm64"
    ;;
  *)
    echo "Unsupported architecture: $ARCH"
    exit 1
    ;;
esac

# Set the download URL and binary name
BINARY_NAME="bemidb-${OS}-${ARCH}"
DOWNLOAD_URL="https://github.com/BemiHQ/BemiDB/releases/download/v$VERSION/$BINARY_NAME"

# Download the binary
echo "Downloading $DOWNLOAD_URL..."
curl -L "$DOWNLOAD_URL" -o ./bemidb

# Make the binary executable
chmod +x ./bemidb
