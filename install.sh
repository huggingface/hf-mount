#!/bin/sh
# Install hf-mount
# Usage: curl -fsSL https://raw.githubusercontent.com/huggingface/hf-mount/main/install.sh | sh
set -e

REPO="huggingface/hf-mount"
INSTALL_DIR="${INSTALL_DIR:-$HOME/.local/bin}"

# Detect platform
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)

case "$OS" in
  linux)  PLATFORM="linux" ;;
  darwin) PLATFORM="apple-darwin" ;;
  *)      echo "Unsupported OS: $OS" >&2; exit 1 ;;
esac

case "$ARCH" in
  x86_64)       ARCH_TAG="x86_64" ;;
  aarch64|arm64) ARCH_TAG="arm64"; [ "$PLATFORM" = "linux" ] && ARCH_TAG="aarch64" ;;
  *)            echo "Unsupported architecture: $ARCH" >&2; exit 1 ;;
esac

BASE_URL="https://github.com/${REPO}/releases/latest/download"
mkdir -p "$INSTALL_DIR"

for bin in hf-mount hf-mount-nfs hf-mount-fuse; do
  BINARY="${bin}-${ARCH_TAG}-${PLATFORM}"
  echo "Downloading ${BINARY}..."
  curl -fSL "${BASE_URL}/${BINARY}" -o "${INSTALL_DIR}/${bin}"
  chmod +x "${INSTALL_DIR}/${bin}"
done

echo "Installed hf-mount, hf-mount-nfs, and hf-mount-fuse to ${INSTALL_DIR}/"

# Check if install dir is in PATH
case ":$PATH:" in
  *":${INSTALL_DIR}:"*) ;;
  *) echo "Add ${INSTALL_DIR} to your PATH: export PATH=\"${INSTALL_DIR}:\$PATH\"" ;;
esac
