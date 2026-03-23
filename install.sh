#!/bin/sh
# Install hf-mount (NFS backend, no root needed)
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

BINARY="hf-mount-nfs-${ARCH_TAG}-${PLATFORM}"
URL="https://github.com/${REPO}/releases/latest/download/${BINARY}"

echo "Downloading ${BINARY}..."
mkdir -p "$INSTALL_DIR"
curl -fSL "$URL" -o "${INSTALL_DIR}/hf-mount-nfs"
chmod +x "${INSTALL_DIR}/hf-mount-nfs"

echo "Installed hf-mount-nfs to ${INSTALL_DIR}/hf-mount-nfs"

# Check if install dir is in PATH
case ":$PATH:" in
  *":${INSTALL_DIR}:"*) ;;
  *) echo "Add ${INSTALL_DIR} to your PATH: export PATH=\"${INSTALL_DIR}:\$PATH\"" ;;
esac
