#!/bin/sh
set -eu

REPO="questdb/middle-manager"
BINARY="middle-manager"
INSTALL_DIR="/usr/local/bin"

main() {
  arch=$(uname -m)
  case "$arch" in
    x86_64|amd64)  target="x86_64-unknown-linux-musl" ;;
    aarch64|arm64) target="aarch64-unknown-linux-musl" ;;
    *) echo "Unsupported architecture: $arch" >&2; exit 1 ;;
  esac

  if [ "$(uname -s)" != "Linux" ]; then
    echo "This script is for Linux. On macOS use: brew install questdb/middle-manager/mm" >&2
    exit 1
  fi

  tag=$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" | grep '"tag_name"' | cut -d'"' -f4)
  if [ -z "$tag" ]; then
    echo "Failed to fetch latest release tag" >&2
    exit 1
  fi

  archive="${BINARY}-${target}.tar.gz"
  url="https://github.com/${REPO}/releases/download/${tag}/${archive}"

  tmpdir=$(mktemp -d)
  trap 'rm -rf "$tmpdir"' EXIT

  echo "Downloading ${BINARY} ${tag} for ${arch}..."
  curl -fsSL "$url" -o "${tmpdir}/${archive}"

  tar -xzf "${tmpdir}/${archive}" -C "$tmpdir"

  if [ -w "$INSTALL_DIR" ]; then
    mv "${tmpdir}/${BINARY}" "${INSTALL_DIR}/${BINARY}"
  else
    echo "Installing to ${INSTALL_DIR} (requires sudo)..."
    sudo mv "${tmpdir}/${BINARY}" "${INSTALL_DIR}/${BINARY}"
  fi
  chmod +x "${INSTALL_DIR}/${BINARY}"

  echo "Installed ${BINARY} ${tag} to ${INSTALL_DIR}/${BINARY}"
}

main
