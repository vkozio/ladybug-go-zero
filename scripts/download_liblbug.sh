#!/usr/bin/env bash
# Download Ladybug prebuilt library and header. Unpacks into output_dir/lib/dynamic/{platform}/ and output_dir/include/.
# Usage: download_liblbug.sh <version> <platform> [output_dir]
#   version:  release tag (e.g. v0.14.2-bindings.0) or "latest"
#   platform: linux-amd64 | linux-arm64 | darwin | windows-amd64
#   output_dir: default .
# Default repo: vkozio/ladybug (fork with bindings built from master). Override: LADYBUG_REPO=Owner/repo

set -euo pipefail

REPO="${LADYBUG_REPO:-vkozio/ladybug}"
VERSION="${1:?Usage: download_liblbug.sh <version> <platform> [output_dir]}"
PLATFORM="${2:?Usage: download_liblbug.sh <version> <platform> [output_dir]}"
OUT_DIR="${3:-.}"

case "$PLATFORM" in
  linux-amd64)   ASSET="liblbug-linux-x86_64.tar.gz"   ;;
  linux-arm64)   ASSET="liblbug-linux-aarch64.tar.gz"  ;;
  darwin)        ASSET="liblbug-osx-universal.tar.gz"  ;;
  windows-amd64) ASSET="liblbug-windows-x86_64.zip"    ;;
  *)
    echo "Unknown platform: $PLATFORM" >&2
    echo "Supported: linux-amd64, linux-arm64, darwin, windows-amd64" >&2
    exit 1
    ;;
esac

if [ "$VERSION" = "latest" ]; then
  VERSION=$(curl -sSf "https://api.github.com/repos/${REPO}/releases/latest" | grep '"tag_name":' | sed -E 's/.*"tag_name":\s*"([^"]+)".*/\1/')
  [ -n "$VERSION" ] || { echo "Could not resolve latest release" >&2; exit 1; }
  echo "Resolved latest: $VERSION"
fi

URL="https://github.com/${REPO}/releases/download/${VERSION}/${ASSET}"
LIB_DIR="${OUT_DIR}/lib/dynamic/${PLATFORM}"
INC_DIR="${OUT_DIR}/include"
mkdir -p "$LIB_DIR" "$INC_DIR"
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

echo "Downloading $URL ..."
curl -sSfL -o "$TMP/asset" "$URL"

case "$ASSET" in
  *.tar.gz)
    tar -xzf "$TMP/asset" -C "$TMP"
    [ -f "$TMP/lbug.h" ] && cp "$TMP/lbug.h" "$INC_DIR/"
    [ -f "$TMP/liblbug.so" ] && cp "$TMP/liblbug.so" "$LIB_DIR/"
    [ -f "$TMP/liblbug.dylib" ] && cp "$TMP/liblbug.dylib" "$LIB_DIR/"
    ;;
  *.zip)
    unzip -q -o "$TMP/asset" -d "$TMP"
    [ -f "$TMP/lbug.h" ] && cp "$TMP/lbug.h" "$INC_DIR/"
    [ -f "$TMP/lbug_shared.dll" ] && cp "$TMP/lbug_shared.dll" "$LIB_DIR/"
    [ -f "$TMP/lbug_shared.lib" ] && cp "$TMP/lbug_shared.lib" "$LIB_DIR/"
    ;;
esac

echo "Done. Library in $LIB_DIR, header in $INC_DIR"
