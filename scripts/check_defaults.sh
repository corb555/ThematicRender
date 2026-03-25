#!/bin/sh
set -eu

ROOT="${1:-.}"

MATCHES="$(grep -RInE '\.get\("([A-Za-z0-9_]+)",' "$ROOT" \
  --include='*.py' \
  --exclude-dir='.git' \
  --exclude-dir='__pycache__' \
  --exclude-dir='.mypy_cache' \
  --exclude-dir='.pytest_cache' || true)"

if [ -n "$MATCHES" ]; then
  echo "❌ Suspicious .get(key, default) usage found:"
  echo
  echo "$MATCHES"
  exit 1
fi

echo "✅ No suspicious .get(key, default) usage found."