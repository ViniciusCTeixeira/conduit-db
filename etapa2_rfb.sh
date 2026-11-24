#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [ -f ".venv/bin/activate" ]; then
    source .venv/bin/activate
fi

TABLES="rfb_empresas,rfb_estabelecimentos,rfb_simples"

python conduit.py transfer \
  --source local \
  --target production \
  --tables "$TABLES" \
  --upsert \
  --chunk-size 10000 "$@"
