#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLONE_DIR="$SCRIPT_DIR/../fixtures/containerd"
if [ ! -d "$CLONE_DIR/.git" ]; then
  echo "Cloning containerd corpus (depth 1)..."
  git clone --depth 1 https://github.com/ankushChatterjee/containerd.git "$CLONE_DIR"
else
  echo "Corpus already present at $CLONE_DIR"
fi
