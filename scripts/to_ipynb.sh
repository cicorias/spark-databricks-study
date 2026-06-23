#!/usr/bin/env bash
# Convert all percent-format `# %%` files under notebooks/local/ to .ipynb
# Usage: ./scripts/to_ipynb.sh
set -euo pipefail
cd "$(dirname "$0")/.."
uv run jupytext --to notebook notebooks/local/*.py
echo "Done. Open the .ipynb files in VS Code or 'uv run jupyter lab'."
