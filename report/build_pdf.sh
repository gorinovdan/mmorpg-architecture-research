#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
tex_file="report.tex"
base_name="${tex_file%.tex}"

cd "$script_dir"

if ! command -v xelatex >/dev/null 2>&1; then
  echo "error: xelatex is not installed or not in PATH" >&2
  exit 1
fi

if ! command -v biber >/dev/null 2>&1; then
  echo "error: biber is not installed or not in PATH" >&2
  exit 1
fi

xelatex -interaction=nonstopmode -halt-on-error "$tex_file"
biber "$base_name"
xelatex -interaction=nonstopmode -halt-on-error "$tex_file"
xelatex -interaction=nonstopmode -halt-on-error "$tex_file"

echo "ok: built ${base_name}.pdf"
