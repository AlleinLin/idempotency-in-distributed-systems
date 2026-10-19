#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
command -v xelatex >/dev/null || { echo "Install XeLaTeX with Chinese support." >&2; exit 1; }
mkdir -p build
needs_rerun=1
for pass in 1 2 3 4; do
    xelatex -interaction=nonstopmode -halt-on-error -file-line-error \
        -synctex=1 -output-directory=build main.tex
    needs_rerun=0
    if grep -Eq 'Rerun to get|Please rerun|Rerun LaTeX|Label\(s\) may have changed' build/main.log; then
        needs_rerun=1
    fi
    if (( pass >= 2 && needs_rerun == 0 )); then break; fi
done
if (( needs_rerun )); then echo "References did not stabilize after four passes." >&2; exit 1; fi
printf '%s\n' "$PWD/build/main.pdf"
