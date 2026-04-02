#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
python -m pip install -q -r requirements.txt
python -m compileall -q .
python -m unittest discover -s tests -v
echo "OK: compileall + unittest"
