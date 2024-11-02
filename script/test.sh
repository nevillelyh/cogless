#!/bin/bash

# Lint and format

set -euo pipefail

base_dir="$(git rev-parse --show-toplevel)"

cd "$base_dir"
go test ./...

cd "$base_dir/python"
.venv/bin/pytest
