#!/bin/sh
# Runs static analysis and the full unit test suite.
set -e

cd "$(dirname "$0")"

echo "==> go vet ./..."
go vet ./...

echo "==> go test ./..."
go test ./...

echo "==> All checks passed."
