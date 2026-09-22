#!/usr/bin/env bash
#
# test.sh - manual unit test script for the concurrent SendEntry refactor.
#
# Runs the focused unit tests for the integration dispatcher (concurrency,
# per-provider timeout, failure aggregation, JSON response shape) plus the
# touched handler packages, and optionally the whole repository.
#
# Usage:
#   ./test.sh            # fast, focused checks (default)
#   ./test.sh full       # full repository test suite (make test equivalent)
#   ./test.sh verbose    # focused checks with -v output
#
# Requirements: Go toolchain available in PATH.

set -euo pipefail

cd "$(dirname "$0")"

# The sandbox/CI may not allow the default build cache location.
export GOCACHE="${GOCACHE:-/tmp/miniflux-gocache}"
mkdir -p "$GOCACHE"

MODE="${1:-focused}"

# Packages touched by this refactor.
FOCUSED_PKGS=(
	"./internal/integration/"
	"./internal/api/"
	"./internal/ui/"
	"./internal/fever/"
	"./internal/googlereader/"
)

echo "==> Go version"
go version

echo "==> gofmt check (new files; pre-existing files use CRLF and are skipped)"
FMT_OUTPUT="$(gofmt -l \
	internal/integration/send_entry_test.go \
	internal/integration/sendentry_http_test.go)"
if [ -n "$FMT_OUTPUT" ]; then
	echo "gofmt reports differences in:"
	echo "$FMT_OUTPUT"
	echo "Run: gofmt -w $FMT_OUTPUT"
	exit 1
fi
echo "OK"

echo "==> go vet"
go vet ./...
echo "OK"

echo "==> go build"
go build ./...
echo "OK"

case "$MODE" in
full)
	echo "==> Full unit test suite (race detector, count=1)"
	go test -race -count=1 ./...
	;;
verbose)
	echo "==> Focused unit tests (verbose, race detector)"
	go test -race -count=1 -v "${FOCUSED_PKGS[@]}" -run 'TestSendEntry|TestRunSendTasks|TestSendResults'
	;;
focused|*)
	echo "==> Focused unit tests (race detector)"
	go test -race -count=1 "${FOCUSED_PKGS[@]}" -run 'TestSendEntry|TestRunSendTasks|TestSendResults'
	echo "OK"
	echo ""
	echo "Focused checks passed. Run './test.sh full' for the entire repository suite."
	;;
esac
