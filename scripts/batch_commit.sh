#!/usr/bin/env bash
#
# batch_commit.sh — Commit untracked/modified files in batches of N
#
# Usage:
#   ./scripts/batch_commit.sh [BATCH_SIZE] [COMMIT_PREFIX]
#
# Defaults:
#   BATCH_SIZE=3000
#   COMMIT_PREFIX="Batch commit"

set -uo pipefail

BATCH_SIZE="${1:-3000}"
COMMIT_PREFIX="${2:-Batch commit}"

cd "$(git rev-parse --show-toplevel)"

echo "=== Batch Commit Script ==="
echo "Batch size: $BATCH_SIZE"
echo ""

# Dump all uncommitted files to a temp file to avoid repeated git status calls
TMPFILE=$(mktemp)
trap 'rm -f "$TMPFILE"' EXIT

git status --porcelain | awk '{print substr($0, 4)}' > "$TMPFILE"
TOTAL=$(wc -l < "$TMPFILE" | tr -d ' ')

if [ "$TOTAL" -eq 0 ]; then
    echo "No uncommitted files found. Nothing to do."
    exit 0
fi

echo "Total files to commit: $TOTAL"
echo "Estimated commits: $(( (TOTAL + BATCH_SIZE - 1) / BATCH_SIZE ))"
echo ""

BATCH_NUM=0
OFFSET=0

while [ "$OFFSET" -lt "$TOTAL" ]; do
    BATCH_NUM=$((BATCH_NUM + 1))

    # Extract batch from the file list
    BATCH_END=$((OFFSET + BATCH_SIZE))
    LINES_TO_SKIP=$((OFFSET))

    BATCH_FILE=$(mktemp)
    tail -n +$((OFFSET + 1)) "$TMPFILE" | head -n "$BATCH_SIZE" > "$BATCH_FILE"
    COUNT=$(wc -l < "$BATCH_FILE" | tr -d ' ')

    if [ "$COUNT" -eq 0 ]; then
        rm -f "$BATCH_FILE"
        break
    fi

    REMAINING=$((TOTAL - OFFSET))
    echo "--- Batch $BATCH_NUM: staging $COUNT files ($REMAINING remaining) ---"

    # Stage files
    tr '\n' '\0' < "$BATCH_FILE" | xargs -0 git add -- 2>/dev/null || true
    rm -f "$BATCH_FILE"

    # Commit
    git commit -m "${COMMIT_PREFIX} ${BATCH_NUM}: ${COUNT} files"

    echo "    Committed batch $BATCH_NUM ($COUNT files)"
    echo ""

    OFFSET=$((OFFSET + BATCH_SIZE))
done

echo "=== Done: $BATCH_NUM batches committed ==="
