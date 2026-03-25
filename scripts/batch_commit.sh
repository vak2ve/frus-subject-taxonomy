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
#
# Compatible with macOS (bash 3.x) and Linux.

set -uo pipefail

BATCH_SIZE="${1:-3000}"
COMMIT_PREFIX="${2:-Batch commit}"

cd "$(git rev-parse --show-toplevel)"

echo "=== Batch Commit Script ==="
echo "Batch size: $BATCH_SIZE"
echo ""

# First, make sure nothing is staged (clean slate)
git reset HEAD --quiet 2>/dev/null || true

# Dump full list of uncommitted files to a temp file (one snapshot)
TMPFILE=$(mktemp)
trap 'rm -f "$TMPFILE"' EXIT

git status --porcelain -u | awk '{print substr($0, 4)}' > "$TMPFILE"
TOTAL=$(wc -l < "$TMPFILE" | tr -d ' ')

if [ "$TOTAL" -eq 0 ]; then
    echo "No uncommitted files found. Nothing to do."
    exit 0
fi

echo "Total files to commit: $TOTAL"
echo "Estimated commits: $(( (TOTAL + BATCH_SIZE - 1) / BATCH_SIZE ))"
echo ""

BATCH_NUM=0
OFFSET=1

while [ "$OFFSET" -le "$TOTAL" ]; do
    BATCH_NUM=$((BATCH_NUM + 1))
    REMAINING=$((TOTAL - OFFSET + 1))
    COUNT=$BATCH_SIZE
    if [ "$REMAINING" -lt "$COUNT" ]; then
        COUNT=$REMAINING
    fi

    echo "--- Batch $BATCH_NUM: staging $COUNT files ($REMAINING remaining) ---"

    # Extract this batch and stage only these files
    sed -n "${OFFSET},$((OFFSET + BATCH_SIZE - 1))p" "$TMPFILE" | tr '\n' '\0' | xargs -0 git add --

    # Verify we only have the batch staged
    STAGED=$(git diff --cached --name-only | wc -l | tr -d ' ')
    echo "    Staged: $STAGED files"

    # Commit only what's staged
    git commit -m "${COMMIT_PREFIX} ${BATCH_NUM}: ${COUNT} files"

    echo "    Committed batch $BATCH_NUM"
    echo ""

    OFFSET=$((OFFSET + BATCH_SIZE))
done

echo "=== Done: $BATCH_NUM batches committed ==="
