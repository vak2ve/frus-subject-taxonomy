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
# The script stages and commits files BATCH_SIZE at a time.
# It handles both untracked and modified files.

set -euo pipefail

BATCH_SIZE="${1:-3000}"
COMMIT_PREFIX="${2:-Batch commit}"

# Move to repo root
cd "$(git rev-parse --show-toplevel)"

echo "=== Batch Commit Script ==="
echo "Batch size: $BATCH_SIZE"
echo ""

# Collect all uncommitted files (untracked + modified/deleted)
get_uncommitted_files() {
    git status --porcelain | awk '{print substr($0, 4)}'
}

TOTAL=$(get_uncommitted_files | wc -l)

if [ "$TOTAL" -eq 0 ]; then
    echo "No uncommitted files found. Nothing to do."
    exit 0
fi

echo "Total files to commit: $TOTAL"
echo "Estimated commits: $(( (TOTAL + BATCH_SIZE - 1) / BATCH_SIZE ))"
echo ""

BATCH_NUM=0

while true; do
    # Get current uncommitted files (recalculate each iteration)
    mapfile -t FILES < <(get_uncommitted_files | head -n "$BATCH_SIZE")

    if [ "${#FILES[@]}" -eq 0 ]; then
        break
    fi

    BATCH_NUM=$((BATCH_NUM + 1))
    COUNT="${#FILES[@]}"
    REMAINING=$(get_uncommitted_files | wc -l)

    echo "--- Batch $BATCH_NUM: staging $COUNT files ($REMAINING remaining) ---"

    # Stage files in this batch
    printf '%s\n' "${FILES[@]}" | xargs -d '\n' git add --

    # Commit
    git commit -m "${COMMIT_PREFIX} ${BATCH_NUM}: ${COUNT} files"

    echo "    Committed batch $BATCH_NUM ($COUNT files)"
    echo ""
done

echo "=== Done: $BATCH_NUM batches committed ==="
