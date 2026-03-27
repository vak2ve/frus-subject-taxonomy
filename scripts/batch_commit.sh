#!/usr/bin/env bash
#
# batch_commit.sh — Commit large numbers of changed files in batches.
#
# Git struggles with staging 300K+ files at once. This script commits
# them in configurable batches to avoid memory/performance issues.
#
# Usage:
#   ./scripts/batch_commit.sh [batch_size] [commit_message_prefix]
#
# Examples:
#   ./scripts/batch_commit.sh 5000 "Inject TEI headers"
#   ./scripts/batch_commit.sh 3000 "Reformat XML"
#
# Default batch size: 5000 files
# Default message: "Inject TEI headers into document XMLs"

set -euo pipefail

BATCH_SIZE="${1:-5000}"
MSG_PREFIX="${2:-Inject TEI headers into document XMLs}"

echo "=== Batch Git Commit ==="
echo "Batch size: $BATCH_SIZE"
echo "Message prefix: $MSG_PREFIX"
echo ""

# Get list of changed XML files in data/documents/
CHANGED_FILES=$(git status --porcelain -- 'data/documents/' | awk '{print $2}')
TOTAL=$(echo "$CHANGED_FILES" | grep -c . || true)

if [ "$TOTAL" -eq 0 ]; then
    echo "No changed files found in data/documents/"
    exit 0
fi

echo "Total changed files: $TOTAL"
echo ""

BATCH_NUM=0
COMMITTED=0

echo "$CHANGED_FILES" | while IFS= read -r batch_chunk; do
    # Accumulate files into a temp list
    echo "$batch_chunk" >> /tmp/batch_files.txt
    COMMITTED=$((COMMITTED + 1))
    
    # When we hit batch size or end of files, commit
    if [ $((COMMITTED % BATCH_SIZE)) -eq 0 ] || [ "$COMMITTED" -eq "$TOTAL" ]; then
        BATCH_NUM=$((BATCH_NUM + 1))
        echo "Committing batch $BATCH_NUM ($COMMITTED / $TOTAL files)..."
        
        # Stage the batch
        xargs git add < /tmp/batch_files.txt
        
        # Commit
        git commit -m "${MSG_PREFIX} (batch ${BATCH_NUM})"
        
        # Reset temp file
        > /tmp/batch_files.txt
    fi
done

# Handle any remaining files
if [ -s /tmp/batch_files.txt ] 2>/dev/null; then
    BATCH_NUM=$((BATCH_NUM + 1))
    echo "Committing final batch $BATCH_NUM..."
    xargs git add < /tmp/batch_files.txt
    git commit -m "${MSG_PREFIX} (batch ${BATCH_NUM})"
fi

rm -f /tmp/batch_files.txt

echo ""
echo "=== Done ==="
echo "Total files committed: $TOTAL"
echo "Batches: $BATCH_NUM"
