#!/bin/bash
# Batch commit and push remaining document directories
# Commits ~20 volumes at a time to avoid HTTP 413 errors

set -e
cd /home/user/frus-subject-taxonomy

BATCH_SIZE=20
BATCH_NUM=0

# Get all untracked volume directories
VOLS=($(git status --short | grep '^??' | grep 'data/documents/' | sed 's|?? data/documents/||;s|/.*||' | sort -u))

# Also check for the batch script
if git status --short | grep -q 'scripts/batch_split_and_annotate.py'; then
    git add scripts/batch_split_and_annotate.py
fi

TOTAL=${#VOLS[@]}
echo "Total volumes to commit: $TOTAL"

i=0
while [ $i -lt $TOTAL ]; do
    BATCH_NUM=$((BATCH_NUM + 1))
    END=$((i + BATCH_SIZE))
    if [ $END -gt $TOTAL ]; then
        END=$TOTAL
    fi

    FIRST=${VOLS[$i]}
    LAST=${VOLS[$((END - 1))]}

    echo ""
    echo "=========================================="
    echo "Batch $BATCH_NUM: volumes $((i+1))-$END of $TOTAL ($FIRST to $LAST)"
    echo "=========================================="

    # Add volumes in this batch
    for j in $(seq $i $((END - 1))); do
        git add "data/documents/${VOLS[$j]}/"
    done

    # Commit
    git commit -m "Split and annotate FRUS volumes: $FIRST through $LAST (batch $BATCH_NUM)

https://claude.ai/code/session_0122iCXx4cyVShp5KqwGDhsH"

    # Push with retry
    for attempt in 1 2 3 4; do
        if git push -u origin claude/frus-document-separation-6kAEg; then
            echo "Push succeeded"
            break
        else
            if [ $attempt -lt 4 ]; then
                WAIT=$((2 ** attempt))
                echo "Push failed, retrying in ${WAIT}s..."
                sleep $WAIT
            else
                echo "Push failed after 4 attempts, aborting"
                exit 1
            fi
        fi
    done

    i=$END
done

echo ""
echo "=========================================="
echo "All batches complete!"
echo "=========================================="
