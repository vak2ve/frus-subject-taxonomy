#!/usr/bin/env python3
"""
Apply curated subject annotations to FRUS TEI documents.

Unlike apply_annotations.py (which applies ALL matches from the full
vocabulary), this script uses the reviewed string_match_results and
only applies matches that were accepted (not rejected) during review.

Reads:
  - string_match_results_<volume>.json  (all matches from annotate_documents.py)
  - annotation_rejections_<volume>.json (exported from string-match-review.html)
  - variant_groups.json                 (for canonical ref mapping)

If no rejections file exists, falls back to accepting all matches
from the string_match_results.

Usage:
    python3 apply_curated_annotations.py <volume-id>
    python3 apply_curated_annotations.py frus1969-76v19p2
"""

import sys
import os
import re
import json
import glob
from collections import defaultdict
from lxml import etree

os.chdir(os.path.dirname(os.path.abspath(__file__)))

TEI_NS = 'http://www.tei-c.org/ns/1.0'

# Elements whose text content should NOT be annotated (already marked up)
SKIP_ANCESTORS = {
    f'{{{TEI_NS}}}rs',
    f'{{{TEI_NS}}}persName',
    f'{{{TEI_NS}}}orgName',
    f'{{{TEI_NS}}}placeName',
    f'{{{TEI_NS}}}gloss',
}

VARIANT_GROUPS_FILE = "../variant_groups.json"


# ── Load review data ─────────────────────────────────────────────────

def load_string_match_results(volume_id):
    """Load string_match_results_<volume>.json."""
    path = f"../data/documents/{volume_id}/string_match_results_{volume_id}.json"
    if not os.path.exists(path):
        print(f"ERROR: String match results not found: {path}")
        print("Run annotate_documents.py first.")
        sys.exit(1)

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    total = data.get("metadata", {}).get("total_matches", 0)
    docs = len(data.get("by_document", {}))
    print(f"  Loaded {path}: {total} matches across {docs} documents")
    return data


def load_rejections(volume_id):
    """Load annotation_rejections_<volume>.json.

    Returns set of rejection keys (docId:ref:position), or empty set.
    """
    path = f"../config/annotation_rejections_{volume_id}.json"
    if not os.path.exists(path):
        print(f"  No rejections file found ({path}) — accepting all matches")
        return set()

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    rejections = set()
    for r in data.get("rejections", []):
        key = r.get("key", "")
        if key:
            rejections.add(key)

    print(f"  Loaded {path}: {len(rejections)} rejections")
    return rejections


def load_variant_groups():
    """Load variant_groups.json for ref-to-canonical mapping."""
    if not os.path.exists(VARIANT_GROUPS_FILE):
        return {}

    with open(VARIANT_GROUPS_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    # The file has a pre-built ref_to_canonical dict
    ref_to_canonical = data.get("ref_to_canonical", {})
    print(f"  Loaded {VARIANT_GROUPS_FILE}: {len(ref_to_canonical)} ref mappings")
    return ref_to_canonical


# ── Build per-document vocabulary with budgets ───────────────────────

def build_document_budgets(results_data, rejections):
    """Build per-document accepted match budgets.

    For each document, computes the allowed occurrence count per
    (matched_text_lower, canonical_ref) pair:
        allowed = total_matches - rejected_matches

    Returns: {doc_id: {(term_lower, ref): {allowed: N, type: str}}}
    """
    by_doc = results_data.get("by_document", {})
    budgets = {}

    total_accepted = 0
    total_rejected = 0

    for doc_id, doc_data in by_doc.items():
        doc_budget = defaultdict(lambda: {"allowed": 0, "type": "topic"})

        for match in doc_data.get("matches", []):
            ref = match.get("canonical_ref", match.get("ref", ""))
            term = match.get("matched_text", match.get("term", ""))
            position = match.get("position", 0)
            rs_type = match.get("type", "topic")

            # Check if this specific match was rejected
            key = f"{doc_id}:{ref}:{position}"
            if key in rejections:
                total_rejected += 1
                continue

            # Add to budget
            budget_key = (term.lower(), ref)
            doc_budget[budget_key]["allowed"] += 1
            doc_budget[budget_key]["type"] = rs_type
            total_accepted += 1

        if doc_budget:
            budgets[doc_id] = dict(doc_budget)

    print(f"  Budget: {total_accepted} accepted, {total_rejected} rejected")
    return budgets


# ── XML annotation (adapted from apply_annotations.py) ──────────────

def is_source_note(el):
    """Check if an element is a <note type='source'> (archival citation)."""
    return (el.tag == f'{{{TEI_NS}}}note' and el.get('type') == 'source')


def should_skip_element(el):
    """Check if an element or any ancestor should not be annotated."""
    if el.tag in SKIP_ANCESTORS:
        return True
    if is_source_note(el):
        return True
    parent = el.getparent()
    while parent is not None:
        if parent.tag in SKIP_ANCESTORS:
            return True
        if is_source_note(parent):
            return True
        parent = parent.getparent()
    return False


def annotate_text_node(text, vocab, sorted_terms, budget):
    """Find non-overlapping matches in text, respecting occurrence budgets.

    Args:
        text: the text string to search
        vocab: {term_text: (ref, type)} — only accepted terms for this document
        sorted_terms: terms sorted longest-first
        budget: {(term_lower, ref): {"allowed": N}} — mutable, decremented on match

    Returns: list of ('text', string) or ('rs', string, ref, type) tuples
    """
    if not text or not text.strip():
        return [('text', text)]

    matches = []
    used = set()

    for term in sorted_terms:
        if len(term) > len(text):
            continue

        ref, rs_type = vocab[term]
        budget_key = (term.lower(), ref)

        # Check remaining budget
        if budget.get(budget_key, {}).get("allowed", 0) <= 0:
            continue

        start = 0
        while True:
            # Case-insensitive search
            idx = text.lower().find(term.lower(), start)
            if idx == -1:
                break
            end = idx + len(term)

            # Check no overlap with existing matches
            if not any(i in used for i in range(idx, end)):
                # Word boundary check
                before_ok = (idx == 0 or not text[idx - 1].isalnum())
                after_ok = (end == len(text) or not text[end].isalnum())
                if before_ok and after_ok:
                    # Check budget
                    if budget.get(budget_key, {}).get("allowed", 0) > 0:
                        matches.append((idx, end, term))
                        used.update(range(idx, end))
                        budget[budget_key]["allowed"] -= 1
            start = idx + 1

    if not matches:
        return [('text', text)]

    matches.sort(key=lambda x: x[0])

    segments = []
    pos = 0
    for start, end, term in matches:
        if start > pos:
            segments.append(('text', text[pos:start]))
        ref, rs_type = vocab[term]
        segments.append(('rs', text[start:end], ref, rs_type))
        pos = end
    if pos < len(text):
        segments.append(('text', text[pos:]))

    return segments


def process_element(el, vocab, sorted_terms, budget, stats):
    """Process an element's text and tail, inserting <rs> annotations."""
    if should_skip_element(el):
        return

    # Process the element's direct text content
    if el.text:
        segments = annotate_text_node(el.text, vocab, sorted_terms, budget)
        if len(segments) > 1 or (len(segments) == 1 and segments[0][0] == 'rs'):
            el.text = None
            insert_idx = 0
            for seg in segments:
                if seg[0] == 'text':
                    if insert_idx == 0:
                        el.text = seg[1]
                    else:
                        prev = el[insert_idx - 1]
                        prev.tail = (prev.tail or '') + seg[1]
                elif seg[0] == 'rs':
                    rs_el = etree.Element(f'{{{TEI_NS}}}rs')
                    rs_el.set('corresp', seg[2])
                    rs_el.set('type', seg[3])
                    rs_el.text = seg[1]
                    rs_el.tail = ''
                    el.insert(insert_idx, rs_el)
                    insert_idx += 1
                    stats['new'] += 1

    # Process children (snapshot the list since we may insert siblings)
    children = list(el)
    for child in children:
        process_element(child, vocab, sorted_terms, budget, stats)

        # Process the child's tail text
        if child.tail and not should_skip_element(el):
            segments = annotate_text_node(child.tail, vocab, sorted_terms, budget)
            if len(segments) > 1 or (len(segments) == 1 and segments[0][0] == 'rs'):
                child.tail = None
                child_idx = list(el).index(child)
                insert_after = child_idx
                first = True
                for seg in segments:
                    if seg[0] == 'text':
                        if first:
                            child.tail = seg[1]
                            first = False
                        else:
                            prev = el[insert_after]
                            prev.tail = (prev.tail or '') + seg[1]
                    elif seg[0] == 'rs':
                        if first:
                            child.tail = ''
                            first = False
                        rs_el = etree.Element(f'{{{TEI_NS}}}rs')
                        rs_el.set('corresp', seg[2])
                        rs_el.set('type', seg[3])
                        rs_el.text = seg[1]
                        rs_el.tail = ''
                        insert_after += 1
                        el.insert(insert_after, rs_el)
                        stats['new'] += 1


def remove_rejected_rs(root, rejected_refs):
    """Remove existing <rs> elements whose corresp is in the rejected set.

    Unwraps each rejected <rs>: its text and children are preserved in-place,
    only the <rs> wrapper is removed.

    Returns: count of removed <rs> elements.
    """
    if not rejected_refs:
        return 0

    removed = 0
    # Iterate over a snapshot since we'll be mutating the tree
    for rs in list(root.iter(f'{{{TEI_NS}}}rs')):
        corresp = rs.get('corresp', '')
        if corresp not in rejected_refs:
            continue

        parent = rs.getparent()
        if parent is None:
            continue

        idx = list(parent).index(rs)

        # Collect the text content that needs to be re-attached
        # rs.text goes before any children of rs
        rs_text = rs.text or ''
        rs_tail = rs.tail or ''

        if idx == 0:
            # rs is first child: prepend rs.text to parent.text
            parent.text = (parent.text or '') + rs_text
        else:
            # Prepend rs.text to the tail of the preceding sibling
            prev = parent[idx - 1]
            prev.tail = (prev.tail or '') + rs_text

        # Move rs's children into parent at the same position
        rs_children = list(rs)
        for i, child in enumerate(rs_children):
            parent.insert(idx + i, child)

        # The last element before the tail position gets the rs.tail appended
        if rs_children:
            last_child = rs_children[-1]
            last_child.tail = (last_child.tail or '') + rs_tail
        elif idx == 0:
            parent.text = (parent.text or '') + rs_tail
        else:
            prev = parent[idx - 1]
            prev.tail = (prev.tail or '') + rs_tail

        parent.remove(rs)
        removed += 1

    return removed


def process_document(doc_path, vocab, sorted_terms, budget, rejected_refs=None):
    """Process a single TEI document file."""
    parser = etree.XMLParser(remove_blank_text=False, strip_cdata=False, recover=True)
    tree = etree.parse(doc_path, parser)
    root = tree.getroot()

    # Remove existing <rs> elements that were rejected in review
    removed = remove_rejected_rs(root, rejected_refs or set())

    existing_rs = len(root.findall(f'.//{{{TEI_NS}}}rs'))

    body = root.find(f'.//{{{TEI_NS}}}body')
    if body is None:
        return 0, existing_rs, removed

    stats = {'new': 0}
    process_element(body, vocab, sorted_terms, budget, stats)

    # Write back preserving XML declaration
    tree.write(doc_path, xml_declaration=True, encoding='UTF-8', pretty_print=False)

    return stats['new'], existing_rs, removed


# ── Main ─────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 apply_curated_annotations.py <volume-id>")
        print("Example: python3 apply_curated_annotations.py frus1969-76v19p2")
        sys.exit(1)

    volume_id = sys.argv[1]

    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    docs_dir = os.path.join(repo_root, 'data', 'documents', volume_id)

    # Fall back to parent directory structure
    if not os.path.isdir(docs_dir):
        docs_dir = os.path.join(
            repo_root,
            'data', 'documents', volume_id
        )

    if not os.path.isdir(docs_dir):
        print(f"Error: Document directory not found: {docs_dir}")
        sys.exit(1)

    print(f"Volume: {volume_id}")
    print(f"Documents: {docs_dir}\n")

    # Load data
    print("Loading review data...")
    results_data = load_string_match_results(volume_id)
    rejections = load_rejections(volume_id)
    ref_to_canonical = load_variant_groups()

    # Build per-document budgets
    print("\nBuilding per-document annotation budgets...")
    budgets = build_document_budgets(results_data, rejections)
    print(f"  Documents with accepted matches: {len(budgets)}")

    # Build per-document rejected ref sets from rejection keys (docId:ref:position)
    doc_rejected_refs = defaultdict(set)
    for key in rejections:
        parts = key.split(':')
        if len(parts) >= 2:
            doc_id_r, ref_r = parts[0], parts[1]
            doc_rejected_refs[doc_id_r].add(ref_r)
    if doc_rejected_refs:
        print(f"  Documents with rejected annotations to remove: {len(doc_rejected_refs)}")

    # Process documents
    doc_files = sorted(glob.glob(os.path.join(docs_dir, '*.xml')))
    print(f"\nProcessing {len(doc_files)} documents...")
    print(f"{'=' * 70}")

    total_new = 0
    total_existing = 0
    total_removed = 0
    docs_with_new = 0

    for doc_path in doc_files:
        fname = os.path.basename(doc_path)
        doc_id = fname.replace('.xml', '')

        # Get this document's budget and rejected refs
        doc_budget = budgets.get(doc_id, {})
        rejected_refs = doc_rejected_refs.get(doc_id, set())

        # Skip if nothing to do for this document
        if not doc_budget and not rejected_refs:
            continue

        # Build vocabulary for this document from its budget
        vocab = {}
        for (term_lower, ref), info in doc_budget.items():
            if info["allowed"] > 0:
                # Map ref through variant groups to get current canonical
                canonical = ref_to_canonical.get(ref, ref)
                rs_type = info.get("type", "topic")
                vocab[term_lower] = (canonical, rs_type)

        sorted_terms = sorted(vocab.keys(), key=len, reverse=True) if vocab else []

        try:
            new_count, existing_count, removed_count = process_document(
                doc_path, vocab, sorted_terms, doc_budget, rejected_refs
            )
            total_new += new_count
            total_existing += existing_count
            total_removed += removed_count
            if new_count > 0 or removed_count > 0:
                docs_with_new += 1
                parts = []
                if new_count > 0:
                    parts.append(f"+{new_count} new")
                if removed_count > 0:
                    parts.append(f"-{removed_count} removed")
                print(f"  {fname}: {', '.join(parts)} (had {existing_count + removed_count} existing)")
        except Exception as e:
            print(f"  {fname}: ERROR - {e}")

    print(f"\n{'=' * 70}")
    print(f"Summary for {volume_id}:")
    print(f"  Documents processed: {len(doc_files)}")
    print(f"  Documents with changes: {docs_with_new}")
    print(f"  Rejected <rs> annotations removed: {total_removed}")
    print(f"  Existing <rs> annotations preserved: {total_existing}")
    print(f"  New <rs> annotations added: {total_new}")
    print(f"  Total <rs> annotations now: {total_existing + total_new}")

    if rejections:
        original_total = results_data.get("metadata", {}).get("total_matches", 0)
        print(f"\n  Review impact:")
        print(f"    Original matches: {original_total}")
        print(f"    Rejected: {len(rejections)}")
        print(f"    Applied: {total_new}")


if __name__ == '__main__':
    main()
