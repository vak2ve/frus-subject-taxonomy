#!/usr/bin/env python3
"""
Apply subject annotations to FRUS TEI documents via strict string matching.

Uses the curated vocabulary from lcsh_mapping.json (1,412 subject terms) to
find exact string matches in target documents and wrap them in <rs> elements.

Usage:
    python3 apply_annotations.py <volume-id>
    python3 apply_annotations.py frus1977-80v24
"""

import sys
import os
import re
import json
import glob
from lxml import etree

TEI_NS = 'http://www.tei-c.org/ns/1.0'

# Elements whose text content should NOT be annotated (already marked up)
SKIP_ANCESTORS = {
    f'{{{TEI_NS}}}rs',
    f'{{{TEI_NS}}}persName',
    f'{{{TEI_NS}}}orgName',
    f'{{{TEI_NS}}}placeName',
    f'{{{TEI_NS}}}gloss',
}

# Minimum term length to avoid matching very short common words
MIN_TERM_LENGTH = 3

# Minimum annotation count in source corpus — filters out rare/contextual terms
MIN_SOURCE_COUNT = 5


def build_vocabulary(tei_dir):
    """Build vocabulary from lcsh_mapping.json — the curated subject list."""
    mapping_path = os.path.join(tei_dir, 'lcsh_mapping.json')
    print(f"Loading vocabulary from {os.path.basename(mapping_path)}...")

    with open(mapping_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Build: term_text -> (corresp_id, type)
    vocab = {}
    skipped_short = 0
    skipped_rare = 0
    for corresp_id, entry in data.items():
        name = entry.get('name', '').strip()
        rs_type = entry.get('type', 'topic')
        count = int(entry.get('count', 0))
        if not name or len(name) < MIN_TERM_LENGTH:
            skipped_short += 1
            continue
        if count < MIN_SOURCE_COUNT:
            skipped_rare += 1
            continue
        # Use the canonical name from lcsh_mapping
        vocab[name] = (corresp_id, rs_type)

    # Sort by length descending for longest-match-first
    sorted_terms = sorted(vocab.keys(), key=len, reverse=True)

    print(f"  Loaded {len(vocab)} terms (skipped {skipped_short} short, {skipped_rare} rare [count < {MIN_SOURCE_COUNT}])")
    print(f"  Longest: '{sorted_terms[0][:60]}...' ({len(sorted_terms[0])} chars)")
    print(f"  Shortest: '{sorted_terms[-1]}' ({len(sorted_terms[-1])} chars)")

    return vocab, sorted_terms


def is_source_note(el):
    """Check if an element is a <note type='source'> (archival citation)."""
    return (el.tag == f'{{{TEI_NS}}}note' and el.get('type') == 'source')


def should_skip_element(el):
    """Check if an element or any of its ancestors should not be annotated."""
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


def annotate_text_node(text, vocab, sorted_terms):
    """
    Find all non-overlapping exact matches in a text string (longest first).

    Returns list of tuples:
      ('text', string)                  - plain text segment
      ('rs', string, corresp, type)     - matched segment
    """
    if not text or not text.strip():
        return [('text', text)]

    matches = []
    used = set()

    for term in sorted_terms:
        if len(term) > len(text):
            continue
        start = 0
        while True:
            idx = text.find(term, start)
            if idx == -1:
                break
            end = idx + len(term)
            # Check no overlap with existing matches
            if not any(i in used for i in range(idx, end)):
                # Word boundary check
                before_ok = (idx == 0 or not text[idx - 1].isalnum())
                after_ok = (end == len(text) or not text[end].isalnum())
                if before_ok and after_ok:
                    matches.append((idx, end, term))
                    used.update(range(idx, end))
            start = idx + 1

    if not matches:
        return [('text', text)]

    matches.sort(key=lambda x: x[0])

    segments = []
    pos = 0
    for start, end, term in matches:
        if start > pos:
            segments.append(('text', text[pos:start]))
        corresp, rs_type = vocab[term]
        segments.append(('rs', term, corresp, rs_type))
        pos = end
    if pos < len(text):
        segments.append(('text', text[pos:]))

    return segments


def process_element(el, vocab, sorted_terms, stats):
    """
    Process an element's text and tail, inserting <rs> annotations.
    Recurses into children. Skips elements already inside annotation markup.
    """
    if should_skip_element(el):
        return

    # Process the element's direct text content
    if el.text:
        segments = annotate_text_node(el.text, vocab, sorted_terms)
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
        process_element(child, vocab, sorted_terms, stats)

        # Process the child's tail text
        if child.tail and not should_skip_element(el):
            segments = annotate_text_node(child.tail, vocab, sorted_terms)
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


def process_document(doc_path, vocab, sorted_terms):
    """Process a single TEI document file."""
    parser = etree.XMLParser(remove_blank_text=False, strip_cdata=False, recover=True)
    tree = etree.parse(doc_path, parser)
    root = tree.getroot()

    existing_rs = len(root.findall(f'.//{{{TEI_NS}}}rs'))

    body = root.find(f'.//{{{TEI_NS}}}body')
    if body is None:
        return 0, existing_rs

    stats = {'new': 0}
    process_element(body, vocab, sorted_terms, stats)

    # Write back preserving XML declaration
    tree.write(doc_path, xml_declaration=True, encoding='UTF-8', pretty_print=False)

    return stats['new'], existing_rs


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 apply_annotations.py <volume-id>")
        print("Example: python3 apply_annotations.py frus1977-80v24")
        sys.exit(1)

    volume_id = sys.argv[1]

    script_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(script_dir)
    tei_dir = os.path.join(repo_root, 'config')
    docs_dir = os.path.join(repo_root, 'data', 'documents', volume_id)

    if not os.path.isdir(docs_dir):
        print(f"Error: Document directory not found: {docs_dir}")
        sys.exit(1)

    vocab, sorted_terms = build_vocabulary(tei_dir)

    doc_files = sorted(glob.glob(os.path.join(docs_dir, '*.xml')))
    print(f"\nProcessing {len(doc_files)} documents in {volume_id}...")
    print(f"{'=' * 70}")

    total_new = 0
    total_existing = 0
    docs_with_new = 0

    for doc_path in doc_files:
        fname = os.path.basename(doc_path)
        try:
            new_count, existing_count = process_document(doc_path, vocab, sorted_terms)
            total_new += new_count
            total_existing += existing_count
            if new_count > 0:
                docs_with_new += 1
                print(f"  {fname}: +{new_count} new (had {existing_count} existing)")
        except Exception as e:
            print(f"  {fname}: ERROR - {e}")

    print(f"\n{'=' * 70}")
    print(f"Summary for {volume_id}:")
    print(f"  Documents processed: {len(doc_files)}")
    print(f"  Documents with new annotations: {docs_with_new}")
    print(f"  Existing <rs> annotations preserved: {total_existing}")
    print(f"  New <rs> annotations added: {total_new}")
    print(f"  Total <rs> annotations now: {total_existing + total_new}")


if __name__ == '__main__':
    main()
