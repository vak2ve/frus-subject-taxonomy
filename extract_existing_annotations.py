#!/usr/bin/env python3
"""
Extract pre-existing <rs> annotations from TEI XML documents.

Reads <rs corresp="recXXX" type="..."> tags from TEI documents and outputs
a JSON file in the same format as string_match_results_*.json so it can be
used with the string-match-review tool.

Usage:
    python3 extract_existing_annotations.py <volume_id>
    python3 extract_existing_annotations.py --all
    python3 extract_existing_annotations.py --all --skip frus1977-80v15 frus1977-80v22 frus1981-88v06
"""

import argparse
import glob
import json
import os
import re
import sys
from datetime import datetime
from lxml import etree

os.chdir(os.path.dirname(os.path.abspath(__file__)))

TEI_NS = "http://www.tei-c.org/ns/1.0"
DOC_DIR = os.path.join("..", "data", "documents")
TAXONOMY_PATH = "subject-taxonomy-lcsh.xml"
LCSH_MAPPING_PATH = "lcsh_mapping.json"


# ── Taxonomy loading ─────────────────────────────────────────

def load_taxonomy(path):
    """Load taxonomy into a dict keyed by ref."""
    tree = etree.parse(path)
    root = tree.getroot()
    terms = {}

    for cat_elem in root.findall("category"):
        cat_label = cat_elem.get("label", "Uncategorized")
        for sub_elem in cat_elem.findall("subcategory"):
            sub_label = sub_elem.get("label", "General")
            for subj in sub_elem.findall("subject"):
                name_el = subj.find("name")
                if name_el is None or not name_el.text:
                    continue
                ref = subj.get("ref", "")
                if not ref:
                    continue
                terms[ref] = {
                    "term": name_el.text.strip(),
                    "type": subj.get("type", "topic"),
                    "count": int(subj.get("count", "0")),
                    "category": cat_label,
                    "subcategory": sub_label,
                    "lcsh_uri": subj.get("lcsh-uri", ""),
                    "lcsh_match": subj.get("lcsh-match", ""),
                }

    return terms


def load_lcsh_mapping(path):
    """Load LCSH mapping to supplement taxonomy terms."""
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        data = json.load(f)
    result = {}
    for ref, entry in data.items():
        uri = entry.get("lcsh_uri")
        if uri:
            result[ref] = {
                "lcsh_uri": uri,
                "lcsh_label": entry.get("lcsh_label", ""),
                "match_quality": entry.get("match_quality", ""),
            }
    return result


# ── TEI document parsing ─────────────────────────────────────

def get_text_content(elem):
    """Get all text content from an element and its descendants."""
    return "".join(elem.itertext()).strip()


def extract_doc_metadata(doc_path):
    """Extract title and date from TEI header."""
    parser = etree.XMLParser(recover=True)
    tree = etree.parse(doc_path, parser)
    root = tree.getroot()

    title_el = root.find(f".//{{{TEI_NS}}}titleStmt/{{{TEI_NS}}}title")
    title = title_el.text.strip() if title_el is not None and title_el.text else ""

    date_el = root.find(f".//{{{TEI_NS}}}settingDesc//{{{TEI_NS}}}date")
    date_text = date_el.text.strip() if date_el is not None and date_el.text else ""

    subtype_el = root.find(f".//{{{TEI_NS}}}bibl[@type='frus-div-subtype']")
    doc_type = subtype_el.text.strip() if subtype_el is not None and subtype_el.text else ""

    return {"title": title, "date": date_text, "type": doc_type}


def extract_body_text(doc_path):
    """Extract full plain text from document body for context extraction."""
    parser = etree.XMLParser(recover=True)
    tree = etree.parse(doc_path, parser)
    root = tree.getroot()
    body = root.find(f".//{{{TEI_NS}}}text/{{{TEI_NS}}}body")
    if body is None:
        return ""
    return " ".join("".join(body.itertext()).split())


def extract_context(full_text, matched_text, max_chars=300):
    """Extract surrounding context for a matched term from the full text."""
    if not full_text or not matched_text:
        return matched_text or ""

    # Find the match in the full text (case-insensitive)
    idx = full_text.lower().find(matched_text.lower())
    if idx == -1:
        # Try with normalized whitespace
        normalized_match = " ".join(matched_text.split())
        idx = full_text.lower().find(normalized_match.lower())
        if idx == -1:
            # Return a truncated window around the middle of the text
            return matched_text

    match_end = idx + len(matched_text)

    # Find sentence boundaries
    sent_start = 0
    for i in range(idx - 1, -1, -1):
        if full_text[i] in ".!?" and i + 2 < len(full_text) and full_text[i + 1] == " " and full_text[i + 2].isupper():
            if i > 0 and full_text[i - 1].isupper() and (i < 2 or not full_text[i - 2].isalpha()):
                continue
            if i > 0 and full_text[i - 1].isdigit():
                continue
            sent_start = i + 2
            break

    sent_end = len(full_text)
    for i in range(match_end, len(full_text)):
        if full_text[i] in ".!?" and i + 2 < len(full_text) and full_text[i + 1] == " " and full_text[i + 2].isupper():
            if i > 0 and full_text[i - 1].isupper() and (i < 2 or not full_text[i - 2].isalpha()):
                continue
            if i > 0 and full_text[i - 1].isdigit():
                continue
            sent_end = i + 1
            break

    sentence = full_text[sent_start:sent_end].strip()

    if len(sentence) > max_chars:
        mid = idx - sent_start + len(matched_text) // 2
        half = max_chars // 2
        trunc_start = max(0, mid - half)
        trunc_end = min(len(sentence), mid + half)
        sentence = ("..." if trunc_start > 0 else "") + sentence[trunc_start:trunc_end] + ("..." if trunc_end < len(sentence) else "")

    return sentence


def extract_rs_annotations(doc_path):
    """Extract all <rs> elements with corresp attributes from a TEI document.

    Returns list of {ref, type, matched_text, position} for each <rs> found.
    """
    parser = etree.XMLParser(recover=True)
    tree = etree.parse(doc_path, parser)
    root = tree.getroot()
    body = root.find(f".//{{{TEI_NS}}}text/{{{TEI_NS}}}body")
    if body is None:
        return []

    annotations = []
    position = 0

    for rs in body.iter(f"{{{TEI_NS}}}rs"):
        corresp = rs.get("corresp")
        if not corresp:
            continue

        rs_type = rs.get("type", "topic")
        matched_text = get_text_content(rs)
        if not matched_text:
            continue

        # Clean up the text
        matched_text = " ".join(matched_text.split())

        annotations.append({
            "ref": corresp,
            "type": rs_type,
            "matched_text": matched_text,
            "position": position,
        })
        position += 1

    return annotations


# ── Main extraction ──────────────────────────────────────────

def process_volume(volume_id, taxonomy, lcsh_mapping):
    """Process a single volume: extract annotations, build results JSON."""
    vol_dir = os.path.join(DOC_DIR, volume_id)
    if not os.path.isdir(vol_dir):
        print(f"  ERROR: Volume directory not found: {vol_dir}")
        return None

    doc_files = sorted(glob.glob(os.path.join(vol_dir, "*.xml")))
    if not doc_files:
        print(f"  ERROR: No XML files in {vol_dir}")
        return None

    print(f"  Processing {len(doc_files)} documents...")

    by_document = {}
    by_term = {}
    total_matches = 0
    docs_with_matches = 0
    all_refs_found = set()
    unknown_refs = set()

    for doc_path in doc_files:
        fname = os.path.basename(doc_path)
        # Extract doc ID: e.g., "d86" from "d86.xml"
        doc_id = os.path.splitext(fname)[0]

        # Get metadata
        meta = extract_doc_metadata(doc_path)

        # Extract existing <rs> annotations
        annotations = extract_rs_annotations(doc_path)

        if not annotations:
            by_document[doc_id] = {
                "title": meta["title"],
                "date": meta["date"],
                "doc_type": meta["type"],
                "match_count": 0,
                "unique_terms": 0,
                "matches": [],
                "body_length": 0,
            }
            continue

        # Get full text for context extraction
        full_text = extract_body_text(doc_path)

        # Track which contexts we've used per ref (to avoid re-searching same position)
        used_contexts = {}

        doc_matches = []
        doc_refs = set()

        for ann in annotations:
            ref = ann["ref"]
            all_refs_found.add(ref)

            # Look up taxonomy info
            tax_info = taxonomy.get(ref)
            if tax_info is None:
                unknown_refs.add(ref)
                continue

            # Get LCSH info
            lcsh_info = lcsh_mapping.get(ref, {})
            lcsh_uri = tax_info.get("lcsh_uri") or lcsh_info.get("lcsh_uri", "")
            lcsh_match_quality = tax_info.get("lcsh_match") or lcsh_info.get("match_quality", "")

            # Extract context around this annotation
            context_key = f"{ref}:{ann['matched_text'][:30]}"
            if context_key not in used_contexts:
                sentence = extract_context(full_text, ann["matched_text"])
                used_contexts[context_key] = sentence
            else:
                sentence = used_contexts[context_key]
                # Try to find a different occurrence
                alt_sentence = extract_context(full_text, ann["matched_text"])
                if alt_sentence != sentence:
                    sentence = alt_sentence

            match_entry = {
                "term": tax_info["term"],
                "ref": ref,
                "canonical_ref": ref,
                "matched_ref": ref,
                "type": ann["type"],
                "category": tax_info["category"],
                "subcategory": tax_info["subcategory"],
                "lcsh_uri": lcsh_uri,
                "lcsh_match": lcsh_match_quality,
                "position": ann["position"],
                "matched_text": ann["matched_text"],
                "sentence": sentence,
                "is_variant_form": False,
                "is_consolidated": False,
            }

            doc_matches.append(match_entry)
            doc_refs.add(ref)
            total_matches += 1

            # Build by_term index
            if ref not in by_term:
                by_term[ref] = {
                    "term": tax_info["term"],
                    "type": ann["type"],
                    "category": tax_info["category"],
                    "subcategory": tax_info["subcategory"],
                    "lcsh_uri": lcsh_uri,
                    "lcsh_match": lcsh_match_quality,
                    "documents": {},
                    "total_occurrences": 0,
                    "document_count": 0,
                    "variant_names": [],
                    "variant_refs": [],
                }

            if doc_id not in by_term[ref]["documents"]:
                by_term[ref]["documents"][doc_id] = []
                by_term[ref]["document_count"] += 1

            by_term[ref]["documents"][doc_id].append({
                "position": ann["position"],
                "matched_text": ann["matched_text"],
                "sentence": sentence,
                "is_consolidated": False,
            })
            by_term[ref]["total_occurrences"] += 1

        if doc_matches:
            docs_with_matches += 1

        by_document[doc_id] = {
            "title": meta["title"],
            "date": meta["date"],
            "doc_type": meta["type"],
            "match_count": len(doc_matches),
            "unique_terms": len(doc_refs),
            "matches": doc_matches,
            "body_length": len(full_text),
        }

    # Collect variant names per term (different annotation text for same ref)
    for ref, term_data in by_term.items():
        all_texts = set()
        for doc_occs in term_data["documents"].values():
            for occ in doc_occs:
                all_texts.add(occ["matched_text"])
        if len(all_texts) > 1:
            term_data["variant_names"] = sorted(all_texts)

    # Build unmatched terms list (taxonomy terms not found in annotations)
    matched_refs = set(by_term.keys())
    total_taxonomy_terms = len(taxonomy)
    unmatched_terms = []
    for ref, info in taxonomy.items():
        if ref not in matched_refs:
            unmatched_terms.append({
                "term": info["term"],
                "ref": ref,
                "category": info["category"],
                "subcategory": info["subcategory"],
            })
    unmatched_terms.sort(key=lambda t: (t["category"], t["term"]))

    results = {
        "metadata": {
            "volume_id": volume_id,
            "generated": datetime.now().isoformat(),
            "source": "extracted_annotations",
            "total_documents": len(doc_files),
            "documents_with_matches": docs_with_matches,
            "total_matches": total_matches,
            "unique_terms_matched": len(by_term),
            "total_terms_searched": total_taxonomy_terms,
            "terms_not_matched": len(unmatched_terms),
        },
        "by_document": by_document,
        "by_term": by_term,
        "unmatched_terms": unmatched_terms,
    }

    if unknown_refs:
        print(f"  Note: {len(unknown_refs)} refs in <rs> tags not found in taxonomy (non-subject annotations like people/orgs)")

    return results


def main():
    parser = argparse.ArgumentParser(description="Extract existing <rs> annotations from TEI documents.")
    parser.add_argument("volume_id", nargs="?", help="Volume ID to process (e.g., frus1981-88v04)")
    parser.add_argument("--all", action="store_true", help="Process all volumes")
    parser.add_argument("--skip", nargs="*", default=[], help="Volume IDs to skip (use with --all)")
    args = parser.parse_args()

    if not args.all and not args.volume_id:
        parser.print_help()
        sys.exit(1)

    # Load taxonomy
    print("Loading taxonomy...")
    taxonomy = load_taxonomy(TAXONOMY_PATH)
    print(f"  {len(taxonomy)} terms loaded")

    lcsh_mapping = load_lcsh_mapping(LCSH_MAPPING_PATH)
    print(f"  {len(lcsh_mapping)} LCSH mappings loaded")

    # Determine volumes to process
    if args.all:
        all_vols = sorted([d for d in os.listdir(DOC_DIR) if os.path.isdir(os.path.join(DOC_DIR, d))])
        volumes = [v for v in all_vols if v not in args.skip]
    else:
        volumes = [args.volume_id]

    print(f"\nProcessing {len(volumes)} volumes...")
    if args.skip:
        print(f"  Skipping: {', '.join(args.skip)}")

    for vol_id in volumes:
        print(f"\n{'='*60}")
        print(f"Volume: {vol_id}")
        print(f"{'='*60}")

        results = process_volume(vol_id, taxonomy, lcsh_mapping)
        if results is None:
            continue

        # Write results
        outfile = f"string_match_results_{vol_id}.json"
        with open(outfile, "w") as f:
            json.dump(results, f, ensure_ascii=False)

        size_mb = os.path.getsize(outfile) / (1024 * 1024)
        meta = results["metadata"]
        print(f"  Wrote {outfile} ({size_mb:.1f} MB)")
        print(f"  {meta['total_matches']:,} annotations extracted")
        print(f"  {meta['unique_terms_matched']} unique terms in {meta['documents_with_matches']} docs")


if __name__ == "__main__":
    main()
