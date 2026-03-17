#!/usr/bin/env python3
"""
Convert inline TEI annotations to string_match_results JSON format.

Reads annotated TEI document files (from hsg-annotate-data) that contain
human annotations as corresp attributes on <placeName>, <orgName>,
<persName>, and <rs> elements, and produces string_match_results JSON
matching the format used by the annotation review tool.

This is for volumes annotated by humans in the hsg-annotate-data project,
as opposed to convert_airtable_annotations.py which reads Airtable XML
exports.

Usage:
    python3 convert_tei_annotations.py <source_dir> <volume_id>

    source_dir:  Path to the annotated documents directory
                 (e.g., /path/to/hsg-annotate-data/data/documents/frus1977-80v19)
    volume_id:   Volume ID (e.g., frus1977-80v19)

Example:
    python3 convert_tei_annotations.py \\
        /path/to/hsg-annotate-data/data/documents/frus1977-80v19 \\
        frus1977-80v19
"""

import glob
import json
import os
import sys
from collections import defaultdict
from datetime import datetime
from lxml import etree

os.chdir(os.path.dirname(os.path.abspath(__file__)))

TEI_NS = "http://www.tei-c.org/ns/1.0"
FRUS_NS = "http://history.state.gov/frus/ns/1.0"
NSMAP = {"tei": TEI_NS, "frus": FRUS_NS}

LCSH_MAPPING_FILE = "../config/lcsh_mapping.json"

# Only import topics and compound subjects — not places, organizations, or people.
# These are <rs> elements with type="topic" or type="compound-subject".
ANNOTATED_TAG = f"{{{TEI_NS}}}rs"
ALLOWED_RS_TYPES = {"topic", "compound-subject"}


def log(msg):
    print(msg, flush=True)


def load_lcsh_mapping():
    """Load LCSH mapping to enrich annotation data."""
    if not os.path.exists(LCSH_MAPPING_FILE):
        return {}
    with open(LCSH_MAPPING_FILE) as f:
        return json.load(f)


def get_element_text(el):
    """Get the full text content of an element, including children."""
    return "".join(el.itertext()).strip()


def extract_doc_metadata(tree):
    """Extract document metadata (title, date, type) from TEI.

    Handles two TEI structures:
    - frus-subject-taxonomy split docs: <div type="document"> with frus:doc-dateTime-min
    - hsg-annotate-data docs: <titleStmt>/<title> and <dateline>/<date>
    """
    root = tree.getroot()

    title = ""
    date = ""
    doc_type = ""

    # Try structure 1: <div type="document"> (frus-subject-taxonomy split docs)
    doc_div = root.xpath('//tei:div[@type="document"]', namespaces=NSMAP)
    if doc_div:
        doc_div = doc_div[0]
        doc_type = doc_div.get("subtype", "")

        date_str = doc_div.get(f"{{{FRUS_NS}}}doc-dateTime-min", "")
        if date_str:
            try:
                date_part = date_str.split("T")[0]
                dt = datetime.fromisoformat(date_part)
                date = dt.strftime("%B %d, %Y")
            except (ValueError, IndexError):
                pass

        head = doc_div.find(".//tei:head", NSMAP)
        if head is not None:
            parts = []
            if head.text:
                parts.append(head.text)
            for child in head:
                if child.tag == f"{{{TEI_NS}}}note":
                    if child.tail:
                        parts.append(child.tail)
                    continue
                parts.append(get_element_text(child))
                if child.tail:
                    parts.append(child.tail)
            title = " ".join("".join(parts).split())

        return {"title": title, "date": date, "doc_type": doc_type}

    # Structure 2: hsg-annotate-data format (titleStmt + dateline)
    # Title from <titleStmt>/<title>
    title_el = root.xpath('//tei:titleStmt/tei:title', namespaces=NSMAP)
    if title_el:
        title = " ".join(get_element_text(title_el[0]).split())

    # Date from <dateline>/<date @when>
    date_els = root.xpath('//tei:dateline//tei:date/@when', namespaces=NSMAP)
    if date_els:
        try:
            date_part = date_els[0].split("T")[0]
            dt = datetime.fromisoformat(date_part)
            date = dt.strftime("%B %d, %Y")
        except (ValueError, IndexError):
            pass

    # Doc type from <bibl type="frus-div-subtype">
    subtype_el = root.xpath(
        '//tei:bibl[@type="frus-div-subtype"]', namespaces=NSMAP
    )
    if subtype_el:
        doc_type = get_element_text(subtype_el[0])

    return {"title": title, "date": date, "doc_type": doc_type}


def extract_annotations(tree):
    """Extract all corresp annotations from TEI elements.

    Returns list of dicts: {ref, matched_text, tag, position}
    """
    root = tree.getroot()
    annotations = []
    position = 0

    for el in root.iter():
        if el.tag != ANNOTATED_TAG:
            continue

        corresp = el.get("corresp", "")
        if not corresp:
            continue

        rs_type = el.get("type", "")
        if rs_type not in ALLOWED_RS_TYPES:
            continue

        text = get_element_text(el)
        if not text:
            continue

        annotations.append({
            "ref": corresp,
            "matched_text": text,
            "tag": "rs",
            "type": rs_type,
            "position": position,
        })
        position += 1

    return annotations


def get_surrounding_text(tree, el, max_chars=120):
    """Try to get some surrounding text for context (best-effort)."""
    parent = el.getparent()
    if parent is not None:
        text = get_element_text(parent)
        if len(text) > max_chars * 2:
            # Find the element's text within parent
            el_text = get_element_text(el)
            idx = text.find(el_text)
            if idx >= 0:
                start = max(0, idx - max_chars)
                end = min(len(text), idx + len(el_text) + max_chars)
                return text[start:end]
        return text[:max_chars * 2]
    return ""


def main():
    if len(sys.argv) < 3:
        log("Usage: python3 convert_tei_annotations.py <source_dir> <volume_id>")
        log("  source_dir:  Path to annotated documents (hsg-annotate-data/data/documents/<vol>)")
        log("  volume_id:   Volume ID (e.g., frus1977-80v19)")
        sys.exit(1)

    source_dir = sys.argv[1]
    volume_id = sys.argv[2]

    if not os.path.isdir(source_dir):
        log(f"ERROR: Source directory not found: {source_dir}")
        sys.exit(1)

    log(f"Converting inline TEI annotations for {volume_id}")
    log(f"  Source: {source_dir}")

    # Load LCSH mapping for enrichment
    lcsh_mapping = load_lcsh_mapping()
    log(f"  LCSH mapping: {len(lcsh_mapping)} entries")

    # Find all document files
    doc_files = sorted(glob.glob(os.path.join(source_dir, "d*.xml")))
    log(f"  Found {len(doc_files)} documents")

    by_document = {}
    by_term = {}
    all_matched_refs = set()
    total_matches = 0
    unknown_refs = set()

    for i, doc_path in enumerate(doc_files):
        doc_id = os.path.splitext(os.path.basename(doc_path))[0]

        try:
            tree = etree.parse(doc_path)
        except etree.XMLSyntaxError as e:
            log(f"  WARNING: Could not parse {doc_id}: {e}")
            continue

        metadata = extract_doc_metadata(tree)
        annotations = extract_annotations(tree)
        total_matches += len(annotations)

        # Build match entries
        matches = []
        for ann in annotations:
            ref = ann["ref"]
            all_matched_refs.add(ref)

            # Look up taxonomy data
            tax_entry = lcsh_mapping.get(ref, {})
            term_name = tax_entry.get("name", ann["matched_text"])
            category = tax_entry.get("category", "")
            subcategory = tax_entry.get("subcategory", "")
            lcsh_uri = tax_entry.get("lcsh_uri", "")
            lcsh_match = tax_entry.get("match_quality", "")
            entry_type = tax_entry.get("type", ann["type"])

            if not tax_entry:
                unknown_refs.add(ref)

            match = {
                "term": term_name,
                "ref": ref,
                "canonical_ref": ref,
                "matched_ref": ref,
                "type": entry_type,
                "category": category,
                "subcategory": subcategory,
                "lcsh_uri": lcsh_uri,
                "lcsh_match": lcsh_match,
                "position": ann["position"],
                "matched_text": ann["matched_text"],
                "sentence": "",
                "is_variant_form": False,
                "is_consolidated": False,
            }
            matches.append(match)

            # Track by_term
            if ref not in by_term:
                by_term[ref] = {
                    "term": term_name,
                    "type": entry_type,
                    "category": category,
                    "subcategory": subcategory,
                    "lcsh_uri": lcsh_uri,
                    "lcsh_match": lcsh_match,
                    "documents": {},
                    "total_occurrences": 0,
                    "variant_names": [],
                    "variant_refs": [],
                }
            bt = by_term[ref]
            bt["total_occurrences"] += 1
            if doc_id not in bt["documents"]:
                bt["documents"][doc_id] = []
            bt["documents"][doc_id].append({
                "sentence": "",
                "matched_text": ann["matched_text"],
                "position": ann["position"],
                "matched_ref": ref,
                "is_consolidated": False,
                "is_variant_form": False,
            })

        by_document[doc_id] = {
            "title": metadata["title"],
            "date": metadata["date"],
            "doc_type": metadata["doc_type"],
            "match_count": len(matches),
            "unique_terms": len(set(m["ref"] for m in matches)),
            "matches": matches,
            "body_length": 0,
        }

        if (i + 1) % 50 == 0 or i == len(doc_files) - 1:
            log(f"  Processed {i + 1}/{len(doc_files)} documents, {total_matches} annotations so far")

    # Finalize by_term document counts
    for ref, bt in by_term.items():
        bt["document_count"] = len(bt["documents"])

    # Build results
    results = {
        "metadata": {
            "volume_id": volume_id,
            "generated": datetime.now().isoformat(),
            "total_documents": len(doc_files),
            "documents_with_matches": sum(1 for d in by_document.values() if d["match_count"] > 0),
            "total_matches": total_matches,
            "unique_terms_matched": len(all_matched_refs),
            "total_terms_searched": 0,
            "terms_not_matched": 0,
            "min_term_length": 0,
            "stoplist_applied": False,
            "stoplisted_terms": 0,
            "variant_consolidation_applied": False,
            "variant_groups_count": 0,
            "variant_names_added": 0,
            "consolidated_matches": 0,
            "source": "human-tei-annotations",
        },
        "by_document": by_document,
        "by_term": by_term,
        "unmatched_terms": [],
    }

    # Write output
    output_dir = os.path.join("..", "data", "documents", volume_id)
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"string_match_results_{volume_id}.json")
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    log(f"\n{'=' * 60}")
    log(f"Results written to: {output_path}")
    log(f"  Total documents: {len(doc_files)}")
    log(f"  Documents with annotations: {results['metadata']['documents_with_matches']}")
    log(f"  Total annotations: {total_matches}")
    log(f"  Unique subjects: {len(all_matched_refs)}")
    if unknown_refs:
        log(f"  Unknown refs (not in lcsh_mapping): {len(unknown_refs)}")

    # Top annotated subjects
    top_terms = sorted(by_term.items(), key=lambda x: x[1]["total_occurrences"], reverse=True)[:15]
    log(f"\nTop 15 annotated subjects:")
    for ref, bt in top_terms:
        log(f"  {bt['term']}: {bt['total_occurrences']} occurrences in {bt['document_count']} docs")


if __name__ == "__main__":
    main()
