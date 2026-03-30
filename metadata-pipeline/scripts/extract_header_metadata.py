#!/usr/bin/env python3
"""
DEPRECATED: Superseded by scan_tei_headers.py which reads TEI headers directly
without producing intermediate JSON files. Kept for reference only.

extract_header_metadata.py — Extract subject taxonomy metadata from TEI headers.

Reads the <teiHeader> of each per-document XML file in data/documents/{vol}/d*.xml
and produces metadata_results_{vol}.json files in the same schema as the existing
string_match_results_{vol}.json files, enabling reuse of the same review tools.

Sources extracted:
  - <keywords scheme="frus-subject-taxonomy"> → subject annotations (ref, type, category, etc.)
  - <settingDesc>/<date> → document dates
  - <titleStmt>/<title> → document titles
  - <sourceDesc>/<bibl> → volume/document IDs, document number, subtype
  - Volume-level <keywords scheme="https://history.state.gov/tags"> → HSG tags
  - Volume-level <keywords scheme="#frus-administration-coverage"> → administration

Usage:
    python3 extract_header_metadata.py                    # all volumes
    python3 extract_header_metadata.py --vol frus1969-76v04  # single volume
    python3 extract_header_metadata.py --stats             # coverage report only
"""

import argparse
import json
import os
import re
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from lxml import etree

# Resolve paths relative to repo root
SCRIPT_DIR = Path(__file__).resolve().parent
PIPELINE_DIR = SCRIPT_DIR.parent
REPO_ROOT = PIPELINE_DIR.parent
DATA_DIR = REPO_ROOT / "data" / "documents"
VOLUMES_DIR = REPO_ROOT / "volumes"
OUTPUT_DIR = PIPELINE_DIR / "data"
CONFIG_DIR = REPO_ROOT / "config"

NS = {"tei": "http://www.tei-c.org/ns/1.0", "frus": "http://history.state.gov/frus/ns/1.0"}

SERIES_RE = re.compile(r"^frus(\d{4}(?:-\d{2,4})?)")


def extract_volume_metadata(vol_id):
    """
    Extract volume-level metadata from the original volume XML:
    - HSG tags (scheme="https://history.state.gov/tags")
    - Administration coverage
    - Production priority
    - Media type
    Returns dict with these fields.
    """
    vol_path = VOLUMES_DIR / f"{vol_id}.xml"
    meta = {
        "hsg_tags": [],
        "administration": "",
        "production_priority": "",
        "media_type": "",
        "volume_title": "",
    }

    if not vol_path.exists():
        return meta

    # Parse only the header to save memory on large volumes
    try:
        context = etree.iterparse(str(vol_path), events=("end",), tag="{http://www.tei-c.org/ns/1.0}teiHeader")
        for event, elem in context:
            # HSG tags
            for kw in elem.findall(".//tei:keywords[@scheme='https://history.state.gov/tags']", NS):
                for term in kw.findall("tei:term", NS):
                    if term.text:
                        meta["hsg_tags"].append(term.text.strip())

            # Administration coverage
            for kw in elem.findall(".//tei:keywords[@scheme='#frus-administration-coverage']", NS):
                for term in kw.findall("tei:term", NS):
                    if term.text:
                        meta["administration"] = term.text.strip()

            # Production priority
            for kw in elem.findall(".//tei:keywords[@scheme='#frus-production-priority']", NS):
                for term in kw.findall("tei:term", NS):
                    if term.text:
                        meta["production_priority"] = term.text.strip()

            # Media type
            for kw in elem.findall(".//tei:keywords[@scheme='#frus-media-type']", NS):
                for term in kw.findall("tei:term", NS):
                    if term.text:
                        meta["media_type"] = term.text.strip()

            # Volume title
            title_el = elem.find(".//tei:titleStmt/tei:title", NS)
            if title_el is not None:
                meta["volume_title"] = "".join(title_el.itertext()).strip()

            elem.clear()
            break  # Only need the first (volume-level) teiHeader
    except Exception as e:
        print(f"  WARNING: Could not parse volume XML {vol_path}: {e}")

    return meta


def extract_doc_header(doc_path):
    """
    Extract all metadata from a document's <teiHeader>.
    Returns dict with: title, date, date_iso, doc_number, subtype,
    annotations (list of term dicts), has_header (bool).
    """
    result = {
        "title": "",
        "date": "",
        "date_not_before": "",
        "date_not_after": "",
        "date_when": "",
        "doc_number": "",
        "subtype": "historical-document",
        "annotations": [],
        "has_header": False,
    }

    try:
        parser = etree.XMLParser(remove_blank_text=True)
        tree = etree.parse(str(doc_path), parser)
        root = tree.getroot()
    except Exception as e:
        return result

    # Find teiHeader (with or without namespace)
    header = root.find("teiHeader")
    if header is None:
        header = root.find("{http://www.tei-c.org/ns/1.0}teiHeader")
    if header is None:
        return result

    result["has_header"] = True

    # === Title ===
    title_el = header.find(".//titleStmt/title")
    if title_el is None:
        title_el = header.find(".//{http://www.tei-c.org/ns/1.0}titleStmt/{http://www.tei-c.org/ns/1.0}title")
    if title_el is not None:
        result["title"] = " ".join("".join(title_el.itertext()).split())

    # === Source desc — volume/doc IDs ===
    for bibl in header.iter():
        tag = bibl.tag.split("}")[-1] if "}" in str(bibl.tag) else str(bibl.tag)
        if tag != "bibl":
            continue
        btype = bibl.get("type", "")
        btext = (bibl.text or "").strip()
        if btype == "frus-document-number":
            result["doc_number"] = btext
        elif btype == "frus-div-subtype":
            result["subtype"] = btext

    # === Dates ===
    for date_el in header.iter():
        tag = date_el.tag.split("}")[-1] if "}" in str(date_el.tag) else str(date_el.tag)
        if tag != "date":
            continue
        result["date_not_before"] = date_el.get("notBefore", "")
        result["date_not_after"] = date_el.get("notAfter", "")
        result["date_when"] = date_el.get("when", "")
        # Build human-readable date
        raw = result["date_when"] or result["date_not_before"]
        if raw:
            try:
                dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                result["date"] = dt.strftime("%B %d, %Y").replace(" 0", " ")
            except (ValueError, TypeError):
                result["date"] = raw[:10]
        break  # Use first date found

    # === Subject annotations from keywords ===
    for keywords_el in header.iter():
        tag = keywords_el.tag.split("}")[-1] if "}" in str(keywords_el.tag) else str(keywords_el.tag)
        if tag != "keywords":
            continue
        scheme = keywords_el.get("scheme", "")
        if scheme != "frus-subject-taxonomy":
            continue

        for term_el in keywords_el:
            term_tag = term_el.tag.split("}")[-1] if "}" in str(term_el.tag) else str(term_el.tag)
            if term_tag != "term":
                continue

            ann = {
                "term": (term_el.text or "").strip(),
                "ref": term_el.get("ref", ""),
                "type": term_el.get("type", "topic"),
                "category": term_el.get("category", ""),
                "subcategory": term_el.get("subcategory", ""),
                "lcsh_uri": term_el.get("lcsh-uri", ""),
                "lcsh_match": term_el.get("lcsh-match", ""),
            }
            if ann["ref"]:
                result["annotations"].append(ann)

    return result


def format_category_label(slug):
    """Convert kebab-case category slug to human-readable label."""
    return slug.replace("-", " ").title().replace("And", "and").replace("Of", "of")


def process_volume(vol_id, vol_meta=None):
    """
    Process all documents in a volume and produce a metadata_results dict
    in the same schema as string_match_results_{vol}.json.
    """
    vol_dir = DATA_DIR / vol_id
    if not vol_dir.is_dir():
        return None

    doc_files = sorted(vol_dir.glob("d*.xml"), key=lambda p: natural_sort_key(p.stem))

    by_document = {}
    by_term = defaultdict(lambda: {"term": "", "ref": "", "type": "", "category": "",
                                     "subcategory": "", "lcsh_uri": "", "lcsh_match": "",
                                     "documents": [], "total_occurrences": 0})
    all_refs = set()
    total_annotations = 0
    docs_with_annotations = 0
    docs_with_header = 0

    for doc_path in doc_files:
        doc_id = doc_path.stem
        header_data = extract_doc_header(doc_path)

        if header_data["has_header"]:
            docs_with_header += 1

        annotations = header_data["annotations"]
        if annotations:
            docs_with_annotations += 1

        # Build matches list compatible with string_match_results schema
        matches = []
        seen_refs = set()
        for ann in annotations:
            ref = ann["ref"]
            if ref in seen_refs:
                continue
            seen_refs.add(ref)

            match_entry = {
                "term": ann["term"],
                "ref": ref,
                "canonical_ref": ref,
                "matched_ref": ref,
                "type": ann["type"],
                "category": format_category_label(ann["category"]),
                "subcategory": format_category_label(ann["subcategory"]),
                "lcsh_uri": ann["lcsh_uri"],
                "lcsh_match": ann["lcsh_match"],
                "position": 0,  # No position for header-sourced metadata
                "matched_text": ann["term"],
                "sentence": "",  # No sentence context for header metadata
                "source": "tei-header",
            }
            matches.append(match_entry)
            all_refs.add(ref)
            total_annotations += 1

            # Accumulate by_term
            term_entry = by_term[ref]
            term_entry["term"] = ann["term"]
            term_entry["ref"] = ref
            term_entry["type"] = ann["type"]
            term_entry["category"] = format_category_label(ann["category"])
            term_entry["subcategory"] = format_category_label(ann["subcategory"])
            term_entry["lcsh_uri"] = ann["lcsh_uri"]
            term_entry["lcsh_match"] = ann["lcsh_match"]
            term_entry["documents"].append(doc_id)
            term_entry["total_occurrences"] += 1

        by_document[doc_id] = {
            "title": header_data["title"],
            "date": header_data["date"],
            "date_not_before": header_data["date_not_before"],
            "date_not_after": header_data["date_not_after"],
            "doc_type": header_data["subtype"],
            "match_count": len(matches),
            "unique_terms": len(matches),
            "matches": matches,
            "has_header": header_data["has_header"],
        }

    # Extract series from volume ID
    m = SERIES_RE.match(vol_id)
    series = m.group(1) if m else "other"

    results = {
        "metadata": {
            "volume_id": vol_id,
            "series": series,
            "generated": datetime.now().isoformat(),
            "source": "tei-header-metadata",
            "total_documents": len(doc_files),
            "documents_with_headers": docs_with_header,
            "documents_with_annotations": docs_with_annotations,
            "total_annotations": total_annotations,
            "unique_terms_found": len(all_refs),
            "header_coverage": round(docs_with_header / len(doc_files) * 100, 1) if doc_files else 0,
            "annotation_coverage": round(docs_with_annotations / len(doc_files) * 100, 1) if doc_files else 0,
            # Compatibility fields for string_match_results schema
            "total_matches": total_annotations,
            "unique_terms_matched": len(all_refs),
            "total_terms_searched": len(all_refs),
            "documents_with_matches": docs_with_annotations,
            "terms_not_matched": 0,
        },
        "by_document": by_document,
        "by_term": {ref: dict(data) for ref, data in by_term.items()},
        "unmatched_terms": [],
    }

    # Add volume-level metadata if available
    if vol_meta:
        results["metadata"]["hsg_tags"] = vol_meta.get("hsg_tags", [])
        results["metadata"]["administration"] = vol_meta.get("administration", "")
        results["metadata"]["volume_title"] = vol_meta.get("volume_title", "")

    return results


def natural_sort_key(s):
    """Sort key that handles embedded numbers naturally (d1, d2, d10)."""
    return [int(c) if c.isdigit() else c.lower() for c in re.split(r"(\d+)", s)]


def print_coverage_report(all_results):
    """Print a summary of metadata coverage across all volumes."""
    print("\n" + "=" * 80)
    print("METADATA COVERAGE REPORT")
    print("=" * 80)

    total_docs = 0
    total_with_headers = 0
    total_with_annotations = 0
    total_annotations = 0
    unique_refs = set()
    categories = defaultdict(int)
    lcsh_quality = defaultdict(int)

    for vol_id, results in sorted(all_results.items()):
        meta = results["metadata"]
        total_docs += meta["total_documents"]
        total_with_headers += meta["documents_with_headers"]
        total_with_annotations += meta["documents_with_annotations"]
        total_annotations += meta["total_annotations"]

        for ref, term_data in results["by_term"].items():
            unique_refs.add(ref)
            cat = term_data.get("category", "Uncategorized")
            categories[cat] += term_data["total_occurrences"]
            lcsh = term_data.get("lcsh_match", "")
            if lcsh:
                lcsh_quality[lcsh] += 1

    print(f"\nVolumes processed: {len(all_results)}")
    print(f"Total documents: {total_docs:,}")
    print(f"Documents with TEI headers: {total_with_headers:,} ({total_with_headers/total_docs*100:.1f}%)" if total_docs else "")
    print(f"Documents with annotations: {total_with_annotations:,} ({total_with_annotations/total_docs*100:.1f}%)" if total_docs else "")
    print(f"Total annotations: {total_annotations:,}")
    print(f"Unique subjects: {len(unique_refs):,}")

    print(f"\nAnnotations by category:")
    for cat, count in sorted(categories.items(), key=lambda x: -x[1]):
        print(f"  {cat}: {count:,}")

    print(f"\nLCSH match quality distribution:")
    for quality, count in sorted(lcsh_quality.items(), key=lambda x: -x[1]):
        print(f"  {quality}: {count:,}")


def main():
    parser = argparse.ArgumentParser(description="Extract subject metadata from TEI headers")
    parser.add_argument("--vol", help="Process a single volume (e.g. frus1969-76v04)")
    parser.add_argument("--stats", action="store_true", help="Print coverage report only (no output files)")
    parser.add_argument("--with-volume-meta", action="store_true",
                        help="Also extract volume-level metadata from original XMLs (slower)")
    args = parser.parse_args()

    print("=== TEI Header Metadata Extraction ===")
    print(f"Source: {DATA_DIR}")
    print(f"Output: {OUTPUT_DIR}")
    print()

    if args.vol:
        volumes = [args.vol]
    else:
        if not DATA_DIR.exists():
            print(f"ERROR: Data directory not found: {DATA_DIR}")
            sys.exit(1)
        volumes = sorted([d.name for d in DATA_DIR.iterdir() if d.is_dir()])

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    all_results = {}
    start_time = time.time()

    for i, vol_id in enumerate(volumes, 1):
        print(f"[{i}/{len(volumes)}] {vol_id}...", end=" ", flush=True)

        vol_meta = None
        if args.with_volume_meta:
            vol_meta = extract_volume_metadata(vol_id)

        results = process_volume(vol_id, vol_meta)
        if results is None:
            print("SKIPPED (no directory)")
            continue

        meta = results["metadata"]
        print(f"{meta['total_documents']} docs, "
              f"{meta['documents_with_annotations']} annotated ({meta['annotation_coverage']}%), "
              f"{meta['unique_terms_found']} terms, "
              f"{meta['total_annotations']} annotations")

        all_results[vol_id] = results

        if not args.stats:
            # Write per-volume results
            vol_out_dir = OUTPUT_DIR / vol_id
            vol_out_dir.mkdir(parents=True, exist_ok=True)
            out_path = vol_out_dir / f"metadata_results_{vol_id}.json"
            with open(out_path, "w") as f:
                json.dump(results, f, indent=2, ensure_ascii=False)

    elapsed = time.time() - start_time

    # Write combined manifest
    if not args.stats:
        manifest = []
        for vol_id, results in sorted(all_results.items()):
            meta = results["metadata"]
            manifest.append({
                "volume_id": vol_id,
                "series": meta.get("series", "other"),
                "filename": f"data/{vol_id}/metadata_results_{vol_id}.json",
                "total_matches": meta["total_annotations"],
                "unique_terms_matched": meta["unique_terms_found"],
                "total_terms_searched": meta["unique_terms_found"],
                "total_documents": meta["total_documents"],
                "documents_with_matches": meta["documents_with_annotations"],
                "documents_with_headers": meta["documents_with_headers"],
                "header_coverage": meta["header_coverage"],
                "annotation_coverage": meta["annotation_coverage"],
                "terms_not_matched": 0,
                "generated": meta["generated"],
            })

        manifest_path = OUTPUT_DIR / "volume_manifest.json"
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2, ensure_ascii=False)
        print(f"\nManifest written to {manifest_path}")

    print_coverage_report(all_results)

    print(f"\nCompleted in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
