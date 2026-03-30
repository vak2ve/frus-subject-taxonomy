#!/usr/bin/env python3
"""
scan_tei_headers.py — Scan TEI document headers and build all pipeline outputs.

Reads <teiHeader> elements directly from per-document XML files in
data/documents/{vol}/d*.xml in a single pass, producing:

  1. subject-taxonomy-metadata.xml — taxonomy organized by category
  2. data/document_appearances.json — ref → {vol → [doc_ids]}
  3. data/doc_metadata.json — vol/doc → {title, date}
  4. data/volume_manifest.json — per-volume summary stats

This replaces the extract → JSON → build chain with a direct
TEI header → outputs path.

Usage:
    python3 scan_tei_headers.py                    # full scan + build
    python3 scan_tei_headers.py --vol frus1969-76v04  # single volume
    python3 scan_tei_headers.py --stats-only       # coverage report, no output files
"""

import argparse
import json
import os
import re
import sys
import time
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path

from lxml import etree

SCRIPT_DIR = Path(__file__).resolve().parent
PIPELINE_DIR = SCRIPT_DIR.parent
REPO_ROOT = PIPELINE_DIR.parent
DATA_DIR = REPO_ROOT / "data" / "documents"
OUTPUT_DIR = PIPELINE_DIR / "data"
CONFIG_DIR = REPO_ROOT / "config"
OUTPUT_TAXONOMY = PIPELINE_DIR / "subject-taxonomy-metadata.xml"

NS = {"tei": "http://www.tei-c.org/ns/1.0"}
SERIES_RE = re.compile(r"^frus(\d{4}(?:-\d{2,4})?)")


def natural_sort_key(s):
    return [int(c) if c.isdigit() else c.lower() for c in re.split(r"(\d+)", s)]


def format_category_label(slug):
    """Convert kebab-case slug to Title Case label."""
    if not slug:
        return "Uncategorized"
    return slug.replace("-", " ").title().replace("And", "and").replace("Of", "of")


# Precompiled regex patterns for fast header extraction
_RE_HEADER = re.compile(rb'<teiHeader\b.*?</teiHeader>', re.DOTALL)
_RE_TITLE = re.compile(rb'<title[^>]*>(.*?)</title>', re.DOTALL)
_RE_DATE = re.compile(rb'<date\s+([^/]*?)/?>', re.DOTALL)
_RE_DATE_ATTR = re.compile(rb'(notBefore|when)="([^"]*)"')
_RE_TERM = re.compile(
    rb'<term\s+(.*?)>(.*?)</term>',
    re.DOTALL
)
_RE_ATTR = re.compile(rb'(\w[\w-]*)="([^"]*)"')
_RE_SCHEME = re.compile(rb'scheme="frus-subject-taxonomy"')


def scan_doc_header(doc_path):
    """
    Ultra-fast extraction of metadata from a document's <teiHeader>.
    Reads raw bytes instead of full XML parsing — ~20x faster.
    Returns (title, date_str, annotations_list) or None if no header.
    """
    try:
        # Read just enough bytes to capture the header (rarely > 8KB)
        with open(doc_path, "rb") as f:
            chunk = f.read(16384)

        # Check for header
        header_match = _RE_HEADER.search(chunk)
        if not header_match:
            # Header might be larger; read more if teiHeader tag started
            if b"<teiHeader" in chunk:
                with open(doc_path, "rb") as f:
                    chunk = f.read(65536)
                header_match = _RE_HEADER.search(chunk)
            if not header_match:
                return None

        header_bytes = header_match.group(0)

        # Extract title
        title = ""
        title_m = _RE_TITLE.search(header_bytes)
        if title_m:
            raw_title = title_m.group(1).decode("utf-8", errors="replace")
            # Strip XML tags from title text
            raw_title = re.sub(r"<[^>]+>", "", raw_title)
            title = " ".join(raw_title.split())

        # Extract date
        date_str = ""
        date_m = _RE_DATE.search(header_bytes)
        if date_m:
            attrs_str = date_m.group(1)
            attr_m = _RE_DATE_ATTR.search(attrs_str)
            if attr_m:
                raw = attr_m.group(2).decode("utf-8", errors="replace")
                try:
                    dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                    date_str = dt.strftime("%B %d, %Y").replace(" 0", " ")
                except (ValueError, TypeError):
                    date_str = raw[:10]

        # Extract annotations from frus-subject-taxonomy keywords
        annotations = []

        # Find the <keywords scheme="frus-subject-taxonomy"> block
        kw_start = header_bytes.find(b'scheme="frus-subject-taxonomy"')
        if kw_start >= 0:
            # Find the enclosing <keywords> ... </keywords>
            kw_block_start = header_bytes.rfind(b"<keywords", 0, kw_start)
            kw_block_end = header_bytes.find(b"</keywords>", kw_start)
            if kw_block_start >= 0 and kw_block_end >= 0:
                kw_block = header_bytes[kw_block_start:kw_block_end + 11]

                for term_m in _RE_TERM.finditer(kw_block):
                    attrs_raw = term_m.group(1)
                    term_text = term_m.group(2).decode("utf-8", errors="replace").strip()
                    # Strip any nested tags
                    term_text = re.sub(r"<[^>]+>", "", term_text).strip()

                    # Parse attributes
                    attrs = {}
                    for attr_m in _RE_ATTR.finditer(attrs_raw):
                        attrs[attr_m.group(1).decode()] = attr_m.group(2).decode("utf-8", errors="replace")

                    ref = attrs.get("ref", "")
                    if ref:
                        annotations.append({
                            "ref": ref,
                            "term": term_text,
                            "type": attrs.get("type", "topic"),
                            "category": format_category_label(attrs.get("category", "")),
                            "subcategory": format_category_label(attrs.get("subcategory", "")),
                            "lcsh_uri": attrs.get("lcsh-uri", ""),
                            "lcsh_match": attrs.get("lcsh-match", ""),
                        })

        return title, date_str, annotations

    except Exception:
        return None


def scan_all_volumes(vol_filter=None):
    """
    Scan all document TEI headers and aggregate data.
    Returns: subjects, doc_appearances, doc_metadata, volume_stats
    """
    subjects = {}  # ref -> {name, type, category, subcategory, lcsh_uri, lcsh_match, count, volumes: set}
    doc_appearances = defaultdict(lambda: defaultdict(list))  # ref -> vol -> [doc_ids]
    doc_metadata = {"documents": {}, "volumes": {}}  # compact metadata for mockup
    volume_stats = []  # per-volume summary

    if vol_filter:
        vol_dirs = [DATA_DIR / vol_filter]
    else:
        vol_dirs = sorted([d for d in DATA_DIR.iterdir() if d.is_dir()], key=lambda p: p.name)

    total_docs = 0
    total_annotated = 0
    total_annotations = 0
    start = time.time()

    for i, vol_dir in enumerate(vol_dirs, 1):
        if not vol_dir.is_dir():
            continue
        vol_id = vol_dir.name
        doc_files = sorted(vol_dir.glob("d*.xml"), key=lambda p: natural_sort_key(p.stem))

        vol_docs = 0
        vol_annotated = 0
        vol_annotations = 0
        vol_terms = set()

        for doc_path in doc_files:
            doc_id = doc_path.stem
            result = scan_doc_header(doc_path)

            vol_docs += 1
            total_docs += 1

            if result is None:
                continue

            title, date_str, annotations = result

            # Store compact doc metadata
            doc_metadata["documents"][f"{vol_id}/{doc_id}"] = {
                "t": title[:200] if title else "",
                "d": date_str,
            }

            if not annotations:
                continue

            vol_annotated += 1
            total_annotated += 1

            seen_refs = set()
            for ann in annotations:
                ref = ann["ref"]
                if ref in seen_refs:
                    continue
                seen_refs.add(ref)

                vol_annotations += 1
                total_annotations += 1
                vol_terms.add(ref)

                # Aggregate subject
                if ref not in subjects:
                    subjects[ref] = {
                        "name": ann["term"],
                        "type": ann["type"],
                        "category": ann["category"],
                        "subcategory": ann["subcategory"],
                        "lcsh_uri": ann["lcsh_uri"],
                        "lcsh_match": ann["lcsh_match"],
                        "count": 0,
                        "volumes": set(),
                    }

                subjects[ref]["count"] += 1
                subjects[ref]["volumes"].add(vol_id)
                doc_appearances[ref][vol_id].append(doc_id)

        # Series extraction
        m = SERIES_RE.match(vol_id)
        series = m.group(1) if m else "other"

        volume_stats.append({
            "volume_id": vol_id,
            "series": series,
            "total_documents": vol_docs,
            "documents_with_annotations": vol_annotated,
            "total_annotations": vol_annotations,
            "unique_terms": len(vol_terms),
            "annotation_coverage": round(vol_annotated / vol_docs * 100, 1) if vol_docs else 0,
            # Compat fields for review tool manifest
            "total_matches": vol_annotations,
            "unique_terms_matched": len(vol_terms),
            "documents_with_matches": vol_annotated,
        })

        if i % 50 == 0 or i == len(vol_dirs):
            elapsed = time.time() - start
            print(f"  [{i}/{len(vol_dirs)}] {vol_id} — "
                  f"{total_docs:,} docs, {total_annotated:,} annotated, "
                  f"{total_annotations:,} annotations ({elapsed:.0f}s)")

    # Finalize subjects
    for ref, data in subjects.items():
        data["volumes_count"] = len(data["volumes"])
        data["appears_in"] = ", ".join(sorted(data["volumes"]))
        data.pop("volumes")

    # Sort and deduplicate doc appearance lists
    doc_apps = {}
    for ref, vols in doc_appearances.items():
        doc_apps[ref] = {
            vol: sorted(set(docs), key=natural_sort_key)
            for vol, docs in vols.items()
        }

    elapsed = time.time() - start
    print(f"\nScan complete: {total_docs:,} docs, {len(subjects):,} subjects, "
          f"{total_annotations:,} annotations in {elapsed:.0f}s")

    return subjects, doc_apps, doc_metadata, volume_stats


def apply_review_state(subjects, doc_apps):
    """Apply decisions from metadata_review_state.json."""
    state_path = PIPELINE_DIR / "metadata_review_state.json"
    if not state_path.exists():
        return subjects, doc_apps

    with open(state_path) as f:
        state = json.load(f)

    exclusions = state.get("exclusions", {})
    merges = state.get("merge_decisions", {})
    cat_overrides = state.get("category_overrides", {})

    for ref in exclusions:
        if ref in subjects:
            subjects[ref]["excluded"] = True

    for src_ref, merge_info in merges.items():
        tgt_ref = merge_info.get("targetRef", "")
        if src_ref in subjects and tgt_ref in subjects:
            subjects[tgt_ref]["count"] += subjects[src_ref]["count"]
            for vol, docs in doc_apps.get(src_ref, {}).items():
                existing = set(doc_apps.get(tgt_ref, {}).get(vol, []))
                existing.update(docs)
                if tgt_ref not in doc_apps:
                    doc_apps[tgt_ref] = {}
                doc_apps[tgt_ref][vol] = sorted(existing, key=natural_sort_key)
            subjects[src_ref]["merged_into"] = tgt_ref

    for ref, override in cat_overrides.items():
        if ref in subjects:
            subjects[ref]["category"] = override.get("to_category", subjects[ref]["category"])
            subjects[ref]["subcategory"] = override.get("to_subcategory", subjects[ref]["subcategory"])

    applied = sum(1 for r in exclusions if r in subjects)
    merged = sum(1 for r in merges if r in subjects)
    if applied or merged:
        print(f"  Applied {applied} exclusions, {merged} merges from review state")

    return subjects, doc_apps


def build_taxonomy_xml(subjects, doc_apps):
    """Build the taxonomy XML from aggregated subject data."""
    subjects, doc_apps = apply_review_state(subjects, doc_apps)

    categories = defaultdict(lambda: defaultdict(list))
    excluded_entries = []

    for ref, data in subjects.items():
        if data.get("excluded") or data.get("merged_into"):
            excluded_entries.append((ref, data))
            continue
        categories[data["category"]][data["subcategory"]].append((ref, data))

    active_count = sum(
        len(subjs)
        for subs in categories.values()
        for subjs in subs.values()
    )

    root = etree.Element("taxonomy", attrib={
        "source": "tei-header-metadata",
        "authority": "Office of the Historian (history.state.gov)",
        "generated": date.today().isoformat(),
        "total-subjects": str(active_count),
        "pipeline": "metadata",
    })

    sorted_cats = sorted(
        categories.items(),
        key=lambda x: sum(d["count"] for subs in x[1].values() for _, d in subs),
        reverse=True,
    )

    for cat_name, subcats in sorted_cats:
        cat_total = sum(d["count"] for subs in subcats.values() for _, d in subs)
        cat_count = sum(len(subs) for subs in subcats.values())

        cat_elem = etree.SubElement(root, "category", attrib={
            "label": cat_name,
            "total-annotations": str(cat_total),
            "total-subjects": str(cat_count),
        })

        sorted_subs = sorted(subcats.items(), key=lambda x: sum(d["count"] for _, d in x[1]), reverse=True)
        for sub_name, subject_list in sorted_subs:
            sub_total = sum(d["count"] for _, d in subject_list)
            sub_elem = etree.SubElement(cat_elem, "subcategory", attrib={
                "label": sub_name,
                "total-annotations": str(sub_total),
                "total-subjects": str(len(subject_list)),
            })

            for ref, sdata in sorted(subject_list, key=lambda x: x[1]["count"], reverse=True):
                apps = doc_apps.get(ref, {})
                count = sum(len(docs) for docs in apps.values()) if apps else sdata["count"]
                volumes = len(apps) if apps else sdata["volumes_count"]

                attribs = {
                    "ref": ref,
                    "type": sdata.get("type", "topic"),
                    "count": str(count),
                    "volumes": str(volumes),
                }
                if sdata.get("lcsh_uri") and sdata.get("lcsh_match") in ("exact", "good_close"):
                    attribs["lcsh-uri"] = sdata["lcsh_uri"]
                    attribs["lcsh-match"] = sdata["lcsh_match"]

                subj_elem = etree.SubElement(sub_elem, "subject", **attribs)
                name_elem = etree.SubElement(subj_elem, "name")
                name_elem.text = sdata["name"]

                if sdata.get("appears_in"):
                    ai_elem = etree.SubElement(subj_elem, "appearsIn")
                    ai_elem.text = sdata["appears_in"]

                if apps:
                    docs_elem = etree.SubElement(subj_elem, "documents")
                    for vol_id, doc_ids in sorted(apps.items()):
                        vol_elem = etree.SubElement(docs_elem, "volume", id=vol_id)
                        vol_elem.text = ", ".join(doc_ids)

    # Excluded section
    if excluded_entries:
        excl_elem = etree.SubElement(root, "excluded", attrib={"total": str(len(excluded_entries))})
        for ref, data in sorted(excluded_entries, key=lambda x: x[1]["name"].lower()):
            attribs = {"ref": ref}
            if data.get("merged_into"):
                attribs["reason"] = "merged"
                attribs["canonical-ref"] = data["merged_into"]
            else:
                attribs["reason"] = "excluded"
            entry_elem = etree.SubElement(excl_elem, "entry", **attribs)
            name_elem = etree.SubElement(entry_elem, "name")
            name_elem.text = data["name"]

    tree = etree.ElementTree(root)
    etree.indent(tree, space="    ")
    tree.write(str(OUTPUT_TAXONOMY), xml_declaration=True, encoding="UTF-8", pretty_print=True)

    print(f"\nTaxonomy: {OUTPUT_TAXONOMY}")
    print(f"  {active_count} subjects in {len(categories)} categories")
    for cat_name, subcats in sorted_cats[:10]:
        cat_count = sum(len(subs) for subs in subcats.values())
        print(f"    {cat_name}: {cat_count} subjects")
    if len(sorted_cats) > 10:
        print(f"    ... and {len(sorted_cats) - 10} more categories")


def save_outputs(subjects, doc_apps, doc_metadata, volume_stats):
    """Save all supporting data files."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Document appearances
    apps_path = OUTPUT_DIR / "document_appearances.json"
    with open(apps_path, "w") as f:
        json.dump(doc_apps, f, separators=(",", ":"))
    print(f"  Document appearances: {apps_path} ({len(doc_apps)} subjects)")

    # Doc metadata (compact, for mockup)
    meta_path = OUTPUT_DIR / "doc_metadata.json"
    with open(meta_path, "w") as f:
        json.dump(doc_metadata, f, separators=(",", ":"))
    print(f"  Doc metadata: {meta_path} ({len(doc_metadata['documents'])} docs)")

    # Volume manifest
    manifest_path = OUTPUT_DIR / "volume_manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(volume_stats, f, indent=2)
    print(f"  Volume manifest: {manifest_path} ({len(volume_stats)} volumes)")


def print_coverage_report(subjects, volume_stats):
    """Print summary statistics."""
    total_docs = sum(v["total_documents"] for v in volume_stats)
    total_annotated = sum(v["documents_with_annotations"] for v in volume_stats)
    total_annotations = sum(v["total_annotations"] for v in volume_stats)

    categories = defaultdict(int)
    lcsh_quality = defaultdict(int)
    for data in subjects.values():
        categories[data["category"]] += data["count"]
        if data.get("lcsh_match"):
            lcsh_quality[data["lcsh_match"]] += 1

    print(f"\n{'='*70}")
    print("METADATA COVERAGE REPORT")
    print(f"{'='*70}")
    print(f"Volumes: {len(volume_stats)}")
    print(f"Documents: {total_docs:,}")
    print(f"Annotated: {total_annotated:,} ({total_annotated/total_docs*100:.1f}%)" if total_docs else "")
    print(f"Annotations: {total_annotations:,}")
    print(f"Unique subjects: {len(subjects):,}")
    print(f"\nBy category:")
    for cat, count in sorted(categories.items(), key=lambda x: -x[1]):
        print(f"  {cat}: {count:,}")
    print(f"\nLCSH match quality:")
    for q, count in sorted(lcsh_quality.items(), key=lambda x: -x[1]):
        print(f"  {q}: {count:,}")


def main():
    parser = argparse.ArgumentParser(description="Scan TEI headers and build metadata pipeline outputs")
    parser.add_argument("--vol", help="Scan single volume (e.g. frus1969-76v04)")
    parser.add_argument("--stats-only", action="store_true", help="Coverage report only, no output files")
    args = parser.parse_args()

    print("=== TEI Header Metadata Scanner ===")
    print(f"Source: {DATA_DIR}")
    print()

    subjects, doc_apps, doc_metadata, volume_stats = scan_all_volumes(args.vol)

    print_coverage_report(subjects, volume_stats)

    if not args.stats_only:
        print(f"\nWriting outputs...")
        save_outputs(subjects, doc_apps, doc_metadata, volume_stats)
        build_taxonomy_xml(subjects, doc_apps)
        print("\nDone.")


if __name__ == "__main__":
    main()
