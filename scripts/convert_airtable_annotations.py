#!/usr/bin/env python3
"""
Convert Airtable annotation XMLs into string_match_results JSON format.

This script reads Airtable annotation XML files and converts them to the
string_match_results JSON format used by the string-match review tool.

Usage:
    python3 scripts/convert_airtable_annotations.py
    python3 scripts/convert_airtable_annotations.py frus1981-88v04 frus1981-88v05
"""

import sys
import json
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional, Set
from collections import defaultdict
import re
from lxml import etree

BASE_DIR = Path(__file__).resolve().parent.parent

# XML namespaces
TEI_NS = "http://www.tei-c.org/ns/1.0"
FRUS_NS = "http://history.state.gov/frus/ns/1.0"
NSMAP = {"tei": TEI_NS, "frus": FRUS_NS}


def get_document_metadata(volume_id: str, doc_id: str) -> Dict[str, Any]:
    """
    Extract metadata (title, date, doc_type) from split document XML.

    Returns dict with keys: title, date, doc_type (empty strings if not found).
    """
    doc_path = BASE_DIR / "data" / "documents" / volume_id / f"{doc_id}.xml"

    result = {
        "title": "",
        "date": "",
        "doc_type": ""
    }

    if not doc_path.exists():
        return result

    try:
        tree = etree.parse(str(doc_path))
        root = tree.getroot()

        # Find the document div
        doc_div = root.xpath('//tei:div[@type="document"]', namespaces=NSMAP)
        if not doc_div:
            return result

        doc_div = doc_div[0]

        # Get doc_type from subtype attribute
        result["doc_type"] = doc_div.get("subtype", "")

        # Get date from frus:doc-dateTime-min attribute
        date_str = doc_div.get(f"{{{FRUS_NS}}}doc-dateTime-min", "")
        if date_str:
            # Parse ISO 8601 datetime and format nicely
            # Format: "2025-11-20T00:00:00-05:00" -> "November 20, 1975"
            try:
                # Extract date part (YYYY-MM-DD)
                date_part = date_str.split("T")[0]
                dt = datetime.fromisoformat(date_part)
                result["date"] = dt.strftime("%B %d, %Y")
            except (ValueError, IndexError):
                pass

        # Get title from head element, excluding notes
        head_elem = doc_div.find(".//tei:head", NSMAP)
        if head_elem is not None:
            # Get text from head but skip note elements
            parts = []
            if head_elem.text:
                parts.append(head_elem.text)
            for child in head_elem:
                tag = child.tag if isinstance(child.tag, str) else ""
                if f"{{{TEI_NS}}}note" not in tag:
                    parts.append(etree.tostring(child, method="text", encoding="unicode"))
                if f"{{{TEI_NS}}}note" not in tag and child.tail:
                    parts.append(child.tail)
            title_text = re.sub(r"\s+", " ", "".join(parts)).strip()
            # Remove leading document number if present (e.g., "1. ")
            title_text = re.sub(r"^\d+\.\s*", "", title_text)
            result["title"] = title_text

        return result

    except Exception as e:
        # Silently fail and return empty metadata
        return result


def ensure_volume_split(volume_id: str) -> bool:
    """
    Check if volume documents are split.
    Returns True if documents are available, False otherwise.
    """
    docs_dir = BASE_DIR / "data" / "documents" / volume_id

    if docs_dir.exists() and len(list(docs_dir.glob("*.xml"))) > 0:
        return True

    # Documents not found
    print(f"    WARNING: Documents not split for {volume_id}")
    print(f"    Run: python3 scripts/split_volume.py {volume_id}")
    return False


def parse_airtable_xml(xml_path: Path) -> tuple[Optional[str], List[Dict[str, Any]]]:
    """
    Parse Airtable annotation XML file using iterparse for memory efficiency.
    Returns (volume_id, list of entry dicts).
    """
    try:
        volume_id = None
        entries = []

        for event, elem in ET.iterparse(str(xml_path), events=("end",)):
            if elem.tag == "volume_id" and volume_id is None:
                volume_id = elem.text
            elif elem.tag == "entry":
                # Extract basic fields
                entry_data = {}
                entry_data["recordID"] = elem.findtext("recordID", "")
                entry_data["table_name"] = elem.findtext("table_name", "")
                entry_data["annotation_content"] = elem.findtext("annotation_content", "")
                entry_data["entity_name"] = elem.findtext("entity_name", "")

                # Extract documents
                entry_data["documents"] = []
                documents = elem.find("documents")
                if documents is not None:
                    for doc in documents.findall("document"):
                        doc_number = doc.findtext("doc_number", "")
                        if doc_number:
                            # Extract doc_id from "frus1981-88v04#d144" format
                            doc_id = doc_number.split("#")[-1] if "#" in doc_number else doc_number
                            entry_data["documents"].append(doc_id)

                entries.append(entry_data)
                # Clear the element to free memory
                elem.clear()

        return volume_id, entries

    except Exception as e:
        print(f"  ERROR parsing {xml_path}: {e}")
        return None, []


def build_results(volume_id: str, entries: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Build string_match_results structure from Airtable entries.
    """
    # Ensure documents are split for metadata extraction
    ensure_volume_split(volume_id)

    # Initialize result structure
    result = {
        "metadata": {
            "volume_id": volume_id,
            "generated": datetime.utcnow().isoformat(),
            "total_documents": 0,
            "documents_with_matches": 0,
            "total_matches": 0,
            "unique_terms_matched": 0,
            "total_terms_searched": 0,
            "terms_not_matched": 0,
            "min_term_length": 0,
            "stoplist_applied": False,
            "stoplisted_terms": 0,
            "variant_consolidation_applied": False,
            "variant_groups_count": 0,
            "variant_names_added": 0,
            "consolidated_matches": 0
        },
        "by_document": {},
        "by_term": {},
        "unmatched_terms": []
    }

    # Track statistics
    all_doc_ids: Set[str] = set()
    term_occurrences: Dict[str, int] = defaultdict(int)

    # Process each entry (each is a unique term/entity)
    entry_count = 0
    for entry in entries:
        entry_count += 1
        if entry_count % 500 == 0:
            print(f"    Processing entry {entry_count}/{len(entries)}...")

        record_id = entry["recordID"]
        term = entry["entity_name"] or entry["annotation_content"]
        matched_text = entry["annotation_content"]
        category = entry["table_name"]

        # Build entry for by_term if not exists
        if record_id not in result["by_term"]:
            result["by_term"][record_id] = {
                "term": term,
                "type": "topic",
                "category": category,
                "subcategory": "",
                "lcsh_uri": "",
                "lcsh_match": "",
                "documents": {},
                "total_occurrences": 0,
                "variant_names": [],
                "variant_refs": [],
                "document_count": 0
            }

        # Process each document where this term appears
        for doc_id in entry["documents"]:
            all_doc_ids.add(doc_id)
            term_occurrences[record_id] += 1

            # Initialize document entry if needed
            if doc_id not in result["by_document"]:
                metadata = get_document_metadata(volume_id, doc_id)
                result["by_document"][doc_id] = {
                    "title": metadata["title"],
                    "date": metadata["date"],
                    "doc_type": metadata["doc_type"],
                    "match_count": 0,
                    "unique_terms": 0,
                    "matches": [],
                    "body_length": 0,
                    "_term_set": set()  # Internal tracking for unique terms
                }

            # Add to matches array for this document
            match_obj = {
                "term": term,
                "ref": record_id,
                "canonical_ref": record_id,
                "matched_ref": record_id,
                "type": "topic",
                "category": category,
                "subcategory": "",
                "lcsh_uri": "",
                "lcsh_match": "",
                "position": 0,
                "matched_text": matched_text,
                "sentence": "",
                "is_variant_form": False,
                "is_consolidated": False
            }
            result["by_document"][doc_id]["matches"].append(match_obj)
            result["by_document"][doc_id]["_term_set"].add(record_id)

            # Add to by_term documents if not exists
            if doc_id not in result["by_term"][record_id]["documents"]:
                result["by_term"][record_id]["documents"][doc_id] = []

            result["by_term"][record_id]["documents"][doc_id].append({
                "sentence": "",
                "matched_text": matched_text,
                "position": 0,
                "matched_ref": record_id,
                "is_consolidated": False,
                "is_variant_form": False
            })

    # Finalize by_document stats
    for doc_id, doc_data in result["by_document"].items():
        doc_data["match_count"] = len(doc_data["matches"])
        doc_data["unique_terms"] = len(doc_data["_term_set"])
        del doc_data["_term_set"]  # Remove internal tracking

    # Finalize by_term stats
    unique_terms = len(result["by_term"])
    for record_id, term_data in result["by_term"].items():
        term_data["total_occurrences"] = term_occurrences[record_id]
        term_data["document_count"] = len(term_data["documents"])

    # Calculate metadata statistics
    docs_with_matches = len(result["by_document"])
    total_matches = sum(doc["match_count"] for doc in result["by_document"].values())

    result["metadata"]["total_documents"] = len(all_doc_ids)
    result["metadata"]["documents_with_matches"] = docs_with_matches
    result["metadata"]["total_matches"] = total_matches
    result["metadata"]["unique_terms_matched"] = unique_terms

    return result


def convert_volume(volume_id: str) -> bool:
    """
    Convert annotations for a single volume.
    Returns True if successful.
    """
    annotation_path = BASE_DIR / "annotations" / f"annotations_{volume_id}.xml"
    output_dir = BASE_DIR / "data" / "documents" / volume_id
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"string_match_results_{volume_id}.json"

    print(f"\nProcessing {volume_id}...")

    if not annotation_path.exists():
        print(f"  ERROR: Annotation file not found: {annotation_path}")
        return False

    # Parse annotations
    parsed_volume_id, entries = parse_airtable_xml(annotation_path)

    if not parsed_volume_id:
        print(f"  ERROR: Could not parse volume_id from {annotation_path}")
        return False

    if parsed_volume_id != volume_id:
        print(f"  WARNING: Volume ID mismatch: expected {volume_id}, got {parsed_volume_id}")

    print(f"  Parsed {len(entries)} entries from annotations")

    # Build results
    results = build_results(volume_id, entries)

    # Write output
    try:
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, ensure_ascii=False)

        print(f"  Wrote {output_path}")
        print(f"    Total matches: {results['metadata']['total_matches']}")
        print(f"    Unique terms: {results['metadata']['unique_terms_matched']}")
        print(f"    Documents with matches: {results['metadata']['documents_with_matches']}")

        return True

    except Exception as e:
        print(f"  ERROR writing output: {e}")
        return False


def main():
    """Main entry point."""
    if len(sys.argv) > 1:
        # Convert specified volumes
        volume_ids = sys.argv[1:]
    else:
        # Find all annotation files
        annotation_dir = BASE_DIR / "annotations"
        annotation_files = sorted(annotation_dir.glob("annotations_*.xml"))
        volume_ids = [
            f.stem.replace("annotations_", "")
            for f in annotation_files
        ]

    if not volume_ids:
        print("No annotation files found")
        return 1

    print(f"Converting {len(volume_ids)} volume(s)...")

    success_count = 0
    for volume_id in volume_ids:
        if convert_volume(volume_id):
            success_count += 1

    print(f"\n{'='*60}")
    print(f"Done. {success_count}/{len(volume_ids)} volumes converted successfully.")

    return 0 if success_count == len(volume_ids) else 1


if __name__ == "__main__":
    sys.exit(main())
