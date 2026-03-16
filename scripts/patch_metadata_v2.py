#!/usr/bin/env python3
"""
Patch metadata in string_match_results_*.json files by extracting information
from the corresponding split document XML files.
"""

import json
import os
import re
import sys
from pathlib import Path
from datetime import datetime
from xml.etree import ElementTree as ET
from collections import defaultdict

# Namespace definitions
TEI_NS = "http://www.tei-c.org/ns/1.0"
FRUS_NS = "http://history.state.gov/frus/ns/1.0"

# Register namespaces
ET.register_namespace('', TEI_NS)
ET.register_namespace('frus', FRUS_NS)

# Base paths
BASE_PATH = "/sessions/adoring-wonderful-faraday/mnt/frus-subject-taxonomy"
JSON_DIR = os.path.join(BASE_PATH, "data/documents")
DOCS_DIR = os.path.join(BASE_PATH, "data/documents")

# Cache for parsed XML
XML_CACHE = {}


def parse_date(date_str):
    """
    Parse a datetime string from frus:doc-dateTime-min and return readable format.
    Expected format: "1975-11-20T00:00:00-05:00" -> "November 20, 1975"
    """
    if not date_str:
        return ""

    try:
        # Parse ISO format datetime
        dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        # Format as "Month Day, Year"
        return dt.strftime("%B %d, %Y")
    except (ValueError, AttributeError):
        return ""


def extract_head_text(head_elem):
    """
    Extract text from head element, excluding note children.
    """
    if head_elem is None:
        return ""

    # Get all text content except from note elements
    text_parts = []

    # Add direct text
    if head_elem.text:
        text_parts.append(head_elem.text)

    # Iterate through children, skipping notes
    for child in head_elem:
        if child.tag != f"{{{TEI_NS}}}note":
            # Add text from child
            if child.text:
                text_parts.append(child.text)
            # Add tail text
            if child.tail:
                text_parts.append(child.tail)
        else:
            # Still add tail text from note element
            if child.tail:
                text_parts.append(child.tail)

    full_text = "".join(text_parts).strip()

    # Strip leading doc number like "5. "
    full_text = re.sub(r"^\d+\.\s+", "", full_text)

    return full_text


def extract_metadata_from_xml(volume_id, doc_id):
    """
    Extract title, date, and doc_type from XML file.

    Args:
        volume_id: e.g., "frus1981-88v01"
        doc_id: e.g., "d1"

    Returns:
        dict with keys: title, date, doc_type
    """
    cache_key = f"{volume_id}:{doc_id}"
    if cache_key in XML_CACHE:
        return XML_CACHE[cache_key]

    xml_path = os.path.join(DOCS_DIR, volume_id, f"{doc_id}.xml")

    if not os.path.exists(xml_path):
        result = {"title": "", "date": "", "doc_type": ""}
        XML_CACHE[cache_key] = result
        return result

    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()

        # Find the document div
        namespaces = {'tei': TEI_NS, 'frus': FRUS_NS}
        doc_div = root.find(".//tei:div[@type='document']", namespaces)

        if doc_div is None:
            result = {"title": "", "date": "", "doc_type": ""}
            XML_CACHE[cache_key] = result
            return result

        # Extract title from head element
        head = doc_div.find("tei:head", namespaces)
        title = extract_head_text(head)

        # Extract date from frus:doc-dateTime-min attribute
        date_attr = doc_div.get(f"{{{FRUS_NS}}}doc-dateTime-min", "")
        date = parse_date(date_attr)

        # Extract doc_type from subtype attribute
        doc_type = doc_div.get("subtype", "")

        result = {
            "title": title,
            "date": date,
            "doc_type": doc_type
        }
        XML_CACHE[cache_key] = result
        return result

    except Exception as e:
        print(f"Error parsing {xml_path}: {e}", file=sys.stderr)
        result = {"title": "", "date": "", "doc_type": ""}
        XML_CACHE[cache_key] = result
        return result


def patch_json_file(json_path):
    """
    Patch metadata in a single JSON file.

    Returns:
        dict with count of patched documents
    """
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"ERROR: Invalid JSON in {json_path}: {e}", file=sys.stderr)
        return {"patched": 0, "volume_id": "unknown", "error": True}

    volume_id = data.get('metadata', {}).get('volume_id')
    if not volume_id:
        return {"patched": 0, "volume_id": "unknown"}

    patch_count = 0
    patched_docs = []

    # Iterate through documents
    for doc_id, doc_data in data.get('by_document', {}).items():
        metadata = extract_metadata_from_xml(volume_id, doc_id)

        # Update fields if we found metadata
        if metadata['title'] or metadata['date'] or metadata['doc_type']:
            doc_data['title'] = metadata['title']
            doc_data['date'] = metadata['date']
            doc_data['doc_type'] = metadata['doc_type']
            patch_count += 1
            patched_docs.append(doc_id)

    # Write back to JSON file only if changes were made
    if patch_count > 0:
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    return {"patched": patch_count, "volume_id": volume_id}


def main():
    """Main function to process all string_match_results_*.json files."""

    # Find all JSON files
    json_files = sorted(Path(JSON_DIR).glob("*/string_match_results_*.json"))

    if not json_files:
        print("No string_match_results_*.json files found in data/documents/*/")
        return

    print(f"Found {len(json_files)} JSON files to process\n", flush=True)

    # Track results by volume
    results = defaultdict(int)
    total_patched = 0

    for json_file in json_files:
        print(f"Processing {json_file.name}...", end=" ", flush=True)

        result = patch_json_file(str(json_file))
        volume_id = result['volume_id']
        patch_count = result['patched']
        has_error = result.get('error', False)

        if has_error:
            print(f"✗ (skipped - invalid JSON)", flush=True)
        else:
            results[volume_id] = patch_count
            total_patched += patch_count
            print(f"✓ ({patch_count} documents patched)", flush=True)

    # Print summary
    print("\n" + "="*60, flush=True)
    print("SUMMARY - Documents patched per volume:", flush=True)
    print("="*60, flush=True)

    for volume_id in sorted(results.keys()):
        count = results[volume_id]
        print(f"{volume_id}: {count} documents", flush=True)

    print("="*60, flush=True)
    print(f"Total documents patched: {total_patched}", flush=True)


if __name__ == "__main__":
    main()
