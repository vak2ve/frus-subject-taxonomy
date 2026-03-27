#!/usr/bin/env python3
"""
generate_taxonomy_id_map.py — Build taxonomy_label_to_id.json from taxonomy XML.

Parses config/taxonomy.xml (or subject-taxonomy-lcsh.xml) and extracts all
<category> elements, mapping their text labels to their xml:id attributes.

This mapping is used by inject_tei_headers.py to resolve human-readable
category/subcategory names to canonical taxonomy IDs for @ref, @category,
and @subcategory attributes on <term> elements.

Usage:
    python3 scripts/generate_taxonomy_id_map.py
    python3 scripts/generate_taxonomy_id_map.py --taxonomy path/to/taxonomy.xml
"""

import argparse
import json
from pathlib import Path

from lxml import etree

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
CONFIG_DIR = REPO_ROOT / "config"


def find_taxonomy_xml():
    """Locate the taxonomy XML file, trying several known paths."""
    candidates = [
        CONFIG_DIR / "taxonomy.xml",
        REPO_ROOT / "subject-taxonomy-lcsh.xml",
        CONFIG_DIR / "subject-taxonomy-lcsh.xml",
    ]
    for p in candidates:
        if p.exists():
            return p
    return None


def extract_mappings(xml_path):
    """
    Parse taxonomy XML and extract label -> ID mappings.
    Handles both namespaced (TEI) and non-namespaced elements.
    """
    tree = etree.parse(str(xml_path))
    root = tree.getroot()
    mapping = {}

    # Try both namespaced and plain element names
    categories = root.findall(".//{http://www.tei-c.org/ns/1.0}category")
    if not categories:
        categories = root.findall(".//category")

    for cat in categories:
        # Get the xml:id attribute (may be stored different ways by lxml)
        xml_id = (
            cat.get("{http://www.w3.org/XML/1998/namespace}id")
            or cat.get("xml:id")
            or cat.get("id")
            or ""
        )
        if not xml_id:
            continue

        # Try multiple child elements that might hold the label
        label_el = None
        for tag in ["catDesc", "gloss", "label"]:
            label_el = cat.find(f"{{{root.nsmap.get(None, '')}}{tag}") if root.nsmap.get(None) else None
            if label_el is None:
                label_el = cat.find(tag)
            if label_el is None:
                label_el = cat.find(f"{{http://www.tei-c.org/ns/1.0}}{tag}")
            if label_el is not None:
                break

        if label_el is not None:
            label_text = (label_el.text or "").strip()
            if label_text:
                mapping[label_text] = xml_id

    return mapping


def main():
    parser = argparse.ArgumentParser(description="Generate taxonomy label-to-ID mapping JSON")
    parser.add_argument("--taxonomy", help="Path to taxonomy XML file")
    args = parser.parse_args()

    if args.taxonomy:
        xml_path = Path(args.taxonomy)
    else:
        xml_path = find_taxonomy_xml()

    if not xml_path or not xml_path.exists():
        print("ERROR: Could not find taxonomy XML file.")
        print("Tried: config/taxonomy.xml, subject-taxonomy-lcsh.xml")
        print("Use --taxonomy to specify the path.")
        return

    print(f"Parsing: {xml_path}")
    mapping = extract_mappings(xml_path)
    print(f"Found {len(mapping)} label -> ID mappings")

    output_path = CONFIG_DIR / "taxonomy_label_to_id.json"
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(mapping, f, indent=2, ensure_ascii=False, sort_keys=True)

    print(f"Saved to: {output_path}")

    # Show a sample
    items = list(mapping.items())[:5]
    print("\nSample entries:")
    for label, xml_id in items:
        print(f"  '{label}' -> '{xml_id}'")


if __name__ == "__main__":
    main()
