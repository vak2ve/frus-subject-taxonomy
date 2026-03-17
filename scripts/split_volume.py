#!/usr/bin/env python3
"""
Split a monolithic FRUS TEI/XML volume into individual document files.

Python equivalent of queries/split-volume.xq for environments without BaseX.

Usage:
    python3 scripts/split_volume.py frus1981-88v10
    python3 scripts/split_volume.py frus1981-88v01 frus1981-88v12 frus1981-88v33
"""

import sys
import os
import re
from pathlib import Path
from lxml import etree

TEI_NS = "http://www.tei-c.org/ns/1.0"
FRUS_NS = "http://history.state.gov/frus/ns/1.0"
NSMAP = {"tei": TEI_NS, "frus": FRUS_NS}

BASE_DIR = Path(__file__).resolve().parent.parent


def split_volume(volume_id: str) -> int:
    """Split a volume XML into individual document files. Returns doc count."""
    volume_path = BASE_DIR / "volumes" / f"{volume_id}.xml"
    output_dir = BASE_DIR / "data" / "documents" / volume_id

    if not volume_path.exists():
        print(f"  ERROR: Volume file not found: {volume_path}")
        return 0

    print(f"  Parsing {volume_path.name} ({volume_path.stat().st_size / 1e6:.1f} MB)...")
    tree = etree.parse(str(volume_path))

    # Find all document divs (historical-document and editorial-note)
    docs = tree.xpath(
        '//tei:div[@type="document"]'
        '[@subtype="historical-document" or @subtype="editorial-note"]',
        namespaces=NSMAP,
    )

    if not docs:
        print(f"  ERROR: No document divs found in {volume_id}")
        return 0

    output_dir.mkdir(parents=True, exist_ok=True)

    for doc_div in docs:
        xml_id = doc_div.get("{http://www.w3.org/XML/1998/namespace}id")
        if not xml_id:
            continue

        # Wrap in minimal TEI envelope
        root = etree.Element(
            "TEI",
            nsmap={None: TEI_NS, "frus": FRUS_NS},
        )
        text_el = etree.SubElement(root, "text")
        body_el = etree.SubElement(text_el, "body")
        body_el.append(doc_div)

        out_path = output_dir / f"{xml_id}.xml"
        etree.ElementTree(root).write(
            str(out_path),
            xml_declaration=True,
            encoding="UTF-8",
            pretty_print=False,
        )

    print(f"  Split {volume_id}: {len(docs)} documents -> {output_dir}")
    return len(docs)


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 scripts/split_volume.py <volume-id> [volume-id ...]")
        sys.exit(1)

    total = 0
    errors = 0
    for volume_id in sys.argv[1:]:
        print(f"\nProcessing {volume_id}...")
        try:
            count = split_volume(volume_id)
            if count == 0:
                errors += 1
            total += count
        except Exception as e:
            print(f"  ERROR: Failed to split {volume_id}: {e}")
            errors += 1

    print(f"\nDone. {total} documents split across {len(sys.argv) - 1} volume(s).")
    if errors > 0:
        print(f"  {errors} volume(s) had errors.")
        sys.exit(1)


if __name__ == "__main__":
    main()
