#!/usr/bin/env python3
"""
Merge annotated document content back into the main TEI volume file.

Reads individual annotated document files from data/documents/<volume>/
and replaces the corresponding <div type="document"> content in the
main TEI file with the annotated version.
"""

import os
import sys
import shutil
from lxml import etree

TEI_NS = "http://www.tei-c.org/ns/1.0"
XML_NS = "http://www.w3.org/XML/1998/namespace"
FRUS_NS = "http://history.state.gov/frus/ns/1.0"
XI_NS = "http://www.w3.org/2001/XInclude"

NSMAP = {
    "tei": TEI_NS,
    "frus": FRUS_NS,
    "xi": XI_NS,
}


def merge_annotations(volume_id: str, base_dir: str, frus_volumes_dir: str = None):
    # Try local tei/ first, then fall back to frus/volumes/
    tei_file = os.path.join(base_dir, "tei", f"{volume_id}.xml")
    if not os.path.exists(tei_file) and frus_volumes_dir:
        tei_file = os.path.join(frus_volumes_dir, f"{volume_id}.xml")
    docs_dir = os.path.join(base_dir, "data", "documents", volume_id)
    output_file = os.path.join(base_dir, "tei", f"{volume_id}-annotated.xml")

    if not os.path.exists(tei_file):
        print(f"ERROR: TEI file not found: {tei_file}")
        return None
    if not os.path.isdir(docs_dir):
        print(f"ERROR: Documents directory not found: {docs_dir}")
        return None

    # Parse the main TEI file, preserving comments and PIs
    parser = etree.XMLParser(remove_blank_text=False, strip_cdata=False)
    # Lenient parser for annotated docs (some have duplicate xml:id in listPerson)
    recover_parser = etree.XMLParser(remove_blank_text=False, strip_cdata=False, recover=True)
    print(f"Parsing main TEI file: {tei_file}")
    tree = etree.parse(tei_file, parser)
    root = tree.getroot()

    # Build index of document divs by xml:id
    doc_divs = {}
    for div in root.iter(f"{{{TEI_NS}}}div"):
        if div.get("type") == "document":
            xml_id = div.get(f"{{{XML_NS}}}id")
            if xml_id:
                doc_divs[xml_id] = div

    print(f"Found {len(doc_divs)} document divs in main TEI")

    # List annotated document files
    def sort_key(filename):
        name = filename.replace(".xml", "")
        if name.startswith("d") and name[1:].isdigit():
            return (0, int(name[1:]), "")
        return (1, 0, name)

    doc_files = sorted(
        [f for f in os.listdir(docs_dir) if f.endswith(".xml")],
        key=sort_key
    )
    print(f"Found {len(doc_files)} annotated document files")

    merged = 0
    skipped = 0
    errors = 0

    for doc_file in doc_files:
        doc_id = doc_file.replace(".xml", "")  # e.g., "d1"
        doc_path = os.path.join(docs_dir, doc_file)

        if doc_id not in doc_divs:
            print(f"  SKIP: No matching div for {doc_id}")
            skipped += 1
            continue

        try:
            # Parse annotated document (use recover parser for duplicate ID tolerance)
            try:
                doc_tree = etree.parse(doc_path, parser)
            except etree.XMLSyntaxError:
                doc_tree = etree.parse(doc_path, recover_parser)
            doc_root = doc_tree.getroot()

            # Find the <body> element
            body = doc_root.find(f".//{{{TEI_NS}}}body")
            if body is None:
                # Try text/body path
                body = doc_root.find(f"{{{TEI_NS}}}text/{{{TEI_NS}}}body")
            if body is None:
                print(f"  SKIP: No <body> found in {doc_file}")
                skipped += 1
                continue

            # Get the target div
            target_div = doc_divs[doc_id]

            # Preserve the div's attributes
            div_attribs = dict(target_div.attrib)

            # Clear the div's children and text
            target_div.text = body.text
            for child in list(target_div):
                target_div.remove(child)

            # Copy children from annotated body into the div
            for child in body:
                target_div.append(child)

            # The div's tail should remain as-is (whitespace after closing tag)
            # Restore attributes (they should still be there, but just in case)
            target_div.attrib.update(div_attribs)

            merged += 1

        except etree.XMLSyntaxError as e:
            print(f"  ERROR parsing {doc_file}: {e}")
            errors += 1
        except Exception as e:
            print(f"  ERROR processing {doc_file}: {e}")
            errors += 1

    print(f"\nResults: {merged} merged, {skipped} skipped, {errors} errors")

    # Write output
    print(f"Writing annotated TEI to: {output_file}")
    tree.write(
        output_file,
        xml_declaration=True,
        encoding="UTF-8",
        pretty_print=False,  # preserve original formatting
    )

    # Verify output
    output_size = os.path.getsize(output_file)
    input_size = os.path.getsize(tei_file)
    print(f"Input size:  {input_size:,} bytes")
    print(f"Output size: {output_size:,} bytes")
    print(f"Difference:  {output_size - input_size:+,} bytes (annotation markup added)")

    return output_file


if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    frus_volumes_dir = os.path.join(base_dir, "..", "frus", "volumes")

    # If --all flag, process every volume that has annotated documents
    if len(sys.argv) > 1 and sys.argv[1] == "--all":
        docs_root = os.path.join(base_dir, "data", "documents")
        # Only process directories that look like volume IDs (start with "frus")
        volumes = sorted([
            d for d in os.listdir(docs_root)
            if os.path.isdir(os.path.join(docs_root, d)) and d.startswith("frus")
            # Skip editorial variant dirs like frus1977-80v11p1-chris
            and d.count("-") <= 3
        ])
        print(f"=== Batch processing {len(volumes)} volumes ===\n")
        results = {}
        for vol in volumes:
            print(f"\n{'='*60}")
            print(f"Processing: {vol}")
            print(f"{'='*60}")
            result = merge_annotations(vol, base_dir, frus_volumes_dir)
            results[vol] = "OK" if result else "FAILED"

        print(f"\n\n{'='*60}")
        print("BATCH SUMMARY")
        print(f"{'='*60}")
        for vol, status in results.items():
            print(f"  {vol}: {status}")
    else:
        volume_id = sys.argv[1] if len(sys.argv) > 1 else "frus1981-88v41"
        print(f"Volume: {volume_id}")
        print(f"Base dir: {base_dir}")
        merge_annotations(volume_id, base_dir, frus_volumes_dir)
