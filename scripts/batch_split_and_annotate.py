#!/usr/bin/env python3
"""
Batch split and annotate all remaining FRUS volumes.

For each volume XML in volumes/ that doesn't yet have documents split
or annotation results, this script:
1. Splits the volume into individual document files
2. Runs string-match annotation against the taxonomy

Skips index volumes (ending in 'Index').
"""

import os
import sys
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
VOLUMES_DIR = BASE_DIR / "volumes"
DOCUMENTS_DIR = BASE_DIR / "data" / "documents"

# Import the split and annotate modules
sys.path.insert(0, str(BASE_DIR / "scripts"))
os.chdir(BASE_DIR / "scripts")


def get_remaining_volumes():
    """Find volumes that need splitting and/or annotation."""
    all_volumes = sorted(
        p.stem for p in VOLUMES_DIR.glob("*.xml")
        if not p.stem.endswith("Index")
    )

    needs_split = []
    needs_annotate = []

    for vol_id in all_volumes:
        doc_dir = DOCUMENTS_DIR / vol_id
        has_docs = doc_dir.exists() and any(doc_dir.glob("d*.xml"))
        has_results = (doc_dir / f"string_match_results_{vol_id}.json").exists()

        if not has_docs:
            needs_split.append(vol_id)
        if not has_results:
            needs_annotate.append(vol_id)

    return needs_split, needs_annotate


def main():
    needs_split, needs_annotate = get_remaining_volumes()

    print(f"Volumes needing split: {len(needs_split)}")
    print(f"Volumes needing annotation: {len(needs_annotate)}")
    print()

    # Import modules
    from split_volume import split_volume

    # For annotation, we need to set up the shared state once
    from annotate_documents import (
        load_stoplist, load_variant_groups, load_lcsh_mapping,
        load_taxonomy, expand_terms_with_variants, compile_term_patterns,
        STOPLIST_FILE, VARIANT_GROUPS_FILE, VARIANT_OVERRIDES_FILE,
        LCSH_MAPPING_FILE, TAXONOMY_FILE, MIN_TERM_LENGTH
    )

    # Pre-load annotation resources (expensive, do once)
    print("Loading annotation resources...")
    stoplist = load_stoplist(STOPLIST_FILE)
    print(f"  Stoplist: {len(stoplist)} terms")

    ref_to_canonical, canonical_info = load_variant_groups(VARIANT_GROUPS_FILE)
    print(f"  Variant groups: {len(canonical_info)}")

    lcsh_mapping = load_lcsh_mapping(LCSH_MAPPING_FILE)
    print(f"  LCSH mapping: {len(lcsh_mapping)} refs")

    terms = load_taxonomy(TAXONOMY_FILE, stoplist=stoplist)

    # Supplement LCSH data
    for t in terms:
        ref = t["ref"]
        if ref in lcsh_mapping:
            lm = lcsh_mapping[ref]
            if not t["lcsh_uri"] and lm["lcsh_uri"]:
                t["lcsh_uri"] = lm["lcsh_uri"]
                t["lcsh_match"] = lm["match_quality"]
            elif t["lcsh_uri"] and not t["lcsh_match"]:
                t["lcsh_match"] = lm.get("match_quality", "")

    if canonical_info:
        terms = expand_terms_with_variants(terms, canonical_info, ref_to_canonical, stoplist)

    compiled = compile_term_patterns(terms)
    print(f"  Terms compiled: {len(terms)}")
    print()

    # Now import the per-document processing functions
    from annotate_documents import (
        extract_body_text, extract_doc_metadata, match_document
    )
    import glob as glob_mod
    import json
    from datetime import datetime

    total_vols = len(needs_split) + len([v for v in needs_annotate if v not in needs_split])
    # Actually, all needs_split also need annotation
    all_to_process = sorted(set(needs_split + needs_annotate))

    processed = 0
    errors = []

    for vol_id in all_to_process:
        processed += 1
        print(f"\n{'='*60}")
        print(f"[{processed}/{len(all_to_process)}] Processing {vol_id}")
        print(f"{'='*60}")

        doc_dir = DOCUMENTS_DIR / vol_id
        has_docs = doc_dir.exists() and any(doc_dir.glob("d*.xml"))
        has_results = (doc_dir / f"string_match_results_{vol_id}.json").exists()

        # Step 1: Split if needed
        if not has_docs:
            try:
                count = split_volume(vol_id)
                if count == 0:
                    print(f"  WARNING: No documents found in {vol_id}, skipping annotation")
                    errors.append((vol_id, "No documents after split"))
                    continue
                print(f"  Split into {count} documents")
            except Exception as e:
                print(f"  ERROR splitting {vol_id}: {e}")
                errors.append((vol_id, f"Split error: {e}"))
                continue
        else:
            print(f"  Already split")

        # Step 2: Annotate if needed
        if not has_results:
            try:
                docs_dir_str = str(DOCUMENTS_DIR / vol_id)
                doc_files = sorted(glob_mod.glob(os.path.join(docs_dir_str, "d*.xml")))

                if not doc_files:
                    print(f"  WARNING: No doc files found for {vol_id}")
                    errors.append((vol_id, "No doc files for annotation"))
                    continue

                by_document = {}
                by_term = {}
                all_matched_refs = set()
                total_matches = 0
                combined_pattern, term_lookup = compiled

                for i, doc_path in enumerate(doc_files):
                    doc_id = os.path.splitext(os.path.basename(doc_path))[0]
                    metadata = extract_doc_metadata(doc_path)
                    text, paragraphs = extract_body_text(doc_path)

                    matches = match_document(text, compiled, ref_to_canonical)
                    total_matches += len(matches)

                    for m in matches:
                        cref = m["canonical_ref"]
                        all_matched_refs.add(cref)
                        if cref not in by_term:
                            by_term[cref] = {
                                "term": m["term"],
                                "type": m["type"],
                                "category": m["category"],
                                "subcategory": m["subcategory"],
                                "lcsh_uri": m["lcsh_uri"],
                                "lcsh_match": m.get("lcsh_match", ""),
                                "documents": {},
                                "total_occurrences": 0,
                                "variant_names": [],
                                "variant_refs": [],
                            }
                            if cref in canonical_info:
                                by_term[cref]["variant_names"] = canonical_info[cref]["variant_names"]
                                by_term[cref]["variant_refs"] = canonical_info[cref]["variant_refs"]
                        bt = by_term[cref]
                        bt["total_occurrences"] += 1
                        if doc_id not in bt["documents"]:
                            bt["documents"][doc_id] = []
                        bt["documents"][doc_id].append({
                            "sentence": m["sentence"],
                            "matched_text": m["matched_text"],
                            "position": m["position"],
                            "matched_ref": m["matched_ref"],
                            "is_consolidated": m["is_consolidated"],
                            "is_variant_form": m["is_variant_form"],
                        })

                    by_document[doc_id] = {
                        "title": metadata["title"],
                        "date": metadata["date"],
                        "doc_type": metadata["type"],
                        "match_count": len(matches),
                        "unique_terms": len(set(m["ref"] for m in matches)),
                        "matches": matches,
                        "body_length": len(text),
                    }

                # Build unmatched terms
                all_refs = {t["ref"] for t in terms if not t.get("is_variant_form")}
                unmatched_refs = all_refs - all_matched_refs
                unmatched_terms = [
                    {"term": t["term"], "ref": t["ref"], "category": t["category"], "subcategory": t["subcategory"]}
                    for t in terms if t["ref"] in unmatched_refs
                ]
                unmatched_terms.sort(key=lambda x: x["term"].lower())

                for ref, bt in by_term.items():
                    bt["document_count"] = len(bt["documents"])

                consolidated_count = sum(
                    1 for d in by_document.values()
                    for m in d["matches"]
                    if m.get("is_consolidated")
                )

                taxonomy_term_count = len([t for t in terms if not t.get("is_variant_form")])
                variant_term_count = len([t for t in terms if t.get("is_variant_form")])

                results = {
                    "metadata": {
                        "volume_id": vol_id,
                        "generated": datetime.now().isoformat(),
                        "total_documents": len(doc_files),
                        "documents_with_matches": sum(1 for d in by_document.values() if d["match_count"] > 0),
                        "total_matches": total_matches,
                        "unique_terms_matched": len(all_matched_refs),
                        "total_terms_searched": taxonomy_term_count,
                        "terms_not_matched": len(unmatched_refs),
                        "min_term_length": MIN_TERM_LENGTH,
                        "stoplist_applied": bool(stoplist),
                        "stoplisted_terms": len(stoplist),
                        "variant_consolidation_applied": bool(ref_to_canonical),
                        "variant_groups_count": len(canonical_info),
                        "variant_names_added": variant_term_count,
                        "consolidated_matches": consolidated_count,
                    },
                    "by_document": by_document,
                    "by_term": by_term,
                    "unmatched_terms": unmatched_terms,
                }

                output_dir = DOCUMENTS_DIR / vol_id
                output_dir.mkdir(parents=True, exist_ok=True)
                output_path = output_dir / f"string_match_results_{vol_id}.json"
                with open(output_path, "w") as f:
                    json.dump(results, f, indent=2, ensure_ascii=False)

                print(f"  Annotated: {len(doc_files)} docs, {total_matches} matches, {len(all_matched_refs)} unique terms")

            except Exception as e:
                print(f"  ERROR annotating {vol_id}: {e}")
                errors.append((vol_id, f"Annotate error: {e}"))
                continue
        else:
            print(f"  Already annotated")

    print(f"\n{'='*60}")
    print(f"BATCH COMPLETE")
    print(f"  Processed: {processed}/{len(all_to_process)}")
    print(f"  Errors: {len(errors)}")
    if errors:
        print(f"\nErrors:")
        for vol_id, err in errors:
            print(f"  {vol_id}: {err}")


if __name__ == "__main__":
    main()
