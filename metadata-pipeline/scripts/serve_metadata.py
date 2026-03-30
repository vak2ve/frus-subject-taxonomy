#!/usr/bin/env python3
"""
serve_metadata.py — Dev server for metadata pipeline review tools.

Serves the metadata pipeline's HTML review tools and provides API endpoints
for saving/loading review decisions. Runs on port 9091 (parallel to the main
pipeline's server on 9090).

Usage:
    python3 serve_metadata.py
    python3 serve_metadata.py --port 9091
"""

import argparse
import json
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path

try:
    from flask import Flask, jsonify, request, send_from_directory
except ImportError:
    print("Flask is required. Install with: pip install flask")
    sys.exit(1)

SCRIPT_DIR = Path(__file__).resolve().parent
PIPELINE_DIR = SCRIPT_DIR.parent
REPO_ROOT = PIPELINE_DIR.parent
DATA_DIR = PIPELINE_DIR / "data"
STATE_FILE = PIPELINE_DIR / "metadata_review_state.json"
BACKUP_DIR = PIPELINE_DIR / "backups"

app = Flask(__name__, static_folder=str(PIPELINE_DIR))

# Import the fast TEI scanner
sys.path.insert(0, str(SCRIPT_DIR))
from scan_tei_headers import scan_doc_header, format_category_label, natural_sort_key

DOC_DATA_DIR = REPO_ROOT / "data" / "documents"

# Cache for scanned volume data (avoid rescanning on every request)
_volume_cache = {}


def scan_volume_for_review(vol_id):
    """Scan a volume's TEI headers on-demand for the annotation review tool."""
    if vol_id in _volume_cache:
        return _volume_cache[vol_id]

    vol_dir = DOC_DATA_DIR / vol_id
    if not vol_dir.is_dir():
        return None

    doc_files = sorted(vol_dir.glob("d*.xml"), key=lambda p: natural_sort_key(p.stem))

    by_document = {}
    by_term = {}
    total_annotations = 0
    docs_with_annotations = 0
    unique_refs = set()

    for doc_path in doc_files:
        doc_id = doc_path.stem
        result = scan_doc_header(doc_path)

        if result is None:
            by_document[doc_id] = {
                "title": "", "date": "", "doc_type": "historical-document",
                "match_count": 0, "unique_terms": 0, "matches": [],
                "has_header": False,
            }
            continue

        title, date_str, annotations = result
        seen_refs = set()
        matches = []

        for ann in annotations:
            ref = ann["ref"]
            if ref in seen_refs:
                continue
            seen_refs.add(ref)
            unique_refs.add(ref)
            total_annotations += 1

            matches.append({
                "term": ann["term"], "ref": ref,
                "canonical_ref": ref, "matched_ref": ref,
                "type": ann["type"],
                "category": ann["category"],
                "subcategory": ann["subcategory"],
                "lcsh_uri": ann["lcsh_uri"],
                "lcsh_match": ann["lcsh_match"],
                "position": 0, "matched_text": ann["term"],
                "sentence": "", "source": "tei-header",
            })

            # Accumulate by_term
            if ref not in by_term:
                by_term[ref] = {
                    "term": ann["term"], "ref": ref,
                    "type": ann["type"],
                    "category": ann["category"],
                    "subcategory": ann["subcategory"],
                    "lcsh_uri": ann["lcsh_uri"],
                    "lcsh_match": ann["lcsh_match"],
                    "documents": [], "total_occurrences": 0,
                }
            by_term[ref]["documents"].append(doc_id)
            by_term[ref]["total_occurrences"] += 1

        if matches:
            docs_with_annotations += 1

        by_document[doc_id] = {
            "title": title, "date": date_str,
            "doc_type": "historical-document",
            "match_count": len(matches),
            "unique_terms": len(matches),
            "matches": matches,
            "has_header": True,
        }

    result = {
        "metadata": {
            "volume_id": vol_id,
            "source": "tei-header-metadata",
            "generated": datetime.now().isoformat(),
            "total_documents": len(doc_files),
            "documents_with_headers": sum(1 for d in by_document.values() if d.get("has_header")),
            "documents_with_annotations": docs_with_annotations,
            "total_annotations": total_annotations,
            "unique_terms_found": len(unique_refs),
            "header_coverage": round(sum(1 for d in by_document.values() if d.get("has_header")) / len(doc_files) * 100, 1) if doc_files else 0,
            "annotation_coverage": round(docs_with_annotations / len(doc_files) * 100, 1) if doc_files else 0,
            # Compat
            "total_matches": total_annotations,
            "unique_terms_matched": len(unique_refs),
            "documents_with_matches": docs_with_annotations,
        },
        "by_document": by_document,
        "by_term": by_term,
        "unmatched_terms": [],
    }

    _volume_cache[vol_id] = result
    return result


# === Static file serving ===

@app.route("/")
def index():
    """List available review tools."""
    tools = []
    for html_file in sorted(PIPELINE_DIR.glob("*.html")):
        tools.append(html_file.name)
    return f"""<!DOCTYPE html>
<html><head><title>Metadata Pipeline</title>
<style>
body {{ font-family: -apple-system, sans-serif; max-width: 600px; margin: 40px auto; }}
h1 {{ color: #0d7377; }}
a {{ color: #0d7377; text-decoration: none; font-size: 16px; }}
a:hover {{ text-decoration: underline; }}
li {{ margin: 8px 0; }}
.badge {{ background: #e6f4f4; color: #0d7377; padding: 2px 8px; border-radius: 4px; font-size: 11px; }}
</style></head>
<body>
<h1>Metadata Pipeline Review Tools</h1>
<p>TEI Header Source <span class="badge">metadata-pipeline</span></p>
<ul>
{"".join(f'<li><a href="/{name}">{name}</a></li>' for name in tools)}
</ul>
<hr>
<p style="color:#71767a;font-size:13px">
State file: {STATE_FILE}<br>
Data directory: {DATA_DIR}
</p>
</body></html>"""


@app.route("/<path:filename>")
def serve_file(filename):
    """Serve files from the metadata pipeline directory."""
    return send_from_directory(str(PIPELINE_DIR), filename)


# === API: Annotation review decisions ===

@app.route("/api/metadata/save-decisions", methods=["POST"])
def save_annotation_decisions():
    """Save annotation-level review decisions for a volume."""
    data = request.json
    vol_id = data.get("volume_id", "")
    if not vol_id:
        return jsonify({"error": "volume_id required"}), 400

    # Save per-volume rejections
    vol_dir = DATA_DIR / vol_id
    vol_dir.mkdir(parents=True, exist_ok=True)
    out_path = vol_dir / f"annotation_rejections_{vol_id}.json"

    payload = {
        "volume_id": vol_id,
        "exported": datetime.now().isoformat(),
        "pipeline": "metadata",
        "rejections": data.get("rejections", []),
        "total_rejections": len(data.get("rejections", [])),
    }

    _backup_file(out_path)
    with open(out_path, "w") as f:
        json.dump(payload, f, indent=2)

    return jsonify({
        "status": "ok",
        "volume_id": vol_id,
        "rejections_saved": payload["total_rejections"],
        "path": str(out_path),
    })


@app.route("/api/metadata/scan-volume/<volume_id>")
def scan_volume_endpoint(volume_id):
    """Scan a volume's TEI headers on-demand and return review-compatible JSON."""
    result = scan_volume_for_review(volume_id)
    if result is None:
        return jsonify({"error": f"Volume {volume_id} not found"}), 404
    return jsonify(result)


@app.route("/api/metadata/load-decisions/<volume_id>")
def load_annotation_decisions(volume_id):
    """Load saved annotation decisions for a volume."""
    path = DATA_DIR / volume_id / f"annotation_rejections_{volume_id}.json"
    if not path.exists():
        return jsonify({"volume_id": volume_id, "rejections": []})

    with open(path) as f:
        return jsonify(json.load(f))


# === API: Taxonomy review decisions ===

@app.route("/api/metadata/save-taxonomy-decisions", methods=["POST"])
def save_taxonomy_decisions():
    """Save taxonomy-level review decisions."""
    data = request.json

    # Load existing state and merge
    state = _load_state()
    state.update(data)
    state["saved"] = datetime.now().isoformat()

    _backup_file(STATE_FILE)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)

    return jsonify({
        "status": "ok",
        "exclusions": len(state.get("exclusions", {})),
        "merges": len(state.get("merge_decisions", {})),
        "overrides": len(state.get("category_overrides", {})),
    })


@app.route("/api/metadata/load-taxonomy-decisions")
def load_taxonomy_decisions():
    """Load taxonomy review state."""
    return jsonify(_load_state())


# === Helpers ===

def _load_state():
    """Load the metadata review state file."""
    if STATE_FILE.exists():
        with open(STATE_FILE) as f:
            return json.load(f)
    return {}


def _backup_file(path):
    """Create a backup of a file before overwriting."""
    if not Path(path).exists():
        return
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    name = Path(path).name
    backup_path = BACKUP_DIR / f"{name}.{ts}"
    shutil.copy2(path, backup_path)

    # Keep only 20 most recent backups per file
    pattern = f"{name}.*"
    backups = sorted(BACKUP_DIR.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    for old in backups[20:]:
        old.unlink()


def main():
    parser = argparse.ArgumentParser(description="Metadata pipeline dev server")
    parser.add_argument("--port", type=int, default=9091, help="Port (default: 9091)")
    parser.add_argument("--host", default="127.0.0.1", help="Host (default: 127.0.0.1)")
    args = parser.parse_args()

    print(f"=== Metadata Pipeline Server ===")
    print(f"URL: http://{args.host}:{args.port}")
    print(f"State: {STATE_FILE}")
    print(f"Data: {DATA_DIR}")
    print()

    app.run(host=args.host, port=args.port, debug=True)


if __name__ == "__main__":
    main()
