#!/usr/bin/env python3
"""
Development server for the FRUS Subject Taxonomy annotation pipeline.

Serves static files and provides API endpoints for saving review decisions
from the browser-based review tools to disk, so they are version-controlled
alongside the annotation data.

Usage:
    python3 serve.py              # Start on port 9090
    python3 serve.py --port 8080  # Custom port

Endpoints:
    GET  /                        Static file server (repo root)
    POST /api/save-decisions      Save annotation review decisions to disk
    GET  /api/load-decisions/<id> Load saved decisions from disk
    POST /api/validate            Run validate_data.py (SSE stream)
    POST /api/pipeline/<id>       Run post-review pipeline for a volume (SSE stream)
    POST /api/rebuild-review      Rebuild string-match-review.html (SSE stream)
"""

import argparse
import json
import subprocess
import sys
import threading
from datetime import datetime
from pathlib import Path

from flask import Flask, request, jsonify, send_from_directory, Response

BASE_DIR = Path(__file__).resolve().parent
app = Flask(__name__, static_folder=None)


# ── Static file serving ──────────────────────────────────

@app.route("/")
def index():
    resp = send_from_directory(BASE_DIR, "string-match-review.html")
    resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    return resp


@app.route("/<path:path>")
def static_files(path):
    resp = send_from_directory(BASE_DIR, path)
    resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    resp.headers["Surrogate-Control"] = "no-store"
    # Vary on everything to prevent proxy caching
    resp.headers["Vary"] = "*"
    return resp


@app.route("/api/preview")
def card_preview():
    """Serve the card layout preview as HTML."""
    preview = BASE_DIR / "card-layout-preview.html"
    if preview.exists():
        return Response(preview.read_text(), mimetype="text/html",
                        headers={"Cache-Control": "no-store"})
    return "Not found", 404


# ── API: Save decisions ──────────────────────────────────

@app.route("/api/save-decisions", methods=["POST"])
def save_decisions():
    """Save annotation review decisions to disk.

    Expects JSON body with:
        volume_id: str
        rejections: dict (match key -> true)
        lcsh_decisions: dict (ref -> "accepted"/"rejected")
        merge_decisions: dict (source_ref -> {targetRef, targetName})

    Writes to: data/documents/{volume_id}/annotation_rejections_{volume_id}.json
    """
    try:
        payload = request.get_json()
        if not payload:
            return jsonify({"error": "No JSON body"}), 400

        volume_id = payload.get("volume_id")
        if not volume_id:
            return jsonify({"error": "Missing volume_id"}), 400

        # Sanitize volume_id to prevent path traversal
        if "/" in volume_id or "\\" in volume_id or ".." in volume_id:
            return jsonify({"error": "Invalid volume_id"}), 400

        # Build the export structure matching the existing format
        rejections = payload.get("rejections", {})
        lcsh_decisions = payload.get("lcsh_decisions", {})
        merge_decisions = payload.get("merge_decisions", {})

        # Build rejection entries
        rejection_entries = []
        for key, val in rejections.items():
            if not val:
                continue
            parts = key.split(":")
            if len(parts) >= 3:
                rejection_entries.append({
                    "key": key,
                    "docId": parts[0],
                    "ref": parts[1],
                    "position": int(parts[2]) if parts[2].isdigit() else 0,
                })

        # Build LCSH entries
        lcsh_entries = []
        for ref, decision in lcsh_decisions.items():
            lcsh_entries.append({
                "ref": ref,
                "decision": decision,
            })

        # Build merge entries and variant_overrides snippets
        merge_entries = []
        override_snippets = []
        for source_ref, decision in merge_decisions.items():
            target_ref = decision.get("targetRef", "")
            target_name = decision.get("targetName", "")
            merge_entries.append({
                "source_ref": source_ref,
                "target_ref": target_ref,
                "target_term": target_name,
            })
            override_snippets.append({
                "action": "merge",
                "canonical_ref": target_ref,
                "variant_refs": [source_ref],
                "reason": f"Fold into \u2018{target_name}\u2019",
            })

        output = {
            "volume_id": volume_id,
            "exported": datetime.now().isoformat(),
            "auto_saved": True,
            "total_rejections": len(rejection_entries),
            "rejections": sorted(rejection_entries, key=lambda x: x.get("ref", "")),
            "total_lcsh_decisions": len(lcsh_entries),
            "lcsh_decisions": sorted(lcsh_entries, key=lambda x: x.get("ref", "")),
            "total_merge_decisions": len(merge_entries),
            "merge_decisions": sorted(merge_entries, key=lambda x: x.get("source_ref", "")),
            "variant_overrides_snippet": override_snippets,
        }

        # Write to disk
        out_dir = BASE_DIR / "data" / "documents" / volume_id
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"annotation_rejections_{volume_id}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)

        total = len(rejection_entries) + len(lcsh_entries) + len(merge_entries)
        print(f"  Saved {total} decisions for {volume_id} -> {out_path.name}")

        return jsonify({
            "status": "ok",
            "volume_id": volume_id,
            "rejections_saved": len(rejection_entries),
            "lcsh_decisions_saved": len(lcsh_entries),
            "merge_decisions_saved": len(merge_entries),
            "path": str(out_path.relative_to(BASE_DIR)),
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── API: Load decisions ──────────────────────────────────

@app.route("/api/load-decisions/<volume_id>", methods=["GET"])
def load_decisions(volume_id):
    """Load saved decisions for a volume from disk.

    Returns the saved decisions JSON, or empty decisions if no file exists.
    This allows the review tool to restore state from disk instead of
    relying solely on localStorage.
    """
    if "/" in volume_id or "\\" in volume_id or ".." in volume_id:
        return jsonify({"error": "Invalid volume_id"}), 400

    decisions_path = (
        BASE_DIR / "data" / "documents" / volume_id
        / f"annotation_rejections_{volume_id}.json"
    )

    if not decisions_path.exists():
        return jsonify({
            "volume_id": volume_id,
            "rejections": [],
            "lcsh_decisions": [],
            "merge_decisions": [],
        })

    try:
        with open(decisions_path) as f:
            data = json.load(f)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── API: Pipeline actions ────────────────────────────────

# Track running tasks so we can prevent double-starts
_running_tasks = {}
_tasks_lock = threading.Lock()


def _sse_line(data_dict):
    """Format a dict as a Server-Sent Events data line."""
    return "data: " + json.dumps(data_dict) + "\n\n"


def _stream_subprocess(cmd, task_key, cwd=None):
    """Run a subprocess and stream output line-by-line as SSE."""
    def generate():
        with _tasks_lock:
            if task_key in _running_tasks:
                yield _sse_line({"type": "error", "line": task_key + " is already running"})
                return
            _running_tasks[task_key] = True

        try:
            # Insert -u (unbuffered) flag after the python executable
            # so child output streams in real time
            run_cmd = list(cmd)
            if run_cmd and run_cmd[0] == sys.executable and "-u" not in run_cmd:
                run_cmd.insert(1, "-u")

            cmd_str = " ".join(run_cmd)
            yield _sse_line({"type": "start", "line": "Running: " + cmd_str})
            proc = subprocess.Popen(
                run_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                cwd=cwd or str(BASE_DIR),
            )
            # Use readline() instead of iterator — the iterator
            # buffers internally and delays output
            while True:
                line = proc.stdout.readline()
                if not line and proc.poll() is not None:
                    break
                if line:
                    yield _sse_line({"type": "output", "line": line.rstrip()})
            proc.wait()
            status = "success" if proc.returncode == 0 else "error"
            yield _sse_line({"type": "done", "status": status, "code": proc.returncode})
        except Exception as e:
            yield _sse_line({"type": "error", "line": str(e)})
        finally:
            with _tasks_lock:
                _running_tasks.pop(task_key, None)

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/api/validate", methods=["POST"])
def api_validate():
    """Run validate_data.py and stream output."""
    return _stream_subprocess(
        [sys.executable, str(BASE_DIR / "scripts" / "validate_data.py")],
        task_key="validate",
    )


@app.route("/api/pipeline/<volume_id>", methods=["POST"])
def api_pipeline(volume_id):
    """Run the full post-review pipeline for a volume and stream output."""
    if "/" in volume_id or "\\" in volume_id or ".." in volume_id:
        return jsonify({"error": "Invalid volume_id"}), 400

    return _stream_subprocess(
        [sys.executable, str(BASE_DIR / "scripts" / "run_reviewed_pipeline.py"), volume_id],
        task_key=f"pipeline-{volume_id}",
    )


@app.route("/api/rebuild-review", methods=["POST"])
def api_rebuild_review():
    """Rebuild string-match-review.html and stream output."""
    return _stream_subprocess(
        [sys.executable, str(BASE_DIR / "scripts" / "build_annotation_review.py")],
        task_key="rebuild-review",
    )


@app.route("/api/rebuild-taxonomy-review", methods=["POST"])
def api_rebuild_taxonomy_review():
    """Rebuild taxonomy (variant groups → doc appearances → XML → HTML) and stream output."""
    return _stream_subprocess(
        [sys.executable, str(BASE_DIR / "scripts" / "rebuild_taxonomy_review.py")],
        task_key="rebuild-taxonomy-review",
    )


@app.route("/api/rebuild-mockup", methods=["POST"])
def api_rebuild_mockup():
    """Rebuild HSG subjects mockup (annotations → mockup data → HTML) and stream output."""
    return _stream_subprocess(
        [sys.executable, str(BASE_DIR / "scripts" / "rebuild_mockup.py")],
        task_key="rebuild-mockup",
    )


@app.route("/api/import-volume", methods=["POST"])
def api_import_volume():
    """Import new volumes: split → annotate → rebuild review HTML. Stream output.

    Accepts optional JSON body with:
        series: str - series filter (e.g., "1981-88" or "all")
    """
    cmd = [sys.executable, str(BASE_DIR / "scripts" / "import_volume.py")]

    # Check for series parameter
    body = request.get_json(silent=True) or {}
    series = body.get("series")
    if series:
        cmd.extend(["--series", series])

    return _stream_subprocess(cmd, task_key="import-volume")


@app.route("/api/list-series", methods=["GET"])
def api_list_series():
    """List available volume series with counts."""
    # Import the function from import_volume
    sys.path.insert(0, str(BASE_DIR / "scripts"))
    try:
        from import_volume import list_series
        series_info = list_series()

        # Convert to JSON-friendly format
        result = []
        for series_id in sorted(series_info.keys()):
            info = series_info[series_id]
            result.append({
                "series": series_id,
                "total": info["total"],
                "unprocessed": info["unprocessed"],
                "volumes": info["volumes"],
            })
        return jsonify({"series": result})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        sys.path.pop(0)


# ── API: Taxonomy review decisions ──────────────────────────

TAXONOMY_DECISIONS_FILE = BASE_DIR / "taxonomy_review_state.json"


@app.route("/api/save-taxonomy-decisions", methods=["POST"])
def save_taxonomy_decisions():
    """Save taxonomy review decisions (LCSH accept/reject + category overrides)."""
    try:
        payload = request.get_json()
        if not payload:
            return jsonify({"error": "No JSON body"}), 400

        lcsh = payload.get("lcsh_decisions", {})
        overrides = payload.get("category_overrides", {})
        merges = payload.get("merge_decisions", {})
        exclusions = payload.get("exclusions", {})
        global_rejections = payload.get("global_rejections", {})

        output = {
            "saved": datetime.now().isoformat(),
            "lcsh_decisions": lcsh,
            "category_overrides": overrides,
            "merge_decisions": merges,
            "exclusions": exclusions,
            "global_rejections": global_rejections,
        }

        with open(TAXONOMY_DECISIONS_FILE, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)

        # Also update category_overrides.json if there are overrides
        if overrides:
            overrides_path = BASE_DIR / "config" / "category_overrides.json"
            overrides_list = list(overrides.values())
            with open(overrides_path, "w", encoding="utf-8") as f:
                json.dump(overrides_list, f, indent=2, ensure_ascii=False)
                f.write("\n")

        # Also write lcsh_decisions.json if there are LCSH decisions
        if lcsh:
            lcsh_path = BASE_DIR / "lcsh_decisions.json"
            decisions = []
            for ref, decision in lcsh.items():
                decisions.append({"ref": ref, "decision": decision})
            lcsh_output = {
                "exported": datetime.now().isoformat(),
                "tool": "taxonomy-review.html",
                "total_decisions": len(decisions),
                "decisions": sorted(decisions, key=lambda x: x.get("ref", "")),
            }
            with open(lcsh_path, "w", encoding="utf-8") as f:
                json.dump(lcsh_output, f, indent=2, ensure_ascii=False)

        # Write merge decisions to variant_overrides.json
        if merges:
            overrides_path = BASE_DIR / "config" / "variant_overrides.json"
            # Load existing overrides to preserve splits and other manual entries
            existing_overrides_data = {"overrides": []}
            if overrides_path.exists():
                with open(overrides_path) as f:
                    existing_overrides_data = json.load(f)

            # Keep non-merge overrides and merges NOT from taxonomy-review
            existing_list = existing_overrides_data.get("overrides", [])
            # Remove old taxonomy-review merges (identified by source field)
            kept = [o for o in existing_list if o.get("action") != "merge" or o.get("source") != "taxonomy-review"]
            # Also keep non-taxonomy-review merges that aren't overridden by new ones
            new_variant_refs = set()
            for d in merges.values():
                new_variant_refs.add(list(merges.keys())[0] if len(merges) == 1 else "")
            # Simpler: just collect source refs from new merges
            new_source_refs = set(merges.keys())
            kept = [o for o in kept if o.get("action") != "merge" or
                    not any(vr in new_source_refs for vr in o.get("variant_refs", []))]

            # Add new merges from taxonomy-review
            for source_ref, decision in merges.items():
                kept.append({
                    "action": "merge",
                    "canonical_ref": decision["targetRef"],
                    "variant_refs": [source_ref],
                    "reason": f"Taxonomy review: fold into '{decision['targetName']}'",
                    "source": "taxonomy-review",
                })

            existing_overrides_data["overrides"] = kept
            existing_overrides_data["updated"] = datetime.now().strftime("%Y-%m-%d")

            with open(overrides_path, "w", encoding="utf-8") as f:
                json.dump(existing_overrides_data, f, indent=2, ensure_ascii=False)
                f.write("\n")

        lcsh_count = len(lcsh)
        override_count = len(overrides)
        merge_count = len(merges)
        print(f"  Saved taxonomy decisions: {lcsh_count} LCSH, {override_count} overrides, {merge_count} merges")

        return jsonify({
            "status": "ok",
            "lcsh_count": lcsh_count,
            "override_count": override_count,
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/load-taxonomy-decisions", methods=["GET"])
def load_taxonomy_decisions():
    """Load saved taxonomy review decisions."""
    if not TAXONOMY_DECISIONS_FILE.exists():
        return jsonify({
            "lcsh_decisions": {},
            "category_overrides": {},
            "exclusions": {},
        })

    try:
        with open(TAXONOMY_DECISIONS_FILE) as f:
            data = json.load(f)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Main ─────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="FRUS Taxonomy development server")
    parser.add_argument("--port", type=int, default=9090, help="Port to serve on")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind to")
    args = parser.parse_args()

    # Check that string-match-review.html exists
    review_html = BASE_DIR / "string-match-review.html"
    if not review_html.exists():
        print("string-match-review.html not found. Building it now...")
        subprocess.run([sys.executable, str(BASE_DIR / "scripts" / "build_annotation_review.py")])

    print(f"\nFRUS Taxonomy Server")
    print(f"  Serving from: {BASE_DIR}")
    print(f"  Annotation review: http://{args.host}:{args.port}/string-match-review.html")
    print(f"  Taxonomy review:   http://{args.host}:{args.port}/taxonomy-review.html")
    print(f"  API:               http://{args.host}:{args.port}/api/")
    print()

    app.run(host=args.host, port=args.port, debug=False, threaded=True, use_reloader=True)


if __name__ == "__main__":
    main()
