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
    # Read file fresh from disk each time to avoid any caching
    html_path = BASE_DIR / "string-match-review.html"
    return Response(
        html_path.read_text(encoding="utf-8"),
        mimetype="text/html",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@app.route("/tax-review")
def tax_review():
    """Serve taxonomy-review.html at an alternate path to bypass proxy cache."""
    html_path = BASE_DIR / "taxonomy-review.html"
    return Response(
        html_path.read_text(encoding="utf-8"),
        mimetype="text/html",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@app.route("/<path:path>")
def static_files(path):
    file_path = BASE_DIR / path
    if not file_path.exists() or not file_path.is_file():
        return "Not found", 404
    # For HTML files, read fresh from disk
    if path.endswith(".html"):
        return Response(
            file_path.read_text(encoding="utf-8"),
            mimetype="text/html",
            headers={
                "Cache-Control": "no-cache, no-store, must-revalidate",
                "Pragma": "no-cache",
                "Expires": "0",
            },
        )
    resp = send_from_directory(BASE_DIR, path)
    resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
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
_running_tasks = {}  # task_key -> subprocess PID (or True if no PID yet)
_tasks_lock = threading.Lock()


def _is_task_actually_running(task_key):
    """Check if a task's subprocess is still alive. Clears stale entries."""
    pid = _running_tasks.get(task_key)
    if pid is None:
        return False
    if pid is True:
        return True  # just started, no PID yet
    try:
        os.kill(pid, 0)  # signal 0 = check if process exists
        return True
    except (ProcessLookupError, OSError):
        # Process is gone — clear the stale entry
        _running_tasks.pop(task_key, None)
        return False


def _sse_line(data_dict):
    """Format a dict as a Server-Sent Events data line."""
    return "data: " + json.dumps(data_dict) + "\n\n"


def _stream_subprocess(cmd, task_key, cwd=None):
    """Run a subprocess and stream output line-by-line as SSE."""
    def generate():
        with _tasks_lock:
            if _is_task_actually_running(task_key):
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
            with _tasks_lock:
                _running_tasks[task_key] = proc.pid
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
        [sys.executable, str(BASE_DIR / "scripts" / "rebuild.py"), "--volume", volume_id],
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
        [sys.executable, str(BASE_DIR / "scripts" / "rebuild.py"), "--taxonomy"],
        task_key="rebuild-taxonomy-review",
    )


@app.route("/api/promote-candidates", methods=["POST"])
def api_promote_candidates():
    """Run promote_candidates.py to push accepted/merged decisions into the pipeline."""
    return _stream_subprocess(
        [sys.executable, str(BASE_DIR / "scripts" / "promote_candidates.py")],
        task_key="promote-candidates",
    )


@app.route("/api/rebuild-candidates-review", methods=["POST"])
def api_rebuild_candidates_review():
    """Rebuild candidates review HTML files for all categories."""
    return _stream_subprocess(
        [sys.executable, str(BASE_DIR / "scripts" / "build_candidates_review.py"),
         "--category", "all"],
        task_key="rebuild-candidates-review",
    )


@app.route("/api/rebuild-mockup", methods=["POST"])
def api_rebuild_mockup():
    """Rebuild HSG subjects mockup (annotations → mockup data → HTML) and stream output."""
    return _stream_subprocess(
        [sys.executable, str(BASE_DIR / "scripts" / "rebuild.py"), "--mockup"],
        task_key="rebuild-mockup",
    )


@app.route("/api/export-tei-headers/<volume_id>", methods=["POST"])
def api_export_tei_headers(volume_id):
    """Export reviewed decisions into TEI headers for a single volume."""
    if "/" in volume_id or "\\" in volume_id or ".." in volume_id:
        return jsonify({"error": "Invalid volume_id"}), 400

    return _stream_subprocess(
        [sys.executable, str(BASE_DIR / "scripts" / "export_to_tei_headers.py"),
         "--vol", volume_id, "--force"],
        task_key=f"export-tei-{volume_id}",
    )


@app.route("/api/export-tei-headers-all", methods=["POST"])
def api_export_tei_headers_all():
    """Export reviewed decisions into TEI headers for all reviewed volumes."""
    return _stream_subprocess(
        [sys.executable, str(BASE_DIR / "scripts" / "export_to_tei_headers.py"),
         "--all", "--force"],
        task_key="export-tei-all",
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

        # Load existing state first so we never drop keys
        existing = {}
        if TAXONOMY_DECISIONS_FILE.exists():
            with open(TAXONOMY_DECISIONS_FILE, encoding="utf-8") as f:
                existing = json.load(f)

        # Merge payload into existing state (payload wins for any key it provides)
        output = {**existing, **payload}
        output["saved"] = datetime.now().isoformat()

        # Extract known keys for downstream processing
        lcsh = output.get("lcsh_decisions", {})
        overrides = output.get("category_overrides", {})
        merges = output.get("merge_decisions", {})
        exclusions = output.get("exclusions", {})
        global_rejections = output.get("global_rejections", {})
        candidate_decisions = output.get("candidate_decisions", {})

        with open(TAXONOMY_DECISIONS_FILE, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)

        # Also update category_overrides.json if there are overrides
        if overrides:
            overrides_path = BASE_DIR / "config" / "category_overrides.json"
            overrides_list = list(overrides.values())
            with open(overrides_path, "w", encoding="utf-8") as f:
                json.dump(overrides_list, f, indent=2, ensure_ascii=False)
                f.write("\n")

        # LCSH decisions are stored in taxonomy_review_state.json (above).
        # No longer writing standalone lcsh_decisions.json.

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
        candidate_count = len(candidate_decisions)
        print(f"  Saved taxonomy decisions: {lcsh_count} LCSH, {override_count} overrides, "
              f"{merge_count} merges, {candidate_count} candidates")

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
