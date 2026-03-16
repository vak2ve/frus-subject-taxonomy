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
import os
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
    return resp


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
            cmd_str = " ".join(cmd)
            yield _sse_line({"type": "start", "line": "Running: " + cmd_str})
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                cwd=cwd or str(BASE_DIR),
            )
            for line in proc.stdout:
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
        os.system("python3 scripts/build_annotation_review.py")

    print(f"\nFRUS Taxonomy Server")
    print(f"  Serving from: {BASE_DIR}")
    print(f"  Review tool:  http://{args.host}:{args.port}/string-match-review.html")
    print(f"  API:          http://{args.host}:{args.port}/api/")
    print()

    app.run(host=args.host, port=args.port, debug=False, threaded=True)


if __name__ == "__main__":
    main()
