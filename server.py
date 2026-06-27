"""
Webhook server — Airflow (or any system) POSTs here when a dbt test fails.
The agent investigates and returns the incident report as JSON.

POST /investigate
    {"test_name": "...", "model": "...", "column": "...", "failing_rows": 0}

GET /health
    {"status": "ok"}

Run:
    python server.py
    # or with auto-reload:
    FLASK_DEBUG=1 python server.py
"""
from __future__ import annotations

import os
from flask import Flask, request, jsonify
from agent import investigate

app = Flask(__name__)
PORT = int(os.environ.get("PORT", 5051))


@app.get("/health")
def health():
    return jsonify({"status": "ok"})


@app.post("/investigate")
def investigate_endpoint():
    data = request.get_json(force=True) or {}
    test_name    = data.get("test_name", "")
    model        = data.get("model", "")
    column       = data.get("column", "")
    failing_rows = int(data.get("failing_rows", 0))
    manifest     = data.get("manifest_path") or os.environ.get("DBT_MANIFEST_PATH", "")

    if not test_name:
        return jsonify({"error": "test_name is required"}), 400

    try:
        result = investigate(
            test_name=test_name,
            model=model,
            column=column,
            failing_rows=failing_rows,
            manifest_path=manifest,
            verbose=False,
            save=True,
        )
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, debug=os.environ.get("FLASK_DEBUG") == "1")
