"""
MCP server — exposes the incident investigator as native tools for any
MCP-compatible AI client: Claude Code, OpenClaw (ChatGPT / Gemini), Cursor, Zed.

Tools:
  investigate_failure  — run a full investigation on a named dbt test
  list_failures        — list currently failing tests from run_results.json
  get_report           — read a saved incident report by filename

Register with Claude Code (user scope):
    claude mcp add -s user \\
      -e GCP_PROJECT=your-project \\
      -e BQ_LOCATION=asia-south1 \\
      -e DBT_MANIFEST_PATH=/path/to/dbt_bank/target/manifest.json \\
      -e DBT_RUN_RESULTS_PATH=/path/to/dbt_bank/target/run_results.json \\
      -e GEMINI_API_KEY=your-key \\
      dbt-investigator \\
      -- /path/to/venv/bin/python /path/to/mcp_server.py

Register with OpenClaw (any AI client):
    openclaw mcp set dbt-investigator '{
      "command": "/path/to/venv/bin/python",
      "args": ["/path/to/mcp_server.py"],
      "env": {
        "GCP_PROJECT": "your-project",
        "GEMINI_API_KEY": "your-key",
        "DBT_MANIFEST_PATH": "/path/to/manifest.json",
        "DBT_RUN_RESULTS_PATH": "/path/to/run_results.json"
      }
    }'
"""
from __future__ import annotations

import os
from pathlib import Path

from mcp.server.fastmcp import FastMCP

from agent import investigate
from tools.dbt_tools import get_failing_tests, load_manifest

mcp = FastMCP("data-quality-agent")

MANIFEST_PATH    = os.environ.get("DBT_MANIFEST_PATH", "")
RUN_RESULTS_PATH = os.environ.get("DBT_RUN_RESULTS_PATH", "")


@mcp.tool()
def investigate_failure(
    test_name: str,
    model: str = "",
    column: str = "",
    failing_rows: int = 0,
) -> dict:
    """
    Run an agentic root-cause investigation on a failing dbt test.
    The agent queries BigQuery, traces dbt lineage, and returns an incident report.

    Args:
        test_name:    dbt test name or unique_id (e.g. not_null_fct_transactions_merchant)
        model:        dbt model the test is on (auto-discovered from run_results if blank)
        column:       column being tested (auto-discovered if blank)
        failing_rows: number of failing rows (auto-discovered if blank)
    """
    return investigate(
        test_name=test_name,
        model=model,
        column=column,
        failing_rows=failing_rows,
        manifest_path=MANIFEST_PATH or None,
        verbose=False,
        save=True,
    )


@mcp.tool()
def list_failures() -> dict:
    """
    List all currently failing dbt tests from run_results.json.
    Use this to discover what to investigate before calling investigate_failure.
    """
    if not MANIFEST_PATH or not Path(MANIFEST_PATH).exists():
        return {"error": f"DBT_MANIFEST_PATH not set or not found: {MANIFEST_PATH}"}
    if not RUN_RESULTS_PATH or not Path(RUN_RESULTS_PATH).exists():
        return {"error": f"DBT_RUN_RESULTS_PATH not set or not found: {RUN_RESULTS_PATH}"}

    manifest = load_manifest(MANIFEST_PATH)
    failures = get_failing_tests(manifest, RUN_RESULTS_PATH)
    return {"failures": failures, "count": len(failures)}


@mcp.tool()
def get_report(filename: str = "") -> dict:
    """
    Read a saved incident report. If filename is blank, returns the most recent one.

    Args:
        filename: report filename (e.g. incident_20260627_120000.md), or blank for latest
    """
    reports_dir = Path("reports")
    if not reports_dir.exists():
        return {"error": "No reports directory found. Run an investigation first."}

    reports = sorted(reports_dir.glob("incident_*.md"), reverse=True)
    if not reports:
        return {"error": "No reports found."}

    if filename:
        path = reports_dir / filename
        if not path.exists():
            return {"error": f"{filename} not found. Available: {[r.name for r in reports[:5]]}"}
    else:
        path = reports[0]

    return {
        "filename": path.name,
        "content":  path.read_text(encoding="utf-8"),
        "available_reports": [r.name for r in reports[:10]],
    }


if __name__ == "__main__":
    mcp.run()
