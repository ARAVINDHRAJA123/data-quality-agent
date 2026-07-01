"""
Data Quality Incident Investigator — agentic root-cause analysis for dbt test failures.

The agent is given a set of tools and a failing dbt test. It autonomously:
  1. Fetches the failing rows from BigQuery
  2. Reads the model's compiled SQL and lineage from dbt manifest
  3. Traces upstream through the dependency graph
  4. Profiles columns in upstream models to locate the root cause
  5. Writes a plain-English incident report

Works on Claude (ANTHROPIC_API_KEY) or Gemini (GEMINI_API_KEY).
Auto-detects whichever key is set.

Trigger modes:
  CLI:     python agent.py --test not_null_fct_transactions_merchant
  Webhook: POST /investigate  {"test_name": "...", "model": "...", "column": "..."}
  MCP:     investigate_failure tool (any MCP-compatible client)
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from langfuse import observe

from tools.bq_tools import (
    get_column_profile,
    get_failing_rows,
    get_row_counts,
    get_source_freshness,
    run_query,
)
from tools.dbt_tools import (
    get_failing_tests,
    get_model_lineage,
    get_model_sql,
    get_source_info,
    load_manifest,
)
from report import format_report, save_report

# ── Config ────────────────────────────────────────────────────────────────────

GCP_PROJECT   = os.environ.get("GCP_PROJECT", "")
BQ_LOCATION   = os.environ.get("BQ_LOCATION", "US")
BQ_DATASET    = os.environ.get("BQ_DATASET", "analytics")
MANIFEST_PATH = os.environ.get("DBT_MANIFEST_PATH", "")
RUN_RESULTS_PATH = os.environ.get("DBT_RUN_RESULTS_PATH", "")

ANTHROPIC_MODEL = "claude-opus-4-8"
GEMINI_MODEL    = "gemini-2.5-flash-lite"   # higher free-tier quota (15 req/min vs 5)


def _get_provider() -> str:
    if os.environ.get("ANTHROPIC_API_KEY"):
        return "anthropic"
    if os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY"):
        return "gemini"
    raise RuntimeError(
        "No LLM key found. Set ANTHROPIC_API_KEY (Claude) or GEMINI_API_KEY (free Gemini)."
    )


# ── Tool definitions (schema for LLM) ────────────────────────────────────────

TOOLS = [
    {
        "name": "get_failing_rows",
        "description": (
            "Fetch rows from BigQuery that fail a specific dbt test. "
            "Use this first to see the actual bad data."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "model":     {"type": "string", "description": "dbt model name (BigQuery table name)"},
                "column":    {"type": "string", "description": "Column being tested"},
                "test_type": {"type": "string", "description": "Test type: not_null, unique, accepted_values, or other"},
                "limit":     {"type": "integer", "description": "Max rows to return (default 20)", "default": 20},
            },
            "required": ["model", "column", "test_type"],
        },
    },
    {
        "name": "get_model_lineage",
        "description": (
            "Get upstream parents and downstream children of a dbt model from the manifest. "
            "Use this to trace where bad data might have originated."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "model_name": {"type": "string", "description": "dbt model name"},
            },
            "required": ["model_name"],
        },
    },
    {
        "name": "get_model_sql",
        "description": "Get the compiled SQL for a dbt model. Use this to understand the transformation logic.",
        "input_schema": {
            "type": "object",
            "properties": {
                "model_name": {"type": "string", "description": "dbt model name"},
            },
            "required": ["model_name"],
        },
    },
    {
        "name": "get_column_profile",
        "description": (
            "Get a statistical profile of a column (null count, distinct count, min, max). "
            "Use this to understand data quality in upstream models."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "table":  {"type": "string", "description": "BigQuery table name (dbt model name)"},
                "column": {"type": "string", "description": "Column to profile"},
            },
            "required": ["table", "column"],
        },
    },
    {
        "name": "run_query",
        "description": (
            "Run a read-only SELECT query against BigQuery for custom investigation. "
            "Only SELECT/WITH allowed. Fully-qualify tables as `project.dataset.table`."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sql": {"type": "string", "description": "A read-only SELECT query"},
            },
            "required": ["sql"],
        },
    },
    {
        "name": "get_source_freshness",
        "description": "Check how stale a source table is by looking at its latest load timestamp.",
        "input_schema": {
            "type": "object",
            "properties": {
                "table":           {"type": "string", "description": "Source table name"},
                "loaded_at_field": {"type": "string", "description": "Timestamp column name"},
            },
            "required": ["table", "loaded_at_field"],
        },
    },
    {
        "name": "write_report",
        "description": (
            "Write the final incident report once you have identified the root cause. "
            "Call this ONLY when you have enough information to explain the root cause."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "root_cause":          {"type": "string"},
                "upstream_trace":      {"type": "array", "items": {"type": "string"}},
                "investigation_steps": {"type": "array", "items": {"type": "string"}},
                "recommended_fix":     {"type": "string"},
                "severity":            {"type": "string", "enum": ["low", "medium", "high", "critical"]},
            },
            "required": ["root_cause", "upstream_trace", "investigation_steps", "recommended_fix", "severity"],
        },
    },
]


# ── Tool dispatcher ────────────────────────────────────────────────────────────

def _dispatch(manifest: dict, tool_name: str, args: dict) -> dict:
    if tool_name == "get_failing_rows":
        return get_failing_rows(
            GCP_PROJECT, BQ_DATASET,
            args["model"], args["column"], args["test_type"],
            args.get("limit", 20),
        )
    if tool_name == "get_model_lineage":
        return get_model_lineage(manifest, args["model_name"])
    if tool_name == "get_model_sql":
        return get_model_sql(manifest, args["model_name"])
    if tool_name == "get_column_profile":
        return get_column_profile(GCP_PROJECT, BQ_DATASET, args["table"], args["column"])
    if tool_name == "run_query":
        return run_query(args["sql"])
    if tool_name == "get_source_freshness":
        return get_source_freshness(
            GCP_PROJECT, "raw", args["table"],
            args.get("loaded_at_field", "_loaded_at"),
        )
    if tool_name == "write_report":
        return args  # handled by the caller
    return {"error": f"Unknown tool: {tool_name}"}


# ── System prompt ─────────────────────────────────────────────────────────────

def _system(test_name: str, model: str, column: str, failing_rows: int) -> str:
    return f"""You are a data quality incident investigator for a BigQuery + dbt analytics warehouse.

A dbt test has failed and you must find the root cause.

Failing test:
- Test name: {test_name}
- Model: {model}
- Column: {column}
- Failing rows: {failing_rows}

BigQuery project: {GCP_PROJECT}
Analytics dataset: {BQ_DATASET}
Raw dataset: raw

Your job:
1. Fetch the failing rows to understand what the bad data looks like.
2. Get the model's lineage to understand upstream dependencies.
3. Get the model's compiled SQL to understand the transformation.
4. Trace upstream — profile columns in parent models/sources to find where the bad data entered.
5. Once you have identified the root cause, call write_report with your findings.

Rules:
- Use run_query for custom investigation. Always fully-qualify: `{GCP_PROJECT}.dataset.table`.
- Be systematic: start at the failing model, go upstream one step at a time.
- The root cause is usually in a source table or an upstream transformation, not the failing model itself.
- Severity: critical = affects revenue/facts, high = fails tests on mart tables, medium = staging, low = dimensions/seeds."""


# ── Claude agent loop ─────────────────────────────────────────────────────────

@observe(name="llm_loop_anthropic")
def _run_anthropic(manifest: dict, test_name: str, model: str, column: str,
                   failing_rows: int, verbose: bool, model_id: str | None) -> dict:
    import anthropic

    client = anthropic.Anthropic()
    messages = [{"role": "user", "content": f"Investigate the dbt test failure: {test_name}"}]

    # Convert tool schemas for Anthropic format
    tools = []
    for t in TOOLS:
        tools.append({
            "name": t["name"],
            "description": t["description"],
            "input_schema": t["input_schema"],
        })

    report_args = None
    for _ in range(12):
        resp = client.messages.create(
            model=model_id or ANTHROPIC_MODEL,
            max_tokens=8000,
            system=_system(test_name, model, column, failing_rows),
            tools=tools,
            thinking={"type": "adaptive"},
            messages=messages,
        )

        if resp.stop_reason == "end_turn":
            break

        if resp.stop_reason == "tool_use":
            messages.append({"role": "assistant", "content": resp.content})
            results = []
            for block in resp.content:
                if block.type != "tool_use":
                    continue
                if verbose:
                    print(f"\n[tool] {block.name}({json.dumps(block.input, default=str)[:120]})", flush=True)
                if block.name == "write_report":
                    report_args = block.input
                    results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": "Report recorded. Provide your final summary.",
                    })
                else:
                    out = _dispatch(manifest, block.name, block.input)
                    if verbose:
                        print(f"   → {str(out)[:200]}", flush=True)
                    results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": json.dumps(out, default=str),
                        "is_error": "error" in out,
                    })
            messages.append({"role": "user", "content": results})
            if report_args:
                break

    return report_args or {}


# ── Gemini agent loop ─────────────────────────────────────────────────────────

@observe(name="llm_loop_gemini")
def _run_gemini(manifest: dict, test_name: str, model: str, column: str,
                failing_rows: int, verbose: bool, model_id: str | None) -> dict:
    from google import genai
    from google.genai import types

    api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    client = genai.Client(api_key=api_key)

    # Build function declarations from TOOLS
    fn_decls = []
    for t in TOOLS:
        props = {}
        for pname, pdef in t["input_schema"].get("properties", {}).items():
            ptype = pdef.get("type", "STRING").upper()
            if ptype == "INTEGER":
                schema = types.Schema(type="INTEGER", description=pdef.get("description", ""))
            elif ptype == "ARRAY":
                schema = types.Schema(type="ARRAY", items=types.Schema(type="STRING"),
                                      description=pdef.get("description", ""))
            else:
                schema = types.Schema(type="STRING", description=pdef.get("description", ""))
            props[pname] = schema

        enum_vals = None
        if "severity" in props:
            props["severity"] = types.Schema(
                type="STRING",
                enum=["low", "medium", "high", "critical"],
                description="Severity level",
            )

        fn_decls.append(types.FunctionDeclaration(
            name=t["name"],
            description=t["description"],
            parameters=types.Schema(
                type="OBJECT",
                properties=props,
                required=t["input_schema"].get("required", []),
            ),
        ))

    config = types.GenerateContentConfig(
        system_instruction=_system(test_name, model, column, failing_rows),
        tools=[types.Tool(function_declarations=fn_decls)],
        automatic_function_calling=types.AutomaticFunctionCallingConfig(disable=True),
    )

    contents = [types.Content(role="user", parts=[
        types.Part(text=f"Investigate the dbt test failure: {test_name}")
    ])]

    import time

    def _gemini_call(contents):
        for attempt in range(5):
            try:
                return client.models.generate_content(
                    model=model_id or GEMINI_MODEL,
                    contents=contents,
                    config=config,
                )
            except Exception as e:
                msg = str(e)
                retryable = "429" in msg or "503" in msg or "UNAVAILABLE" in msg or "RESOURCE_EXHAUSTED" in msg
                if retryable and attempt < 4:
                    wait = (attempt + 1) * 20
                    if verbose:
                        print(f"   [gemini {msg[:30]}…] waiting {wait}s (attempt {attempt+1}/5)…", flush=True)
                    time.sleep(wait)
                else:
                    raise

    report_args = None
    for _ in range(12):
        resp = _gemini_call(contents)
        calls = resp.function_calls
        if not calls:
            break

        contents.append(resp.candidates[0].content)
        for call in calls:
            args = dict(call.args or {})
            if verbose:
                print(f"\n[tool] {call.name}({json.dumps(args, default=str)[:120]})", flush=True)
            if call.name == "write_report":
                report_args = args
                contents.append(types.Content(role="user", parts=[
                    types.Part.from_function_response(
                        name=call.name,
                        response={"result": "Report recorded. Provide your final summary."},
                    )
                ]))
            else:
                out = _dispatch(manifest, call.name, args)
                if verbose:
                    print(f"   → {str(out)[:200]}", flush=True)
                contents.append(types.Content(role="user", parts=[
                    types.Part.from_function_response(
                        name=call.name,
                        response={"result": json.dumps(out, default=str)},
                    )
                ]))
        if report_args:
            break

    return report_args or {}


# ── Public interface ──────────────────────────────────────────────────────────

@observe(name="dqa_investigate")
def investigate(
    test_name: str,
    model: str,
    column: str,
    failing_rows: int = 0,
    manifest_path: str | None = None,
    verbose: bool = False,
    model_id: str | None = None,
    save: bool = True,
) -> dict:
    """
    Run the investigation agent and return the report as a dict.
    Also saves a markdown report to reports/ if save=True.
    """
    mp = manifest_path or MANIFEST_PATH
    if not mp or not Path(mp).exists():
        raise FileNotFoundError(
            f"dbt manifest not found at '{mp}'. "
            "Set DBT_MANIFEST_PATH or pass manifest_path=."
        )

    manifest = load_manifest(mp)
    provider  = _get_provider()

    if verbose:
        print(f"[investigate] provider={provider} test={test_name} model={model}", flush=True)

    if provider == "anthropic":
        report_args = _run_anthropic(manifest, test_name, model, column, failing_rows, verbose, model_id)
    else:
        report_args = _run_gemini(manifest, test_name, model, column, failing_rows, verbose, model_id)

    if not report_args:
        report_args = {
            "root_cause": "Agent did not reach a conclusion — check logs.",
            "upstream_trace": [model],
            "investigation_steps": ["Investigation incomplete"],
            "recommended_fix": "Review the failing rows manually.",
            "severity": "medium",
        }

    report_md = format_report(
        test_name=test_name,
        model=model,
        column=column,
        failing_rows=failing_rows,
        **report_args,
    )

    report_path = None
    if save:
        report_path = save_report(report_md)
        if verbose:
            print(f"\n[report saved] {report_path}", flush=True)

    return {
        "test_name":    test_name,
        "model":        model,
        "column":       column,
        "failing_rows": failing_rows,
        "severity":     report_args.get("severity", "medium"),
        "root_cause":   report_args.get("root_cause", ""),
        "recommended_fix": report_args.get("recommended_fix", ""),
        "report_md":    report_md,
        "report_path":  str(report_path) if report_path else None,
    }


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Investigate a dbt test failure.")
    parser.add_argument("--test",          required=True, help="dbt test unique_id or name")
    parser.add_argument("--model",         default="",    help="Model the test is on")
    parser.add_argument("--column",        default="",    help="Column being tested")
    parser.add_argument("--failing-rows",  type=int, default=0)
    parser.add_argument("--manifest",      default=None,  help="Path to manifest.json")
    parser.add_argument("--no-save",       action="store_true")
    parser.add_argument("--verbose", "-v", action="store_true")

    # Auto-discover from run_results.json if available
    args = parser.parse_args()

    if not args.model:
        mp = args.manifest or MANIFEST_PATH
        rr = RUN_RESULTS_PATH
        if mp and rr and Path(mp).exists() and Path(rr).exists():
            manifest = load_manifest(mp)
            failures = get_failing_tests(manifest, rr)
            match = next((f for f in failures if args.test in f.get("test_name", "") or
                         args.test in f.get("unique_id", "")), None)
            if match:
                args.model        = match.get("model", "")
                args.column       = match.get("column", "")
                args.failing_rows = match.get("failures", 0)

    result = investigate(
        test_name=args.test,
        model=args.model,
        column=args.column,
        failing_rows=args.failing_rows,
        manifest_path=args.manifest,
        verbose=args.verbose,
        save=not args.no_save,
    )

    print("\n" + "="*60)
    print(result["report_md"])


if __name__ == "__main__":
    main()
