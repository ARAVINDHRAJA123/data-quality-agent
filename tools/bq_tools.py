"""
BigQuery tools — get failing rows, run read-only investigative queries,
check source freshness.

All queries are read-only (SELECT/WITH only). The safety wall is identical
to the one in the Bank Statement Analyser's ask_statement.py.
"""
from __future__ import annotations

import os
import re
from datetime import datetime, timezone

from google.cloud import bigquery

PROJECT  = os.environ.get("GCP_PROJECT", "")
LOCATION = os.environ.get("BQ_LOCATION", "US")
MAX_BYTES_BILLED = 200_000_000   # 200 MB scan cap
MAX_ROWS = 200

_FORBIDDEN = re.compile(
    r"\b(insert|update|delete|merge|drop|create|alter|truncate|grant|revoke|call)\b",
    re.IGNORECASE,
)
_STARTS_OK = re.compile(r"^\s*(with|select)\b", re.IGNORECASE | re.DOTALL)


def _bq_client() -> bigquery.Client:
    return bigquery.Client(project=PROJECT, location=LOCATION)


def _is_safe(sql: str) -> bool:
    stripped = sql.strip().rstrip(";")
    return bool(_STARTS_OK.match(stripped)) and not _FORBIDDEN.search(stripped)


def run_query(sql: str) -> dict:
    """Execute a read-only query and return rows as a list of dicts."""
    if not _is_safe(sql):
        return {"error": "Rejected: only SELECT/WITH queries are allowed."}
    if not PROJECT:
        return {"error": "GCP_PROJECT is not set."}
    try:
        client = _bq_client()
        cfg = bigquery.QueryJobConfig(
            maximum_bytes_billed=MAX_BYTES_BILLED,
            use_query_cache=True,
        )
        rows = [dict(r) for r in client.query(sql, job_config=cfg).result(max_results=MAX_ROWS)]
        return {"row_count": len(rows), "rows": rows}
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


def get_failing_rows(project: str, dataset: str, model: str, column: str, test_type: str, limit: int = 20) -> dict:
    """
    Return rows that fail a specific dbt test.

    Supports: not_null, unique, accepted_values (generic SELECT for others).
    """
    table = f"`{project}.{dataset}.{model}`"
    if test_type == "not_null":
        sql = f"SELECT * FROM {table} WHERE `{column}` IS NULL LIMIT {limit}"
    elif test_type == "unique":
        sql = f"""
            SELECT `{column}`, COUNT(*) as duplicate_count
            FROM {table}
            GROUP BY `{column}`
            HAVING COUNT(*) > 1
            ORDER BY duplicate_count DESC
            LIMIT {limit}
        """
    else:
        sql = f"SELECT * FROM {table} LIMIT {limit}"
    return run_query(sql)


def get_row_counts(project: str, dataset: str, tables: list[str]) -> dict:
    """Return row counts for multiple tables — useful for freshness checking."""
    results = {}
    for table in tables:
        r = run_query(f"SELECT COUNT(*) as cnt FROM `{project}.{dataset}.{table}`")
        results[table] = r.get("rows", [{}])[0].get("cnt", "error") if "error" not in r else r["error"]
    return results


def get_source_freshness(project: str, dataset: str, table: str, loaded_at_field: str) -> dict:
    """Check how stale the source data is."""
    sql = f"""
        SELECT
            MAX(`{loaded_at_field}`) as latest_load,
            TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(`{loaded_at_field}`), HOUR) as hours_since_load,
            COUNT(*) as total_rows
        FROM `{project}.{dataset}.{table}`
    """
    return run_query(sql)


def get_column_profile(project: str, dataset: str, table: str, column: str) -> dict:
    """Basic column profile: null count, distinct count, min, max."""
    sql = f"""
        SELECT
            COUNT(*) as total_rows,
            COUNTIF(`{column}` IS NULL) as null_count,
            COUNT(DISTINCT `{column}`) as distinct_count,
            CAST(MIN(`{column}`) AS STRING) as min_value,
            CAST(MAX(`{column}`) AS STRING) as max_value
        FROM `{project}.{dataset}.{table}`
    """
    return run_query(sql)
