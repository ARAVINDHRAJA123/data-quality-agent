"""Unit tests for dbt_tools and bq_tools (no BigQuery or LLM connection needed)."""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from tools.bq_tools import _is_safe
from tools.dbt_tools import get_model_lineage, get_model_sql, load_manifest


# ── Fixtures ──────────────────────────────────────────────────────────────────

SAMPLE_MANIFEST = {
    "metadata": {"project_id": "test_project"},
    "nodes": {
        "model.test_project.fct_transactions": {
            "name": "fct_transactions",
            "resource_type": "model",
            "schema": "analytics",
            "description": "Fact table",
            "depends_on": {"nodes": ["model.test_project.int_transactions_categorised"]},
            "columns": {"transaction_id": {}, "txn_date": {}, "debit_amount": {}},
            "tags": ["mart"],
            "original_file_path": "models/marts/fct_transactions.sql",
            "compiled_code": "SELECT * FROM int_transactions_categorised",
            "raw_code": "SELECT * FROM {{ ref('int_transactions_categorised') }}",
        },
        "model.test_project.int_transactions_categorised": {
            "name": "int_transactions_categorised",
            "resource_type": "model",
            "schema": "analytics",
            "description": "Intermediate model",
            "depends_on": {"nodes": ["model.test_project.stg_bank__transactions"]},
            "columns": {"transaction_id": {}, "category": {}},
            "tags": [],
            "original_file_path": "models/intermediate/int_transactions_categorised.sql",
            "compiled_code": "SELECT *, 'Food' as category FROM stg_bank__transactions",
            "raw_code": "SELECT * FROM {{ ref('stg_bank__transactions') }}",
        },
    },
    "sources": {},
}


@pytest.fixture()
def manifest_file(tmp_path: Path) -> Path:
    p = tmp_path / "manifest.json"
    p.write_text(json.dumps(SAMPLE_MANIFEST), encoding="utf-8")
    return p


# ── dbt_tools tests ───────────────────────────────────────────────────────────

def test_load_manifest(manifest_file):
    m = load_manifest(str(manifest_file))
    assert "nodes" in m
    assert len(m["nodes"]) == 2


def test_get_model_lineage_found(manifest_file):
    m = load_manifest(str(manifest_file))
    result = get_model_lineage(m, "fct_transactions")
    assert result["model"] == "fct_transactions"
    assert "int_transactions_categorised" in result["parents"]
    assert result["children"] == []


def test_get_model_lineage_not_found(manifest_file):
    m = load_manifest(str(manifest_file))
    result = get_model_lineage(m, "nonexistent_model")
    assert "error" in result


def test_get_model_sql_found(manifest_file):
    m = load_manifest(str(manifest_file))
    result = get_model_sql(m, "fct_transactions")
    assert "compiled_sql" in result
    assert "SELECT" in result["compiled_sql"]


def test_get_model_sql_not_found(manifest_file):
    m = load_manifest(str(manifest_file))
    result = get_model_sql(m, "ghost_model")
    assert "error" in result


# ── bq_tools safety wall tests ────────────────────────────────────────────────

def test_safe_select():
    assert _is_safe("SELECT * FROM `project.dataset.table`")


def test_safe_with():
    assert _is_safe("WITH cte AS (SELECT 1) SELECT * FROM cte")


def test_reject_delete():
    assert not _is_safe("DELETE FROM `project.dataset.table` WHERE 1=1")


def test_reject_drop():
    assert not _is_safe("DROP TABLE `project.dataset.table`")


def test_reject_insert():
    assert not _is_safe("INSERT INTO t SELECT * FROM s")


def test_reject_update():
    assert not _is_safe("UPDATE t SET col = 1")
