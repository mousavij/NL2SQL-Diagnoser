from __future__ import annotations

import asyncio
import copy
import os
from typing import Any, Dict, Optional

from .exec_eval import exec_on_db, postprocess, result_eq, unwrap_exec_result
from .evaluation import (
    Evaluator,
    build_foreign_key_map_from_json,
    build_valid_col_units,
    rebuild_sql_col,
    rebuild_sql_val,
)
from .process_sql import Schema, get_schema, get_sql

TIMEOUT_SECONDS = 300


def build_sqlite_path(db_root: str, db_id: str) -> str:
    return os.path.join(db_root, db_id, f"{db_id}.sqlite")


def load_fk_maps(fk_map_path: str):
    return build_foreign_key_map_from_json(fk_map_path)


def _normalize_sql(sql: str) -> str:
    if sql is None:
        return None
    if not isinstance(sql, str):
        sql = str(sql)
    return postprocess(sql)


def _safe_parse_sql(schema: Schema, sql: str) -> Optional[Dict[str, Any]]:
    try:
        return get_sql(schema, sql)
    except Exception:
        return None


def compute_exact_match(gold_sql: str, pred_sql: str, sqlite_path: str, fk_map: Dict[str, str]) -> int:
    schema = Schema(get_schema(sqlite_path))
    evaluator = Evaluator()

    gold_parsed = _safe_parse_sql(schema, gold_sql)
    pred_parsed = _safe_parse_sql(schema, pred_sql)
    if gold_parsed is None or pred_parsed is None:
        return 0

    gold_valid_col_units = build_valid_col_units(gold_parsed['from']['table_units'], schema)
    pred_valid_col_units = build_valid_col_units(pred_parsed['from']['table_units'], schema)

    gold_norm = rebuild_sql_col(gold_valid_col_units, rebuild_sql_val(copy.deepcopy(gold_parsed)), fk_map)
    pred_norm = rebuild_sql_col(pred_valid_col_units, rebuild_sql_val(copy.deepcopy(pred_parsed)), fk_map)
    return int(evaluator.eval_exact_match(pred_norm, gold_norm))


def _run_sql(sqlite_path: str, sql: str, timeout: int):
    flag, payload = exec_on_db(sqlite_path, sql, timeout=timeout)
    rows, shape, columns, error = unwrap_exec_result(flag, payload)
    return {
        "flag": flag,
        "results": rows,
        "shape": shape,
        "columns": columns,
        "error": error,
    }


def evaluate_sql_pair(
    gold_sql: str,
    pred_sql: str,
    db_id: str,
    db_root: str,
    fk_map_path: str,
    timeout: int = TIMEOUT_SECONDS,
) -> Dict[str, Any]:
    sqlite_path = build_sqlite_path(db_root=db_root, db_id=db_id)
    fk_maps = load_fk_maps(fk_map_path)
    fk_map = fk_maps.get(db_id, {})

    gold_sql = _normalize_sql(gold_sql)
    pred_sql = _normalize_sql(pred_sql)

    gold_exec = _run_sql(sqlite_path=sqlite_path, sql=gold_sql, timeout=timeout)
    pred_exec = _run_sql(sqlite_path=sqlite_path, sql=pred_sql, timeout=timeout)

    order_matters = 'order by' in gold_sql.lower() if isinstance(gold_sql, str) else False
    execution_match = int(
        gold_exec["flag"] == "result"
        and pred_exec["flag"] == "result"
        and result_eq(gold_exec["results"], pred_exec["results"], order_matters=order_matters)
    )

    exact_match = compute_exact_match(
        gold_sql=gold_sql,
        pred_sql=pred_sql,
        sqlite_path=sqlite_path,
        fk_map=fk_map,
    )

    return {
        "db_id": db_id,
        "db_path": sqlite_path,
        "gold_sql": gold_sql,
        "pred_sql": pred_sql,
        "gold_execution_flag": gold_exec["flag"],
        "pred_execution_flag": pred_exec["flag"],
        "gold_execution_results": gold_exec["results"],
        "pred_execution_results": pred_exec["results"],
        "gold_columns": gold_exec["columns"],
        "pred_columns": pred_exec["columns"],
        "gold_shape": gold_exec["shape"],
        "pred_shape": pred_exec["shape"],
        "gold_error": gold_exec["error"],
        "pred_error": pred_exec["error"],
        "execution_match": execution_match,
        "exact_match": exact_match,
    }


def execute_gold_sql(
    gold_sql: str,
    db_id: str,
    db_root: str,
    fk_map_path: str,
    timeout: int = TIMEOUT_SECONDS,
) -> Dict[str, Any]:
    sqlite_path = build_sqlite_path(db_root=db_root, db_id=db_id)
    gold_sql = _normalize_sql(gold_sql)
    gold_exec = _run_sql(sqlite_path=sqlite_path, sql=gold_sql, timeout=timeout)
    return {
        "id": None,
        "db_id": db_id,
        "db_path": sqlite_path,
        "gold_sql": gold_sql,
        "gold_execution_flag": gold_exec["flag"],
        "gold_execution_results": gold_exec["results"],
        "gold_columns": gold_exec["columns"],
        "gold_shape": gold_exec["shape"],
        "gold_error": gold_exec["error"],
    }
