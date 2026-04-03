from __future__ import annotations

import argparse
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List

import sys
CURRENT_DIR = Path(__file__).resolve().parent
AGENTS_DIR = CURRENT_DIR.parent
ROOT_DIR = AGENTS_DIR.parent
if str(AGENTS_DIR) not in sys.path:
    sys.path.insert(0, str(AGENTS_DIR))
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import prompts  # type: ignore
from agents.agent_utils import (
    AdaptiveThrottle,
    JsonlStore,
    build_client,
    build_default_experiment_name,
    build_effective_context,
    call_model_json,
    generate_run_id,
    get_experiment_dir,
    load_dataset_rows,
    load_model_config,
    merge_args_with_settings,
    normalize_mapping,
    now_utc_iso,
    read_json,
    save_json,
    stringify_schema,
    update_manifest,
    validate_dataset,
)
from agents.nl2sql_system.evaluation.sql_eval_utils import TIMEOUT_SECONDS, evaluate_sql_pair, execute_gold_sql

AGENT_NAME = "prediction_diagnosis"
SYSTEM_PROMPT = "You are a helpful assistant for classifying errors of predicted SQL against the ground truth SQL."
ERROR_FIELDS = [
    "question", "schema_linking", "projection_fields", "aggregation", "predicate_value",
    "temporal_predicate", "comparison_operation", "equation", "redundancy", "null",
    "sort_order", "group_by", "nesting", "join", "db_number",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run SQL evaluation and prediction diagnosis in one experiment-aware pipeline.")
    parser.add_argument("--settings-file", default=None)
    parser.add_argument("--settings-key", default=None)
    parser.add_argument("--dataset", default=None)
    parser.add_argument("--split", default=None)
    parser.add_argument("--database-keys", default=None)
    parser.add_argument("--model-config-file", default=None)
    parser.add_argument("--model-key", default=None)
    parser.add_argument("--schema-file", default=None)
    parser.add_argument("--input-path", default=None)
    parser.add_argument("--use-schema", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--use-context", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--additional-context-name", action="append", default=None)
    parser.add_argument("--additional-context-file", action="append", default=None)
    parser.add_argument("--strict-additional-context", action="store_true", default=None)
    parser.add_argument("--additional-context-separator", default=None)
    parser.add_argument("--result-root", default=None)
    parser.add_argument("--experiment-name", default=None)
    parser.add_argument("--solution-name", default=None)
    parser.add_argument("--predictions-path", default=None)
    parser.add_argument("--max-workers", type=int, default=None)
    parser.add_argument("--max-attempts", type=int, default=None)
    parser.add_argument("--base-sleep-seconds", type=float, default=None)
    parser.add_argument("--evaluation-timeout-seconds", type=int, default=None)
    parser.add_argument("--save-full-execution-results", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--execution-preview-rows", type=int, default=None)
    parser.add_argument("--run-id", default=None)
    return parser.parse_args()


def merge_settings(args: argparse.Namespace) -> argparse.Namespace:
    defaults = {
        "database_keys": "database_keys.json",
        "schema_file": "full_pkfk_json.json",
        "use_schema": True,
        "use_context": True,
        "additional_context_name": [],
        "additional_context_file": [],
        "strict_additional_context": False,
        "additional_context_separator": "\n\n",
        "result_root": "experiments",
        "experiment_name": None,
        "solution_name": None,
        "predictions_path": None,
        "max_workers": 4,
        "max_attempts": 8,
        "base_sleep_seconds": 2.0,
        "evaluation_timeout_seconds": TIMEOUT_SECONDS,
        "save_full_execution_results": True,
        "execution_preview_rows": 10,
        "input_path": None,
        "run_id": None,
    }
    required = ["dataset", "split", "model_config_file", "model_key", "solution_name"]
    return merge_args_with_settings(args, defaults, required)


def load_predictions(args: argparse.Namespace, experiment_dir: Path) -> Dict[Any, Dict[str, Any]]:
    path = Path(args.predictions_path) if args.predictions_path else experiment_dir / "nl2sql_predictions" / args.solution_name / "predictions.jsonl"
    if not path.exists():
        raise FileNotFoundError(f"Predictions file does not exist: {path}")
    store = JsonlStore(path)
    data = store.load_by_id()
    if not data:
        raise ValueError(f"Predictions file is empty or does not contain 'id' keys: {path}")
    return data


def get_preview_rows(results: Any, preview_rows: int) -> Any:
    if results is None or not isinstance(results, list):
        return results
    if len(results) <= preview_rows * 2:
        return results
    return results[:preview_rows] + results[-preview_rows:]


def build_execution_block(prefix: str, metrics: Dict[str, Any], preview_rows: int) -> Dict[str, Any]:
    result_key = f"{prefix}_execution_results"
    rows = metrics.get(result_key)
    return {
        "execution_flag": metrics.get(f"{prefix}_execution_flag"),
        "error_message": str(metrics.get(f"{prefix}_error")),
        "shape": metrics.get(f"{prefix}_shape"),
        "columns": metrics.get(f"{prefix}_columns"),
        "rows": get_preview_rows(rows, preview_rows),
    }


def normalize_assessment(value: Any, description: str = "") -> Dict[str, Any]:
    if isinstance(value, dict) and "classification" in value:
        return {
            "classification": value.get("classification"),
            "description": value.get("description", description),
        }
    return {"classification": value, "description": description}


def flatten_diagnosis_output(parsed: Any, api_time: float) -> Dict[str, Any]:
    if not isinstance(parsed, dict):
        raise ValueError(f"Expected diagnosis output to be a JSON object, got {type(parsed).__name__}")
    current = dict(parsed)
    for wrapper_key in ["json_row", "output", "result", "answer", "diagnosis", "classification"]:
        inner = current.get(wrapper_key)
        if isinstance(inner, dict):
            current = dict(inner)
            break
    errors = normalize_mapping(current.get("errors", {}))
    row: Dict[str, Any] = {
        "error_reasoning": current.get("error_reasoning", ""),
        "execution_reasoning": current.get("execution_reasoning", ""),
        "revise_reasoning": current.get("revise_reasoning", ""),
        "execution_match_assessment": normalize_assessment(
            current.get("execution_match_assessment"),
            current.get("execution_match_reasoning", ""),
        ),
        "api_time": api_time,
    }
    for field in ERROR_FIELDS:
        value = errors.get(field)
        if isinstance(value, dict) and "classification" in value:
            row[field] = {"classification": value.get("classification"), "description": value.get("description", "")}
        else:
            row[field] = {"classification": value, "description": ""}
    return row


def main() -> None:
    args = merge_settings(parse_args())
    cfg = validate_dataset(args.database_keys, args.dataset, args.split)
    model = load_model_config(args.model_config_file, args.model_key)
    rows = load_dataset_rows(args)
    args.run_id = args.run_id or generate_run_id()

    experiment_name = args.experiment_name or build_default_experiment_name(args)
    experiment_dir = get_experiment_dir(args.result_root, args.dataset, args.split, experiment_name)
    predictions_by_id = load_predictions(args, experiment_dir)

    benchmark_cache_dir = Path(args.result_root) / args.dataset / args.split / "_benchmark_cache"
    benchmark_cache_dir.mkdir(parents=True, exist_ok=True)
    gold_cache_store = JsonlStore(benchmark_cache_dir / "gold_execution_cache.jsonl")
    gold_cache = gold_cache_store.load_by_id()

    agent_dir = experiment_dir / AGENT_NAME / args.solution_name
    agent_dir.mkdir(parents=True, exist_ok=True)
    settings_payload = vars(args).copy()
    settings_payload["agent_name"] = AGENT_NAME
    save_json(agent_dir / "settings.json", settings_payload)
    update_manifest(
        experiment_dir,
        experiment_name,
        args.run_id,
        args.dataset,
        args.split,
        AGENT_NAME,
        agent_dir,
        {
            "sql_eval_summary": str(agent_dir / "sql_eval_summary.jsonl"),
            "sql_execution_details": str(agent_dir / "sql_execution_details.jsonl"),
            "diagnosis": str(agent_dir / "prediction_diagnosis.jsonl"),
            "failures": str(agent_dir / "failures.jsonl"),
            "progress": str(agent_dir / "progress.json"),
        },
        extra={"solution_name": args.solution_name, "gold_execution_cache": str(gold_cache_store.path)},
    )

    eval_summary_store = JsonlStore(agent_dir / "sql_eval_summary.jsonl")
    exec_detail_store = JsonlStore(agent_dir / "sql_execution_details.jsonl")
    diag_store = JsonlStore(agent_dir / "prediction_diagnosis.jsonl")
    fail_store = JsonlStore(agent_dir / "failures.jsonl")
    existing_diag = diag_store.load_by_id()

    pending_rows = [row for row in rows if row["id"] in predictions_by_id and row["id"] not in existing_diag]
    save_json(agent_dir / "progress.json", {"total_rows": len(rows), "pending_rows": len(pending_rows), "run_id": args.run_id, "updated_at": now_utc_iso()})

    throttle = AdaptiveThrottle(args.max_workers, args.base_sleep_seconds)
    deployment = model["deployment"]
    db_root = cfg["db_location"]
    fk_map_path = cfg["fk_map"]

    gold_cache_lock = __import__("threading").Lock()

    def ensure_gold_cache(row: Dict[str, Any]) -> Dict[str, Any]:
        row_id = row["id"]
        if row_id in gold_cache:
            return gold_cache[row_id]
        gold = execute_gold_sql(row["sql"], row["db_id"], db_root, fk_map_path, timeout=args.evaluation_timeout_seconds)
        gold["id"] = row_id
        with gold_cache_lock:
            if row_id not in gold_cache:
                gold_cache[row_id] = gold
                gold_cache_store.append(gold)
        return gold

    def worker(row: Dict[str, Any]) -> Dict[str, Any]:
        pred_row = predictions_by_id[row["id"]]
        gold = ensure_gold_cache(row)
        metrics = evaluate_sql_pair(row["sql"], pred_row["pred_sql"], row["db_id"], db_root, fk_map_path, timeout=args.evaluation_timeout_seconds)
        summary_row = {
            "id": row["id"],
            "pred_sql": pred_row["pred_sql"],
            "gold_execution_flag": gold["gold_execution_flag"],
            "pred_execution_flag": metrics["pred_execution_flag"],
            "gold_columns": gold["gold_columns"],
            "pred_columns": metrics["pred_columns"],
            "gold_shape": gold["gold_shape"],
            "pred_shape": metrics["pred_shape"],
            "gold_error": gold["gold_error"],
            "pred_error": metrics["pred_error"],
            "execution_match": metrics["execution_match"],
            "exact_match": metrics["exact_match"],
        }
        detail_row = {
            "id": row["id"],
            "gold_execution_results": gold["gold_execution_results"],
            "pred_execution_results": metrics["pred_execution_results"],
        }
        effective_context = build_effective_context(row, args.use_context)
        schema_text = stringify_schema(row.get("schema")) if args.use_schema else ""
        gold_block = build_execution_block("gold", {**metrics, **gold}, args.execution_preview_rows)
        pred_block = build_execution_block("pred", metrics, args.execution_preview_rows)
        if effective_context:
            user_prompt = prompts.prediction_errors_with_context.format(
                schema=schema_text,
                nl=row["nl"],
                context=effective_context,
                gold_sql=row["sql"],
                pred_sql=pred_row["pred_sql"],
                gold_execution=gold_block,
                pred_execution=pred_block,
                execution_match=str(bool(metrics["execution_match"])),
            )
        else:
            user_prompt = prompts.prediction_errors_without_context.format(
                schema=schema_text,
                nl=row["nl"],
                gold_sql=row["sql"],
                pred_sql=pred_row["pred_sql"],
                gold_execution=gold_block,
                pred_execution=pred_block,
                execution_match=str(bool(metrics["execution_match"])),
            )
        client = build_client(model)
        parsed, elapsed, attempts, _ = call_model_json(client, deployment, SYSTEM_PROMPT, user_prompt, throttle, args.max_attempts, args.base_sleep_seconds)
        diagnosis_row = {
            "id": row["id"],
            **flatten_diagnosis_output(parsed, elapsed),
            "meta": {
                "agent_name": AGENT_NAME,
                "run_id": args.run_id,
                "diagnosis_attempts": attempts,
                "used_context_prompt": bool(effective_context),
                "additional_context_sources_used": row.get("additional_context_sources_used", []),
                "inputs_files": row.get("inputs_files", []),
                "created_at": now_utc_iso(),
            },
        }
        return {"summary": summary_row, "details": detail_row, "diagnosis": diagnosis_row}

    completed = 0
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        future_map = {executor.submit(worker, row): row for row in pending_rows}
        for future in as_completed(future_map):
            row = future_map[future]
            try:
                result = future.result()
                eval_summary_store.append(result["summary"])
                if args.save_full_execution_results:
                    exec_detail_store.append(result["details"])
                diag_store.append(result["diagnosis"])
                completed += 1
            except Exception as e:
                fail_store.append({"id": row["id"], "error": str(e), "run_id": args.run_id, "created_at": now_utc_iso(), "throttle": throttle.snapshot()})
            if completed % 10 == 0 or completed == len(pending_rows):
                save_json(agent_dir / "progress.json", {"total_rows": len(rows), "pending_rows": max(0, len(pending_rows)-completed), "completed_new_rows": completed, "run_id": args.run_id, "updated_at": now_utc_iso(), "throttle": throttle.snapshot()})

    print(f"Saved prediction diagnosis artifacts to: {agent_dir}")


if __name__ == "__main__":
    main()
