from __future__ import annotations

import argparse
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List

from openai import AzureOpenAI

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
    now_utc_iso,
    normalize_pred_sql_value,
    save_json,
    stringify_schema,
    update_manifest,
    validate_dataset,
)

AGENT_NAME = "nl2sql_predictions"
SYSTEM_PROMPT = "You are a helpful assistant for generating SQL from natural language questions."


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate predicted SQL using the unified dataset/schema/context loader.")
    parser.add_argument("--settings-file", default=None)
    parser.add_argument("--settings-key", default=None)
    parser.add_argument("--dataset", default=None)
    parser.add_argument("--split", default=None)
    parser.add_argument("--database-keys", default=None)
    parser.add_argument("--model-config-file", default=None)
    parser.add_argument("--system-model-key", default=None)
    parser.add_argument("--schema-file", default=None)
    parser.add_argument("--input-path", default=None)
    parser.add_argument("--use-schema", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--use-context", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--additional-context-name", action="append", default=None)
    parser.add_argument("--additional-context-file", action="append", default=None)
    parser.add_argument("--strict-additional-context", action="store_true", default=None)
    parser.add_argument("--additional-context-separator", default=None)
    parser.add_argument("--sample-size", type=int, default=None)
    parser.add_argument("--sample-seed", type=int, default=None)
    parser.add_argument("--max-workers", type=int, default=None)
    parser.add_argument("--max-attempts", type=int, default=None)
    parser.add_argument("--base-sleep-seconds", type=float, default=None)
    parser.add_argument("--result-root", default=None)
    parser.add_argument("--experiment-name", default=None)
    parser.add_argument("--solution-name", default=None)
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
        "sample_size": 0,
        "sample_seed": 42,
        "max_workers": 4,
        "max_attempts": 8,
        "base_sleep_seconds": 2.0,
        "result_root": "experiments",
        "experiment_name": None,
        "solution_name": None,
        "input_path": None,
        "run_id": None,
    }
    required = ["dataset", "split", "model_config_file", "system_model_key"]
    merged = merge_args_with_settings(args, defaults, required)
    if not merged.solution_name:
        merged.solution_name = f"system_model={merged.system_model_key}"
    return merged


def build_prompt(row: Dict[str, Any], use_schema: bool, use_context: bool) -> str:
    schema_text = stringify_schema(row.get("schema")) if use_schema else ""
    effective_context = build_effective_context(row, use_context)
    if effective_context:
        return prompts.gen_nl2sql_with_context.format(schema=schema_text, nl=row["nl"], context=effective_context)
    return prompts.gen_nl2sql_without_context.format(schema=schema_text, nl=row["nl"])


def extract_predicted_sql(parsed: Any) -> str:
    if isinstance(parsed, str):
        return parsed
    if isinstance(parsed, dict):
        for key in ["sql", "pred_sql", "query", "prediction", "answer"]:
            if key in parsed:
                return normalize_pred_sql_value(parsed[key])
    if isinstance(parsed, list) and parsed:
        return extract_predicted_sql(parsed[0])
    raise ValueError("Could not extract predicted SQL from model output.")


def get_agent_dir(args: argparse.Namespace) -> Path:
    experiment_name = args.experiment_name or build_default_experiment_name(args, model_field="system_model_key")
    experiment_dir = get_experiment_dir(args.result_root, args.dataset, args.split, experiment_name)
    return experiment_dir / AGENT_NAME / args.solution_name


def main() -> None:
    args = merge_settings(parse_args())
    validate_dataset(args.database_keys, args.dataset, args.split)
    model = load_model_config(args.model_config_file, args.system_model_key)
    rows = load_dataset_rows(args)
    if args.sample_size and args.sample_size > 0 and args.sample_size < len(rows):
        import random
        rng = random.Random(args.sample_seed)
        rows = sorted(rng.sample(rows, args.sample_size), key=lambda x: x["id"])

    args.run_id = args.run_id or generate_run_id()
    experiment_name = args.experiment_name or build_default_experiment_name(args, model_field="system_model_key")
    experiment_dir = get_experiment_dir(args.result_root, args.dataset, args.split, experiment_name)
    agent_dir = get_agent_dir(args)
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
            "predictions": str(agent_dir / "predictions.jsonl"),
            "failures": str(agent_dir / "failures.jsonl"),
            "progress": str(agent_dir / "progress.json"),
        },
        extra={"solution_name": args.solution_name},
    )

    pred_store = JsonlStore(agent_dir / "predictions.jsonl")
    fail_store = JsonlStore(agent_dir / "failures.jsonl")
    existing = pred_store.load_by_id()
    pending = [row for row in rows if row["id"] not in existing]
    save_json(agent_dir / "progress.json", {"total_rows": len(rows), "pending_rows": len(pending), "run_id": args.run_id, "updated_at": now_utc_iso()})

    throttle = AdaptiveThrottle(args.max_workers, args.base_sleep_seconds)
    deployment = model["deployment"]

    def worker(row: Dict[str, Any]) -> Dict[str, Any]:
        client = build_client(model)
        prompt = build_prompt(row, args.use_schema, args.use_context)
        parsed, _, _, _ = call_model_json(client, deployment, SYSTEM_PROMPT, prompt, throttle, args.max_attempts, args.base_sleep_seconds)
        return {"id": row["id"], "pred_sql": extract_predicted_sql(parsed)}

    completed = 0
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        future_map = {executor.submit(worker, row): row for row in pending}
        for future in as_completed(future_map):
            row = future_map[future]
            try:
                pred_store.append(future.result())
                completed += 1
            except Exception as e:
                fail_store.append({"id": row["id"], "error": str(e), "run_id": args.run_id, "created_at": now_utc_iso(), "throttle": throttle.snapshot()})
            if completed % 10 == 0 or completed == len(pending):
                save_json(agent_dir / "progress.json", {"total_rows": len(rows), "pending_rows": max(0, len(pending)-completed), "completed_new_predictions": completed, "run_id": args.run_id, "updated_at": now_utc_iso(), "throttle": throttle.snapshot()})

    print(f"Saved predictions to: {agent_dir / 'predictions.jsonl'}")


if __name__ == "__main__":
    main()
