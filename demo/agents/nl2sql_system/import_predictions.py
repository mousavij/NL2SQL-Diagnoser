from __future__ import annotations

import argparse
from pathlib import Path

import sys
CURRENT_DIR = Path(__file__).resolve().parent
AGENTS_DIR = CURRENT_DIR.parent
ROOT_DIR = AGENTS_DIR.parent
if str(AGENTS_DIR) not in sys.path:
    sys.path.insert(0, str(AGENTS_DIR))
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from agents.agent_utils import (
    JsonlStore,
    build_default_experiment_name,
    generate_run_id,
    get_experiment_dir,
    load_dataset_rows,
    match_rows_to_dataset,
    merge_args_with_settings,
    now_utc_iso,
    read_json,
    save_json,
    update_manifest,
    validate_dataset,
)
from nl2sql_dataset_utils import load_records

AGENT_NAME = "nl2sql_predictions"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import user-supplied SQL predictions into the shared experiment structure.")
    parser.add_argument("--settings-file", default=None)
    parser.add_argument("--settings-key", default=None)
    parser.add_argument("--dataset", default=None)
    parser.add_argument("--split", default=None)
    parser.add_argument("--database-keys", default=None)
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
    parser.add_argument("--system-model-key", default=None)
    parser.add_argument("--predictions-file", default=None)
    parser.add_argument("--pred-sql-key", default=None)
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
        "system_model_key": None,
        "pred_sql_key": "pred_sql",
        "input_path": None,
        "run_id": None,
    }
    required = ["dataset", "split", "predictions_file", "solution_name"]
    return merge_args_with_settings(args, defaults, required)


def main() -> None:
    args = merge_settings(parse_args())
    validate_dataset(args.database_keys, args.dataset, args.split)
    rows = load_dataset_rows(args)
    supplied = load_records(args.predictions_file)
    matched = match_rows_to_dataset(rows, supplied, args.pred_sql_key)

    args.run_id = args.run_id or generate_run_id()
    experiment_name = args.experiment_name or build_default_experiment_name(args, model_field="system_model_key")
    experiment_dir = get_experiment_dir(args.result_root, args.dataset, args.split, experiment_name)
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
            "predictions": str(agent_dir / "predictions.jsonl"),
            "import_summary": str(agent_dir / "import_summary.json"),
        },
        extra={"solution_name": args.solution_name},
    )

    pred_store = JsonlStore(agent_dir / "predictions.jsonl")
    existing = pred_store.load_by_id()
    new_rows = [row for row in matched if row["id"] not in existing]
    for row in new_rows:
        pred_store.append(row)

    save_json(agent_dir / "import_summary.json", {
        "run_id": args.run_id,
        "dataset": args.dataset,
        "split": args.split,
        "solution_name": args.solution_name,
        "system_model_key": args.system_model_key,
        "input_predictions_file": args.predictions_file,
        "pred_sql_key": args.pred_sql_key,
        "matched_rows": len(matched),
        "new_rows": len(new_rows),
        "skipped_existing_rows": len(matched) - len(new_rows),
        "created_at": now_utc_iso(),
    })
    print(f"Imported predictions to: {agent_dir / 'predictions.jsonl'}")


if __name__ == "__main__":
    main()
