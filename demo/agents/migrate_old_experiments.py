from __future__ import annotations

import argparse
import json
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import sys
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent

sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "schema"))
#CURRENT_DIR = Path(__file__).resolve().parent
#PARENT_DIR = CURRENT_DIR.parent
#if str(PARENT_DIR) not in sys.path:
#    sys.path.insert(0, str(PARENT_DIR))

from schema.nl2sql_dataset_utils import load_database_keys, resolve_dataset_split, load_records, normalize_dataset_records

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def generate_run_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{timestamp}_{uuid.uuid4().hex[:8]}"


def load_json(path: str | Path) -> Any:
    with Path(path).open("r", encoding="utf-8") as f:
        return json.load(f)


def _find_normalized_dataset_path(cfg: Dict[str, Any], dataset: str, split: str) -> Path:
    normalized_dir = Path(cfg["normalized_location"])
    candidates = [
        normalized_dir / f"{dataset}_{split}_normalized.json",
        normalized_dir / f"{dataset}_{split}_normalized.jsonl",
        normalized_dir / f"{dataset}_{split}_normalized.parquet",
        normalized_dir / f"{dataset}_{split}_normalized.csv",
    ]
    for path in candidates:
        if path.exists():
            return path
    raise FileNotFoundError(
        f"Could not find normalized dataset for {dataset}/{split} in {normalized_dir}. "
        f"Expected one of: {', '.join(str(p.name) for p in candidates)}"
    )


class JsonlStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def load_by_id(self) -> Dict[Any, Dict[str, Any]]:
        if not self.path.exists():
            return {}
        out: Dict[Any, Dict[str, Any]] = {}
        with self.path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                row = json.loads(line)
                if isinstance(row, dict) and "id" in row:
                    out[row["id"]] = row
        return out

    def append_many(self, rows: List[Dict[str, Any]], overwrite: bool = False) -> int:
        existing = self.load_by_id()
        written = 0
        with self.path.open("a", encoding="utf-8") as f:
            for row in rows:
                row_id = row["id"]
                if row_id in existing and not overwrite:
                    continue
                f.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
                written += 1
        return written


CHAR_REASON_FIELDS = [
    "category_reasoning",
    "ambiguous_reasoning",
    "missing_reasoning",
    "inaccurate_reasoning",
    "ambiguous_nl2sql_not_possible",
    "missing_nl2sql_not_possible",
    "inaccurate_nl2sql_not_possible",
    "ambiguous_question",
    "missing_question",
    "inaccurate_question",
    "ambiguous_schema_linking",
    "missing_schema_linking",
    "inaccurate_schema_linking",
    "ambiguous_projection_fields",
    "missing_projection_fields",
    "inaccurate_projection_fields",
    "ambiguous_aggregation",
    "missing_aggregation",
    "inaccurate_aggregation",
    "ambiguous_predicate_value",
    "missing_predicate_value",
    "inaccurate_predicate_value",
    "ambiguous_temporal_predicate",
    "missing_temporal_predicate",
    "inaccurate_temporal_predicate",
    "ambiguous_comparison_operation",
    "missing_comparison_operation",
    "inaccurate_comparison_operation",
    "ambiguous_equation",
    "missing_equation",
    "inaccurate_equation",
    "ambiguous_redundancy",
    "missing_redundancy",
    "inaccurate_redundancy",
    "ambiguous_null",
    "missing_null",
    "inaccurate_null",
    "ambiguous_sort_order",
    "missing_sort_order",
    "inaccurate_sort_order",
    "ambiguous_group_by",
    "missing_group_by",
    "inaccurate_group_by",
    "ambiguous_nesting",
    "missing_nesting",
    "inaccurate_nesting",
    "ambiguous_join",
    "missing_join",
    "inaccurate_join",
    "ambiguous_db_number",
    "missing_db_number",
    "inaccurate_db_number",
    "api_time",
]

DIAG_FIELDS = [
    "error_reasoning",
    "execution_reasoning",
    "revise_reasoning",
    "execution_match_assessment",
    "question",
    "schema_linking",
    "projection_fields",
    "aggregation",
    "predicate_value",
    "temporal_predicate",
    "comparison_operation",
    "equation",
    "redundancy",
    "null",
    "sort_order",
    "group_by",
    "nesting",
    "join",
    "db_number",
    "api_time",
]

SUMMARY_FIELDS = [
    "pred_sql",
    "gold_execution_flag",
    "pred_execution_flag",
    "gold_columns",
    "pred_columns",
    "gold_shape",
    "pred_shape",
    "gold_error",
    "pred_error",
    "execution_match",
    "exact_match",
]

DETAIL_FIELDS = ["gold_execution_results", "pred_execution_results"]

CACHE_FIELDS = [
    "gold_sql",
    "db_id",
    "db_path",
    "gold_execution_flag",
    "gold_execution_results",
    "gold_columns",
    "gold_shape",
    "gold_error",
]


def ensure_dir(path: str | Path) -> Path:
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def agent_key_for_manifest(agent_name: str, solution_name: Optional[str]) -> str:
    return f"{agent_name}:{solution_name}" if solution_name else agent_name


def update_manifest(experiment_dir: Path, dataset: str, split: str, experiment_name: str, run_id: str, agent_name: str, agent_dir: Path, output_files: Dict[str, str], solution_name: Optional[str] = None) -> None:
    manifest_path = experiment_dir / "manifest.json"
    if manifest_path.exists():
        try:
            manifest = load_json(manifest_path)
        except Exception:
            manifest = {}
    else:
        manifest = {}
    agents = manifest.get("agents", {}) if isinstance(manifest.get("agents", {}), dict) else {}
    agents[agent_key_for_manifest(agent_name, solution_name)] = {
        "agent_name": agent_name,
        "solution_name": solution_name,
        "agent_dir": str(agent_dir),
        "output_files": output_files,
    }
    out = {
        "experiment_name": experiment_name,
        "run_id": run_id,
        "dataset": dataset,
        "split": split,
        "created_at": manifest.get("created_at", now_utc_iso()),
        "updated_at": now_utc_iso(),
        "agents": agents,
    }
    with manifest_path.open("w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)


def save_experiment_settings(experiment_dir: Path, payload: Dict[str, Any]) -> None:
    path = experiment_dir / "settings.json"
    if path.exists():
        return
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def infer_agent_type(rows: List[Dict[str, Any]]) -> str:
    row = rows[0]
    if "error_reasoning" in row or "execution_match_assessment" in row:
        return "diagnosis"
    if "execution_match" in row and "pred_sql" in row:
        return "eval"
    if "pred_sql" in row:
        return "predictions"
    if "category_reasoning" in row or "distribution" in row or "generation_distribution" in row:
        return "characterization"
    raise ValueError("Could not infer agent type from the input rows. Please pass --agent-type explicitly.")


def normalize_classification_block(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict) and "classification" in value:
        return {"classification": value.get("classification"), "description": value.get("description", "")}
    return {"classification": value, "description": ""}


def infer_legacy_settings(file_name: str, dataset: str, split: str, agent_type: str) -> Dict[str, Any]:
    stem = Path(file_name).stem
    parts = stem.split("_")
    dataset_idx = parts.index(dataset) if dataset in parts else None
    diag_model_key = None
    solution_name = None

    if agent_type == "characterization":
        if dataset_idx is not None and dataset_idx >= 1:
            diag_model_key = parts[dataset_idx - 1]
    else:
        if dataset_idx is not None and dataset_idx >= 2:
            diag_model_key = parts[dataset_idx - 2]
            solution_name = parts[dataset_idx - 1]
        elif dataset_idx is not None and dataset_idx >= 1:
            solution_name = parts[dataset_idx - 1]

    context_match = re.search(r"context=(True|False)", stem)
    schema_match = re.search(r"full-schema=(True|False)", stem)
    schema_type_match = re.search(r"schemaType=([^_]+(?:_[^_]+)*)", stem)

    schema_type_legacy = schema_type_match.group(1) if schema_type_match else None
    schema_file_map = {
        "origFullpkfk_2": "full_pkfk_json.json",
        "origFullpkfk_2_scrapped": "full_pkfk_scrapped_json.json",
    }
    schema_file = schema_file_map.get(schema_type_legacy, "full_pkfk_json.json")

    return {
        "use_context": context_match.group(1) == "True" if context_match else False,
        "use_schema": schema_match.group(1) == "True" if schema_match else True,
        "schema_type_legacy": schema_type_legacy,
        "schema_file": schema_file,
        "model_key": diag_model_key,
        "prediction_diagnosis_model_key": diag_model_key,
        "system_model_key": solution_name,
        "solution_name": solution_name,
        "additional_context_name": [],
        "additional_context_file": [],
    }


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    value = str(value)
    value = re.sub(r"\s+", " ", value).strip()
    return value.lower()


def _normalize_sql(value: Any) -> str:
    if value is None:
        return ""
    value = str(value)
    value = re.sub(r"\s+", " ", value).strip()
    value = value.replace("> =", ">=").replace("< =", "<=").replace("! =", "!=")
    return value.lower()


def _normalize_db_id(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().lower()


def load_normalized_rows(database_keys_path: str, dataset: str, split: str) -> tuple[List[Dict[str, Any]], Dict[tuple[str, str, str], List[Dict[str, Any]]]]:
    cfg = resolve_dataset_split(load_database_keys(database_keys_path), dataset, split)
    path = _find_normalized_dataset_path(cfg, dataset, split)
    rows = normalize_dataset_records(
        load_records(path),
        dataset_name=dataset,
        default_db_id=("stackexchange" if dataset == "sede" else None),
    )
    by_triplet: Dict[tuple[str, str, str], List[Dict[str, Any]]] = {}
    for row in rows:
        key = (
            _normalize_text(row.get("nl")),
            _normalize_sql(row.get("sql")),
            _normalize_db_id(row.get("db_id")),
        )
        by_triplet.setdefault(key, []).append(row)
    return rows, by_triplet


def remap_rows_to_normalized_ids(old_rows: List[Dict[str, Any]], database_keys_path: str, dataset: str, split: str) -> tuple[List[Dict[str, Any]], Dict[str, int], List[Dict[str, Any]]]:
    _, by_triplet = load_normalized_rows(database_keys_path, dataset, split)
    remapped: List[Dict[str, Any]] = []
    unmatched: List[Dict[str, Any]] = []
    stats = {"matched_by_triplet": 0, "ambiguous_triplet": 0, "unmatched": 0}
    used_new_ids = set()

    for row in old_rows:
        old_nl = row.get("nl") or row.get("question") or row.get("Title")
        old_sql = row.get("sql") or row.get("gold_sql") or row.get("QueryBody") or row.get("query")
        old_db_id = row.get("db_id") or row.get("database_id") or ("stackexchange" if dataset == "sede" else None)
        key = (_normalize_text(old_nl), _normalize_sql(old_sql), _normalize_db_id(old_db_id))
        candidates = by_triplet.get(key, [])

        if len(candidates) == 1:
            target = candidates[0]
            stats["matched_by_triplet"] += 1
        elif len(candidates) > 1:
            stats["ambiguous_triplet"] += 1
            unmatched.append({"reason": "ambiguous_triplet_match", "original_row": row})
            continue
        else:
            stats["unmatched"] += 1
            unmatched.append({"reason": "no_triplet_match", "original_row": row})
            continue

        new_row = dict(row)
        new_row["id"] = target["id"]
        new_row["nl"] = target["nl"]
        new_row["sql"] = target["sql"]
        new_row["db_id"] = target["db_id"]
        if "context" not in new_row:
            new_row["context"] = target.get("context", "")
        if target["id"] not in used_new_ids:
            remapped.append(new_row)
            used_new_ids.add(target["id"])

    return remapped, stats, unmatched


def migrate_characterization(rows: List[Dict[str, Any]], agent_dir: Path, overwrite: bool) -> Dict[str, int]:
    char_rows: List[Dict[str, Any]] = []
    dist_rows: List[Dict[str, Any]] = []
    for row in rows:
        row_id = row["id"]
        char_row = {"id": row_id}
        has_any_char = False
        for key in CHAR_REASON_FIELDS:
            if key not in row:
                continue
            value = row[key]
            if key.endswith("_nl2sql_not_possible") or (key.startswith(("ambiguous_", "missing_", "inaccurate_")) and key not in {"ambiguous_reasoning", "missing_reasoning", "inaccurate_reasoning", "api_time"}):
                if key != "api_time":
                    value = normalize_classification_block(value)
            char_row[key] = value
            has_any_char = True
        if has_any_char:
            char_rows.append(char_row)
        dist = row.get("generation_distribution", row.get("distribution"))
        if dist is not None:
            dist_rows.append({"id": row_id, "generation_distribution": dist})
    return {
        "characterization_written": JsonlStore(agent_dir / "characterization.jsonl").append_many(char_rows, overwrite=overwrite),
        "distribution_written": JsonlStore(agent_dir / "generation_distribution.jsonl").append_many(dist_rows, overwrite=overwrite),
    }


def migrate_predictions(rows: List[Dict[str, Any]], agent_dir: Path, overwrite: bool) -> Dict[str, int]:
    pred_rows = [{"id": row["id"], "pred_sql": row["pred_sql"]} for row in rows if "id" in row and "pred_sql" in row]
    return {"predictions_written": JsonlStore(agent_dir / "predictions.jsonl").append_many(pred_rows, overwrite=overwrite)}


def _summary_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {"id": row["id"], **{k: row.get(k) for k in SUMMARY_FIELDS if k in row}}


def _detail_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {"id": row["id"], **{k: row.get(k) for k in DETAIL_FIELDS if k in row}}


def _cache_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {"id": row["id"], **{k: row.get(k) for k in CACHE_FIELDS if k in row}}


def migrate_eval(rows: List[Dict[str, Any]], agent_dir: Path, benchmark_cache_dir: Path, overwrite: bool) -> Dict[str, int]:
    summary_rows = [_summary_row(row) for row in rows]
    detail_rows = [_detail_row(row) for row in rows if "gold_execution_results" in row or "pred_execution_results" in row]
    cache_rows = [_cache_row(row) for row in rows if "gold_execution_flag" in row]
    return {
        "sql_eval_summary_written": JsonlStore(agent_dir / "sql_eval_summary.jsonl").append_many(summary_rows, overwrite=overwrite),
        "sql_execution_details_written": JsonlStore(agent_dir / "sql_execution_details.jsonl").append_many(detail_rows, overwrite=overwrite),
        "gold_cache_written": JsonlStore(benchmark_cache_dir / "gold_execution_cache.jsonl").append_many(cache_rows, overwrite=overwrite),
    }


def migrate_diagnosis(rows: List[Dict[str, Any]], agent_dir: Path, overwrite: bool) -> Dict[str, int]:
    diag_rows = []
    for row in rows:
        out = {"id": row["id"]}
        has_any = False
        for key in DIAG_FIELDS:
            if key not in row:
                continue
            value = row[key]
            if key == "execution_match_assessment" or key in {"question", "schema_linking", "projection_fields", "aggregation", "predicate_value", "temporal_predicate", "comparison_operation", "equation", "redundancy", "null", "sort_order", "group_by", "nesting", "join", "db_number"}:
                value = normalize_classification_block(value)
            out[key] = value
            has_any = True
        if has_any:
            diag_rows.append(out)
    return {"prediction_diagnosis_written": JsonlStore(agent_dir / "prediction_diagnosis.jsonl").append_many(diag_rows, overwrite=overwrite)}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate old large JSON experiment files into the new experiment-centered file structure.")
    parser.add_argument("--input-json", required=True, help="Path to the old large JSON file.")
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--split", required=True)
    parser.add_argument("--experiment-name", required=True)
    parser.add_argument("--database-keys", default="database_keys.json")
    parser.add_argument("--result-root", default="experiments")
    parser.add_argument("--agent-type", choices=["auto", "characterization", "predictions", "eval", "diagnosis"], default="auto")
    parser.add_argument("--solution-name", default=None, help="Required for predictions/eval/diagnosis migrations. If omitted, inferred from filename.")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rows = load_json(args.input_json)
    if not isinstance(rows, list):
        raise ValueError("The input experiment JSON must contain a list of rows.")
    if not rows:
        raise ValueError("The input experiment JSON is empty.")

    inferred = infer_agent_type(rows) if args.agent_type == "auto" else args.agent_type
    legacy = infer_legacy_settings(Path(args.input_json).name, args.dataset, args.split, inferred)
    if inferred in {"predictions", "eval", "diagnosis"} and not (args.solution_name or legacy.get("solution_name")):
        raise ValueError("--solution-name is required for predictions, eval, and diagnosis migrations when it cannot be inferred from the filename.")

    remapped_rows, match_stats, unmatched_rows = remap_rows_to_normalized_ids(rows, args.database_keys, args.dataset, args.split)

    run_id = args.run_id or generate_run_id()
    experiment_dir = ensure_dir(Path(args.result_root) / args.dataset / args.split / args.experiment_name)
    benchmark_cache_dir = ensure_dir(Path(args.result_root) / args.dataset / args.split / "_benchmark_cache")

    output_counts: Dict[str, int] = {}
    solution_name = args.solution_name or legacy.get("solution_name")

    if inferred == "characterization":
        agent_name = "benchmark_characterization"
        agent_dir = ensure_dir(experiment_dir / agent_name)
        output_counts.update(migrate_characterization(remapped_rows, agent_dir, overwrite=args.overwrite))
        output_files = {
            "characterization": str(agent_dir / "characterization.jsonl"),
            "generation_distribution": str(agent_dir / "generation_distribution.jsonl"),
        }
    elif inferred == "predictions":
        agent_name = "nl2sql_predictions"
        agent_dir = ensure_dir(experiment_dir / agent_name / solution_name)
        output_counts.update(migrate_predictions(remapped_rows, agent_dir, overwrite=args.overwrite))
        output_files = {"predictions": str(agent_dir / "predictions.jsonl")}
    elif inferred == "eval":
        agent_name = "prediction_diagnosis"
        agent_dir = ensure_dir(experiment_dir / agent_name / solution_name)
        output_counts.update(migrate_eval(remapped_rows, agent_dir, benchmark_cache_dir, overwrite=args.overwrite))
        output_files = {
            "sql_eval_summary": str(agent_dir / "sql_eval_summary.jsonl"),
            "sql_execution_details": str(agent_dir / "sql_execution_details.jsonl"),
            "gold_execution_cache": str(benchmark_cache_dir / "gold_execution_cache.jsonl"),
        }
    elif inferred == "diagnosis":
        agent_name = "prediction_diagnosis"
        agent_dir = ensure_dir(experiment_dir / agent_name / solution_name)
        output_counts.update(migrate_eval(remapped_rows, agent_dir, benchmark_cache_dir, overwrite=args.overwrite))
        output_counts.update(migrate_diagnosis(remapped_rows, agent_dir, overwrite=args.overwrite))
        output_files = {
            "sql_eval_summary": str(agent_dir / "sql_eval_summary.jsonl"),
            "sql_execution_details": str(agent_dir / "sql_execution_details.jsonl"),
            "prediction_diagnosis": str(agent_dir / "prediction_diagnosis.jsonl"),
            "gold_execution_cache": str(benchmark_cache_dir / "gold_execution_cache.jsonl"),
        }
    else:
        raise ValueError(f"Unsupported agent type: {inferred}")

    update_manifest(
        experiment_dir=experiment_dir,
        dataset=args.dataset,
        split=args.split,
        experiment_name=args.experiment_name,
        run_id=run_id,
        agent_name=agent_name,
        agent_dir=agent_dir,
        output_files=output_files,
        solution_name=solution_name,
    )

    settings_payload = {
        "dataset": args.dataset,
        "split": args.split,
        "experiment_name": args.experiment_name,
        "run_id": run_id,
        "database_keys": args.database_keys,
        "result_root": args.result_root,
        "migrated_from": str(Path(args.input_json)),
        "agent_type": inferred,
        "use_context": legacy["use_context"],
        "use_schema": legacy["use_schema"],
        "schema_file": legacy["schema_file"],
        "schema_type_legacy": legacy["schema_type_legacy"],
        "model_key": legacy.get("model_key"),
        "prediction_diagnosis_model_key": legacy.get("prediction_diagnosis_model_key"),
        "system_model_key": legacy.get("system_model_key"),
        "solution_name": solution_name,
        "additional_context_name": [],
        "additional_context_file": [],
        "normalization_match_stats": match_stats,
    }
    save_experiment_settings(experiment_dir, settings_payload)

    summary = {
        "input_json": str(Path(args.input_json)),
        "agent_type": inferred,
        "solution_name": solution_name,
        "run_id": run_id,
        "rows_in_input": len(rows),
        "rows_after_id_remap": len(remapped_rows),
        "match_stats": match_stats,
        "unmatched_rows_file": str(agent_dir / "unmatched_rows.json") if unmatched_rows else None,
        **output_counts,
        "experiment_dir": str(experiment_dir),
        "agent_dir": str(agent_dir),
        "output_files": output_files,
    }
    if unmatched_rows:
        with (agent_dir / "unmatched_rows.json").open("w", encoding="utf-8") as f:
            json.dump(unmatched_rows, f, indent=2, ensure_ascii=False)
    with (agent_dir / "migration_summary.json").open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
