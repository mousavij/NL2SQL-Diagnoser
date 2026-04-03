from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Dict

from nl2sql_dataset_utils import (
    attach_schema_fields,
    build_schema_maps,
    load_database_keys,
    load_json,
    load_records,
    normalize_dataset_records,
    resolve_dataset_split,
    save_json,
    save_records,
)


def create_dataset_artifacts(database_keys_path: str, dataset: str, split: str, normalized_ext: str = ".json") -> Dict[str, Any]:
    database_keys = load_database_keys(database_keys_path)
    cfg = resolve_dataset_split(database_keys, dataset, split)

    dataset_records = load_records(cfg["dataset"])
    default_db_id = "stackexchange" if dataset == "sede" else None
    normalized_records = normalize_dataset_records(
        dataset_records,
        dataset_name=dataset,
        default_db_id=default_db_id,
    )

    fk_map_entries = load_json(cfg["fk_map"])
    schema_maps = build_schema_maps(fk_map_entries, cfg)

    schema_dir = Path(cfg["schema_location"])
    schema_dir.mkdir(parents=True, exist_ok=True)

    saved_schema_files = []
    for schema_name, schema_content in schema_maps.items():
        out_path = schema_dir / f"{schema_name}.json"
        save_json(out_path, schema_content)
        saved_schema_files.append(str(out_path))

    normalized_dir = Path(cfg["normalized_location"])
    normalized_dir.mkdir(parents=True, exist_ok=True)
    normalized_path = normalized_dir / f"{dataset}_{split}_normalized{normalized_ext}"
    save_records(normalized_path, normalized_records)

    default_schema_file = "full_pkfk_json.json"
    ready_records = attach_schema_fields(
        records=normalized_records,
        schema_map=schema_maps["full_pkfk_json"],
        schema_location=str(schema_dir),
        schema_file=default_schema_file,
    )
    ready_path = normalized_dir / f"{dataset}_{split}_full_pkfk_ready{normalized_ext}"
    save_records(ready_path, ready_records)

    summary = {
        "dataset": dataset,
        "split": split,
        "normalized_dataset": str(normalized_path),
        "full_pkfk_ready_dataset": str(ready_path),
        "schema_location": str(schema_dir),
        "inputs_location": cfg["inputs_location"],
        "saved_schema_files": saved_schema_files,
    }
    save_json(Path(cfg["ds_location"]) / split / f"{dataset}_{split}_artifact_summary.json", summary)
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create normalized NL2SQL dataset artifacts and shared schema files.")
    parser.add_argument("--database-keys", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--split", required=True)
    parser.add_argument("--normalized-ext", default=".json", choices=[".json", ".jsonl", ".csv", ".parquet"])
    args = parser.parse_args()
    print(create_dataset_artifacts(args.database_keys, args.dataset, args.split, args.normalized_ext))
