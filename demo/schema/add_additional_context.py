from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Dict, List, Optional

from nl2sql_dataset_utils import (
    DatasetContentError,
    load_database_keys,
    load_records,
    normalize_dataset_records,
    resolve_dataset_split,
    save_json,
    save_records,
)


TEXT_MATCH_KEYS = ["nl", "question", "Question", "Title", "title", "utterance"]
ID_MATCH_KEYS = ["id", "question_id", "QuerySetId", "query_set_id", "qid"]



def _build_lookup(normalized_records: List[Dict[str, Any]]) -> tuple[Dict[Any, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    by_id = {}
    by_nl = {}
    for row in normalized_records:
        by_id[row["id"]] = row
        by_nl[str(row["nl"])] = row
    return by_id, by_nl



def _get_first_key(row: Dict[str, Any], keys: List[str]) -> Optional[Any]:
    for key in keys:
        if key in row and row[key] not in (None, ""):
            return row[key]
    return None



def add_additional_context(
    database_keys_path: str,
    dataset: str,
    split: str,
    context_file_path: str,
    output_name: str,
    context_key: str = "additional_context",
    output_ext: str = ".json",
) -> Dict[str, Any]:
    database_keys = load_database_keys(database_keys_path)
    cfg = resolve_dataset_split(database_keys, dataset, split)

    normalized_default_path = Path(cfg["normalized_location"]) / f"{dataset}_{split}_normalized.json"
    if not normalized_default_path.exists():
        raise DatasetContentError(
            f"Normalized dataset not found at {normalized_default_path}. Run unified_schema_creator.py first."
        )

    normalized_records = normalize_dataset_records(load_records(normalized_default_path))
    by_id, by_nl = _build_lookup(normalized_records)

    incoming_records = load_records(context_file_path)
    output_rows: List[Dict[str, Any]] = []
    unmatched_rows: List[Dict[str, Any]] = []

    for row in incoming_records:
        if context_key not in row:
            raise DatasetContentError(
                f"Key '{context_key}' was not found in the supplied context file: {context_file_path}"
            )

        matched = None
        supplied_id = _get_first_key(row, ID_MATCH_KEYS)
        if supplied_id in by_id:
            matched = by_id[supplied_id]
        else:
            supplied_nl = _get_first_key(row, TEXT_MATCH_KEYS)
            if supplied_nl is not None:
                matched = by_nl.get(str(supplied_nl))

        if matched is None:
            unmatched_rows.append(row)
            continue

        output_rows.append({"id": matched["id"], "additional_context": row[context_key]})

    inputs_dir = Path(cfg["inputs_location"])
    inputs_dir.mkdir(parents=True, exist_ok=True)
    output_path = inputs_dir / f"{output_name}{output_ext}"
    save_records(output_path, output_rows)

    summary = {
        "dataset": dataset,
        "split": split,
        "inputs_location": str(inputs_dir),
        "output_file": str(output_path),
        "matched_rows": len(output_rows),
        "unmatched_rows": len(unmatched_rows),
    }
    save_json(inputs_dir / f"{output_name}_summary.json", summary)

    if unmatched_rows:
        save_records(inputs_dir / f"{output_name}_unmatched.json", unmatched_rows)

    return summary



def main() -> None:
    parser = argparse.ArgumentParser(description="Attach user-provided additional context to normalized NL2SQL dataset ids.")
    parser.add_argument("--database-keys", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--split", required=True)
    parser.add_argument("--context-file", required=True, help="Input file containing extra prompt/context content.")
    parser.add_argument("--output-name", required=True, help="Output file stem to save under the dataset inputs folder.")
    parser.add_argument("--context-key", default="additional_context")
    parser.add_argument("--output-ext", default=".json", choices=[".json", ".jsonl", ".csv", ".parquet"])
    args = parser.parse_args()

    summary = add_additional_context(
        database_keys_path=args.database_keys,
        dataset=args.dataset,
        split=args.split,
        context_file_path=args.context_file,
        output_name=args.output_name,
        context_key=args.context_key,
        output_ext=args.output_ext,
    )
    print(summary)


if __name__ == "__main__":
    main()
