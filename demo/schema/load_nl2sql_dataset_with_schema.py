from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Dict, List, Sequence

from nl2sql_dataset_utils import (
    DatasetContentError,
    attach_schema_fields,
    load_database_keys,
    load_records,
    load_schema_map,
    normalize_dataset_records,
    resolve_dataset_split,
    save_records,
)


VALID_INPUT_EXTENSIONS = [".json", ".jsonl", ".csv", ".parquet"]


def _resolve_named_context_path(cfg: Dict[str, Any], context_name: str) -> Path:
    inputs_dir = Path(cfg["inputs_location"])
    matches = [inputs_dir / f"{context_name}{ext}" for ext in VALID_INPUT_EXTENSIONS]
    existing = [p for p in matches if p.exists()]
    if not existing:
        tried = ", ".join(str(p) for p in matches)
        raise DatasetContentError(
            f"Additional context file '{context_name}' was not found in {inputs_dir}. Tried: {tried}"
        )
    if len(existing) > 1:
        found = ", ".join(str(p) for p in existing)
        raise DatasetContentError(
            f"Multiple additional context files matched '{context_name}'. "
            f"Please use --additional-context-file explicitly. Found: {found}"
        )
    return existing[0]


def _resolve_additional_context_paths(
    cfg: Dict[str, Any],
    additional_context_files: Sequence[str] | None,
    additional_context_names: Sequence[str] | None,
) -> List[Path]:
    files = list(additional_context_files or [])
    names = list(additional_context_names or [])

    paths: List[Path] = []

    for file_str in files:
        path = Path(file_str)
        if not path.exists():
            raise DatasetContentError(f"Additional context file does not exist: {path}")
        paths.append(path)

    for context_name in names:
        paths.append(_resolve_named_context_path(cfg, context_name))

    if not paths:
        return []

    seen: set[str] = set()
    deduped: List[Path] = []
    for path in paths:
        key = str(path.resolve())
        if key in seen:
            raise DatasetContentError(
                f"Duplicate additional context source provided: {path}"
            )
        seen.add(key)
        deduped.append(path)

    return deduped



def _load_additional_context_map(path: Path) -> Dict[Any, Any]:
    rows = load_records(path)
    context_map: Dict[Any, Any] = {}

    for idx, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            raise DatasetContentError(
                f"Additional context row {idx} in {path} is not an object."
            )
        if "id" not in row:
            raise DatasetContentError(
                f"Additional context row {idx} in {path} is missing required key 'id'."
            )
        if "additional_context" not in row:
            raise DatasetContentError(
                f"Additional context row {idx} in {path} is missing required key 'additional_context'."
            )

        row_id = row["id"]
        if row_id in context_map:
            raise DatasetContentError(
                f"Duplicate id '{row_id}' found in additional context file: {path}"
            )
        context_map[row_id] = row["additional_context"]

    return context_map



def _normalize_context_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return str(value)



def _combine_context_values(values: Sequence[Any], separator: str = "\n\n") -> str:
    parts = [_normalize_context_value(v) for v in values]
    parts = [p for p in parts if p != ""]
    return separator.join(parts)



def _attach_additional_contexts(
    records: List[Dict[str, Any]],
    context_sources: Sequence[tuple[str, Dict[Any, Any]]],
    strict: bool = False,
    separator: str = "\n\n",
) -> List[Dict[str, Any]]:
    output: List[Dict[str, Any]] = []
    missing_by_source: Dict[str, List[Any]] = {source_name: [] for source_name, _ in context_sources}

    for row in records:
        merged = dict(row)
        row_id = merged.get("id")

        stacked_values: List[Any] = []
        used_files: List[str] = []

        for source_name, context_map in context_sources:
            if row_id in context_map:
                stacked_values.append(context_map[row_id])
                used_files.append(source_name)
            else:
                if strict:
                    missing_by_source[source_name].append(row_id)

        merged["additional_context"] = _combine_context_values(stacked_values, separator=separator)
        merged["inputs_files"] = [source_name for source_name, _ in context_sources]
        merged["inputs_file"] = merged["inputs_files"][0] if merged["inputs_files"] else None
        merged["additional_context_sources_used"] = used_files
        output.append(merged)

    if strict:
        error_parts: List[str] = []
        for source_name, missing_ids in missing_by_source.items():
            if missing_ids:
                preview = ", ".join(str(x) for x in missing_ids[:20])
                suffix = " ..." if len(missing_ids) > 20 else ""
                error_parts.append(
                    f"{source_name}: missing {len(missing_ids)} ids ({preview}{suffix})"
                )
        if error_parts:
            raise DatasetContentError(
                "Strict additional context checking failed. " + " | ".join(error_parts)
            )

    return output



def load_dataset_with_schema(
    database_keys_path: str,
    dataset: str,
    split: str,
    input_path: str | None = None,
    schema_file: str = "full_pkfk_json.json",
    output_path: str | None = None,
    additional_context_file: str | Sequence[str] | None = None,
    additional_context_name: str | Sequence[str] | None = None,
    strict_additional_context: bool = False,
    additional_context_separator: str = "\n\n",
) -> List[Dict[str, Any]]:
    database_keys = load_database_keys(database_keys_path)
    cfg = resolve_dataset_split(database_keys, dataset, split)

    source_path = input_path or str(Path(cfg["normalized_location"]) / f"{dataset}_{split}_normalized.json")
    records = normalize_dataset_records(load_records(source_path))
    schema_map = load_schema_map(cfg["schema_location"], schema_file)
    attached = attach_schema_fields(records, schema_map, cfg["schema_location"], schema_file)

    files: List[str]
    names: List[str]

    if additional_context_file is None:
        files = []
    elif isinstance(additional_context_file, str):
        files = [additional_context_file]
    else:
        files = list(additional_context_file)

    if additional_context_name is None:
        names = []
    elif isinstance(additional_context_name, str):
        names = [additional_context_name]
    else:
        names = list(additional_context_name)

    context_paths = _resolve_additional_context_paths(cfg, files, names)
    if context_paths:
        context_sources = [(path.name, _load_additional_context_map(path)) for path in context_paths]
        attached = _attach_additional_contexts(
            attached,
            context_sources,
            strict=strict_additional_context,
            separator=additional_context_separator,
        )

    if output_path:
        save_records(output_path, attached)
    return attached



def main() -> None:
    parser = argparse.ArgumentParser(description="Load a normalized NL2SQL dataset and attach a shared schema map on the fly.")
    parser.add_argument("--database-keys", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--split", required=True)
    parser.add_argument("--input-path", default=None, help="Optional normalized dataset path. If omitted, uses the default saved normalized file.")
    parser.add_argument("--schema-file", default="full_pkfk_json.json")
    parser.add_argument("--output-path", default=None)
    parser.add_argument(
        "--additional-context-file",
        action="append",
        default=[],
        help="Optional path to an additional context file containing 'id' and 'additional_context'. Repeat this flag to stack multiple files.",
    )
    parser.add_argument(
        "--additional-context-name",
        action="append",
        default=[],
        help="Optional saved additional-context file stem to load from the dataset inputs folder. Repeat this flag to stack multiple saved inputs.",
    )
    parser.add_argument(
        "--strict-additional-context",
        action="store_true",
        help="Raise an error if any dataset row does not have a matching additional_context entry in any requested context source.",
    )
    parser.add_argument(
        "--additional-context-separator",
        default="\n\n",
        help="Separator used when stacking multiple additional_context values together.",
    )
    args = parser.parse_args()

    attached = load_dataset_with_schema(
        database_keys_path=args.database_keys,
        dataset=args.dataset,
        split=args.split,
        input_path=args.input_path,
        schema_file=args.schema_file,
        output_path=args.output_path,
        additional_context_file=args.additional_context_file,
        additional_context_name=args.additional_context_name,
        strict_additional_context=args.strict_additional_context,
        additional_context_separator=args.additional_context_separator,
    )

    message = f"Loaded {len(attached)} records with schema file '{args.schema_file}'."
    total_context_sources = len(args.additional_context_file) + len(args.additional_context_name)
    if total_context_sources:
        message += f" Attached {total_context_sources} additional context source(s)."
    print(message)


if __name__ == "__main__":
    main()
