from __future__ import annotations

import csv
import json
import math
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Tuple

try:
    import pandas as pd
except Exception:
    pd = None

SUPPORTED_DATA_EXTENSIONS = {".json", ".jsonl", ".parquet", ".csv"}

DATASET_KEY_ALIASES = {
    "id": ["id", "question_id", "QuerySetId", "query_set_id", "qid"],
    "nl": ["nl", "question", "Question", "Title", "title", "utterance"],
    "db_id": ["db_id", "database_id", "db", "database", "Database", "DB_ID"],
    "sql": ["sql", "SQL", "query", "QueryBody", "query_body", "gold_sql"],
    "context": ["context", "evidence", "Evidence", "description", "Description"],
}


class DatasetConfigError(ValueError):
    pass


class DatasetContentError(ValueError):
    pass


def ensure_supported_extension(path: Path) -> None:
    if path.suffix.lower() not in SUPPORTED_DATA_EXTENSIONS:
        raise DatasetContentError(
            f"Unsupported file type '{path.suffix}'. Supported types: {sorted(SUPPORTED_DATA_EXTENSIONS)}"
        )



def load_records(path: str | Path) -> List[Dict[str, Any]]:
    path = Path(path)
    ensure_supported_extension(path)
    suffix = path.suffix.lower()

    if suffix == ".json":
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return [dict(x) for x in data]
        if isinstance(data, dict):
            return [{"key": k, **v} if isinstance(v, dict) else {"key": k, "value": v} for k, v in data.items()]
        raise DatasetContentError(f"JSON file must contain a list or object: {path}")

    if suffix == ".jsonl":
        records: List[Dict[str, Any]] = []
        with path.open("r", encoding="utf-8") as f:
            for line_no, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                if not isinstance(obj, dict):
                    raise DatasetContentError(f"JSONL row {line_no} in {path} is not an object.")
                records.append(obj)
        return records

    if suffix == ".parquet":
        if pd is None:
            raise ImportError("pandas is required for parquet support.")
        df = pd.read_parquet(path)
        df = df.where(pd.notnull(df), None)
        return df.to_dict(orient="records")

    if suffix == ".csv":
        if pd is not None:
            df = pd.read_csv(path)
            df = df.where(pd.notnull(df), None)
            return df.to_dict(orient="records")
        with path.open("r", encoding="utf-8", newline="") as f:
            return list(csv.DictReader(f))

    raise DatasetContentError(f"Unsupported file type: {path}")



def save_records(path: str | Path, records: List[Dict[str, Any]]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    ensure_supported_extension(path)
    suffix = path.suffix.lower()

    if suffix == ".json":
        with path.open("w", encoding="utf-8") as f:
            json.dump(records, f, indent=2, ensure_ascii=False)
        return

    if suffix == ".jsonl":
        with path.open("w", encoding="utf-8") as f:
            for row in records:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
        return

    if suffix == ".parquet":
        if pd is None:
            raise ImportError("pandas is required for parquet support.")
        pd.DataFrame(records).to_parquet(path, index=False)
        return

    if suffix == ".csv":
        if pd is not None:
            pd.DataFrame(records).to_csv(path, index=False)
            return
        if not records:
            path.write_text("", encoding="utf-8")
            return
        fieldnames = sorted({k for row in records for k in row})
        with path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(records)
        return



def load_json(path: str | Path) -> Any:
    with Path(path).open("r", encoding="utf-8") as f:
        return json.load(f)



def save_json(path: str | Path, obj: Any) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)



def load_database_keys(path: str | Path) -> Dict[str, Any]:
    keys = load_json(path)
    if not isinstance(keys, dict):
        raise DatasetConfigError("database_keys.json must contain a top-level object.")
    return keys



def resolve_dataset_split(database_keys: Dict[str, Any], dataset: str, split: str) -> Dict[str, Any]:
    if dataset not in database_keys:
        raise DatasetConfigError(f"Unknown dataset '{dataset}'. Valid datasets: {', '.join(sorted(database_keys))}")
    dataset_cfg = database_keys[dataset]
    if split not in dataset_cfg:
        raise DatasetConfigError(
            f"Unknown split '{split}' for dataset '{dataset}'. Valid splits: {', '.join(sorted(dataset_cfg))}"
        )

    cfg = dict(dataset_cfg[split])
    ds_location = cfg.get("ds_location")
    if not ds_location:
        raise DatasetConfigError(f"Missing 'ds_location' for {dataset}/{split}.")
    cfg["schema_location"] = str(Path(ds_location) / split / "schemas")
    cfg["inputs_location"] = str(Path(ds_location) / split / "inputs")
    cfg["normalized_location"] = str(Path(ds_location) / split / "normalized")
    cfg["dataset_name"] = dataset
    cfg["split_name"] = split
    return cfg



def _first_present(row: Dict[str, Any], aliases: Iterable[str]) -> Tuple[bool, Any]:
    for key in aliases:
        if key in row:
            return True, row.get(key)
    return False, None



def _clean_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    return value



def normalize_dataset_records(
    records: List[Dict[str, Any]],
    dataset_name: str | None = None,
    default_db_id: str | None = None,
) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    next_generated_id = 0

    for row in records:
        row = {k: _clean_value(v) for k, v in dict(row).items()}
        out = dict(row)

        has_id, existing_id = _first_present(row, DATASET_KEY_ALIASES["id"])
        if not has_id or existing_id in (None, ""):
            existing_id = next_generated_id
        out["id"] = existing_id
        next_generated_id = max(next_generated_id, int(existing_id) + 1) if isinstance(existing_id, int) else next_generated_id + 1

        has_nl, nl_value = _first_present(row, DATASET_KEY_ALIASES["nl"])
        has_db_id, db_id_value = _first_present(row, DATASET_KEY_ALIASES["db_id"])
        has_sql, sql_value = _first_present(row, DATASET_KEY_ALIASES["sql"])
        has_context, context_value = _first_present(row, DATASET_KEY_ALIASES["context"])

        if not has_nl or nl_value in (None, ""):
            raise DatasetContentError("Could not infer natural-language question field.")
        if not has_db_id or db_id_value in (None, ""):
            if default_db_id is not None:
                db_id_value = default_db_id
            elif dataset_name == "sede":
                db_id_value = "stackexchange"
            else:
                raise DatasetContentError("Could not infer db_id field.")
        if not has_sql or sql_value in (None, ""):
            raise DatasetContentError("Could not infer sql field.")

        out["nl"] = nl_value
        out["db_id"] = db_id_value
        out["sql"] = sql_value
        out["context"] = context_value if context_value is not None else ""
        normalized.append(out)

    seen = set()
    if any(row["id"] in seen or seen.add(row["id"]) for row in normalized):
        for i, row in enumerate(normalized):
            row["id"] = i

    return normalized



def _stringify_nullable(value: Any) -> str:
    value = _clean_value(value)
    if value is None:
        return ""
    return str(value)



def load_column_metadata_csv(csv_path: str | Path) -> List[Dict[str, Any]]:
    csv_path = Path(csv_path)
    if not csv_path.exists():
        return []

    rows: List[Dict[str, Any]] = []
    with csv_path.open("r", newline="", encoding="latin-1") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        if not fieldnames:
            return []
        for row in reader:
            col_name = row.get(fieldnames[0]) if len(fieldnames) >= 1 else ""
            col_desc = row.get(fieldnames[2]) if len(fieldnames) >= 3 else ""
            col_type = row.get(fieldnames[3]) if len(fieldnames) >= 4 else ""
            val_desc = row.get(fieldnames[4]) if len(fieldnames) >= 5 else ""
            rows.append(
                {
                    "column_name": _stringify_nullable(col_name),
                    "column_description": _stringify_nullable(col_desc),
                    "column_type": _stringify_nullable(col_type),
                    "value_description": _stringify_nullable(val_desc),
                }
            )
    return rows



def _pk_membership_set(pk_by_table: Dict[str, List[Any]]) -> Dict[str, set]:
    result: Dict[str, set] = {}
    for table, pk_values in pk_by_table.items():
        flattened = set()
        for value in pk_values:
            if isinstance(value, list):
                flattened.update(value)
            else:
                flattened.add(value)
        result[table] = flattened
    return result



def build_join_paths(columns: List[List[Any]], foreign_keys: List[List[int]], pk_by_table: Dict[str, List[Any]], table_names: Dict[int, str]) -> List[Dict[str, Any]]:
    pk_flat = _pk_membership_set(pk_by_table)
    join_paths: List[Dict[str, Any]] = []
    seen = set()
    for fk in foreign_keys or []:
        if not isinstance(fk, (list, tuple)) or len(fk) != 2:
            continue
        left_idx, right_idx = fk
        if left_idx >= len(columns) or right_idx >= len(columns):
            continue
        left_table_id, left_col = columns[left_idx]
        right_table_id, right_col = columns[right_idx]
        if left_table_id == -1 or right_table_id == -1:
            continue
        left_table = table_names.get(left_table_id)
        right_table = table_names.get(right_table_id)
        if not left_table or not right_table or not left_col or not right_col:
            continue
        left_card = "one" if left_col in pk_flat.get(left_table, set()) else "many"
        right_card = "one" if right_col in pk_flat.get(right_table, set()) else "many"
        entry = {
            "Table_A": left_table,
            "Column_A": left_col,
            "Table_B": right_table,
            "Column_B": right_col,
            "Relationship": f"{left_card}-to-{right_card}",
        }
        key = tuple(entry.items())
        if key not in seen:
            seen.add(key)
            join_paths.append(entry)
    return join_paths



def format_schema_text(schema_json: Dict[str, Any]) -> str:
    lines = [f"Database Name: {schema_json['Database']}"]
    for table in schema_json.get("Tables", []):
        lines.append(f"\nTable Name: {table['table_name']}")
        if table.get("table_description"):
            lines.append(f"Description: {table['table_description']}")
        if table.get("Primary_Keys"):
            lines.append(f"Primary Keys: {json.dumps(table['Primary_Keys'], ensure_ascii=False)}")
        col_lines = []
        for col in table.get("Columns", []):
            parts = [col.get("column_name", "")]
            if col.get("column_type"):
                parts.append(f"type={col['column_type']}")
            if col.get("column_description"):
                parts.append(f"description={col['column_description']}")
            if col.get("value_description"):
                parts.append(f"values={col['value_description']}")
            if col.get("column_values"):
                parts.append(f"values={json.dumps(col['column_values'], ensure_ascii=False)}")
            col_lines.append(parts[0] if len(parts) == 1 else f"{parts[0]} ({', '.join(parts[1:])})")
        if col_lines:
            lines.append("Columns: " + "; ".join(col_lines))
    if schema_json.get("Join_Paths"):
        lines.append("\nJoin Paths:")
        for path in schema_json["Join_Paths"]:
            lines.append(
                f"- {path['Table_A']}.{path['Column_A']} -> {path['Table_B']}.{path['Column_B']} [{path['Relationship']}]"
            )
    return "\n".join(lines)



def _build_pk_by_table(db: Dict[str, Any], tables: List[str], columns: List[List[Any]]) -> Dict[str, List[Any]]:
    table_names = {i: table for i, table in enumerate(tables)}
    pk_by_table: Dict[str, List[Any]] = {table: [] for table in tables}
    for pk in db.get("primary_keys", []):
        if isinstance(pk, list):
            key_list = []
            origin_table = None
            for pk_idx in pk:
                table_id, col_name = columns[pk_idx]
                if table_id == -1:
                    continue
                origin_table = table_names[table_id]
                key_list.append(col_name)
            if origin_table and key_list:
                pk_by_table[origin_table].append(key_list)
        else:
            table_id, col_name = columns[pk]
            if table_id != -1:
                pk_by_table[table_names[table_id]].append(col_name)
    return pk_by_table



def _build_basic_schema_json(db: Dict[str, Any]) -> Dict[str, Any]:
    database = db["db_id"]
    tables = db["table_names_original"]
    columns = db["column_names_original"]
    column_types = db.get("column_types", [""] * len(columns))
    pk_by_table = _build_pk_by_table(db, tables, columns)
    table_names = {i: t for i, t in enumerate(tables)}
    join_paths = build_join_paths(columns, db.get("foreign_keys", []), pk_by_table, table_names)

    schema_json = {"Database": database, "Tables": [], "Join_Paths": join_paths}
    for t_id, table in enumerate(tables):
        cols = []
        for c_id, c in enumerate(columns):
            if c[0] == t_id:
                cols.append({"column_name": c[1], "column_type": column_types[c_id]})
        schema_json["Tables"].append({"table_name": table, "Columns": cols, "Primary_Keys": pk_by_table[table]})
    return schema_json



def _build_schema_with_column_descriptions(db: Dict[str, Any]) -> Dict[str, Any]:
    schema_json = _build_basic_schema_json(db)
    columns = db["column_names_original"]
    descriptions = db.get("column_descriptions", [""] * len(columns))
    column_types = db.get("column_types", [""] * len(columns))
    table_to_idx = {table: i for i, table in enumerate(db["table_names_original"])}
    for table in schema_json["Tables"]:
        t_id = table_to_idx[table["table_name"]]
        enriched_cols = []
        for c_id, c in enumerate(columns):
            if c[0] == t_id:
                enriched_cols.append(
                    {
                        "column_name": c[1],
                        "column_type": column_types[c_id],
                        "column_description": _stringify_nullable(descriptions[c_id] if c_id < len(descriptions) else ""),
                    }
                )
        table["Columns"] = enriched_cols
    return schema_json



def _candidate_bird_csv_paths(db_location: str | Path, database: str, table: str, renamed_table: str) -> List[Path]:
    base = Path(db_location) / database / "database_description"
    candidates = [base / f"{renamed_table}.csv", base / f"{table}.csv"]
    return list(dict.fromkeys(candidates))



def _fallback_bird_columns(db: Dict[str, Any], table: str) -> List[Dict[str, Any]]:
    columns = db["column_names_original"]
    column_types = db.get("column_types", [""] * len(columns))
    t_id = db["table_names_original"].index(table)
    fallback = []
    for c_id, c in enumerate(columns):
        if c[0] == t_id:
            fallback.append(
                {
                    "column_name": c[1],
                    "column_description": c[1],
                    "column_type": _stringify_nullable(column_types[c_id]),
                    "value_description": "",
                }
            )
    return fallback



def _build_bird_full_schema_json(db: Dict[str, Any], db_location: str | Path) -> Dict[str, Any]:
    database = db["db_id"]
    tables = db["table_names_original"]
    tables_new = db.get("table_names", tables)
    table_rename = {orig: new for orig, new in zip(tables, tables_new)}
    columns = db["column_names_original"]
    pk_by_table = _build_pk_by_table(db, tables, columns)
    table_names = {i: t for i, t in enumerate(tables)}
    join_paths = build_join_paths(columns, db.get("foreign_keys", []), pk_by_table, table_names)

    schema_json = {"Database": database, "Tables": [], "Join_Paths": join_paths}
    for table in tables:
        renamed_table = table_rename.get(table, table)
        cols: List[Dict[str, Any]] = []
        for csv_path in _candidate_bird_csv_paths(db_location, database, table, renamed_table):
            cols = load_column_metadata_csv(csv_path)
            if cols:
                break
        if not cols:
            cols = _fallback_bird_columns(db, table)
        schema_json["Tables"].append({
            "table_name": table,
            "Columns": cols,
            "Primary_Keys": pk_by_table[table],
        })
    return schema_json



def _build_sede_scrapped_schema_json(db: Dict[str, Any], scrapped_schema: Dict[str, Any]) -> Dict[str, Any]:
    database = db["db_id"]
    tables = db["table_names_original"]
    columns = db["column_names_original"]
    column_types = db.get("column_types", [""] * len(columns))
    pk_by_table = _build_pk_by_table(db, tables, columns)
    table_names = {i: t for i, t in enumerate(tables)}
    join_paths = build_join_paths(columns, db.get("foreign_keys", []), pk_by_table, table_names)

    schema_json = {"Database": database, "Tables": [], "Join_Paths": join_paths}
    for t_id, table in enumerate(tables):
        table_desc_block = scrapped_schema.get(str(table), {}) if isinstance(scrapped_schema, dict) else {}
        table_description = _stringify_nullable(table_desc_block.get("description", ""))
        table_cols = []
        table_scrapped_cols = table_desc_block.get("columns", {}) if isinstance(table_desc_block, dict) else {}
        for c_id, c in enumerate(columns):
            if c[0] != t_id:
                continue
            scraped_col = table_scrapped_cols.get(str(c[1]), {}) if isinstance(table_scrapped_cols, dict) else {}
            table_cols.append(
                {
                    "column_name": c[1],
                    "column_type": _stringify_nullable(column_types[c_id]),
                    "column_description": _stringify_nullable(scraped_col.get("description", "")),
                    "column_values": scraped_col.get("values", []) if isinstance(scraped_col, dict) else [],
                }
            )
        schema_json["Tables"].append(
            {
                "table_name": table,
                "table_description": table_description,
                "Columns": table_cols,
                "Primary_Keys": pk_by_table[table],
            }
        )
    return schema_json



def build_schema_maps(fk_map_entries: List[Dict[str, Any]], cfg: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    dataset = cfg["dataset_name"]
    schema_maps: Dict[str, Dict[str, Any]] = {
        "full_pkfk_json": {},
        "full_pkfk_txt": {},
    }

    has_scrapped = bool(cfg.get("scrapped_schema"))
    scrapped_schema = load_json(cfg["scrapped_schema"]) if has_scrapped else None
    if has_scrapped:
        schema_maps["full_pkfk_scrapped_json"] = {}
        schema_maps["full_pkfk_scrapped_txt"] = {}

    for db in fk_map_entries:
        database = db["db_id"]

        if dataset == "bird":
            full_json = _build_bird_full_schema_json(db, cfg["db_location"])
        elif dataset == "kaggle":
            full_json = _build_schema_with_column_descriptions(db)
        else:
            full_json = _build_basic_schema_json(db)

        schema_maps["full_pkfk_json"][database] = full_json
        schema_maps["full_pkfk_txt"][database] = format_schema_text(full_json)

        if has_scrapped:
            scrapped_json = _build_sede_scrapped_schema_json(db, scrapped_schema or {})
            schema_maps["full_pkfk_scrapped_json"][database] = scrapped_json
            schema_maps["full_pkfk_scrapped_txt"][database] = format_schema_text(scrapped_json)

    return schema_maps



def load_schema_map(schema_location: str | Path, schema_file: str) -> Dict[str, Any]:
    return load_json(Path(schema_location) / schema_file)



def attach_schema_fields(records: List[Dict[str, Any]], schema_map: Dict[str, Any], schema_location: str, schema_file: str) -> List[Dict[str, Any]]:
    output = []
    missing_db_ids = []
    for row in records:
        db_id = row.get("db_id")
        if db_id not in schema_map:
            missing_db_ids.append(db_id)
            continue
        merged = dict(row)
        merged["schema_location"] = schema_location
        merged["schema_file"] = schema_file
        merged["schema"] = schema_map[db_id]
        output.append(merged)
    if missing_db_ids:
        missing = ", ".join(sorted({str(x) for x in missing_db_ids}))
        raise DatasetContentError(f"Some db_id values were missing from schema map '{schema_file}': {missing}")
    return output
