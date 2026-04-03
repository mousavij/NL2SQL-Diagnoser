from __future__ import annotations

import argparse
import ast
import json
import threading
import time
import uuid
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

from openai import AzureOpenAI

import sys
CURRENT_DIR = Path(__file__).resolve().parent
PARENT_DIR = CURRENT_DIR.parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))
if str(PARENT_DIR) not in sys.path:
    sys.path.insert(0, str(PARENT_DIR))

from ..schema.load_nl2sql_dataset_with_schema import load_dataset_with_schema  # type: ignore
from ..schema.nl2sql_dataset_utils import (  # type: ignore
    DatasetContentError,
    load_database_keys,
    load_records,
    resolve_dataset_split,
    save_json,
    save_records,
)


class AdaptiveThrottle:
    def __init__(self, max_limit: int, base_sleep: float = 2.0) -> None:
        self.initial_limit = max(1, max_limit)
        self.current_limit = max(1, max_limit)
        self.base_sleep = max(0.0, base_sleep)
        self.extra_sleep = 0.0
        self.inflight = 0
        self.success_streak = 0
        self.timeout_count = 0
        self.lock = threading.Lock()
        self.condition = threading.Condition(self.lock)

    def acquire_slot(self) -> None:
        with self.condition:
            while self.inflight >= self.current_limit:
                self.condition.wait(timeout=0.5)
            self.inflight += 1
            sleep_for = self.extra_sleep
        if sleep_for > 0:
            time.sleep(sleep_for)

    def release_slot(self) -> None:
        with self.condition:
            self.inflight = max(0, self.inflight - 1)
            self.condition.notify_all()

    def record_success(self) -> None:
        with self.condition:
            self.success_streak += 1
            if self.success_streak >= max(5, self.current_limit) and self.current_limit < self.initial_limit:
                self.current_limit += 1
                self.success_streak = 0
            self.extra_sleep = max(0.0, self.extra_sleep * 0.8)
            self.condition.notify_all()

    def record_timeout(self) -> None:
        with self.condition:
            self.timeout_count += 1
            self.success_streak = 0
            if self.current_limit > 1:
                self.current_limit -= 1
            self.extra_sleep = min(30.0, max(self.base_sleep, self.extra_sleep + self.base_sleep))
            self.condition.notify_all()

    def record_error(self) -> None:
        with self.condition:
            self.success_streak = 0
            self.extra_sleep = min(15.0, max(self.base_sleep / 2.0, self.extra_sleep + self.base_sleep / 2.0))
            self.condition.notify_all()

    def snapshot(self) -> Dict[str, Any]:
        with self.lock:
            return {
                "initial_limit": self.initial_limit,
                "current_limit": self.current_limit,
                "extra_sleep": self.extra_sleep,
                "inflight": self.inflight,
                "timeout_count": self.timeout_count,
            }


class JsonlStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()

    def append(self, row: Dict[str, Any]) -> None:
        with self.lock:
            with self.path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")

    def load_by_id(self) -> Dict[Any, Dict[str, Any]]:
        if not self.path.exists():
            return {}
        data: Dict[Any, Dict[str, Any]] = {}
        with self.path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                if isinstance(obj, dict) and "id" in obj:
                    data[obj["id"]] = obj
        return data

    def count_lines(self) -> int:
        if not self.path.exists():
            return 0
        with self.path.open("r", encoding="utf-8") as f:
            return sum(1 for line in f if line.strip())


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def generate_run_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{timestamp}_{uuid.uuid4().hex[:8]}"


def save_json(path: str | Path, obj: Any) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False, default=str)


def read_json(path: str | Path) -> Any:
    with Path(path).open("r", encoding="utf-8") as f:
        return json.load(f)


def load_agent_settings(path: str, settings_key: Optional[str] = None) -> Dict[str, Any]:
    settings_path = Path(path)
    if not settings_path.exists():
        raise FileNotFoundError(f"Settings file does not exist: {settings_path}")
    try:
        raw = read_json(settings_path)
    except json.JSONDecodeError as e:
        raise ValueError(f"Settings file is not valid JSON: {settings_path}: {e}") from e
    if not isinstance(raw, dict):
        raise ValueError(f"Settings file must contain a JSON object: {settings_path}")
    selected: Any = raw
    if settings_key is not None:
        if settings_key not in raw:
            valid = ", ".join(sorted(raw.keys()))
            raise KeyError(f"Settings key '{settings_key}' was not found in {settings_path}. Valid keys: {valid}")
        selected = raw[settings_key]
    if not isinstance(selected, dict):
        raise ValueError("Selected settings entry must be a JSON object.")
    normalized: Dict[str, Any] = {}
    for k, v in selected.items():
        normalized[k.replace('-', '_')] = v
    return normalized


def merge_args_with_settings(args: argparse.Namespace, defaults: Dict[str, Any], required: Sequence[str]) -> argparse.Namespace:
    file_settings: Dict[str, Any] = {}
    if getattr(args, "settings_file", None):
        file_settings = load_agent_settings(args.settings_file, getattr(args, "settings_key", None))
    merged = vars(args).copy()
    for key, value in defaults.items():
        merged[key] = value
    for key, value in file_settings.items():
        if value is not None:
            merged[key] = value
    for key, value in vars(args).items():
        if value is not None:
            merged[key] = value

    for key in ["additional_context_name", "additional_context_file"]:
        value = merged.get(key)
        if value is None:
            merged[key] = []
        elif isinstance(value, str):
            merged[key] = [value]
        elif isinstance(value, list):
            merged[key] = value
        else:
            raise ValueError(f"'{key}' must be a string or list of strings.")

    missing = [key for key in required if not merged.get(key)]
    if missing:
        raise ValueError("Missing required arguments after applying settings file and CLI overrides: " + ", ".join(missing))
    return argparse.Namespace(**merged)


def load_model_config(path: str, model_key: str) -> Dict[str, Any]:
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Model config file does not exist: {config_path}")
    models = read_json(config_path)
    if not isinstance(models, dict):
        raise ValueError(f"Model config JSON must contain an object keyed by model name: {config_path}")
    if model_key not in models:
        valid = ", ".join(sorted(models.keys()))
        raise KeyError(f"Model key '{model_key}' was not found in {config_path}. Valid keys: {valid}")
    model = models[model_key]
    required = ["endpoint", "deployment", "key", "api_version"]
    missing = [k for k in required if k not in model or not model.get(k)]
    if missing:
        raise ValueError(f"Model config for '{model_key}' is missing required keys: {missing}")
    return model


def build_client(model: Dict[str, Any]) -> AzureOpenAI:
    return AzureOpenAI(
        api_version=model["api_version"],
        azure_endpoint=model["endpoint"],
        api_key=model["key"],
    )


def robust_json_loads(text: str) -> Any:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.strip("`")
        if cleaned.startswith("json"):
            cleaned = cleaned[4:]
    cleaned = cleaned.strip().strip('"')
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        return ast.literal_eval(cleaned)


def normalize_mapping(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, list):
        merged: Dict[str, Any] = {}
        for item in value:
            if isinstance(item, dict):
                merged.update(item)
        return merged
    return {}


def call_model_json(
    client: AzureOpenAI,
    deployment: str,
    system_prompt: str,
    user_prompt: str,
    throttle: AdaptiveThrottle,
    max_attempts: int,
    base_sleep_seconds: float,
) -> tuple[Any, float, int, str]:
    last_error: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        throttle.acquire_slot()
        started = time.perf_counter()
        try:
            response = client.chat.completions.create(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                model=deployment,
            )
            elapsed = time.perf_counter() - started
            raw_response = response.choices[0].message.content or ""
            parsed = robust_json_loads(raw_response)
            throttle.record_success()
            return parsed, elapsed, attempt, raw_response
        except Exception as e:
            last_error = e
            lowered = str(e).lower()
            if any(token in lowered for token in ["timeout", "timed out", "rate limit", "429", "503"]):
                throttle.record_timeout()
            else:
                throttle.record_error()
            if attempt < max_attempts:
                time.sleep(min(60.0, base_sleep_seconds * attempt + throttle.snapshot()["extra_sleep"]))
        finally:
            throttle.release_slot()
    raise RuntimeError(f"Model call failed after {max_attempts} attempts: {last_error}")


def load_dataset_rows(args: argparse.Namespace) -> List[Dict[str, Any]]:
    return load_dataset_with_schema(
        database_keys_path=args.database_keys,
        dataset=args.dataset,
        split=args.split,
        input_path=getattr(args, "input_path", None),
        schema_file=args.schema_file,
        additional_context_file=args.additional_context_file,
        additional_context_name=args.additional_context_name,
        strict_additional_context=args.strict_additional_context,
        additional_context_separator=args.additional_context_separator,
    )


def build_effective_context(row: Dict[str, Any], use_context: bool) -> str:
    parts: List[str] = []
    if use_context:
        base_context = row.get("context") or ""
        if base_context:
            parts.append(str(base_context))
    extra_context = row.get("additional_context") or ""
    if extra_context:
        parts.append(str(extra_context))
    return "\n\n".join([p for p in parts if p])


def stringify_schema(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return json.dumps(value, indent=4, ensure_ascii=False)


def build_default_experiment_name(args: argparse.Namespace, model_field: str = "model_key") -> str:
    context_count = len(getattr(args, "additional_context_name", []) or []) + len(getattr(args, "additional_context_file", []) or [])
    model_name = getattr(args, model_field, None) or "na"
    return f"model={model_name}__schema={getattr(args, 'use_schema', True)}__context={getattr(args, 'use_context', True)}__inputs={context_count}"


def get_experiment_dir(result_root: str, dataset: str, split: str, experiment_name: str) -> Path:
    return Path(result_root) / dataset / split / experiment_name


def update_manifest(experiment_dir: Path, experiment_name: str, run_id: str, dataset: str, split: str, agent_name: str, agent_dir: Path, output_files: Dict[str, str], extra: Optional[Dict[str, Any]] = None) -> None:
    manifest_path = experiment_dir / "manifest.json"
    existing_manifest: Dict[str, Any] = {}
    if manifest_path.exists():
        try:
            existing_manifest = read_json(manifest_path)
        except Exception:
            existing_manifest = {}
    agents = existing_manifest.get("agents", {}) if isinstance(existing_manifest.get("agents", {}), dict) else {}
    agents[agent_name] = {
        "agent_dir": str(agent_dir),
        "output_files": output_files,
    }
    if extra:
        agents[agent_name].update(extra)
    manifest = {
        "experiment_name": experiment_name,
        "run_id": run_id,
        "dataset": dataset,
        "split": split,
        "created_at": existing_manifest.get("created_at", now_utc_iso()),
        "updated_at": now_utc_iso(),
        "agents": agents,
    }
    save_json(manifest_path, manifest)


def validate_dataset(database_keys_path: str, dataset: str, split: str) -> Dict[str, Any]:
    database_keys = load_database_keys(database_keys_path)
    return resolve_dataset_split(database_keys, dataset, split)


def normalize_pred_sql_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        for key in ["sql", "pred_sql", "query", "prediction"]:
            if key in value:
                return normalize_pred_sql_value(value[key])
    return str(value)


def match_rows_to_dataset(dataset_rows: List[Dict[str, Any]], input_rows: List[Dict[str, Any]], pred_sql_key: str) -> List[Dict[str, Any]]:
    by_id = {row["id"]: row for row in dataset_rows}
    by_nl = {str(row["nl"]): row for row in dataset_rows}
    matched: List[Dict[str, Any]] = []
    used_ids = set()
    for row in input_rows:
        target = None
        row_id = row.get("id")
        if row_id in by_id:
            target = by_id[row_id]
        else:
            nl = row.get("nl") or row.get("question") or row.get("Title")
            if nl is not None:
                target = by_nl.get(str(nl))
        if target is None:
            raise DatasetContentError("Could not match a supplied prediction row to the normalized dataset by id or nl/question.")
        if pred_sql_key not in row:
            raise DatasetContentError(f"Supplied predictions file is missing required key '{pred_sql_key}'.")
        if target["id"] in used_ids:
            raise DatasetContentError(f"Duplicate matched prediction for dataset id {target['id']}.")
        used_ids.add(target["id"])
        matched.append({"id": target["id"], "pred_sql": normalize_pred_sql_value(row[pred_sql_key])})
    return matched
