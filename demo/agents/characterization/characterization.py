from __future__ import annotations

import argparse
import json
import random
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from openai import AzureOpenAI

# Allow importing sibling prompt file and parent schema/data-loader files.
import sys
CURRENT_DIR = Path(__file__).resolve().parent
PARENT_DIR = CURRENT_DIR.parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))
if str(PARENT_DIR) not in sys.path:
    sys.path.insert(0, str(PARENT_DIR))

import prompts  # type: ignore
from ..schema.load_nl2sql_dataset_with_schema import load_dataset_with_schema  # type: ignore
from nl2sql_dataset_utils import (  # type: ignore
    DatasetContentError,
    load_database_keys,
    resolve_dataset_split,
)


SYSTEM_PROMPT_DISTRIBUTION_WITH_CONTEXT = (
    "You are a helpful assistant generating a probability distribution of generated SQL "
    "queries from a given schema, natural language question, and additional context."
)
SYSTEM_PROMPT_DISTRIBUTION_NO_CONTEXT = (
    "You are a helpful assistant generating a probability distribution of generated SQL "
    "queries from a given schema and natural language question."
)
SYSTEM_PROMPT_CHARACTERIZATION = (
    "You are a helpful assistant for classifying whether it is possible for a large "
    "language model to generate SQL queries from natural language questions."
)
AGENT_NAME = "benchmark_characterization"

REASON_FIELDS = [
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
]


@dataclass
class RunSettings:
    dataset: str
    split: str
    model_key: str
    model_config_file: str
    settings_file: Optional[str]
    settings_key: Optional[str]
    schema_file: str
    use_schema: bool
    use_context: bool
    additional_context_names: List[str]
    additional_context_files: List[str]
    strict_additional_context: bool
    additional_context_separator: str
    sample_size: int
    sample_seed: int
    max_workers: int
    max_attempts: int
    base_sleep_seconds: float
    result_root: str
    experiment_name: Optional[str]
    run_name: Optional[str]
    input_path: Optional[str]
    run_id: Optional[str]


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
                f.write(json.dumps(row, ensure_ascii=False) + "\n")

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
            return sum(1 for _ in f if _.strip())



def load_agent_settings(path: str, settings_key: Optional[str] = None) -> Dict[str, Any]:
    settings_path = Path(path)
    if not settings_path.exists():
        raise FileNotFoundError(f"Settings file does not exist: {settings_path}")
    try:
        with settings_path.open("r", encoding="utf-8") as f:
            raw = json.load(f)
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



def merge_args_with_settings(args: argparse.Namespace) -> argparse.Namespace:
    defaults: Dict[str, Any] = {
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
        "run_name": None,
        "input_path": None,
        "run_id": None,
    }
    file_settings: Dict[str, Any] = {}
    if args.settings_file:
        file_settings = load_agent_settings(args.settings_file, args.settings_key)

    merged = vars(args).copy()
    for key, value in defaults.items():
        merged[key] = value
    for key, value in file_settings.items():
        if key in merged and value is not None:
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

    required = ["dataset", "split", "model_config_file", "model_key"]
    missing = [key for key in required if not merged.get(key)]
    if missing:
        raise ValueError(
            "Missing required arguments after applying settings file and CLI overrides: " + ", ".join(missing)
        )

    return argparse.Namespace(**merged)



def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Unified characterization agent built on top of the new schema/data loader.")
    parser.add_argument("--settings-file", default=None, help="Optional JSON settings file containing experiment arguments.")
    parser.add_argument("--settings-key", default=None, help="Optional top-level key/profile inside the settings JSON.")
    parser.add_argument("--dataset", default=None)
    parser.add_argument("--split", default=None)
    parser.add_argument("--database-keys", default=None)
    parser.add_argument("--model-config-file", default=None, help="Path to a JSON file containing model configs, e.g. openai.json")
    parser.add_argument("--model-key", default=None, help="Key to select from the model-config JSON")
    parser.add_argument("--schema-file", default=None)
    parser.add_argument("--input-path", default=None)
    parser.add_argument("--use-schema", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--use-context", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--additional-context-name", action="append", default=None)
    parser.add_argument("--additional-context-file", action="append", default=None)
    parser.add_argument("--strict-additional-context", action="store_true", default=None)
    parser.add_argument("--additional-context-separator", default=None)
    parser.add_argument("--sample-size", type=int, default=None, help="0 means use the full dataset")
    parser.add_argument("--sample-seed", type=int, default=None)
    parser.add_argument("--max-workers", type=int, default=None)
    parser.add_argument("--max-attempts", type=int, default=None)
    parser.add_argument("--base-sleep-seconds", type=float, default=None)
    parser.add_argument("--result-root", default=None, help="Experiment root directory. Default: experiments")
    parser.add_argument("--experiment-name", default=None, help="Shared experiment folder name used across multiple agents.")
    parser.add_argument("--run-name", default=None, help="Legacy alias. If experiment-name is omitted, this is used as the experiment folder name.")
    parser.add_argument("--run-id", default=None, help="Optional unique run id. Auto-generated if omitted.")
    return parser.parse_args()



def load_model_config(path: str, model_key: str) -> Dict[str, Any]:
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Model config file does not exist: {config_path}")
    try:
        with config_path.open("r", encoding="utf-8") as f:
            models = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"Model config file is not valid JSON: {config_path}: {e}") from e

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



def load_dataset_rows(args: argparse.Namespace) -> List[Dict[str, Any]]:
    rows = load_dataset_with_schema(
        database_keys_path=args.database_keys,
        dataset=args.dataset,
        split=args.split,
        input_path=args.input_path,
        schema_file=args.schema_file,
        additional_context_file=args.additional_context_file,
        additional_context_name=args.additional_context_name,
        strict_additional_context=args.strict_additional_context,
        additional_context_separator=args.additional_context_separator,
    )

    if not args.use_schema:
        for row in rows:
            row.pop("schema", None)
            row.pop("schema_file", None)
            row.pop("schema_location", None)

    if args.sample_size and args.sample_size > 0 and args.sample_size < len(rows):
        rng = random.Random(args.sample_seed)
        rows = rng.sample(rows, args.sample_size)
        rows = sorted(rows, key=lambda r: r["id"])

    return rows



def stringify_schema(value: Any) -> str:
    if not value:
        return ""
    if isinstance(value, str):
        return value
    return json.dumps(value, indent=4, ensure_ascii=False)



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
        import ast
        return ast.literal_eval(cleaned)


def _normalize_reason_mapping(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, list):
        merged: Dict[str, Any] = {}
        for item in value:
            if isinstance(item, dict):
                merged.update(item)
        return merged
    return {}


def _unwrap_characterization_payload(parsed: Any) -> Dict[str, Any]:
    if not isinstance(parsed, dict):
        raise DatasetContentError(f"Expected characterization output to be a JSON object, got: {type(parsed).__name__}")

    current = dict(parsed)
    # Common unexpected-but-expected wrappers from newer models.
    for wrapper_key in ["json_row", "output", "result", "answer", "classification", "characterization"]:
        inner = current.get(wrapper_key)
        if isinstance(inner, dict):
            current = dict(inner)
            break

    if "reasons" not in current:
        for wrapper_key in ["json_row", "output", "result", "answer", "classification", "characterization"]:
            inner = current.get(wrapper_key)
            if isinstance(inner, dict) and "reasons" in inner:
                current = dict(inner)
                break

    current["reasons"] = _normalize_reason_mapping(current.get("reasons", {}))
    return current


def _normalize_classification_block(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return {
            "classification": value.get("classification"),
            "description": value.get("description", ""),
        }
    return {
        "classification": value,
        "description": "",
    }


def flatten_characterization_output(parsed: Any, api_time: float) -> Dict[str, Any]:
    parsed = _unwrap_characterization_payload(parsed)
    reasons = _normalize_reason_mapping(parsed.get("reasons", {}))

    flat_row: Dict[str, Any] = {
        "category_reasoning": parsed.get("category_reasoning", ""),
        "ambiguous_reasoning": parsed.get("ambiguous_reasoning", ""),
        "missing_reasoning": parsed.get("missing_reasoning", ""),
        "inaccurate_reasoning": parsed.get("inaccurate_reasoning", ""),
        "ambiguous_nl2sql_not_possible": _normalize_classification_block(parsed.get("ambiguous_nl2sql_not_possible")),
        "missing_nl2sql_not_possible": _normalize_classification_block(parsed.get("missing_nl2sql_not_possible")),
        "inaccurate_nl2sql_not_possible": _normalize_classification_block(parsed.get("inaccurate_nl2sql_not_possible")),
        "api_time": api_time,
    }

    for prefix in ["ambiguous", "missing", "inaccurate"]:
        for field in REASON_FIELDS:
            key = f"{prefix}_{field}"
            flat_row[key] = _normalize_classification_block(reasons.get(key))

    return flat_row


def build_distribution_prompt(row: Dict[str, Any], schema_text: str, effective_context: str) -> str:
    if effective_context:
        return prompts.nl2sql_prompt_context.format(
            schema=schema_text,
            sample_size=5,
            nl=row["nl"],
            context=effective_context,
        )
    return prompts.nl2sql_prompt.format(
        schema=schema_text,
        sample_size=5,
        nl=row["nl"],
    )



def build_characterization_prompt(row: Dict[str, Any], schema_text: str, effective_context: str, distribution: Any) -> str:
    distribution_text = json.dumps(distribution, indent=4, ensure_ascii=False)
    if effective_context:
        return prompts.task_prompt_with_context_and_dist.format(
            schema=schema_text,
            nl=row["nl"],
            context=effective_context,
            sql=row["sql"],
            distribution=distribution_text,
        )
    return prompts.task_prompt_without_context_and_dist.format(
        schema=schema_text,
        nl=row["nl"],
        sql=row["sql"],
        distribution=distribution_text,
    )



def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()



def generate_run_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{timestamp}_{uuid.uuid4().hex[:8]}"



def build_default_experiment_name(settings: RunSettings) -> str:
    context_count = len(settings.additional_context_names) + len(settings.additional_context_files)
    return (
        f"model={settings.model_key}__schema={settings.use_schema}__context={settings.use_context}"
        f"__inputs={context_count}"
    )



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



def get_experiment_dir(settings: RunSettings) -> Path:
    experiment_name = settings.experiment_name or settings.run_name or build_default_experiment_name(settings)
    return Path(settings.result_root) / settings.dataset / settings.split / experiment_name



def get_agent_dir(settings: RunSettings) -> Path:
    return get_experiment_dir(settings) / AGENT_NAME



def save_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)



def prepare_rows_to_process(
    rows: List[Dict[str, Any]],
    dist_store: JsonlStore,
    char_store: JsonlStore,
) -> tuple[List[Dict[str, Any]], Dict[Any, Dict[str, Any]], Dict[Any, Dict[str, Any]]]:
    existing_dist = dist_store.load_by_id()
    existing_char = char_store.load_by_id()
    pending = [row for row in rows if row["id"] not in existing_char]
    return pending, existing_dist, existing_char



def process_one_row(
    row: Dict[str, Any],
    client: AzureOpenAI,
    deployment: str,
    settings: RunSettings,
    throttle: AdaptiveThrottle,
    existing_dist: Dict[Any, Dict[str, Any]],
) -> Dict[str, Any]:
    row_id = row["id"]
    schema_text = stringify_schema(row.get("schema")) if settings.use_schema else ""
    effective_context = build_effective_context(row, settings.use_context)
    used_context_prompt = bool(effective_context)

    distribution_record = existing_dist.get(row_id)
    if distribution_record is None:
        distribution_prompt = build_distribution_prompt(row, schema_text, effective_context)
        parsed_dist, dist_time, dist_attempts, dist_raw = call_model_json(
            client=client,
            deployment=deployment,
            system_prompt=(SYSTEM_PROMPT_DISTRIBUTION_WITH_CONTEXT if used_context_prompt else SYSTEM_PROMPT_DISTRIBUTION_NO_CONTEXT),
            user_prompt=distribution_prompt,
            throttle=throttle,
            max_attempts=settings.max_attempts,
            base_sleep_seconds=settings.base_sleep_seconds,
        )
        generation_distribution = parsed_dist["generation_distribution"]
        distribution_record = {
            "id": row_id,
            "generation_distribution": generation_distribution,
            "meta": {
                "agent_name": AGENT_NAME,
                "run_id": settings.run_id,
                "distribution_api_time": dist_time,
                "distribution_attempts": dist_attempts,
                "used_context_prompt": used_context_prompt,
                "additional_context_sources_used": row.get("additional_context_sources_used", []),
                "inputs_files": row.get("inputs_files", []),
                "created_at": now_utc_iso(),
            },
        }
    else:
        generation_distribution = distribution_record["generation_distribution"]

    characterization_prompt = build_characterization_prompt(
        row=row,
        schema_text=schema_text,
        effective_context=effective_context,
        distribution=generation_distribution,
    )
    parsed_char, char_time, char_attempts, char_raw = call_model_json(
        client=client,
        deployment=deployment,
        system_prompt=SYSTEM_PROMPT_CHARACTERIZATION,
        user_prompt=characterization_prompt,
        throttle=throttle,
        max_attempts=settings.max_attempts,
        base_sleep_seconds=settings.base_sleep_seconds,
    )
    flat_characterization = flatten_characterization_output(parsed_char, api_time=char_time)
    characterization_record = {
        "id": row_id,
        **flat_characterization,
        "meta": {
            "agent_name": AGENT_NAME,
            "run_id": settings.run_id,
            "characterization_api_time": char_time,
            "characterization_attempts": char_attempts,
            "used_context_prompt": used_context_prompt,
            "additional_context_sources_used": row.get("additional_context_sources_used", []),
            "inputs_files": row.get("inputs_files", []),
            "created_at": now_utc_iso(),
        },
    }
    return {
        "distribution_record": distribution_record,
        "characterization_record": characterization_record,
    }



def write_experiment_manifest(settings: RunSettings, experiment_dir: Path, agent_dir: Path, row_count: int) -> None:
    settings_path = experiment_dir / "settings.json"
    manifest_path = experiment_dir / "manifest.json"

    settings_payload = asdict(settings)
    settings_payload["agent_name"] = AGENT_NAME
    settings_payload["experiment_name"] = settings.experiment_name or settings.run_name or build_default_experiment_name(settings)
    settings_payload["run_id"] = settings.run_id
    save_json(settings_path, settings_payload)

    existing_manifest: Dict[str, Any] = {}
    if manifest_path.exists():
        try:
            existing_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception:
            existing_manifest = {}

    agents = existing_manifest.get("agents", {}) if isinstance(existing_manifest.get("agents", {}), dict) else {}
    agents[AGENT_NAME] = {
        "agent_dir": str(agent_dir),
        "output_files": {
            "generation_distribution": str(agent_dir / "generation_distribution.jsonl"),
            "characterization": str(agent_dir / "characterization.jsonl"),
            "failures": str(agent_dir / "failures.jsonl"),
            "progress": str(agent_dir / "progress.json"),
        },
    }

    manifest = {
        "experiment_name": settings.experiment_name or settings.run_name or build_default_experiment_name(settings),
        "run_id": settings.run_id,
        "dataset": settings.dataset,
        "split": settings.split,
        "created_at": existing_manifest.get("created_at", now_utc_iso()),
        "updated_at": now_utc_iso(),
        "row_count": row_count,
        "agents": agents,
    }
    save_json(manifest_path, manifest)



def main() -> None:
    args = merge_args_with_settings(parse_args())

    resolved_experiment_name = args.experiment_name or args.run_name
    resolved_run_id = args.run_id or generate_run_id()

    settings = RunSettings(
        dataset=args.dataset,
        split=args.split,
        model_key=args.model_key,
        model_config_file=args.model_config_file,
        settings_file=args.settings_file,
        settings_key=args.settings_key,
        schema_file=args.schema_file,
        use_schema=args.use_schema,
        use_context=args.use_context,
        additional_context_names=list(args.additional_context_name),
        additional_context_files=list(args.additional_context_file),
        strict_additional_context=args.strict_additional_context,
        additional_context_separator=args.additional_context_separator,
        sample_size=args.sample_size,
        sample_seed=args.sample_seed,
        max_workers=max(1, args.max_workers),
        max_attempts=max(1, args.max_attempts),
        base_sleep_seconds=max(0.0, args.base_sleep_seconds),
        result_root=args.result_root,
        experiment_name=resolved_experiment_name,
        run_name=args.run_name,
        input_path=args.input_path,
        run_id=resolved_run_id,
    )

    database_keys = load_database_keys(args.database_keys)
    resolve_dataset_split(database_keys, args.dataset, args.split)

    model = load_model_config(args.model_config_file, args.model_key)
    rows = load_dataset_rows(args)

    experiment_dir = get_experiment_dir(settings)
    agent_dir = get_agent_dir(settings)
    agent_dir.mkdir(parents=True, exist_ok=True)

    write_experiment_manifest(settings, experiment_dir, agent_dir, len(rows))

    dist_store = JsonlStore(agent_dir / "generation_distribution.jsonl")
    char_store = JsonlStore(agent_dir / "characterization.jsonl")
    fail_store = JsonlStore(agent_dir / "failures.jsonl")

    pending_rows, existing_dist, existing_char = prepare_rows_to_process(rows, dist_store, char_store)
    save_json(
        agent_dir / "progress.json",
        {
            "agent_name": AGENT_NAME,
            "run_id": settings.run_id,
            "total_rows": len(rows),
            "already_have_distribution": len(existing_dist),
            "already_have_characterization": len(existing_char),
            "pending_rows": len(pending_rows),
            "started_at": now_utc_iso(),
        },
    )

    throttle = AdaptiveThrottle(settings.max_workers, settings.base_sleep_seconds)
    deployment = model["deployment"]

    def worker(row: Dict[str, Any]) -> Dict[str, Any]:
        client = build_client(model)
        return process_one_row(
            row=row,
            client=client,
            deployment=deployment,
            settings=settings,
            throttle=throttle,
            existing_dist=existing_dist,
        )

    completed = 0
    with ThreadPoolExecutor(max_workers=settings.max_workers) as executor:
        future_map = {executor.submit(worker, row): row for row in pending_rows}
        for future in as_completed(future_map):
            row = future_map[future]
            try:
                result = future.result()
                if row["id"] not in existing_dist:
                    dist_store.append(result["distribution_record"])
                char_store.append(result["characterization_record"])
                completed += 1
            except Exception as e:
                fail_store.append(
                    {
                        "id": row.get("id"),
                        "error": str(e),
                        "created_at": now_utc_iso(),
                        "run_id": settings.run_id,
                        "throttle": throttle.snapshot(),
                    }
                )
            if completed % 10 == 0 or completed == len(pending_rows):
                save_json(
                    agent_dir / "progress.json",
                    {
                        "agent_name": AGENT_NAME,
                        "run_id": settings.run_id,
                        "total_rows": len(rows),
                        "completed_new_characterizations": completed,
                        "pending_rows": max(0, len(pending_rows) - completed),
                        "throttle": throttle.snapshot(),
                        "updated_at": now_utc_iso(),
                    },
                )

    save_json(
        agent_dir / "progress.json",
        {
            "agent_name": AGENT_NAME,
            "run_id": settings.run_id,
            "total_rows": len(rows),
            "final_distribution_count": len(dist_store.load_by_id()),
            "final_characterization_count": len(char_store.load_by_id()),
            "failure_count": fail_store.count_lines(),
            "throttle": throttle.snapshot(),
            "finished_at": now_utc_iso(),
        },
    )

    write_experiment_manifest(settings, experiment_dir, agent_dir, len(rows))
    print(f"Saved experiment to: {experiment_dir}")
    print(f"Agent output directory: {agent_dir}")


if __name__ == "__main__":
    main()
