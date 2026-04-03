from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batch-migrate old large JSON experiment files into the new experiment-centered structure.")
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--split", required=True)
    parser.add_argument("--experiment-name", required=True)
    parser.add_argument("--database-keys", default="../database_keys.json")
    parser.add_argument("--result-root", default="experiments")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--recursive", action="store_true")
    parser.add_argument("--solution-map", default=None, help="Optional JSON file mapping filename substrings to solution names.")
    return parser.parse_args()


def load_solution_map(path: Optional[str]) -> Dict[str, str]:
    if not path:
        return {}
    with Path(path).open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError("solution-map file must be a JSON object mapping filename substrings to solution names.")
    return {str(k): str(v) for k, v in data.items()}


def infer_solution_name(file_name: str, solution_map: Dict[str, str], dataset: str) -> Optional[str]:
    for pattern, solution_name in solution_map.items():
        if pattern in file_name:
            return solution_name
    stem = Path(file_name).stem
    parts = stem.split("_")
    if dataset in parts:
        dataset_idx = parts.index(dataset)
        if "prediction" in stem and dataset_idx >= 2:
            return parts[dataset_idx - 1]
        if dataset_idx >= 1:
            return parts[dataset_idx - 1]
    return None


def main() -> None:
    args = parse_args()
    input_dir = Path(args.input_dir)
    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory does not exist: {input_dir}")

    solution_map = load_solution_map(args.solution_map)
    globber = input_dir.rglob if args.recursive else input_dir.glob
    files = sorted([p for p in globber("*.json") if p.is_file()])
    if not files:
        raise ValueError(f"No JSON files found in {input_dir}")

    results: List[Dict[str, Any]] = []
    migrate_script = Path(__file__).resolve().parent / "migrate_old_experiments.py"

    for path in files:
        solution_name = infer_solution_name(path.name, solution_map, args.dataset)
        cmd = [
            "python", str(migrate_script),
            "--input-json", str(path),
            "--dataset", args.dataset,
            "--split", args.split,
            "--experiment-name", args.experiment_name,
            "--database-keys", args.database_keys,
            "--result-root", args.result_root,
        ]
        if args.overwrite:
            cmd.append("--overwrite")
        if solution_name:
            cmd.extend(["--solution-name", solution_name])
        proc = subprocess.run(cmd, capture_output=True, text=True)
        results.append({
            "file": str(path),
            "solution_name": solution_name,
            "returncode": proc.returncode,
            "stdout": proc.stdout,
            "stderr": proc.stderr,
        })

    summary_path = Path(args.result_root) / args.dataset / args.split / args.experiment_name / "migration_batch_summary.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    with summary_path.open("w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(json.dumps({
        "processed_files": len(results),
        "successes": sum(1 for r in results if r["returncode"] == 0),
        "failures": sum(1 for r in results if r["returncode"] != 0),
        "summary_file": str(summary_path),
    }, indent=2))


if __name__ == "__main__":
    main()
