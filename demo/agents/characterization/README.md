# Benchmark Characterization Agent

This agent diagnoses data quality issues in NL2SQL benchmarks.

It identifies:

* ambiguous annotations
* missing information
* inaccurate annotations
* fine-grained NL2SQL problem types

---

## Output Location

experiments/{dataset}/{split}/{experiment_name}/benchmark_characterization/

Files:

* generation_distribution.jsonl
* characterization.jsonl
* failures.jsonl
* progress.json

---

## characterization.jsonl Format

{
"id": 5,
"category_reasoning": "...",
"ambiguous_reasoning": "...",
"missing_reasoning": "...",
"inaccurate_reasoning": "...",
"ambiguous_question": { "classification": false, "description": "" },
...
"api_time": 4.8
}

Flat structure (no nested json_row).

---

Supports GPT-5.2 "reasons as list" format and normalizes automatically.
