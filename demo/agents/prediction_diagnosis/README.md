# Prediction Diagnosis Agent

Performs:

* SQL execution evaluation
* execution match
* exact match
* LLM error diagnosis

---

## Output

experiments/{dataset}/{split}/{experiment_name}/
prediction_diagnosis/{solution_name}/

Files:

* sql_eval_summary.jsonl
* sql_execution_details.jsonl
* prediction_diagnosis.jsonl
* failures.jsonl
* progress.json

---

## sql_eval_summary.jsonl

Small UI-friendly metrics file.

## sql_execution_details.jsonl

Large execution outputs (lazy loaded by UI).

## prediction_diagnosis.jsonl

Flat classification output keyed by id.

All outputs joinable using id.
