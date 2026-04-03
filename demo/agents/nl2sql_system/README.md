# NL2SQL Prediction System

This module handles:

* generating predictions
* importing user predictions
* SQL evaluation utilities

---

## Prediction Output

experiments/{dataset}/{split}/{experiment_name}/
nl2sql_predictions/{solution_name}/

predictions.jsonl:

{"id": 5, "pred_sql": "SELECT ..."}

---

## Importing Predictions

Supports:

* json
* jsonl
* csv
* parquet

Matching:

* by id
* fallback by question text

---

## SQL Evaluation

Uses evaluation stack under:

evaluation/

Gold SQL executions cached:

experiments/{dataset}/{split}/_benchmark_cache/
