# Agents Overview

This directory contains the unified NL2SQL benchmark analysis pipeline. The pipeline is organized into three major components:

1. Benchmark Characterization
2. NL2SQL System Predictions
3. Prediction Diagnosis (Evaluation + Error Analysis)

All components share:

* a unified dataset/schema loader
* shared `settings.json`
* experiment-based storage structure
* compatible outputs for UI aggregation

---

## Directory Structure

agents/
├── settings.json
├── characterization/
│   └── characterization.py
├── nl2sql_system/
│   ├── predict_query.py
│   ├── import_predictions.py
│   └── evaluation/
└── prediction_diagnosis/
└── run_prediction_diagnosis.py

---

## Experiment Storage Structure

All agents write into:

experiments/{dataset}/{split}/{experiment_name}/

This folder contains:

settings.json
manifest.json
benchmark_characterization/
nl2sql_predictions/{solution_name}/
prediction_diagnosis/{solution_name}/

---

## Pipeline Flow

1. Characterize benchmark
2. Generate or import predictions
3. Evaluate and diagnose predictions

---

## Join Key

All outputs use:

id

This allows UI aggregation without duplication.
