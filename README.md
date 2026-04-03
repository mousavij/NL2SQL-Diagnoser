# NL2SQL-Diagnoser
Interactive Analysis of Benchmarks, Predictions, and Evaluation Metrics

---

## Overview

NL2SQL-Diagnoser is an interactive system for analyzing NL2SQL benchmarks, model predictions, and execution-based evaluation metrics. The system jointly characterizes benchmark quality issues and prediction behavior to provide interpretable evaluation beyond aggregate accuracy.

---

# Project Structure
```bash
project_root/
├── client/
├── server/
├── demo/
├── datadrive/
```
---

## System Architecture

![architecture](/imgs/architecture.png)

The system consists of:
- Benchmark Characterization Agent
- Prediction Diagnosis Agent
- SQL Execution Engine
- Aggregation Layer
- Interactive UI

---

# Demo Interface

## 1. Overview — Benchmark Characterization

![overview_characterization](/imgs/overview_characterization.png)

The Overview tab summarizes benchmark quality issues independently of model predictions.

### Coarse Categories
- Ambiguous
- Missing
- Inaccurate
- None

### Fine-Grained Problem Types
- question
- schema linking
- joins
- aggregation
- predicate values
- grouping
- temporal predicate
- equation
- predicate value
- ordering
- projection fields
- nesting
- comparison operations

Features:
- hierarchical visualization
- clickable slices
- dataset filtering
- percentage summaries

---

## 2. Benchmark Characterization — Instance Exploration

![characterization](/imgs/characterization.png)

Displays:
- question
- schema
- gold SQL
- hierarchical labels
- explanations

---

## 3. Overview — Prediction Diagnosis

![overview_prediction](/imgs/overview_prediction.png)

Summarizes prediction behavior.

### Prediction Error Types
- question errors
- schema linking errors
- join errors
- aggregation errors
- predicate value errors
- grouping errors
- temporal predicate errors
- equation errors
- predicate value errors
- ordering errors
- projection errors
- nesting errors
- comparison operation errors

### Execution Outcomes
- match
- mismatch
- flagged match
- flagged mismatch

---

## 4. Prediction Diagnosis — Instance Exploration

![prediction](/imgs/prediction.png)

Displays:
- gold SQL
- predicted SQL
- execution tables
- error labels
- flag status

---

## 5. Cross-Benchmark Insights

![cross_benchmark_characterization](/imgs/cross_benchmark_characterization.png)
![cross_benchmark_prediction](/imgs/cross_benchmark_prediction.png)

Compare:
- datasets
- splits
- experiments
- models

Charts:
- benchmark error distributions
- flagged match rates
- model comparison

---

## 6. Slice Builder

![slice_builder](/imgs/slice_builder.png)

Supports slicing across:
- benchmark labels
- prediction errors
- flags
- datasets
- models

Example:
Dataset → Benchmark Error → Flag → Model

---

## 7. Cross-Model Comparison

![cross_model](/imgs/cross_model.png)

Combines:
- side-by-side predictions
- error distribution comparison
- execution flag analysis

---

## 8. Aggregated Analysis

![aggregated_analysis](/imgs/aggregated_analysis.png)

Combines:
- benchmark characteristics
- prediction diagnosis
- execution flags

---

## 9. Instance Explorer

![instance_explorer](/imgs/instance_explorer.png)

Displays:
- NL question
- schema
- gold SQL
- multiple model predictions
- execution results
- diagnostics

---

## Project Structure

```
project_root/
├── schema/
├── agents/
├── experiments/
├── database_keys.json
```

---

## Setup Status / TODO

Before running the demo, make sure the following assets are available locally.

### Required external assets
- [ ] Download precomputed experiment outputs
- [ ] Download `datadrive/databases/`
- [ ] Place experiment files under `demo/experiments/`
- [ ] Place dataset artifacts under `datadrive/databases/`
- [ ] Confirm paths in `.env` or server configuration
- [ ] Confirm paths in `settings.json` 

---

## Data Download

This repository expects large precomputed artifacts and dataset files to be downloaded separately.

### 1. Precomputed experiments
Download the precomputed experiment bundle here:

**[PLACEHOLDER: link to experiments (Will update soon)]**

After downloading, extract it into:
```bash
demo/experiments/
```

Expected structure:
```bash
demo/experiments/{dataset}/{split}/{experiment_name}/
```

### 2. Dataset artifacts
Download the dataset artifact bundle here:

**[PLACEHOLDER: link to datadrive databases (Will update soon)]**

After downloading, extract it into:
```bash
datadrive/databases/
```

Expected structure:
```bash
datadrive/databases/{dataset}/{split}/
```

Notes:
- These files are not stored directly in the repository because they are large.
- The demo UI and backend expect these folders to be populated before launch.
- If these folders are missing, some pages or experiments will not load correctly.

---

## Installation

### Backend
```
pip install -r requirements.txt
```

### UI Frontend and Backend
```bash
cd server
npm install
cd ../client
npm install
cd ..
npm install
```

---

## Running the Demo

Run the backend and frontend in parallel
```bash
npm run start:watch
```

Open:
```
http://localhost:5050
```

---

## Supported Benchmarks

- Spider
- BIRD
- KaggleDBQA
- SEDE

---


# Workflow

The typical workflow for NL2SQL-Diagnoser is:

1. Select dataset and split
2. Run benchmark characterization agent
3. Generate or import model predictions
4. Execute SQL evaluation
5. Run prediction diagnosis agent
6. Load experiment in UI
7. Explore benchmark issues
8. Analyze prediction behavior
9. Compare models
10. Slice and drill down into instances

---

# Data Organization

Datasets are stored under:

/datadrive/databases/{dataset}/{split}/

Each dataset contains:

- normalized/ : canonical dataset rows
- schemas/ : schema JSON files
- inputs/ : additional context
- artifact summary files

Experiments are stored as:

experiments/{dataset}/{split}/{experiment_name}/

Each experiment includes:

- benchmark_characterization/
- nl2sql_predictions/
- prediction_diagnosis/
- settings.json
- manifest.json

All components are joined by normalized instance IDs.

---

# settings.json

Each experiment contains a settings.json file that controls:

- dataset name
- split
- schema configuration
- additional context
- model configuration
- prompt settings
- execution evaluation settings

Example:

{
  "dataset": "bird",
  "split": "dev",
  "schema_type": "full",
  "additional_context": true,
  "model": "gpt-4o"
}

The settings file ensures reproducibility across:

- characterization
- predictions
- evaluation
- diagnosis

---

# Running Agents

## 1. Benchmark Characterization

python agents/characterization/characterization.py

Outputs:

benchmark_characterization/
- characterization.jsonl
- generation_distribution.jsonl
- failures.jsonl

---

## 2. Generate Predictions

python agents/nl2sql_system/predict_query.py

Outputs:

nl2sql_predictions/{solution_name}/
- predictions.jsonl
- settings.json

---

## 3. Prediction Diagnosis

python agents/prediction_diagnosis/prediction_diagnosis.py

Outputs:

prediction_diagnosis/{solution_name}/
- prediction_diagnosis.jsonl
- sql_execution_details.jsonl

---

# Human-in-the-Loop Annotation

The UI supports manual review and correction of:

- benchmark characterization labels
- prediction diagnosis labels
- flagged execution outcomes
- explanations

Edits immediately update:

- aggregated statistics
- charts
- slice builder
- cross-benchmark comparisons

This allows users to refine LLM-generated annotations.

---

# Example Analysis

Example scenario:

Question:
"If sample 6480 is imported, which country is it originally from?"

Gold SQL:
WHERE sample_pk = 6480 AND origin = 2

Prediction:
WHERE sample_pk = 6480 AND country IS NOT NULL

Both queries return identical execution results.

System output:
- Benchmark label: Missing predicate value
- Prediction error: Predicate value
- Execution: Match
- Flag: True

Interpretation:
The match depends on dataset coincidence rather than semantic equivalence. Evaluation may overestimate model performance.

---

