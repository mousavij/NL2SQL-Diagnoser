
# Unified NL2SQL Schema + Dataset Loader

This toolkit provides:
- unified schema creation
- normalized dataset loading
- optional stacking of additional context inputs
- dynamic schema attachment at runtime

---

# Overview

Workflow:

1. Normalize dataset
2. Create schema files
3. (Optional) Add additional context files
4. Load dataset with schema and stacked contexts

---

# Files

- `nl2sql_dataset_utils.py`: shared utilities for reading/writing json, jsonl, parquet, csv; dataset normalization; schema construction; and schema attachment.
- `unified_schema_creator.py`: builds normalized datasets plus shared schema files.
- `load_nl2sql_dataset_with_schema.py`: attaches a chosen schema map on the fly later.
- `add_additional_context.py`: saves user-supplied extra prompt/context files aligned to normalized dataset ids.

## Example

```bash
python unified_schema_creator.py \
  --database-keys /mnt/data/database_keys.json \
  --dataset spider \
  --split train
```

This writes:
- normalized dataset: `/datadrive/databases/spider/normalized/spider_train_normalized.json`
- ready dataset with attached main schema: `/datadrive/databases/spider/normalized/spider_train_full_pkfk_ready.json`
- schema folder: `/datadrive/databases/spider/schemas/`
  - `full_pkfk_json.json`
  - `full_pkfk_txt.json`
  - `full_pkfk_scrapped_json.json`
  - `full_pkfk_scrapped_txt.json`

## Add additional context

```bash
python add_additional_context.py \
  --database-keys /mnt/data/database_keys.json \
  --dataset spider \
  --split train \
  --context-file /path/to/my_context.jsonl \
  --output-name cot_prompt_v1 \
  --context-key evidence
```

This writes into `/datadrive/databases/{dataset}/inputs/`.

## Load later on the fly

```bash
python load_nl2sql_dataset_with_schema.py \
  --database-keys /mnt/data/database_keys.json \
  --dataset spider \
  --split train \
  --schema-file full_pkfk_json.json \
  --output-path /tmp/spider_train_ready.json
```

---


# Additional Context System

Additional context allows you to attach **extra prompt inputs** to each NL2SQL example.

Examples:
- schema linking hints
- few-shot demonstrations
- retrieved examples
- reasoning instructions
- system prompts

These are stored separately and merged **at load time**, not baked into the dataset.

Location:
```
/datadrive/databases/{dataset}/inputs/
```

Each context file contains:
```
[
  {"id": 0, "additional_context": "..."},
  {"id": 1, "additional_context": "..."}
]
```

---

# Context Arguments

## --additional-context-name

Loads a saved context from the dataset inputs folder.

Example:
```
--additional-context-name schema_linking
```

This resolves to:
```
/datadrive/databases/{dataset}/inputs/schema_linking.json
```

You can repeat this argument to stack contexts:
```
--additional-context-name schema_linking
--additional-context-name few_shot
--additional-context-name reasoning
```

Contexts are merged in order.

---

## --additional-context-file

Loads a context file directly from any location.

Example:
```
--additional-context-file /tmp/debug_context.json
```

Useful for:
- temporary experiments
- generated context
- debugging

You can repeat this argument:
```
--additional-context-file file1.json
--additional-context-file file2.json
```

---

## Mixing both

You can mix saved names and direct files:

```
--additional-context-name schema_linking
--additional-context-file /tmp/debug.json
```

---

## --additional-context-separator

Controls how stacked contexts are combined.

Default:
```
"\n\n"
```

Example:
```
--additional-context-separator "\n---\n"
```

---

## --strict-additional-context

Raises an error if any dataset row is missing context.

Default behavior:
missing ids → additional_context = ""

Strict behavior:
missing ids → error

Example:
```
--strict-additional-context
```

---

# Resulting Output Fields

After loading:

```
additional_context
inputs_files
additional_context_sources_used
inputs_file
```

Explanation:

- additional_context  
  Combined stacked context text

- inputs_files  
  List of all requested context files

- additional_context_sources_used  
  Which contexts matched this row

- inputs_file  
  First context (backward compatibility)

---

# Example

```
python load_nl2sql_dataset_with_schema.py \
  --database-keys database_keys.json \
  --dataset spider \
  --split train \
  --schema-file full_pkfk_json.json \
  --additional-context-name schema_linking \
  --additional-context-name few_shot \
  --additional-context-name reasoning \
  --output-path spider_ready.json
```

Stacking result:

```
additional_context =
schema_linking_text

few_shot_examples

reasoning_instructions
```

---

# Order Matters

Contexts are merged in the order supplied:

```
A
B
C
```

NOT:

```
C
B
A
```

---

# Recommended Usage

Baseline:
(no context)

Schema linking:
```
--additional-context-name schema_linking
```

Few-shot:
```
--additional-context-name few_shot
```

Stacked:
```
--additional-context-name schema_linking
--additional-context-name few_shot
--additional-context-name reasoning
```

---

# Notes

- Context files are never merged into normalized dataset
- You can create unlimited context variants
- Switching contexts does not require dataset rebuild
- Contexts match using normalized dataset IDs
