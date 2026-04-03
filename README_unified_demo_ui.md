# Unified NL2SQL-Diagnoser demo UI

This bundle contains a first-pass unified app shell for the demo:

- `App_unified.jsx`: single React view with tabs for Overview, Benchmark Characterization, Prediction Diagnosis, Cross-Model Comparison, Instance Explorer, and Run Agents.
- `server_unified.js`: Express server that scans experiment folders on disk, loads normalized rows + schema JSON from `/datadrive/databases/{dataset}/{split}/...`, and merges characterization / prediction / diagnosis files by id.

## Expected data layout

### Experiments

```text
experiments/{dataset}/{split}/{experiment}/
  manifest.json
  settings.json
  benchmark_characterization/
  nl2sql_predictions/{solution}/
  prediction_diagnosis/{solution}/
```

### Canonical dataset artifacts

```text
/datadrive/databases/{dataset}/{split}/normalized/{dataset}_{split}_normalized.json
/datadrive/databases/{dataset}/{split}/schemas/{schema_file_from_settings}
/datadrive/databases/{dataset}/{split}/inputs/{context_name}.json
```

## Key behavior

- Uses the schema file named in `settings.json`.
- Treats schema as JSON and renders it with a pretty JSON view.
- Supports optional stacked additional context using `additional_context_name`, `additional_context_file`, and `additional_context_separator`.
- Converts legacy prediction-diagnosis terminology from `fair/unfair` to `unflagged/flagged` in the UI.
- Saves corrected edits back into the original `.jsonl` file and creates a timestamped backup named `_old_<timestamp>.jsonl`.

## Suggested integration

- Replace your current `client/src/App.jsx` with `App_unified.jsx`.
- Add `server_unified.js` in your server folder and run it instead of the current annotation-specific server.
- Keep the existing Vite setup.

## Notes

This version intentionally keeps dependencies light:

- no charting library required
- no Tailwind dependency required in the file itself
- no upload flow required

It is meant to be a strong starting point for the demo and can be refined with your existing styling system.
