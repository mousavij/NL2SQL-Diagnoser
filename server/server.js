import express from "express";
import cors from "cors";
import fs from "fs/promises";
import { existsSync } from "fs";
import path from "path";
import { spawn } from "child_process";
import { fileURLToPath } from "url";
import dotenv from "dotenv";
dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const CLIENT_DIST = path.resolve(__dirname, "dist");





const app = express();
app.use(cors());
app.use(express.json({ limit: "10mb" }));

const PORT = process.env.PORT || 5050;
const EXPERIMENTS_ROOT = process.env.EXPERIMENTS_ROOT || path.resolve("experiments");
const DATABASES_ROOT = process.env.DATABASES_ROOT || "/datadrive/databases";
const AGENTS_ROOT = process.env.AGENTS_ROOT || path.resolve("agents");

const cache = {
  json: new Map(),
  jsonl: new Map(),
  schema: new Map(),
  normalized: new Map(),
};
function experimentsBaseRoot() {
  return path.dirname(EXPERIMENTS_ROOT);
}

function resolveArtifactPath(filePath) {
  if (!filePath) return null;
  if (path.isAbsolute(filePath)) return filePath;
  return path.resolve(experimentsBaseRoot(), filePath);
}
function nowStamp() {
  return new Date().toISOString().replace(/[:.]/g, "-");
}

async function readJson(filePath, useCache = true) {
  if (useCache && cache.json.has(filePath)) return cache.json.get(filePath);
  const raw = await fs.readFile(filePath, "utf-8");
  const parsed = JSON.parse(raw);
  if (useCache) cache.json.set(filePath, parsed);
  return parsed;
}

async function readJsonl(filePath, useCache = true) {
  if (useCache && cache.jsonl.has(filePath)) return cache.jsonl.get(filePath);
  const raw = await fs.readFile(filePath, "utf-8");
  const rows = raw
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => JSON.parse(line));
  if (useCache) cache.jsonl.set(filePath, rows);
  return rows;
}

function mapById(rows) {
  const out = new Map();
  for (const row of rows || []) {
    if (row && row.id != null) out.set(String(row.id), row);
  }
  return out;
}

function basePathForDataset(dataset, split) {
  return path.join(DATABASES_ROOT, dataset, split);
}


async function loadNormalized(dataset, split) {
  const key = `${dataset}::${split}`;
  if (cache.normalized.has(key)) return cache.normalized.get(key);
  const filePath = path.join(basePathForDataset(dataset, split), "normalized", `${dataset}_${split}_normalized.json`);
  const data = await readJson(filePath);
  const mapped = mapById(data);
  cache.normalized.set(key, mapped);
  return mapped;
}

async function loadSchemaMap(dataset, split, schemaFile) {
  const key = `${dataset}::${split}::${schemaFile}`;
  if (cache.schema.has(key)) return cache.schema.get(key);
  const filePath = path.join(basePathForDataset(dataset, split), "schemas", schemaFile);
  const json = await readJson(filePath);
  cache.schema.set(key, json);
  return json;
}

async function loadAdditionalContexts(dataset, split, settings) {
  const result = { byId: new Map(), files: [] };
  const names = Array.isArray(settings.additional_context_name) ? settings.additional_context_name : [];
  const directFiles = Array.isArray(settings.additional_context_file) ? settings.additional_context_file : [];
  const separator = settings.additional_context_separator || "\n\n";
  const orderedFiles = [];

  for (const name of names) {
    const withJson = path.join(basePathForDataset(dataset, split), "inputs", `${name}.json`);
    const withJsonl = path.join(basePathForDataset(dataset, split), "inputs", `${name}.jsonl`);
    if (existsSync(withJson)) orderedFiles.push(withJson);
    else if (existsSync(withJsonl)) orderedFiles.push(withJsonl);
  }
  for (const file of directFiles) {
    orderedFiles.push(file);
  }

  result.files = orderedFiles;
  for (const filePath of orderedFiles) {
    const rows = filePath.endsWith(".jsonl") ? await readJsonl(filePath) : await readJson(filePath);
    for (const row of rows) {
      const rowId = String(row.id);
      const prev = result.byId.get(rowId) || { values: [], sources: [] };
      if (row.additional_context != null && row.additional_context !== "") {
        prev.values.push(String(row.additional_context));
        prev.sources.push(path.basename(filePath));
      }
      result.byId.set(rowId, prev);
    }
  }

  for (const [id, payload] of result.byId.entries()) {
    result.byId.set(id, {
      additional_context: payload.values.join(separator),
      additional_context_sources_used: payload.sources,
      inputs_files: orderedFiles.map((x) => path.basename(x)),
      inputs_file: orderedFiles.length ? path.basename(orderedFiles[0]) : null,
    });
  }

  return result;
}

async function scanCatalog() {
  const datasets = [];
  if (!existsSync(EXPERIMENTS_ROOT)) return { datasets };
  const datasetNames = (await fs.readdir(EXPERIMENTS_ROOT, { withFileTypes: true }))
    .filter((d) => d.isDirectory())
    .map((d) => d.name)
    .sort();

  for (const dataset of datasetNames) {
    const datasetPath = path.join(EXPERIMENTS_ROOT, dataset);
    const splitEntries = (await fs.readdir(datasetPath, { withFileTypes: true }))
      .filter((d) => d.isDirectory())
      .map((d) => d.name)
      .sort();
    const splits = [];

    for (const split of splitEntries) {
      const splitPath = path.join(datasetPath, split);
      const experimentEntries = (await fs.readdir(splitPath, { withFileTypes: true }))
        .filter((d) => d.isDirectory() && !d.name.startsWith("_"))
        .map((d) => d.name)
        .sort();
      const experiments = [];

      for (const experiment of experimentEntries) {
        const manifestPath = path.join(splitPath, experiment, "manifest.json");
        const settingsPath = path.join(splitPath, experiment, "settings.json");
        if (!existsSync(manifestPath)) continue;
        const manifest = await readJson(manifestPath);
        const settings = existsSync(settingsPath) ? await readJson(settingsPath) : {};
        const models = [];
        for (const [agentKey, agentValue] of Object.entries(manifest.agents || {})) {
          if (String(agentKey).startsWith("prediction_diagnosis:") || String(agentKey).startsWith("nl2sql_predictions:")) {
            if (agentValue.solution_name && !models.includes(agentValue.solution_name)) models.push(agentValue.solution_name);
          }
        }
        experiments.push({
          name: experiment,
          manifest_path: manifestPath,
          settings_path: settingsPath,
          models: models.sort(),
          updated_at: manifest.updated_at,
          schema_file: settings.schema_file,
        });
      }
      splits.push({ name: split, experiments });
    }
    datasets.push({ name: dataset, splits });
  }

  return { datasets };
}

async function buildView({ dataset, split, experiment, model }) {
  const experimentPath = path.join(EXPERIMENTS_ROOT, dataset, split, experiment);
  const manifest = await readJson(path.join(experimentPath, "manifest.json"), false);
  const settings = existsSync(path.join(experimentPath, "settings.json"))
    ? await readJson(path.join(experimentPath, "settings.json"), false)
    : {};

  const normalizedMap = await loadNormalized(dataset, split);
  const schemaMap = settings.schema_file ? await loadSchemaMap(dataset, split, settings.schema_file) : {};
  const extraContext = await loadAdditionalContexts(dataset, split, settings);

  const agents = manifest.agents || {};

  const characterizationPath = resolveArtifactPath(
    agents.benchmark_characterization?.output_files?.characterization
  );
  const characterizationRows =
    characterizationPath && existsSync(characterizationPath)
      ? await readJsonl(characterizationPath, false)
      : [];
  const characterizationById = mapById(characterizationRows);

  const allModels = [];
  const predictionsByModel = {};
  const diagnosesByModel = {};
  const evalSummaryByModel = {};
  const executionByModel = {};

  console.log("EXPERIMENTS_ROOT", EXPERIMENTS_ROOT);
console.log("characterizationPath", characterizationPath, characterizationPath ? existsSync(characterizationPath) : null);

for (const [agentKey, agentValue] of Object.entries(agents)) {
  if (String(agentKey).startsWith("nl2sql_predictions:")) {
    const modelName = agentValue.solution_name;
    if (!allModels.includes(modelName)) allModels.push(modelName);

    const filePath = resolveArtifactPath(agentValue.output_files?.predictions);
    console.log("predictionPath", modelName, filePath, filePath ? existsSync(filePath) : null);

    predictionsByModel[modelName] =
      filePath && existsSync(filePath)
        ? mapById(await readJsonl(filePath, false))
        : new Map();
  }

  if (String(agentKey).startsWith("prediction_diagnosis:")) {
    const modelName = agentValue.solution_name;
    if (!allModels.includes(modelName)) allModels.push(modelName);

    const summaryPath = resolveArtifactPath(agentValue.output_files?.sql_eval_summary);
    const detailsPath = resolveArtifactPath(agentValue.output_files?.sql_execution_details);
    const diagnosisPath = resolveArtifactPath(
      agentValue.output_files?.prediction_diagnosis ||
      path.join(agentValue.agent_dir || "", "prediction_diagnosis.jsonl")
    );

    console.log("summaryPath", modelName, summaryPath, summaryPath ? existsSync(summaryPath) : null);
    console.log("detailsPath", modelName, detailsPath, detailsPath ? existsSync(detailsPath) : null);
    console.log("diagnosisPath", modelName, diagnosisPath, diagnosisPath ? existsSync(diagnosisPath) : null);

    evalSummaryByModel[modelName] =
      summaryPath && existsSync(summaryPath)
        ? mapById(await readJsonl(summaryPath, false))
        : new Map();

    executionByModel[modelName] =
      detailsPath && existsSync(detailsPath)
        ? mapById(await readJsonl(detailsPath, false))
        : new Map();

    diagnosesByModel[modelName] =
      diagnosisPath && existsSync(diagnosisPath)
        ? mapById(await readJsonl(diagnosisPath, false))
        : new Map();
  }
}

  
  const rows = [];
  for (const [id, base] of normalizedMap.entries()) {
    const baseRow = { ...base };
    const schemaJson = schemaMap?.[base.db_id] ?? null;
    const extra = extraContext.byId.get(String(id)) || {
      additional_context: "",
      additional_context_sources_used: [],
      inputs_files: extraContext.files.map((x) => path.basename(x)),
      inputs_file: extraContext.files.length ? path.basename(extraContext.files[0]) : null,
    };

    const row = {
      id: baseRow.id,
      dataset,
      split,
      db_id: baseRow.db_id,
      nl: baseRow.nl || baseRow.question || "",
      gold_sql: baseRow.sql || baseRow.query || "",
      context: baseRow.context || "",
      schema_json: schemaJson,
      additional_context: extra.additional_context || "",
      additional_context_sources_used: extra.additional_context_sources_used || [],
      inputs_files: extra.inputs_files || [],
      inputs_file: extra.inputs_file || null,
      characterization: characterizationById.get(String(id)) || {},
      predictions: {},
      diagnoses: {},
      sql_eval_summary: {},
      sql_execution_details: {},
    };

    for (const modelName of allModels) {
      row.predictions[modelName] = predictionsByModel[modelName]?.get(String(id)) || {};
      const diagnosis = diagnosesByModel[modelName]?.get(String(id)) || {};

      if (diagnosis.execution_match_assessment && typeof diagnosis.execution_match_assessment === "object") {
        const cls = String(diagnosis.execution_match_assessment.classification || "").toLowerCase();
        if (cls === "fair") diagnosis.execution_match_assessment.classification = "unflagged";
        if (cls === "unfair") diagnosis.execution_match_assessment.classification = "flagged";
      }

      row.diagnoses[modelName] = diagnosis;
      row.sql_eval_summary[modelName] = evalSummaryByModel[modelName]?.get(String(id)) || {};
      row.sql_execution_details[modelName] = executionByModel[modelName]?.get(String(id)) || {};
    }

    rows.push(row);
  }

  return {
    manifest,
    settings,
    models: allModels.sort(),
    selected_model: model,
    rows,
  };
}


async function backupAndRewriteJsonl(filePath, rows) {
  const backupPath = filePath.replace(/\.jsonl$/i, `_old_${nowStamp()}.jsonl`);
  if (existsSync(filePath)) {
    await fs.copyFile(filePath, backupPath);
  }
  const payload = rows.map((row) => JSON.stringify(row)).join("\n") + "\n";
  await fs.writeFile(filePath, payload, "utf-8");
  cache.jsonl.delete(filePath);
  return backupPath;
}

async function saveField({ dataset, split, experiment, model, id, field, classification, description, scope }) {
  const experimentPath = path.join(EXPERIMENTS_ROOT, dataset, split, experiment);
  const manifest = await readJson(path.join(experimentPath, "manifest.json"), false);
  const agents = manifest.agents || {};
  let filePath;
  if (scope === "characterization") {
    filePath = resolveArtifactPath(
      agents.benchmark_characterization?.output_files?.characterization
    );
  } else {
    const key = `prediction_diagnosis:${model}`;
    filePath = resolveArtifactPath(
      agents[key]?.output_files?.prediction_diagnosis ||
      path.join(agents[key]?.agent_dir || "", "prediction_diagnosis.jsonl")
    );
  }
  if (!filePath || !existsSync(filePath)) {
    throw new Error(`Target file not found for ${scope}.`);
  }

  const rows = await readJsonl(filePath, false);
  const nextRows = rows.map((row) => {
    if (String(row.id) !== String(id)) return row;
    const updated = { ...row };
    if (field === "execution_match_assessment") {
      updated[field] = {
        classification: String(classification).toLowerCase() === "flagged" ? "flagged" : "unflagged",
        description: description || "",
      };
    } else {
      updated[field] = {
        classification: Boolean(classification),
        description: description || "",
      };
    }
    return updated;
  });

  const backup = await backupAndRewriteJsonl(filePath, nextRows);
  return { filePath, backup };
}

app.get("/api/empty", (_req, res) => res.json({ rows: [], summary: {}, models: [] }));

app.get("/api/catalog", async (_req, res) => {
  try {
    res.json(await scanCatalog());
  } catch (error) {
    res.status(500).send(String(error));
  }
});

app.get("/api/view", async (req, res) => {
  try {
    const { dataset, split, experiment, model } = req.query;
    if (!dataset || !split || !experiment) {
      return res.status(400).send("Missing dataset/split/experiment.");
    }
    res.json(await buildView({ dataset, split, experiment, model }));
  } catch (error) {
    res.status(500).send(String(error));
  }
});

app.post("/api/save/characterization", async (req, res) => {
  try {
    const result = await saveField({ ...req.body, scope: "characterization" });
    res.json({ ok: true, backup_file: result.backup, file: result.filePath });
  } catch (error) {
    res.status(500).send(String(error));
  }
});

app.post("/api/save/diagnosis", async (req, res) => {
  try {
    const result = await saveField({ ...req.body, scope: "diagnosis" });
    res.json({ ok: true, backup_file: result.backup, file: result.filePath });
  } catch (error) {
    res.status(500).send(String(error));
  }
});

app.post("/api/run-agent", async (req, res) => {
  try {
    const { dataset, split, experiment, model, agent } = req.body || {};
    if (!agent || !dataset || !split || !experiment) {
      return res.status(400).json({ ok: false, error: "Missing required fields." });
    }
    let scriptPath;
    if (agent === "benchmark_characterization") scriptPath = path.join(AGENTS_ROOT, "characterization", "characterization.py");
    else if (agent === "prediction_diagnosis") scriptPath = path.join(AGENTS_ROOT, "prediction_diagnosis", "run_prediction_diagnosis.py");
    else scriptPath = path.join(AGENTS_ROOT, "nl2sql_system", "predict_query.py");

    const args = [scriptPath, "--dataset", dataset, "--split", split, "--experiment-name", experiment];
    if (model) args.push("--solution-name", model);

    const child = spawn("python", args, { cwd: process.cwd() });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (d) => { stdout += d.toString(); });
    child.stderr.on("data", (d) => { stderr += d.toString(); });
    child.on("close", (code) => {
      res.json({ ok: code === 0, code, stdout, stderr, command: ["python", ...args].join(" ") });
    });
  } catch (error) {
    res.status(500).json({ ok: false, error: String(error) });
  }
});
app.use(express.static(CLIENT_DIST));

// Serve built frontend
app.use(express.static(path.join(__dirname, '../client/dist')));

app.get(/.*/, (req, res) => {
  res.sendFile(path.join(__dirname, '../client/dist/index.html'));
});
app.listen(PORT, () => {
  console.log(`NL2SQL-Diagnoser unified server listening on port ${PORT}`);
});
