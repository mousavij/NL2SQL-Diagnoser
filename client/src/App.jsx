import React, { useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import { PROBLEM_DESCRIPTIONS } from "./constants/problemDescriptions";
import { USER_FIELDS } from "./constants/userFields";
import { createPortal } from "react-dom";

const API_BASE =
  (typeof import.meta !== "undefined" && import.meta.env && import.meta.env.VITE_API_BASE) || "";

const TABS = [
  { key: "overview", label: "Overview" },
  { key: "characterization", label: "Benchmark Characterization" },
  { key: "diagnosis", label: "Prediction Diagnosis" },
  { key: "comparison", label: "Cross-Model Comparison" },
  { key: "cross_benchmark", label: "Cross-Benchmark" },
  { key: "aggregated", label: "Aggregated Analysis" },
  { key: "explorer", label: "Instance Explorer" },
  { key: "slice_builder", label: "Slice Builder" },
  { key: "agents", label: "Run Agents" },
];

const DIAGNOSIS_FIELDS_ORDER = [
  "execution_match_assessment",
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
];

const CHARACTERIZATION_GROUPS = ["ambiguous", "missing", "inaccurate"];

const appStyle = {
  fontFamily: 'Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif',
  color: "#1f2937",
  background: "#f8fafc",
  minHeight: "100vh",
};

const cardStyle = {
  background: "white",
  border: "1px solid #e5e7eb",
  borderRadius: 16,
  boxShadow: "0 1px 2px rgba(0,0,0,0.04)",
};
const GROUP_COLORS = {
  ambiguous: {
    bg: "#faf5ff",
    border: "#d8b4fe",
    text: "#7c3aed",
  },
  missing: {
    bg: "#fffbeb",
    border: "#fcd34d",
    text: "#b45309",
  },
  inaccurate: {
    bg: "#fef2f2",
    border: "#fca5a5",
    text: "#dc2626",
  },
  diagnosis: {
    bg: "#eff6ff",
    border: "#93c5fd",
    text: "#1d4ed8",
  },
};

const USER_FIELD_MAP = Object.fromEntries(
  USER_FIELDS.map((f) => [f.name, f])
);

const BASE_TITLE_OVERRIDES = {
  nl2sql_not_possible: "Benchmark Errors",
};

const FILTER_CONTROL_HEIGHT = 52;

const filterControlShellStyle = {
  border: "1px solid #d1d5db",
  borderRadius: 12,
  background: "white",
  width: "100%",
  minHeight: FILTER_CONTROL_HEIGHT,
  height: FILTER_CONTROL_HEIGHT,
  boxSizing: "border-box",
};
function splitCharField(field) {
  const [group, ...rest] = String(field).split("_");
  return { group, base: rest.join("_") };
}

function groupCharacterizationFields(fields) {
  return CHARACTERIZATION_GROUPS.map((group) => ({
    group,
    fields: fields.filter((field) => field.startsWith(`${group}_`)),
  }));
}

function getCharacterizationBaseGroups(fields) {
  const present = new Set(fields || []);
  const ordered = [];
  const seenBases = new Set();

  for (const fieldDef of USER_FIELDS) {
    const name = fieldDef?.name;
    if (!present.has(name)) continue;
    const { base } = splitCharField(name);
    if (!seenBases.has(base)) {
      ordered.push(base);
      seenBases.add(base);
    }
  }

  for (const name of fields || []) {
    const { base } = splitCharField(name);
    if (!seenBases.has(base)) {
      ordered.push(base);
      seenBases.add(base);
    }
  }

  return ordered.map((base) => ({
    base,
    title: BASE_TITLE_OVERRIDES[base] || titleize(base),
    fields: CHARACTERIZATION_GROUPS.map((group) => ({
      group,
      field: `${group}_${base}`,
    })).filter(({ field }) => present.has(field)),
  }));
}

function isActiveClassification(field, obj) {
  if (!obj) return false;
  if (field === "execution_match_assessment") return inferFlaggedValue(obj.classification);
  return Boolean(obj.classification);
}

function getDefinitionForField(field, scope) {
  if (scope === "characterization") {
    const direct = USER_FIELD_MAP[field]?.definition;
    if (direct) return direct;

    const { group, base } = splitCharField(field);
    return [PROBLEM_DESCRIPTIONS[group], PROBLEM_DESCRIPTIONS[base]]
      .filter(Boolean)
      .join("\n\n");
  }

  if (field === "execution_match_assessment") {
    return PROBLEM_DESCRIPTIONS.execution_match_assessment || "";
  }

  return PROBLEM_DESCRIPTIONS[field] || USER_FIELD_MAP[field]?.definition || "";
}

function displayBooleanClassification(value) {
  return value ? "TRUE" : "FALSE";
}

function titleize(value) {
  return String(value || "")
    .replace("nl2sql_not_possible", "benchmark_errors")
    .replaceAll("_", " ")
    .replace(/\b\w/g, (c) => c.toUpperCase());
}
function formatSelectionSummary(labels = [], emptyLabel = "All") {
  if (!labels.length) return emptyLabel;
  if (labels.length <= 2) return labels.join(", ");
  return `${labels.slice(0, 2).join(", ")} +${labels.length - 2} more`;
}

function getCharSelectionSummary(selected = [], charOptions = []) {
  if (!selected.length) return "All characterization labels";

  const fieldToDisplay = new Map();

  getCharacterizationFilterGroups(charOptions).forEach((group) => {
    group.options.forEach((opt) => {
      fieldToDisplay.set(opt.value, `${group.title}: ${opt.label}`);
    });
  });

  const labels = selected.map((value) => fieldToDisplay.get(value) || titleize(value));
  return formatSelectionSummary(labels, "All characterization labels");
}

function getDiagnosisSelectionSummary(
  selected = [],
  titleOverrideMap = {}
) {
  if (!selected.length) return "All diagnosis labels";

  const labels = selected.map((opt) => titleOverrideMap[opt] || titleize(opt));
  return formatSelectionSummary(labels, "All diagnosis labels");
}

function safeJsonParse(text, fallback = null) {
  try {
    return JSON.parse(text);
  } catch {
    return fallback;
  }
}

function formatSql(sql) {
  if (!sql) return "";
  return String(sql)
    .replace(/\s+/g, " ")
    .replace(/\b(FROM|WHERE|GROUP BY|ORDER BY|HAVING|LIMIT|JOIN|LEFT JOIN|RIGHT JOIN|INNER JOIN|OUTER JOIN|UNION|SELECT)\b/gi, "\n$1")
    .trim();
}

function prettyJson(value) {
  if (value == null || value === "") return "";
  if (typeof value === "string") {
    const parsed = safeJsonParse(value, null);
    if (parsed !== null) return JSON.stringify(parsed, null, 2);
    return value;
  }
  return JSON.stringify(value, null, 2);
}

function inferFlaggedValue(raw) {
  if (raw == null) return false;
  if (typeof raw === "string") {
    const normalized = raw.toLowerCase();
    return normalized === "flagged" || normalized === "unfair" || normalized === "true";
  }
  return Boolean(raw);
}

function displayFlagged(raw) {
  return inferFlaggedValue(raw) ? "flagged" : "unflagged";
}

function formatPercent(value, total, digits = 1) {
  if (!total) return "0%";
  return `${((100 * value) / total).toFixed(digits)}%`;
}

function formatCountPercent(value, total, digits = 1) {
  return `${value} (${formatPercent(value, total, digits)})`;
}

function collectCharFields(rows = []) {
  const fields = new Set();
  for (const row of rows) {
    for (const key of Object.keys(row?.characterization || {})) {
      const value = row?.characterization?.[key];
      if (value && typeof value === "object" && Object.prototype.hasOwnProperty.call(value, "classification")) {
        fields.add(key);
      }
    }
  }
  return Array.from(fields).sort((a, b) => {
    const { group: groupA, base: baseA } = splitCharField(a);
    const { group: groupB, base: baseB } = splitCharField(b);
    const groupIndexA = CHARACTERIZATION_GROUPS.indexOf(groupA);
    const groupIndexB = CHARACTERIZATION_GROUPS.indexOf(groupB);
    if (groupIndexA !== groupIndexB) return groupIndexA - groupIndexB;
    return baseA.localeCompare(baseB);
  });
}

function collectDiagnosisFields(rows = [], model) {
  const fields = new Set();
  for (const row of rows) {
    const diagnosis = row?.diagnoses?.[model] || {};
    for (const key of Object.keys(diagnosis)) {
      const value = diagnosis[key];
      if (value && typeof value === "object" && Object.prototype.hasOwnProperty.call(value, "classification")) {
        fields.add(key);
      }
    }
  }
  const ordered = DIAGNOSIS_FIELDS_ORDER.filter((key) => fields.has(key));
  const extras = Array.from(fields)
    .filter((key) => !ordered.includes(key))
    .sort();
  return [...ordered, ...extras];
}

function getActiveCharacterizationFields(row) {
  return Object.entries(row?.characterization || {})
    .filter(([, obj]) => Boolean(obj?.classification))
    .map(([key]) => key);
}

function getActiveDiagnosisFields(row, model) {
  const diagnosis = row?.diagnoses?.[model] || {};
  return Object.entries(diagnosis)
    .filter(([key, obj]) => {
      if (!obj || typeof obj !== "object") return false;
      if (key === "execution_match_assessment") return inferFlaggedValue(obj.classification);
      return Boolean(obj.classification);
    })
    .map(([key]) => key);
}

function buildPairCounts(fieldLists = [], labelFormatter = (value) => titleize(value)) {
  const counts = new Map();
  for (const fields of fieldLists) {
    const unique = Array.from(new Set(fields)).sort();
    for (let i = 0; i < unique.length; i += 1) {
      for (let j = i + 1; j < unique.length; j += 1) {
        const left = unique[i];
        const right = unique[j];
        const key = `${left}|||${right}`;
        counts.set(key, (counts.get(key) || 0) + 1);
      }
    }
  }
  return Array.from(counts, ([key, value]) => {
    const [left, right] = key.split("|||");
    return {
      key,
      left,
      right,
      label: `${labelFormatter(left)} × ${labelFormatter(right)}`,
      value,
    };
  }).sort((a, b) => b.value - a.value);
}

function getOverviewData(rows = [], model) {
  const total = rows.length;
  const charFieldCounts = new Map();
  const coarseCounts = new Map(CHARACTERIZATION_GROUPS.map((group) => [group, 0]));
  const diagnosisCounts = new Map();
  let flagged = 0;
  let execMatches = 0;
  let benchmarkIssues = 0;

  const charFieldLists = [];
  const diagnosisFieldLists = [];

  for (const row of rows) {
    const activeCharFields = getActiveCharacterizationFields(row);
    charFieldLists.push(activeCharFields);

    const activeGroups = new Set();
    for (const field of activeCharFields) {
      charFieldCounts.set(field, (charFieldCounts.get(field) || 0) + 1);
      activeGroups.add(splitCharField(field).group);
    }
    for (const group of activeGroups) {
      coarseCounts.set(group, (coarseCounts.get(group) || 0) + 1);
    }

    if (hasAnyBenchmarkIssue(row)) benchmarkIssues += 1;

    const activeDiagnosisFields = getActiveDiagnosisFields(row, model);
    diagnosisFieldLists.push(activeDiagnosisFields);

    for (const field of activeDiagnosisFields) {
      if (field === "execution_match_assessment") {
        flagged += 1;
      } else {
        diagnosisCounts.set(field, (diagnosisCounts.get(field) || 0) + 1);
      }
    }

    const execMatch = row?.sql_eval_summary?.[model]?.execution_match;
    if (execMatch === 1 || execMatch === true) execMatches += 1;
  }

  const charGroups = CHARACTERIZATION_GROUPS.map((group) => {
    const groupItems = Array.from(charFieldCounts, ([key, value]) => ({ key, value }))
      .filter(({ key }) => splitCharField(key).group === group)
      .map(({ key, value }) => ({
        key,
        label: titleize(splitCharField(key).base),
        value,
        percent: total ? (100 * value) / total : 0,
        definition: getDefinitionForField(key, "characterization"),
      }))
      .sort((a, b) => b.value - a.value);

    return {
      key: group,
      label: titleize(group),
      value: coarseCounts.get(group) || 0,
      percent: total ? (100 * (coarseCounts.get(group) || 0)) / total : 0,
      definition: PROBLEM_DESCRIPTIONS[group] || "",
      items: groupItems,
      color: GROUP_COLORS[group].text,
      accent: GROUP_COLORS[group],
    };
  });

  const diagnosisItems = Array.from(diagnosisCounts, ([key, value]) => ({
    key,
    label: titleize(key),
    value,
    percent: total ? (100 * value) / total : 0,
    definition: getDefinitionForField(key, "diagnosis"),
    color: GROUP_COLORS.diagnosis.text,
  })).sort((a, b) => b.value - a.value);

  const charPairs = buildPairCounts(
    charFieldLists,
    (field) => titleize(splitCharField(field).base)
  );

  const diagnosisPairs = buildPairCounts(
    diagnosisFieldLists.map((fields) => fields.filter((field) => field !== "execution_match_assessment")),
    (field) => titleize(field)
  );

  const execBuckets = {
    flagged_match: 0,
    flagged_mismatch: 0,
    unflagged_match: 0,
    unflagged_mismatch: 0,
  };

  for (const row of rows) {
    const isFlagged = inferFlaggedValue(row?.diagnoses?.[model]?.execution_match_assessment?.classification);
    const execMatch = row?.sql_eval_summary?.[model]?.execution_match === 1 || row?.sql_eval_summary?.[model]?.execution_match === true;
    if (isFlagged && execMatch) execBuckets.flagged_match += 1;
    else if (isFlagged && !execMatch) execBuckets.flagged_mismatch += 1;
    else if (!isFlagged && execMatch) execBuckets.unflagged_match += 1;
    else execBuckets.unflagged_mismatch += 1;
  }

  return {
    total,
    benchmarkIssues,
    noBenchmarkIssues: Math.max(0, total - benchmarkIssues),
    flagged,
    execMatches,
    execMismatches: Math.max(0, total - execMatches),
    charGroups,
    diagnosisItems,
    charPairs: charPairs.slice(0, 8),
    diagnosisPairs: diagnosisPairs.slice(0, 8),
    execBuckets: [
      {
        key: "flagged_match",
        label: "Flagged + execution match",
        value: execBuckets.flagged_match,
        flaggedStatus: "flagged",
      },
      {
        key: "flagged_mismatch",
        label: "Flagged + execution mismatch",
        value: execBuckets.flagged_mismatch,
        flaggedStatus: "flagged",
      },
      {
        key: "unflagged_match",
        label: "Unflagged + execution match",
        value: execBuckets.unflagged_match,
        flaggedStatus: "unflagged",
      },
      {
        key: "unflagged_mismatch",
        label: "Unflagged + execution mismatch",
        value: execBuckets.unflagged_mismatch,
        flaggedStatus: "unflagged",
      },
    ],
  };
}

function getCharFields(instance) {
  const keys = Object.keys(instance?.characterization || {});
  return keys
    .filter((k) => {
      const v = instance.characterization[k];
      return v && typeof v === "object" && Object.prototype.hasOwnProperty.call(v, "classification");
    })
    .sort((a, b) => {
      const [aGroup, ...aRest] = a.split("_");
      const [bGroup, ...bRest] = b.split("_");
      const ga = CHARACTERIZATION_GROUPS.indexOf(aGroup);
      const gb = CHARACTERIZATION_GROUPS.indexOf(bGroup);
      if (ga !== gb) return ga - gb;
      return aRest.join("_").localeCompare(bRest.join("_"));
    });
}

function getDiagnosisFields(instance, model) {
  const diagnosis = instance?.diagnoses?.[model] || {};
  const keys = Object.keys(diagnosis);

  // remove execution_match_assessment (handled separately as flag filter)
  const filtered = keys.filter((k) => k !== "execution_match_assessment");

  const known = DIAGNOSIS_FIELDS_ORDER.filter((k) => filtered.includes(k));

  const extra = filtered
    .filter((k) => !known.includes(k))
    .filter((k) => {
      const v = diagnosis[k];
      return v && typeof v === "object" && Object.prototype.hasOwnProperty.call(v, "classification");
    })
    .sort();

  return [...known, ...extra];
}

function getEditableDiagnosisFields(instance, model) {
  const diagnosis = instance?.diagnoses?.[model] || {};
  const keys = Object.keys(diagnosis);

  const known = DIAGNOSIS_FIELDS_ORDER.filter((k) => keys.includes(k));

  const extra = keys
    .filter((k) => !known.includes(k))
    .filter((k) => {
      const v = diagnosis[k];
      return v && typeof v === "object" && Object.prototype.hasOwnProperty.call(v, "classification");
    })
    .sort();

  return [...known, ...extra];
}

function metricCard(label, value, sublabel) {
  return (
    <div style={{ ...cardStyle, padding: 16, minWidth: 0, overflow: "hidden" }}>
      <div style={{ fontSize: 12, color: "#6b7280", textTransform: "uppercase", letterSpacing: 0.5 }}>{label}</div>
      <div style={{ fontSize: 28, fontWeight: 700, marginTop: 8 }}>{value}</div>
      {sublabel ? <div style={{ fontSize: 12, color: "#6b7280", marginTop: 6 }}>{sublabel}</div> : null}
    </div>
  );
}

function SectionTitle({ title, subtitle, right }) {
  return (
    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-end", gap: 16, marginBottom: 12 }}>
      <div>
        <div style={{ fontSize: 22, fontWeight: 700 }}>{title}</div>
        {subtitle ? <div style={{ color: "#6b7280", marginTop: 4 }}>{subtitle}</div> : null}
      </div>
      {right}
    </div>
  );
}

function BarList({ title, items, onSelect, selectedKey, valueFormatter = (v) => v }) {
  const max = Math.max(1, ...items.map((x) => x.value || 0));
  return (
    <div style={{ ...cardStyle, padding: 16 }}>
      <div style={{ fontSize: 14, fontWeight: 600, marginBottom: 14 }}>{title}</div>
      <div style={{ display: "grid", gap: 10 }}>
        {items.length === 0 ? <div style={{ color: "#6b7280" }}>(no data)</div> : null}
        {items.map((item) => (
          <button
            key={item.key}
            type="button"
            onClick={() => onSelect?.(item.key)}
            style={{
              textAlign: "left",
              border: item.key === selectedKey ? "1px solid #2563eb" : "1px solid #e5e7eb",
              borderRadius: 12,
              background: item.key === selectedKey ? "#eff6ff" : "white",
              padding: 10,
              cursor: onSelect ? "pointer" : "default",
            }}
          >
            <div style={{ display: "flex", justifyContent: "space-between", gap: 12, marginBottom: 6 }}>
              <div style={{ fontSize: 13, fontWeight: 500 }}>{item.label}</div>
              <div style={{ fontSize: 12, color: "#6b7280" }}>{valueFormatter(item.value)}</div>
            </div>
            <div style={{ height: 8, background: "#e5e7eb", borderRadius: 999 }}>
              <div
                style={{
                  width: `${(100 * item.value) / max}%`,
                  height: "100%",
                  background: item.color || "#2563eb",
                  borderRadius: 999,
                }}
              />
            </div>
          </button>
        ))}
      </div>
    </div>
  );
}

function FilterPill({ active, children, onClick }) {
  return (
    <button
      type="button"
      onClick={onClick}
      style={{
        border: active ? "1px solid #2563eb" : "1px solid #d1d5db",
        background: active ? "#eff6ff" : "white",
        color: active ? "#1d4ed8" : "#374151",
        padding: "6px 10px",
        borderRadius: 999,
        cursor: "pointer",
        fontSize: 12,
      }}
    >
      {children}
    </button>
  );
}

function CodeBlock({ title, value, maxHeight = 240, containerMinHeight = 0, bodyMinHeight = 0 }) {
  return (
    <div
      style={{
        ...cardStyle,
        padding: 10,
        minHeight: containerMinHeight || undefined,
        display: "grid",
        gridTemplateRows: "auto minmax(0, 1fr)",
        minWidth: 0,
        overflow: "hidden",
      }}
    >
      <div style={{ fontSize: 12, textTransform: "uppercase", color: "#6b7280", marginBottom: 8 }}>{title}</div>
      <pre
        style={{
          margin: 0,
          whiteSpace: "pre",
          wordBreak: "normal",
          background: "#f8fafc",
          border: "1px solid #e5e7eb",
          borderRadius: 12,
          padding: 12,
          maxHeight,
          minHeight: bodyMinHeight || undefined,
          overflowX: "auto",
          overflowY: "auto",
          maxWidth: "100%",
          fontSize: 12,
          lineHeight: 1.45,
        }}
      >
        {value || "(empty)"}
      </pre>
    </div>
  );
}

function JsonBlock({ title, value, maxHeight = 300, containerMinHeight = 0, bodyMinHeight = 0 }) {
  return (
    <CodeBlock
      title={title}
      value={prettyJson(value)}
      maxHeight={maxHeight}
      containerMinHeight={containerMinHeight}
      bodyMinHeight={bodyMinHeight}
    />
  );
}

function LabeledText({ label, value }) {
  return (
    <div style={{ display: "grid", gap: 6 }}>
      <div style={{ fontSize: 12, textTransform: "uppercase", color: "#6b7280" }}>{label}</div>
      <div style={{ background: "#f8fafc", border: "1px solid #e5e7eb", borderRadius: 12, padding: 10, minHeight: 42, whiteSpace: "pre-wrap" }}>
        {value || "(empty)"}
      </div>
    </div>
  );
}

function useFetchJson(path, deps = []) {
  const [state, setState] = useState({ data: null, loading: false, error: null });
  useEffect(() => {
    let cancelled = false;
    async function run() {
      setState((s) => ({ ...s, loading: true, error: null }));
      try {
        const res = await fetch(`${API_BASE}${path}`);
        if (!res.ok) throw new Error(await res.text());
        const json = await res.json();
        if (!cancelled) setState({ data: json, loading: false, error: null });
      } catch (err) {
        if (!cancelled) setState({ data: null, loading: false, error: String(err) });
      }
    }
    run();
    return () => {
      cancelled = true;
    };
  }, deps);
  return state;
}
function useElementHeight(deps = []) {
  const [height, setHeight] = useState(620);
  const [node, setNode] = useState(null);

  useEffect(() => {
    if (!node) return;

    function update() {
      const next = Math.max(420, Math.ceil(node.getBoundingClientRect().height));
      setHeight(next);
    }

    update();

    const observer = new ResizeObserver(() => update());
    observer.observe(node);
    window.addEventListener("resize", update);

    return () => {
      observer.disconnect();
      window.removeEventListener("resize", update);
    };
  }, [node, ...deps]);

  return [setNode, height];
}

function useMeasuredHeight(deps = []) {
  const [height, setHeight] = useState(620);
  const [node, setNode] = useState(null);

  useEffect(() => {
    if (!node) return;

    function update() {
      const next = Math.max(320, Math.ceil(node.getBoundingClientRect().height));
      setHeight(next);
    }

    update();

    const observer = new ResizeObserver(() => update());
    observer.observe(node);
    window.addEventListener("resize", update);

    return () => {
      observer.disconnect();
      window.removeEventListener("resize", update);
    };
  }, [node, ...deps]);

  return [setNode, height];
}
function useSyncedBlockHeights(deps = []) {
  const nodesRef = React.useRef({});
  const callbackCacheRef = React.useRef({});
  const [heights, setHeights] = useState({});

  const registerNode = React.useCallback((sectionKey, panelKey) => {
    const compositeKey = `${sectionKey}::${panelKey}`;
    if (!callbackCacheRef.current[compositeKey]) {
      callbackCacheRef.current[compositeKey] = (node) => {
        nodesRef.current[compositeKey] = node || null;
      };
    }
    return callbackCacheRef.current[compositeKey];
  }, []);

  useEffect(() => {
    const entries = Object.entries(nodesRef.current).filter(([, node]) => node);
    if (!entries.length) {
      setHeights((prev) => (Object.keys(prev).length ? {} : prev));
      return;
    }

    function update() {
      const next = {};
      for (const [compositeKey, node] of entries) {
        if (!node) continue;
        const [sectionKey] = compositeKey.split("::");
        const measured = Math.ceil(node.getBoundingClientRect().height);
        next[sectionKey] = Math.max(next[sectionKey] || 0, measured);
      }
      setHeights((prev) => {
        const prevKeys = Object.keys(prev);
        const nextKeys = Object.keys(next);
        if (prevKeys.length === nextKeys.length && prevKeys.every((key) => prev[key] === next[key])) {
          return prev;
        }
        return next;
      });
    }

    update();

    const observer = new ResizeObserver(() => update());
    for (const [, node] of entries) {
      if (node) observer.observe(node);
    }
    window.addEventListener("resize", update);

    return () => {
      observer.disconnect();
      window.removeEventListener("resize", update);
    };
  }, deps);

  return { registerNode, heights };
}

function normalizeColumns(columns) {
  if (!Array.isArray(columns)) return [];
  return columns.map((c, i) => {
    if (c == null || c === "") return `col_${i + 1}`;
    return String(c);
  });
}

function parseMaybeJsonArray(value) {
  if (Array.isArray(value)) return value;
  if (typeof value !== "string") return value;

  const trimmed = value.trim();
  if (!trimmed) return value;

  try {
    return JSON.parse(trimmed);
  } catch {
    return value;
  }
}

function normalizeRows(results, columns = []) {
  const parsed = parseMaybeJsonArray(results);
  if (!Array.isArray(parsed)) return [];

  const normalizedColumns = normalizeColumns(parseMaybeJsonArray(columns));

  return parsed.map((row) => {
    if (Array.isArray(row)) return row;

    if (row && typeof row === "object") {
      if (normalizedColumns.length) {
        return normalizedColumns.map((col) => row[col]);
      }
      return Object.values(row);
    }

    return [row];
  });
}

function truncateExecutionTable(columns, rows, maxCols = 5, maxRows = 20) {
  const normalizedColumns = normalizeColumns(parseMaybeJsonArray(columns));
  const normalizedRows = normalizeRows(rows, normalizedColumns);

  const hasExtraCols = normalizedColumns.length > maxCols;
  const shownColumns = hasExtraCols
    ? [...normalizedColumns.slice(0, maxCols), "…"]
    : normalizedColumns;

  let shownRows = normalizedRows.map((row) =>
    hasExtraCols ? [...row.slice(0, maxCols), "…"] : row
  );

  let hasHiddenMiddleRows = false;
  if (shownRows.length > maxRows) {
    shownRows = [
      ...shownRows.slice(0, 10),
      "__ELLIPSIS__",
      ...shownRows.slice(-10),
    ];
    hasHiddenMiddleRows = true;
  }

  return {
    columns: shownColumns,
    rows: shownRows,
    hasExtraCols,
    totalColumnCount: normalizedColumns.length,
    totalRowCount: normalizedRows.length,
    hasHiddenMiddleRows,
  };
}

function cellToDisplay(value) {
  if (value == null) return "null";
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function ExecutionResultsTable({
  title,
  columns,
  rows,
  emptyMessage = "(no execution results)",
  maxHeight = 220,
}) {
  const table = truncateExecutionTable(columns, rows, 5, 20);

  return (
    <div style={{ ...cardStyle, padding: 12, minWidth: 0, overflow: "hidden", display: "grid", gridTemplateRows: "auto auto minmax(0, 1fr)" }}>
      <div style={{ fontSize: 12, textTransform: "uppercase", color: "#6b7280", marginBottom: 8 }}>
        {title}
      </div>

      {!table.columns.length && !table.rows.length ? (
        <div
          style={{
            background: "#f8fafc",
            border: "1px solid #e5e7eb",
            borderRadius: 12,
            padding: 12,
            color: "#6b7280",
          }}
        >
          {emptyMessage}
        </div>
      ) : (
        <>
          <div style={{ fontSize: 12, color: "#6b7280", marginBottom: 8 }}>
            Showing{" "}
            {Math.min(table.totalRowCount, 20)} rows out of {table.totalRowCount}
            {" · "}
            {Math.min(table.totalColumnCount, 5)} columns out of {table.totalColumnCount}
          </div>

          <div
            style={{
              border: "1px solid #e5e7eb",
              borderRadius: 12,
              overflowX: "auto",
              overflowY: "auto",
              maxHeight,
              background: "white",
              minWidth: 0,
            }}
          >
            <table
              style={{
                borderCollapse: "separate",
                borderSpacing: 0,
                minWidth: "max-content",
                width: "100%",
                fontSize: 12,
              }}
            >
              <thead style={{ position: "sticky", top: 0, zIndex: 1 }}>
                <tr>
                  {table.columns.map((col, idx) => (
                    <th
                      key={`${title}-col-${idx}`}
                      style={{
                        background: "#f8fafc",
                        borderBottom: "1px solid #e5e7eb",
                        padding: "8px 10px",
                        textAlign: "left",
                        whiteSpace: "nowrap",
                        fontWeight: 600,
                        fontSize: 12,
                        color: "#374151",
                        maxWidth: 220,
                        overflow: "hidden",
                        textOverflow: "ellipsis",
                      }}
                    >
                      {col === "…" ? "…" : String(col)}
                    </th>
                  ))}
                </tr>
              </thead>

              <tbody>
                {table.rows.map((row, rowIdx) => {
                  if (row === "__ELLIPSIS__") {
                    return (
                      <tr key={`${title}-ellipsis-${rowIdx}`}>
                        <td
                          colSpan={table.columns.length}
                          style={{
                            padding: "10px",
                            textAlign: "center",
                            color: "#6b7280",
                            background: "#fafafa",
                            borderBottom: "1px solid #e5e7eb",
                            fontStyle: "italic",
                          }}
                        >
                          …
                        </td>
                      </tr>
                    );
                  }

                  return (
                    <tr key={`${title}-row-${rowIdx}`}>
                      {row.map((cell, colIdx) => (
                        <td
                          key={`${title}-cell-${rowIdx}-${colIdx}`}
                          style={{
                            borderBottom: "1px solid #e5e7eb",
                            padding: "8px 10px",
                            verticalAlign: "top",
                            minWidth: 120,
                            maxWidth: 320,
                            whiteSpace: "pre-wrap",
                            wordBreak: "break-word",
                            textAlign: cell === "…" ? "center" : "left",
                            color: cell === "…" ? "#6b7280" : "#1f2937",
                          }}
                        >
                          {cell === "…" ? "…" : cellToDisplay(cell)}
                        </td>
                      ))}
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  );
}


function hasCoarseBenchmarkIssue(instance, group) {
  const prefix = `${group}_`;
  const coarseField = `${group}_nl2sql_not_possible`;
  const characterization = instance?.characterization || {};

  if (Boolean(characterization?.[coarseField]?.classification)) return true;

  return Object.entries(characterization).some(([key, obj]) => {
    if (!key.startsWith(prefix)) return false;
    if (!obj || typeof obj !== "object") return false;
    return Boolean(obj.classification);
  });
}

function hasAnyBenchmarkIssue(instance) {
  return CHARACTERIZATION_GROUPS.some((group) => hasCoarseBenchmarkIssue(instance, group));
}

function coarseGroupsToCharFields(groups = []) {
  return Array.from(new Set((groups || []).map((group) => `${group}_nl2sql_not_possible`)));
}

function normalizeInstanceFilterPayload(payload = {}) {
  return {
    ...payload,
    benchmarkIssueStatus: payload.benchmarkIssueStatus || "all",
    coarseGroups: [],
    charFields: Array.from(
      new Set([
        ...(payload.charFields || []),
        ...coarseGroupsToCharFields(payload.coarseGroups || []),
      ])
    ),
  };
}

function getBenchmarkIssueSliceStats(rows = [], model, selectedGroups = [], flagMode = "all") {
  const filtered = rows.filter((row) => {
    const passesGroups = selectedGroups.every((group) => hasCoarseBenchmarkIssue(row, group));
    if (!passesGroups) return false;

    const flagged = inferFlaggedValue(row?.diagnoses?.[model]?.execution_match_assessment?.classification);
    if (flagMode === "flagged") return flagged;
    if (flagMode === "unflagged") return !flagged;
    return true;
  });

  const total = filtered.length;
  const matches = filtered.filter(
    (row) => row?.sql_eval_summary?.[model]?.execution_match === 1 || row?.sql_eval_summary?.[model]?.execution_match === true
  ).length;

  return {
    rows: filtered,
    total,
    matches,
    rate: total ? (100 * matches) / total : 0,
    label: formatPercent(matches, total, 1),
  };
}

function getRowCountFromShape(shape) {
  if (Array.isArray(shape)) {
    const first = Number(shape[0]);
    return Number.isFinite(first) ? first : null;
  }

  if (typeof shape === "string") {
    const trimmed = shape.trim();
    if (!trimmed) return null;
    try {
      const parsed = JSON.parse(trimmed);
      if (Array.isArray(parsed)) {
        const first = Number(parsed[0]);
        return Number.isFinite(first) ? first : null;
      }
    } catch {
      const match = trimmed.match(/\[\s*(\d+)/);
      if (match) {
        const first = Number(match[1]);
        return Number.isFinite(first) ? first : null;
      }
    }
  }

  if (shape && typeof shape === "object") {
    const candidate = shape.rows ?? shape.row_count ?? shape.num_rows ?? shape.rowCount;
    const first = Number(candidate);
    return Number.isFinite(first) ? first : null;
  }

  return null;
}

function getExecutionShapeCategoryForRow(row, model) {
  const details = row?.sql_execution_details?.[model] || {};
  const summary = row?.sql_eval_summary?.[model] || {};
  const shape = details.pred_shape ?? summary.pred_shape ?? details.gold_shape ?? summary.gold_shape;
  const rowCount = getRowCountFromShape(shape);

  if (rowCount == null) return "unknown";
  if (rowCount === 0) return "empty";
  if (rowCount === 1) return "single";
  return "multi";
}

function getExecutionShapeBreakdown(rows = [], model, selectedGroups = [], flagMode = "all") {
  const slice = getBenchmarkIssueSliceStats(rows, model, selectedGroups, flagMode);
  const matchedRows = slice.rows.filter(
    (row) => row?.sql_eval_summary?.[model]?.execution_match === 1 || row?.sql_eval_summary?.[model]?.execution_match === true
  );

  let empty = 0;
  let single = 0;
  let multi = 0;
  let unknown = 0;

  matchedRows.forEach((row) => {
    const category = getExecutionShapeCategoryForRow(row, model);

    if (category === "unknown") unknown += 1;
    else if (category === "empty") empty += 1;
    else if (category === "single") single += 1;
    else multi += 1;
  });

  return {
    totalMatches: matchedRows.length,
    empty,
    single,
    multi,
    unknown,
  };
}


function getActiveCharacterizationBases(row, group = null) {
  const characterization = row?.characterization || {};
  const active = [];

  Object.entries(characterization).forEach(([key, obj]) => {
    if (!obj || typeof obj !== "object" || !obj.classification) return;
    const parts = splitCharField(key);
    if (!CHARACTERIZATION_GROUPS.includes(parts.group)) return;
    if (parts.base === "nl2sql_not_possible") return;
    if (group && parts.group !== group) return;
    active.push(parts.base);
  });

  const unique = Array.from(new Set(active));
  return group ? unique : (unique.length ? unique : ["none"]);
}

function getDiagnosisProblemTypesForRow(row, model) {
  const active = getActiveDiagnosisFields(row, model)
    .filter((field) => field !== "execution_match_assessment");
  return active.length ? active : ["none"];
}

function rowMatchesFilter(row, filter, model) {
  const needle = (filter?.search || "").trim().toLowerCase();
  if (needle) {
    const haystack = [row.id, row.db_id, row.nl, row.gold_sql, row.predictions?.[model]?.pred_sql]
      .filter(Boolean)
      .join(" ")
      .toLowerCase();
    if (!haystack.includes(needle)) return false;
  }

  const flagged = inferFlaggedValue(row?.diagnoses?.[model]?.execution_match_assessment?.classification);
  if ((filter?.flaggedStatus || "all") === "flagged" && !flagged) return false;
  if ((filter?.flaggedStatus || "all") === "unflagged" && flagged) return false;

  if ((filter?.benchmarkIssueStatus || "all") === "has_issue" && !hasAnyBenchmarkIssue(row)) return false;
  if ((filter?.benchmarkIssueStatus || "all") === "no_issue" && hasAnyBenchmarkIssue(row)) return false;

  const activeCharFields = Array.from(
    new Set([
      ...((filter?.charFields) || []),
      ...coarseGroupsToCharFields((filter?.coarseGroups) || []),
    ])
  );
  if (activeCharFields.length) {
    const hasAllChar = activeCharFields.every((field) => Boolean(row.characterization?.[field]?.classification));
    if (!hasAllChar) return false;
  }

  const execMatch = row?.sql_eval_summary?.[model]?.execution_match === 1 || row?.sql_eval_summary?.[model]?.execution_match === true;
  if ((filter?.executionMatchStatus || "all") === "match" && !execMatch) return false;
  if ((filter?.executionMatchStatus || "all") === "non-match" && execMatch) return false;

  if ((filter?.resultShapeCategory || "all") !== "all" && getExecutionShapeCategoryForRow(row, model) !== filter.resultShapeCategory) return false;

  if ((filter?.diagnosisFields || []).length) {
    const hasAllDiagnosis = filter.diagnosisFields.every((field) => {
      const obj = row.diagnoses?.[model]?.[field];
      if (!obj) return false;
      if (field === "execution_match_assessment") return inferFlaggedValue(obj.classification);
      return Boolean(obj.classification);
    });
    if (!hasAllDiagnosis) return false;
  }

  return true;
}

function filterRowsByPayload(rows = [], filter = {}, model) {
  return rows.filter((row) => rowMatchesFilter(row, filter, model));
}

function DeltaBar({ leftLabel, rightLabel, leftRate, rightRate }) {
  const diff = leftRate - rightRate;
  const maxAbs = Math.max(1, Math.abs(diff));
  const widthPct = Math.min(50, (Math.abs(diff) / maxAbs) * 50);

  return (
    <div style={{ display: "grid", gap: 8 }}>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 12, fontSize: 12, color: "#6b7280" }}>
        <span>{leftLabel}</span>
        <span>{rightLabel}</span>
      </div>
      <div style={{ position: "relative", height: 24, borderRadius: 999, background: "#f3f4f6", overflow: "hidden" }}>
        <div style={{ position: "absolute", left: "50%", top: 0, bottom: 0, width: 1, background: "#9ca3af" }} />
        <div
          style={{
            position: "absolute",
            top: 4,
            bottom: 4,
            left: diff >= 0 ? "50%" : `calc(50% - ${widthPct}%)`,
            width: `${widthPct}%`,
            background: diff >= 0 ? "#10b981" : "#ef4444",
            borderRadius: 999,
            opacity: 0.95,
          }}
        />
      </div>
      <div style={{ fontSize: 12, fontWeight: 600, color: diff >= 0 ? "#065f46" : "#b91c1c" }}>
        Δ {diff > 0 ? "+" : ""}{diff.toFixed(1)} pts
      </div>
    </div>
  );
}

function AggregatedAnalysisTab({ rows, primaryModel, secondaryModel, onOpenSlice }) {
  const [selectedGroups, setSelectedGroups] = useState([]);
  const [flagMode, setFlagMode] = useState("all");

  const benchmarkGroups = [
    { key: "ambiguous", label: "Ambiguous", icon: "⚠️" },
    { key: "missing", label: "Missing", icon: "🧩" },
    { key: "inaccurate", label: "Inaccurate", icon: "❌" },
  ];

  const sliceBuilderStats = useMemo(() => {
    return {
      left: getBenchmarkIssueSliceStats(rows, primaryModel, selectedGroups, flagMode),
      right: getBenchmarkIssueSliceStats(rows, secondaryModel, selectedGroups, flagMode),
    };
  }, [rows, primaryModel, secondaryModel, selectedGroups, flagMode]);

  const sliceShapeStats = useMemo(() => {
    return {
      left: getExecutionShapeBreakdown(rows, primaryModel, selectedGroups, flagMode),
      right: getExecutionShapeBreakdown(rows, secondaryModel, selectedGroups, flagMode),
    };
  }, [rows, primaryModel, secondaryModel, selectedGroups, flagMode]);

  const fineGrainedStats = useMemo(() => {
    return getFineGrainedBenchmarkStats(rows, primaryModel, secondaryModel);
  }, [rows, primaryModel, secondaryModel]);

  const combinedSliceRows = useMemo(() => {
    const combinations = [
      [],
      ["ambiguous"],
      ["missing"],
      ["inaccurate"],
      ["ambiguous", "missing"],
      ["ambiguous", "inaccurate"],
      ["missing", "inaccurate"],
      ["ambiguous", "missing", "inaccurate"],
    ];

    return combinations.map((groups) => {
      const left = getBenchmarkIssueSliceStats(rows, primaryModel, groups, "all");
      const right = getBenchmarkIssueSliceStats(rows, secondaryModel, groups, "all");
      return {
        key: groups.join("|") || "none",
        groups,
        label: groups.length ? groups.map((g) => g[0].toUpperCase() + g.slice(1)).join(" + ") : "No coarse filter",
        left,
        right,
        diff: left.rate - right.rate,
      };
    });
  }, [rows, primaryModel, secondaryModel]);

  function toggleGroup(group) {
    setSelectedGroups((prev) =>
      prev.includes(group) ? prev.filter((x) => x !== group) : [...prev, group]
    );
  }

  function openSlice(overrides = {}) {
    onOpenSlice?.({
      coarseGroups: selectedGroups,
      charFields: [],
      diagnosisFields: [],
      flaggedStatus: flagMode,
      executionMatchStatus: "all",
      resultShapeCategory: "all",
      ...overrides,
    });
  }

  function renderModelBars(left, right) {
    return (
      <div style={{ display: "grid", gridTemplateColumns: "repeat(2, minmax(0, 1fr))", gap: 12 }}>
        <div style={{ display: "grid", gap: 6 }}>
          <div style={{ display: "flex", justifyContent: "space-between", gap: 12, fontSize: 13 }}>
            <span>{primaryModel}</span>
            <span style={{ fontWeight: 700 }}>{left.label}</span>
          </div>
          <div style={{ height: 10, borderRadius: 999, background: "#e5e7eb", overflow: "hidden" }}>
            <div style={{ width: `${left.rate}%`, height: "100%", background: "#2563eb", borderRadius: 999 }} />
          </div>
          <div style={{ fontSize: 12, color: "#6b7280" }}>{left.matches}/{left.total} matches</div>
        </div>

        <div style={{ display: "grid", gap: 6 }}>
          <div style={{ display: "flex", justifyContent: "space-between", gap: 12, fontSize: 13 }}>
            <span>{secondaryModel}</span>
            <span style={{ fontWeight: 700 }}>{right.label}</span>
          </div>
          <div style={{ height: 10, borderRadius: 999, background: "#e5e7eb", overflow: "hidden" }}>
            <div style={{ width: `${right.rate}%`, height: "100%", background: "#7c3aed", borderRadius: 999 }} />
          </div>
          <div style={{ fontSize: 12, color: "#6b7280" }}>{right.matches}/{right.total} matches</div>
        </div>
      </div>
    );
  }

  function renderShapeBreakdownCard(modelLabel, stats, barColor) {
    const total = Math.max(1, stats.totalMatches);
    const items = [
      { key: "empty", label: "Empty matches", value: stats.empty },
      { key: "single", label: "Single-row matches", value: stats.single },
      { key: "multi", label: "Multi-row matches", value: stats.multi },
    ];

    return (
      <div style={{ border: "1px solid #e5e7eb", borderRadius: 14, padding: 14, background: "#fff" }}>
        <div style={{ display: "flex", justifyContent: "space-between", gap: 12, marginBottom: 12 }}>
          <div style={{ fontWeight: 700 }}>{modelLabel}</div>
          <div style={{ fontSize: 12, color: "#6b7280" }}>{stats.totalMatches} matched rows</div>
        </div>

        <div style={{ display: "grid", gap: 10 }}>
          {items.map((item) => (
            <button
              key={item.key}
              type="button"
              onClick={() => openSlice({ executionMatchStatus: "match", resultShapeCategory: item.key })}
              style={{ display: "grid", gap: 6, border: "1px solid #e5e7eb", borderRadius: 12, background: "white", padding: 10, textAlign: "left", cursor: "pointer" }}
            >
              <div style={{ display: "flex", justifyContent: "space-between", gap: 12, fontSize: 13 }}>
                <span>{item.label}</span>
                <span style={{ fontWeight: 700 }}>{item.value}</span>
              </div>
              <div style={{ height: 9, borderRadius: 999, background: "#e5e7eb", overflow: "hidden" }}>
                <div
                  style={{
                    width: `${(100 * item.value) / total}%`,
                    height: "100%",
                    background: barColor,
                    borderRadius: 999,
                  }}
                />
              </div>
            </button>
          ))}
          {stats.unknown ? (
            <div style={{ fontSize: 12, color: "#6b7280" }}>
              {stats.unknown} matched rows had unknown shape metadata.
            </div>
          ) : null}
        </div>
      </div>
    );
  }

  return (
    <div style={{ display: "grid", gap: 16 }}>
      <SectionTitle
        title="Aggregated Analysis"
        subtitle="Slice execution accuracy by hierarchical benchmark errors and compare both models side by side under all, unflagged, and flagged settings."
      />

      <div style={{ ...cardStyle, padding: 16 }}>
        <div style={{ fontWeight: 600, marginBottom: 12 }}>Interactive slice builder</div>

        <div style={{ display: "grid", gap: 12 }}>
          <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
            {benchmarkGroups.map((group) => (
              <button
                key={group.key}
                type="button"
                onClick={() => toggleGroup(group.key)}
                style={{
                  border: selectedGroups.includes(group.key) ? "1px solid #2563eb" : "1px solid #d1d5db",
                  background: selectedGroups.includes(group.key) ? "#eff6ff" : "white",
                  color: "#374151",
                  padding: "8px 12px",
                  borderRadius: 999,
                  cursor: "pointer",
                  fontSize: 13,
                  fontWeight: 600,
                }}
              >
                {group.icon} {group.label}
              </button>
            ))}
          </div>

          <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
            {["all", "unflagged", "flagged"].map((mode) => (
              <FilterPill key={mode} active={flagMode === mode} onClick={() => setFlagMode(mode)}>
                {titleize(mode)}
              </FilterPill>
            ))}
          </div>

          <div style={{ display: "flex", justifyContent: "space-between", gap: 12, alignItems: "center", flexWrap: "wrap" }}>
            <div style={{ fontSize: 13, color: "#6b7280" }}>
              Current slice: {selectedGroups.length ? selectedGroups.map((g) => titleize(g)).join(" + ") : "No benchmark error filter"} · {titleize(flagMode)}
            </div>
            <button
              type="button"
              onClick={() => openSlice()}
              style={{ border: "1px solid #d1d5db", background: "white", borderRadius: 10, padding: "8px 10px", cursor: "pointer", fontWeight: 600 }}
            >
              Open in instance explorer
            </button>
          </div>

          {renderModelBars(sliceBuilderStats.left, sliceBuilderStats.right)}

          <div style={{ display: "grid", gridTemplateColumns: "repeat(2, minmax(0, 1fr))", gap: 12 }}>
            {renderShapeBreakdownCard(primaryModel, sliceShapeStats.left, "#2563eb")}
            {renderShapeBreakdownCard(secondaryModel, sliceShapeStats.right, "#7c3aed")}
          </div>
        </div>
      </div>

      <div style={{ ...cardStyle, padding: 16 }}>
        <div style={{ fontWeight: 600, marginBottom: 12 }}>Fine-grained slices grouped by coarse benchmark error</div>
        <div style={{ display: "grid", gap: 14 }}>
          {fineGrainedStats.map((group) => (
            <div key={group.group} style={{ border: "1px solid #e5e7eb", borderRadius: 14, padding: 14, background: "#fff" }}>
              <div style={{ fontWeight: 700, marginBottom: 12 }}>{group.label}</div>
              <div style={{ display: "grid", gap: 10 }}>
                {group.items.length ? group.items.map((item) => (
                  <button
                    key={item.key}
                    type="button"
                    onClick={() => onOpenSlice?.({ coarseGroups: [group.group], charFields: [item.key], diagnosisFields: [], flaggedStatus: "all", executionMatchStatus: "all", resultShapeCategory: "all" })}
                    style={{ border: "1px solid #e5e7eb", borderRadius: 12, padding: 12, background: "#fafafa", cursor: "pointer", textAlign: "left" }}
                  >
                    <div style={{ display: "grid", gridTemplateColumns: "220px 1fr 160px", gap: 14, alignItems: "center" }}>
                      <div>
                        <div style={{ fontWeight: 600 }}>{item.label}</div>
                        <div style={{ fontSize: 12, color: "#6b7280" }}>n={item.left.total}</div>
                      </div>
                      {renderModelBars(item.left, item.right)}
                      <DeltaBar
                        leftLabel={primaryModel}
                        rightLabel={secondaryModel}
                        leftRate={item.left.rate}
                        rightRate={item.right.rate}
                      />
                    </div>
                  </button>
                )) : <div style={{ fontSize: 13, color: "#6b7280" }}>(no active fine-grained labels)</div>}
              </div>
            </div>
          ))}
        </div>
      </div>

      <div style={{ ...cardStyle, padding: 16 }}>
        <div style={{ fontWeight: 600, marginBottom: 12 }}>Combined coarse slices</div>
        <div style={{ display: "grid", gap: 10 }}>
          {combinedSliceRows.map((row) => (
            <button
              key={row.key}
              type="button"
              onClick={() => onOpenSlice?.({ coarseGroups: row.groups || [], charFields: [], diagnosisFields: [], flaggedStatus: "all", executionMatchStatus: "all", resultShapeCategory: "all" })}
              style={{ border: "1px solid #e5e7eb", borderRadius: 12, padding: 12, background: "#fff", cursor: "pointer", textAlign: "left" }}
            >
              <div style={{ display: "grid", gridTemplateColumns: "220px 1fr 160px", gap: 14, alignItems: "center" }}>
                <div>
                  <div style={{ fontWeight: 600 }}>{row.label}</div>
                  <div style={{ fontSize: 12, color: "#6b7280" }}>n={row.left.total}</div>
                </div>
                {renderModelBars(row.left, row.right)}
                <DeltaBar
                  leftLabel={primaryModel}
                  rightLabel={secondaryModel}
                  leftRate={row.left.rate}
                  rightRate={row.right.rate}
                />
              </div>
            </button>
          ))}
        </div>
      </div>
    </div>
  );
}



function getFineGrainedBenchmarkStats(rows = [], primaryModel, secondaryModel) {
  const groupMap = {};

  rows.forEach((row) => {
    const characterization = row?.characterization || {};
    Object.entries(characterization).forEach(([key, obj]) => {
      if (!obj || typeof obj !== "object" || !obj.classification) return;
      const { group, base } = splitCharField(key);
      if (!CHARACTERIZATION_GROUPS.includes(group)) return;
      if (base === "nl2sql_not_possible") return;

      if (!groupMap[group]) groupMap[group] = new Map();
      if (!groupMap[group].has(key)) {
        groupMap[group].set(key, {
          key,
          label: BASE_TITLE_OVERRIDES[base] || titleize(base),
          left: getBenchmarkIssueSliceStats(rows.filter((r) => Boolean(r?.characterization?.[key]?.classification)), primaryModel, [], "all"),
          right: getBenchmarkIssueSliceStats(rows.filter((r) => Boolean(r?.characterization?.[key]?.classification)), secondaryModel, [], "all"),
        });
      }
    });
  });

  return CHARACTERIZATION_GROUPS.map((group) => ({
    group,
    label: titleize(group),
    items: Array.from((groupMap[group] || new Map()).values()).sort((a, b) => Math.abs((b.left?.rate || 0) - (b.right?.rate || 0)) - Math.abs((a.left?.rate || 0) - (a.right?.rate || 0))),
  }));
}

function App() {
  const [tab, setTab] = useState("overview");
  const [selection, setSelection] = useState({ dataset: "", split: "", experiment: "", model: "" });
  const [comparisonModel, setComparisonModel] = useState("");
  const [overviewMode, setOverviewMode] = useState("characterization");
  const [selectedInstanceId, setSelectedInstanceId] = useState(null);
  const [instanceFilter, setInstanceFilter] = useState({
    search: "",
    benchmarkIssueStatus: "all",
    coarseGroups: [],
    charFields: [],
    diagnosisFields: [],
    flaggedStatus: "all",
    executionMatchStatus: "all",
    resultShapeCategory: "all",
  });
  const [editingField, setEditingField] = useState(null);
  const [editBuffer, setEditBuffer] = useState({});
  const [saveMessage, setSaveMessage] = useState("");

  const catalogState = useFetchJson("/api/catalog", []);
  const catalog = catalogState.data || { datasets: [] };

  useEffect(() => {
    if (!catalog.datasets?.length) return;
    const firstDataset = selection.dataset || catalog.datasets[0]?.name;
    const ds = catalog.datasets.find((x) => x.name === firstDataset) || catalog.datasets[0];
    const split = selection.split || ds?.splits?.[0]?.name || "";
    const splitObj = ds?.splits?.find((x) => x.name === split) || ds?.splits?.[0];
    const experiment = selection.experiment || splitObj?.experiments?.[0]?.name || "";
    const expObj = splitObj?.experiments?.find((x) => x.name === experiment) || splitObj?.experiments?.[0];
    const model = selection.model || expObj?.models?.[0] || "";
    const next = { dataset: ds?.name || "", split: splitObj?.name || "", experiment: expObj?.name || "", model };
    if (JSON.stringify(next) !== JSON.stringify(selection)) {
      setSelection(next);
      setComparisonModel((prev) => prev || (expObj?.models?.find((m) => m !== model) || model || ""));
    }
  }, [catalogState.data]);

  const query = useMemo(() => {
    if (!selection.dataset || !selection.split || !selection.experiment) return "";
    const params = new URLSearchParams(selection);
    return `/api/view?${params.toString()}`;
  }, [selection]);

  const viewState = useFetchJson(query || "/api/empty", [query]);
  const view = viewState.data || { rows: [], summary: {}, models: [], settings: {}, manifest: {} };

  useEffect(() => {
    if (!view.rows?.length) {
      setSelectedInstanceId(null);
      return;
    }
    if (!view.rows.some((r) => String(r.id) === String(selectedInstanceId))) {
      setSelectedInstanceId(view.rows[0].id);
    }
  }, [viewState.data]);

  const selectedInstance = useMemo(
    () => view.rows?.find((r) => String(r.id) === String(selectedInstanceId)) || null,
    [view.rows, selectedInstanceId]
  );

  const filteredRows = useMemo(() => {
    let rows = [...(view.rows || [])];
    const needle = instanceFilter.search.trim().toLowerCase();
    if (needle) {
      rows = rows.filter((row) =>
        [row.id, row.db_id, row.nl, row.gold_sql, row.predictions?.[selection.model]?.pred_sql]
          .filter(Boolean)
          .join(" \n ")
          .toLowerCase()
          .includes(needle)
      );
    }
    if (instanceFilter.flaggedStatus === "flagged") {
      rows = rows.filter((row) =>
        inferFlaggedValue(row.diagnoses?.[selection.model]?.execution_match_assessment?.classification)
      );
    } else if (instanceFilter.flaggedStatus === "unflagged") {
      rows = rows.filter((row) =>
        !inferFlaggedValue(row.diagnoses?.[selection.model]?.execution_match_assessment?.classification)
      );
    }
    if (instanceFilter.benchmarkIssueStatus === "has_issue") {
      rows = rows.filter((row) => hasAnyBenchmarkIssue(row));
    } else if (instanceFilter.benchmarkIssueStatus === "no_issue") {
      rows = rows.filter((row) => !hasAnyBenchmarkIssue(row));
    }

    const activeCharFields = Array.from(
      new Set([
        ...(instanceFilter.charFields || []),
        ...coarseGroupsToCharFields(instanceFilter.coarseGroups || []),
      ])
    );
    if (activeCharFields.length) {
      rows = rows.filter((row) =>
        activeCharFields.every((field) =>
          Boolean(row.characterization?.[field]?.classification)
        )
      );
    }

    if (instanceFilter.executionMatchStatus === "match") {
      rows = rows.filter((row) => row?.sql_eval_summary?.[selection.model]?.execution_match === 1 || row?.sql_eval_summary?.[selection.model]?.execution_match === true);
    } else if (instanceFilter.executionMatchStatus === "non-match") {
      rows = rows.filter((row) => !(row?.sql_eval_summary?.[selection.model]?.execution_match === 1 || row?.sql_eval_summary?.[selection.model]?.execution_match === true));
    }

    if (instanceFilter.resultShapeCategory && instanceFilter.resultShapeCategory !== "all") {
      rows = rows.filter((row) => getExecutionShapeCategoryForRow(row, selection.model) === instanceFilter.resultShapeCategory);
    }

    if (instanceFilter.diagnosisFields?.length) {
      rows = rows.filter((row) =>
        instanceFilter.diagnosisFields.every((field) => {
          const obj = row.diagnoses?.[selection.model]?.[field];
          if (!obj) return false;
          if (field === "execution_match_assessment") {
            return inferFlaggedValue(obj.classification);
          }
          return Boolean(obj.classification);
        })
      );
    }
    return rows;
  }, [view.rows, instanceFilter, selection.model]);

  const overviewData = useMemo(
    () => getOverviewData(view.rows || [], selection.model),
    [view.rows, selection.model]
  );

  function updateSelection(key, value) {
    setSelection((prev) => {
      const next = { ...prev, [key]: value };
      if (key === "dataset") {
        const ds = catalog.datasets.find((d) => d.name === value);
        const split = ds?.splits?.[0];
        const experiment = split?.experiments?.[0];
        next.split = split?.name || "";
        next.experiment = experiment?.name || "";
        next.model = experiment?.models?.[0] || "";
        setComparisonModel(experiment?.models?.[1] || experiment?.models?.[0] || "");
      }
      if (key === "split") {
        const ds = catalog.datasets.find((d) => d.name === prev.dataset);
        const split = ds?.splits?.find((s) => s.name === value);
        const experiment = split?.experiments?.[0];
        next.experiment = experiment?.name || "";
        next.model = experiment?.models?.[0] || "";
        setComparisonModel(experiment?.models?.[1] || experiment?.models?.[0] || "");
      }
      if (key === "experiment") {
        const ds = catalog.datasets.find((d) => d.name === prev.dataset);
        const split = ds?.splits?.find((s) => s.name === prev.split);
        const experiment = split?.experiments?.find((e) => e.name === value);
        next.model = experiment?.models?.[0] || "";
        setComparisonModel(experiment?.models?.[1] || experiment?.models?.[0] || "");
      }
      return next;
    });
  }

  function beginEdit(scope, field, instance) {
    const payload = scope === "characterization"
      ? instance.characterization?.[field]
      : instance.diagnoses?.[selection.model]?.[field];
    setEditingField({ scope, field, id: instance.id });
    setEditBuffer({
      classification: field === "execution_match_assessment"
        ? displayFlagged(payload?.classification)
        : Boolean(payload?.classification),
      description: payload?.description || "",
    });
    setSaveMessage("");
  }

  async function saveEdit() {
    if (!editingField || !selectedInstance) return;
    const endpoint = editingField.scope === "characterization" ? "/api/save/characterization" : "/api/save/diagnosis";
    const body = {
      dataset: selection.dataset,
      split: selection.split,
      experiment: selection.experiment,
      model: selection.model,
      id: editingField.id,
      field: editingField.field,
      classification: editingField.field === "execution_match_assessment"
        ? editBuffer.classification
        : Boolean(editBuffer.classification),
      description: editBuffer.description,
    };
    const res = await fetch(`${API_BASE}${endpoint}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      setSaveMessage(`Save failed: ${await res.text()}`);
      return;
    }
    const json = await res.json();
    setSaveMessage(`Saved. Backup created at ${json.backup_file}`);
    setEditingField(null);
    window.location.reload();
  }

  const dsObj = catalog.datasets.find((d) => d.name === selection.dataset);
  const splitObj = dsObj?.splits?.find((s) => s.name === selection.split);
  const experimentObj = splitObj?.experiments?.find((e) => e.name === selection.experiment);

  return (
    <div style={appStyle}>
      <div style={{ position: "sticky", top: 0, zIndex: 20, background: "rgba(248,250,252,0.95)", backdropFilter: "blur(10px)", borderBottom: "1px solid #e5e7eb" }}>
        <div style={{ maxWidth: 1500, margin: "0 auto", padding: "18px 20px 14px" }}>
          <div style={{ display: "flex", justifyContent: "space-between", gap: 20, alignItems: "flex-end", flexWrap: "wrap" }}>
            <div>
              <div style={{ fontSize: 28, fontWeight: 800 }}>NL2SQL-Diagnoser</div>
              <div style={{ color: "#6b7280", marginTop: 4 }}>
                Interactive analysis of benchmark quality, flagged execution outcomes, and model behavior.
              </div>
            </div>
            <div style={{ display: "grid", gridTemplateColumns: "repeat(4, minmax(150px, 1fr))", gap: 10, minWidth: 720 }}>
              <Selector label="Dataset" value={selection.dataset} onChange={(v) => updateSelection("dataset", v)} options={catalog.datasets.map((d) => d.name)} />
              <Selector label="Split" value={selection.split} onChange={(v) => updateSelection("split", v)} options={(dsObj?.splits || []).map((s) => s.name)} />
              <Selector label="Experiment" value={selection.experiment} onChange={(v) => updateSelection("experiment", v)} options={(splitObj?.experiments || []).map((e) => e.name)} />
              <Selector label="Model" value={selection.model} onChange={(v) => setSelection((p) => ({ ...p, model: v }))} options={experimentObj?.models || []} />
            </div>
          </div>
          <div style={{ display: "flex", gap: 10, marginTop: 14, flexWrap: "wrap" }}>
            {TABS.map((item) => (
              <FilterPill key={item.key} active={tab === item.key} onClick={() => setTab(item.key)}>{item.label}</FilterPill>
            ))}
          </div>
        </div>
      </div>

      <div style={{ maxWidth: 1500, margin: "0 auto", padding: 20 }}>
        {catalogState.loading || viewState.loading ? <div>Loading…</div> : null}
        {catalogState.error ? <div style={{ color: "#b91c1c" }}>{catalogState.error}</div> : null}
        {viewState.error && query ? <div style={{ color: "#b91c1c" }}>{viewState.error}</div> : null}

        {tab === "overview" && (
          <OverviewTab
            overviewData={overviewData}
            overviewMode={overviewMode}
            setOverviewMode={setOverviewMode}
            setTab={setTab}
            setInstanceFilter={setInstanceFilter}
            setSelectedInstanceId={setSelectedInstanceId}
            rows={view.rows || []}
            model={selection.model}
          />
        )}

        {tab === "characterization" && selectedInstance && (
          <CharacterizationTab
            rows={filteredRows}
            selectedInstance={selectedInstance}
            model={selection.model}
            setSelectedInstanceId={setSelectedInstanceId}
            instanceFilter={instanceFilter}
            setInstanceFilter={setInstanceFilter}
            onEdit={beginEdit}
          />
        )}

        {tab === "diagnosis" && selectedInstance && (
          <DiagnosisTab
            rows={filteredRows}
            selectedInstance={selectedInstance}
            model={selection.model}
            setSelectedInstanceId={setSelectedInstanceId}
            instanceFilter={instanceFilter}
            setInstanceFilter={setInstanceFilter}
            onEdit={beginEdit}
          />
        )}

        {tab === "comparison" && (
          <ComparisonTab rows={view.rows || []} primaryModel={selection.model} secondaryModel={comparisonModel} setSecondaryModel={setComparisonModel} modelOptions={view.models || []} />
        )}

        {tab === "cross_benchmark" && (
          <CrossBenchmarkTab catalog={catalog} />
        )}

        {tab === "aggregated" && (
          <AggregatedAnalysisTab
            rows={view.rows || []}
            primaryModel={selection.model}
            secondaryModel={comparisonModel}
            onOpenSlice={(nextFilter) => {
              const normalizedNextFilter = normalizeInstanceFilterPayload(nextFilter || {});
              setInstanceFilter((prev) => ({
                ...prev,
                search: "",
                benchmarkIssueStatus: normalizedNextFilter.benchmarkIssueStatus || "all",
                coarseGroups: normalizedNextFilter.coarseGroups || [],
                charFields: normalizedNextFilter.charFields || [],
                diagnosisFields: normalizedNextFilter.diagnosisFields || [],
                flaggedStatus: normalizedNextFilter.flaggedStatus || "all",
                executionMatchStatus: normalizedNextFilter.executionMatchStatus || "all",
                resultShapeCategory: normalizedNextFilter.resultShapeCategory || "all",
              }));
              setTab("explorer");
              if (typeof window !== "undefined") {
                requestAnimationFrame(() => {
                  window.scrollTo({ top: 0, behavior: "smooth" });
                });
              }
              const targetRows = (view.rows || []).filter((row) => {
                const flagged = inferFlaggedValue(row.diagnoses?.[selection.model]?.execution_match_assessment?.classification);
                if ((normalizedNextFilter.flaggedStatus || "all") === "flagged" && !flagged) return false;
                if ((normalizedNextFilter.flaggedStatus || "all") === "unflagged" && flagged) return false;
                if ((normalizedNextFilter.benchmarkIssueStatus || "all") === "has_issue" && !hasAnyBenchmarkIssue(row)) return false;
                if ((normalizedNextFilter.benchmarkIssueStatus || "all") === "no_issue" && hasAnyBenchmarkIssue(row)) return false;
                if ((normalizedNextFilter.charFields || []).some((field) => !Boolean(row.characterization?.[field]?.classification))) return false;
                if ((normalizedNextFilter.diagnosisFields || []).some((field) => {
                  const obj = row.diagnoses?.[selection.model]?.[field];
                  if (!obj) return true;
                  if (field === "execution_match_assessment") return !inferFlaggedValue(obj.classification);
                  return !Boolean(obj.classification);
                })) return false;
                const execMatch = row?.sql_eval_summary?.[selection.model]?.execution_match === 1 || row?.sql_eval_summary?.[selection.model]?.execution_match === true;
                if ((normalizedNextFilter.executionMatchStatus || "all") === "match" && !execMatch) return false;
                if ((normalizedNextFilter.executionMatchStatus || "all") === "non-match" && execMatch) return false;
                if ((normalizedNextFilter.resultShapeCategory || "all") !== "all" && getExecutionShapeCategoryForRow(row, selection.model) !== normalizedNextFilter.resultShapeCategory) return false;
                return true;
              });
              if (targetRows.length) setSelectedInstanceId(targetRows[0].id);
            }}
          />
        )}

        {tab === "explorer" && selectedInstance && (
          <ExplorerTab rows={filteredRows} selectedInstance={selectedInstance} model={selection.model} secondaryModel={comparisonModel} setSelectedInstanceId={setSelectedInstanceId} instanceFilter={instanceFilter} setInstanceFilter={setInstanceFilter} />
        )}

        {tab === "slice_builder" && (
          <SliceBuilderTab
            rows={view.rows || []}
            model={selection.model}
            instanceFilter={instanceFilter}
            setInstanceFilter={setInstanceFilter}
            setTab={setTab}
            setSelectedInstanceId={setSelectedInstanceId}
          />
        )}

        {tab === "agents" && <AgentsTab selection={selection} />}

        {editingField ? (
          <EditModal
            editingField={editingField}
            editBuffer={editBuffer}
            setEditBuffer={setEditBuffer}
            onClose={() => setEditingField(null)}
            onSave={saveEdit}
          />
        ) : null}

        {saveMessage ? <div style={{ marginTop: 14, color: "#065f46" }}>{saveMessage}</div> : null}
      </div>
    </div>
  );
}

function Selector({ label, value, onChange, options }) {
  return (
    <label style={{ display: "grid", gap: 6 }}>
      <span style={{ fontSize: 12, color: "#6b7280", textTransform: "uppercase" }}>{label}</span>

      <div
        style={{
          ...filterControlShellStyle,
          position: "relative",
          display: "flex",
          alignItems: "center",
        }}
      >
        <select
          value={value || ""}
          onChange={(e) => onChange(e.target.value)}
          style={{
            width: "100%",
            height: "100%",
            border: "none",
            borderRadius: 12,
            padding: "10px 36px 10px 12px",
            background: "transparent",
            color: "#1f2937",
            fontSize: 14,
            lineHeight: 1.4,
            appearance: "none",
            WebkitAppearance: "none",
            MozAppearance: "none",
            cursor: "pointer",
            boxSizing: "border-box",
          }}
        >
          {(options || []).map((option) => (
            <option key={option} value={option}>
              {option ? titleize(option) : ""}
            </option>
          ))}
        </select>

        <span
          style={{
            position: "absolute",
            right: 14,
            top: "50%",
            transform: "translateY(-50%)",
            pointerEvents: "none",
            width: 0,
            height: 0,
            borderLeft: "4px solid transparent",
            borderRight: "4px solid transparent",
            borderTop: "5px solid #6b7280",
          }}
        />
      </div>
    </label>
  );
}

function getCharacterizationFilterGroups(charOptions = []) {
  return getCharacterizationBaseGroups(charOptions).map((group) => ({
    base: group.base,
    title: group.title,
    options: group.fields.map(({ group: coarse, field }) => ({
      value: field,
      label: titleize(coarse),
      accent: GROUP_COLORS[coarse],
    })),
  }));
}

function FilterPanel({ label, summary, children }) {
  const [open, setOpen] = useState(false);

  return (
    <div style={{ position: "relative" }}>
      <div style={{ display: "grid", gap: 6 }}>
        <span style={{ fontSize: 12, color: "#6b7280", textTransform: "uppercase" }}>{label}</span>
        <button
          type="button"
          onClick={() => setOpen((v) => !v)}
          style={{
            ...filterControlShellStyle,
            padding: "8px 12px",
            textAlign: "left",
            cursor: "pointer",
            display: "grid",
            gridTemplateColumns: "1fr auto",
            gap: 8,
            alignItems: "center",
            color: "#1f2937",
            fontSize: 14,
            lineHeight: 1.3,
          }}
        >
          <span
            style={{
              minWidth: 0,
              display: "-webkit-box",
              WebkitLineClamp: 2,
              WebkitBoxOrient: "vertical",
              overflow: "hidden",
            }}
          >
            {summary}
          </span>

          <span
            style={{
              width: 0,
              height: 0,
              borderLeft: "4px solid transparent",
              borderRight: "4px solid transparent",
              borderTop: "5px solid #6b7280",
              transform: open ? "rotate(180deg)" : "rotate(0deg)",
              transition: "transform 0.15s ease",
              justifySelf: "end",
            }}
          />
        </button>
      </div>

      {open ? (
        <div
          style={{
            position: "absolute",
            top: "100%",
            left: 0,
            marginTop: 8,
            width: 420,
            maxWidth: "min(420px, 90vw)",
            background: "white",
            border: "1px solid #d1d5db",
            borderRadius: 14,
            boxShadow: "0 12px 30px rgba(15,23,42,0.12)",
            padding: 12,
            zIndex: 4000,
          }}
        >
          {children}
        </div>
      ) : null}
    </div>
  );
}

function HierarchicalCharFilter({ selected = [], onChange, charOptions = [] }) {
  const groups = getCharacterizationFilterGroups(charOptions);

  function toggle(value) {
    onChange(
      selected.includes(value)
        ? selected.filter((x) => x !== value)
        : [...selected, value]
    );
  }

  const summary = getCharSelectionSummary(selected, charOptions);

  return (
    <FilterPanel label="Char labels" summary={summary}>
      <div style={{ display: "grid", gap: 12, maxHeight: 360, overflow: "auto" }}>
        {groups.map((group) => (
          <div
            key={group.base}
            style={{
              border: "1px solid #e5e7eb",
              borderRadius: 12,
              overflow: "hidden",
            }}
          >
            <div
              style={{
                background: "#f8fafc",
                padding: "10px 12px",
                borderBottom: "1px solid #e5e7eb",
                fontWeight: 600,
              }}
            >
              {group.title}
            </div>

            <div style={{ display: "grid", gap: 8, padding: 10 }}>
              {group.options.map((opt) => {
                const checked = selected.includes(opt.value);
                return (
                  <label
                    key={opt.value}
                    style={{
                      display: "flex",
                      alignItems: "center",
                      gap: 10,
                      padding: "8px 10px",
                      borderRadius: 10,
                      border: `1px solid ${checked ? opt.accent.border : "#e5e7eb"}`,
                      background: checked ? opt.accent.bg : "white",
                      cursor: "pointer",
                    }}
                  >
                    <input
                      type="checkbox"
                      checked={checked}
                      onChange={() => toggle(opt.value)}
                    />
                    <span style={{ fontWeight: 500 }}>{opt.label}</span>
                  </label>
                );
              })}
            </div>
          </div>
        ))}
      </div>
    </FilterPanel>
  );
}

function MultiSelectFilter({ label, selected = [], onChange, options = [], titleOverrideMap = {} }) {
  function toggle(value) {
    onChange(
      selected.includes(value)
        ? selected.filter((x) => x !== value)
        : [...selected, value]
    );
  }

  const summary = getDiagnosisSelectionSummary(selected, titleOverrideMap);

  return (
    <FilterPanel label={label} summary={summary}>
      <div style={{ display: "grid", gap: 8, maxHeight: 360, overflow: "auto" }}>
        {options.map((opt) => {
          const checked = selected.includes(opt);
          const display =
            titleOverrideMap[opt] || titleize(opt);

          return (
            <label
              key={opt}
              style={{
                display: "flex",
                alignItems: "center",
                gap: 10,
                padding: "8px 10px",
                borderRadius: 10,
                border: `1px solid ${checked ? GROUP_COLORS.diagnosis.border : "#e5e7eb"}`,
                background: checked ? GROUP_COLORS.diagnosis.bg : "white",
                cursor: "pointer",
              }}
            >
              <input
                type="checkbox"
                checked={checked}
                onChange={() => toggle(opt)}
              />
              <span>{display}</span>
            </label>
          );
        })}
      </div>
    </FilterPanel>
  );
}

function FilterBar({ instanceFilter, setInstanceFilter, charOptions = [], diagnosisOptions = [] }) {
  return (
    <div
      style={{
        ...cardStyle,
        padding: 12,
        display: "grid",
        gridTemplateColumns: "repeat(6, minmax(0, 1fr))",
        gap: 10,
        alignItems: "start",
      }}
    >
      <label style={{ display: "grid", gap: 6, height: "100%" }}>
        <span style={{ fontSize: 12, color: "#6b7280", textTransform: "uppercase" }}>Search</span>
        <div
          style={{
            ...filterControlShellStyle,
            display: "flex",
            alignItems: "stretch",
          }}
        >
          <input
            value={instanceFilter.search}
            onChange={(e) => setInstanceFilter((f) => ({ ...f, search: e.target.value }))}
            placeholder="Search question, SQL, db_id, id"
            style={{
              width: "100%",
              height: "100%",
              minHeight: 42,
              border: "none",
              outline: "none",
              borderRadius: 12,
              padding: "10px 12px",
              background: "transparent",
              color: "#1f2937",
              fontSize: 14,
              lineHeight: 1.4,
              boxSizing: "border-box",
            }}
          />
        </div>
      </label>


      <HierarchicalCharFilter
        charOptions={charOptions}
        selected={instanceFilter.charFields || []}
        onChange={(values) => setInstanceFilter((f) => ({ ...f, charFields: values }))}
      />

      <MultiSelectFilter
        label="Diagnosis labels"
        options={diagnosisOptions}
        selected={instanceFilter.diagnosisFields || []}
        onChange={(values) => setInstanceFilter((f) => ({ ...f, diagnosisFields: values }))}
        titleOverrideMap={{
          execution_match_assessment: "Flagged Execution Outcome",
        }}
      />

      <Selector
        label="Flag status"
        value={instanceFilter.flaggedStatus || "all"}
        onChange={(v) => setInstanceFilter((f) => ({ ...f, flaggedStatus: v }))}
        options={["all", "flagged", "unflagged"]}
      />

      <Selector
        label="Execution match"
        value={instanceFilter.executionMatchStatus || "all"}
        onChange={(v) => setInstanceFilter((f) => ({ ...f, executionMatchStatus: v }))}
        options={["all", "match", "non-match"]}
      />

      <Selector
        label="Result shape"
        value={instanceFilter.resultShapeCategory || "all"}
        onChange={(v) => setInstanceFilter((f) => ({ ...f, resultShapeCategory: v }))}
        options={["all", "empty", "single", "multi", "unknown"]}
      />
    </div>
  );
}

function InstanceList({ rows, selectedId, setSelectedId, rightLabel, height = 620 }) {
  return (
    <div
      style={{
        ...cardStyle,
        padding: 12,
        height,
        overflow: "auto",
      }}
    >
      <div style={{ fontWeight: 600, marginBottom: 10 }}>Instances</div>
      <div style={{ display: "grid", gap: 6 }}>
        {rows.map((row) => (
          <button
            key={row.id}
            type="button"
            onClick={() => setSelectedId(row.id)}
            style={{
              border: String(selectedId) === String(row.id) ? "1px solid #2563eb" : "1px solid #e5e7eb",
              borderRadius: 12,
              background: String(selectedId) === String(row.id) ? "#eff6ff" : "white",
              padding: "8px 10px",
              textAlign: "left",
              cursor: "pointer",
            }}
          >
            <div style={{ display: "flex", justifyContent: "space-between", gap: 12, alignItems: "center" }}>
              <div style={{ fontSize: 12, color: "#6b7280", minWidth: 0 }}>#{row.id} · {row.db_id}</div>
              {rightLabel ? <div style={{ fontSize: 11, color: "#6b7280", whiteSpace: "nowrap", flexShrink: 0 }}>{rightLabel(row)}</div> : null}
            </div>
            <div
              style={{
                fontWeight: 500,
                marginTop: 4,
                fontSize: 13,
                lineHeight: 1.35,
                display: "-webkit-box",
                WebkitLineClamp: 2,
                WebkitBoxOrient: "vertical",
                overflow: "hidden",
              }}
            >
              {row.nl}
            </div>
          </button>
        ))}
      </div>
    </div>
  );
}

function InfoDot({ text }) {
  const [open, setOpen] = useState(false);
  const [tooltipStyle, setTooltipStyle] = useState(null);
  const anchorRef = useRef(null);

  useLayoutEffect(() => {
    if (!open || !anchorRef.current || typeof window === "undefined") return;

    function updatePosition() {
      const rect = anchorRef.current.getBoundingClientRect();
      const tooltipWidth = Math.min(320, Math.max(220, window.innerWidth - 32));
      const left = Math.min(
        Math.max(16, rect.left),
        Math.max(16, window.innerWidth - tooltipWidth - 16)
      );

      setTooltipStyle({
        position: "fixed",
        top: Math.min(rect.bottom + 8, window.innerHeight - 24),
        left,
        width: tooltipWidth,
        maxWidth: `calc(100vw - 32px)`,
        zIndex: 10000,
      });
    }

    updatePosition();
    window.addEventListener("scroll", updatePosition, true);
    window.addEventListener("resize", updatePosition);

    return () => {
      window.removeEventListener("scroll", updatePosition, true);
      window.removeEventListener("resize", updatePosition);
    };
  }, [open, text]);

  if (!text) return null;

  return (
    <div
      ref={anchorRef}
      style={{ position: "relative", display: "inline-flex", zIndex: open ? 100 : "auto" }}
      onMouseEnter={() => setOpen(true)}
      onMouseLeave={() => setOpen(false)}
      onFocus={() => setOpen(true)}
      onBlur={() => setOpen(false)}
    >
      <button
        type="button"
        aria-label="Show definition"
        style={{
          width: 18,
          height: 18,
          borderRadius: 999,
          border: "1px solid #d1d5db",
          background: "#ffffff",
          color: "#6b7280",
          fontSize: 11,
          fontWeight: 700,
          cursor: "help",
          padding: 0,
          lineHeight: 1,
        }}
      >
        i
      </button>

      {open && tooltipStyle && typeof document !== "undefined" ? createPortal(
        <div
          style={{
            ...tooltipStyle,
            background: "#ffffff",
            color: "#374151",
            border: "1px solid #d1d5db",
            borderRadius: 12,
            padding: 12,
            fontSize: 12,
            lineHeight: 1.5,
            boxShadow: "0 12px 30px rgba(15,23,42,0.14)",
            whiteSpace: "pre-wrap",
            pointerEvents: "none",
          }}
        >
          {text}
        </div>,
        document.body
      ) : null}
    </div>
  );
}

function AnnotationFieldCard({
  title,
  value,
  description,
  onEdit,
  definition,
  accent = GROUP_COLORS.diagnosis,
  active = false,
}) {
  return (
    <div
      style={{
        border: `1px solid ${active ? accent.border : "#e5e7eb"}`,
        background: "#ffffff",
        borderRadius: 14,
        padding: 12,
        minHeight: "100%",
        boxShadow: active ? `inset 0 0 0 1px ${accent.border}` : "none",
      }}
    >
      <div style={{ display: "flex", justifyContent: "space-between", gap: 12, alignItems: "flex-start" }}>
        <div style={{ minWidth: 0 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
            <div style={{ fontWeight: 600 }}>{title}</div>
            <InfoDot text={definition} />
          </div>
          <div
            style={{
              display: "inline-flex",
              alignItems: "center",
              marginTop: 8,
              padding: "3px 8px",
              borderRadius: 999,
              fontSize: 11,
              fontWeight: 700,
              border: `1px solid ${active ? accent.border : "#d1d5db"}`,
              background: active ? accent.bg : "#f8fafc",
              color: active ? accent.text : "#6b7280",
            }}
          >
            {value}
          </div>
        </div>

        <button
          type="button"
          onClick={onEdit}
          style={{
            border: "1px solid #d1d5db",
            background: "white",
            borderRadius: 10,
            padding: "8px 10px",
            cursor: "pointer",
            flexShrink: 0,
          }}
        >
          Edit
        </button>
      </div>

      <div
        style={{
          marginTop: 10,
          background: "#f8fafc",
          border: "1px solid #e5e7eb",
          borderRadius: 10,
          padding: 10,
          whiteSpace: "pre-wrap",
          minHeight: 64,
        }}
      >
        {description || "(empty)"}
      </div>
    </div>
  );
}

function getInstanceCharacterizationSummary(instance) {
  const activeFields = getActiveCharacterizationFields(instance);
  const byGroup = Object.fromEntries(
    CHARACTERIZATION_GROUPS.map((group) => [group, []])
  );

  activeFields.forEach((field) => {
    const { group, base } = splitCharField(field);
    if (!byGroup[group]) byGroup[group] = [];
    byGroup[group].push({ field, label: BASE_TITLE_OVERRIDES[base] || titleize(base) });
  });

  return CHARACTERIZATION_GROUPS.map((group) => ({
    group,
    label: titleize(group),
    items: byGroup[group] || [],
    active: (byGroup[group] || []).length > 0,
    accent: GROUP_COLORS[group],
  }));
}

function getInstanceDiagnosisSummary(instance, model) {
  const diagnosis = instance?.diagnoses?.[model] || {};
  const flagged = inferFlaggedValue(diagnosis.execution_match_assessment?.classification);
  const activeFields = getActiveDiagnosisFields(instance, model).filter((field) => field !== "execution_match_assessment");
  const execMatch = instance?.sql_eval_summary?.[model]?.execution_match === 1 || instance?.sql_eval_summary?.[model]?.execution_match === true;

  return {
    flagged,
    execMatch,
    activeFields: activeFields.map((field) => ({ field, label: titleize(field) })),
  };
}

function getFilteredExecutionStats(rows = [], model) {
  const total = rows.length;
  let matches = 0;
  let flagged = 0;

  rows.forEach((row) => {
    const execMatch = row?.sql_eval_summary?.[model]?.execution_match === 1 || row?.sql_eval_summary?.[model]?.execution_match === true;
    if (execMatch) matches += 1;
    if (inferFlaggedValue(row?.diagnoses?.[model]?.execution_match_assessment?.classification)) flagged += 1;
  });

  return {
    total,
    matches,
    mismatches: Math.max(0, total - matches),
    flagged,
    unflagged: Math.max(0, total - flagged),
    accuracyLabel: formatPercent(matches, total, 1),
  };
}

function MiniStatCard({ label, value, sublabel, accent = { bg: "#f8fafc", border: "#e5e7eb", text: "#111827" } }) {
  return (
    <div
      style={{
        border: `1px solid ${accent.border || "#e5e7eb"}`,
        background: accent.bg || "#f8fafc",
        borderRadius: 14,
        padding: 12,
      }}
    >
      <div style={{ fontSize: 11, textTransform: "uppercase", letterSpacing: 0.4, color: "#6b7280" }}>{label}</div>
      <div style={{ fontSize: 24, fontWeight: 800, color: accent.text || "#111827", marginTop: 6 }}>{value}</div>
      {sublabel ? <div style={{ fontSize: 12, color: "#6b7280", marginTop: 4 }}>{sublabel}</div> : null}
    </div>
  );
}

function SummaryPill({ children, accent }) {
  return (
    <span
      style={{
        display: "inline-flex",
        alignItems: "center",
        padding: "4px 9px",
        borderRadius: 999,
        border: `1px solid ${(accent && accent.border) || "#d1d5db"}`,
        background: (accent && accent.bg) || "#f8fafc",
        color: (accent && accent.text) || "#374151",
        fontSize: 12,
        fontWeight: 600,
      }}
    >
      {children}
    </span>
  );
}

function CharacterizationInstanceSummary({ instance }) {
  const groups = useMemo(() => getInstanceCharacterizationSummary(instance), [instance]);

  const iconMap = {
    ambiguous: "⚠️",
    missing: "🧩",
    inaccurate: "❌",
  };

  return (
    <div style={{ ...cardStyle, padding: 14 }}>
      <div
        style={{
          display: "flex",
          flexWrap: "wrap",
          gap: 10,
          alignItems: "center",
          fontSize: 14,
          lineHeight: 1.5,
        }}
      >
        <span style={{ fontWeight: 700, color: "#111827" }}>Benchmark issues:</span>
        {groups.map((group) => (
          <span
            key={group.group}
            style={{
              display: "inline-flex",
              alignItems: "center",
              gap: 6,
              color: "#4b5563",
            }}
          >
            <span>{iconMap[group.group] || "•"}</span>
            <span style={{ fontWeight: 600, color: "#374151" }}>{group.label}</span>
            <span style={{ color: "#6b7280" }}>
              ({group.items.length ? group.items.map((item) => item.label).join(", ") : "none"})
            </span>
          </span>
        ))}
      </div>
    </div>
  );
}

function DiagnosisInstanceSummary({ instance, model, rows }) {
  const summary = useMemo(() => getInstanceDiagnosisSummary(instance, model), [instance, model]);
  const filteredStats = useMemo(() => getFilteredExecutionStats(rows, model), [rows, model]);

  return (
    <div style={{ ...cardStyle, padding: 14 }}>
      <div
        style={{
          display: "flex",
          flexWrap: "wrap",
          gap: 10,
          alignItems: "center",
          fontSize: 14,
          lineHeight: 1.5,
        }}
      >
        <span style={{ fontWeight: 700, color: "#111827" }}>Prediction:</span>

        <span style={{ fontWeight: 600, color: summary.flagged ? "#2563eb" : "#6b7280" }}>
          {summary.flagged ? "flagged" : "unflagged"}
        </span>

        <span style={{ color: "#9ca3af" }}>•</span>

        <span style={{ fontWeight: 600, color: summary.execMatch ? "#059669" : "#dc2626" }}>
          {summary.execMatch ? "match" : "unmatched"}
        </span>

        <span style={{ color: "#9ca3af" }}>•</span>

        <span style={{ color: "#374151" }}>
          accuracy (filtered): <span style={{ fontWeight: 700, color: "#111827" }}>{filteredStats.accuracyLabel}</span>
        </span>

        <span style={{ color: "#9ca3af" }}>•</span>

        <span style={{ color: "#374151" }}>
          issues:{" "}
          <span style={{ fontWeight: 600, color: "#7c3aed" }}>
            {summary.activeFields.length ? summary.activeFields.map((field) => field.label).join(", ") : "none"}
          </span>
        </span>
      </div>
    </div>
  );
}

function AnnotationGroupColumn({ title, fields, scope, values, onEdit, instance, accent }) {
  return (
    <div
      style={{
        border: "1px solid #e5e7eb",
        borderRadius: 16,
        background: "white",
        overflow: "visible",
      }}
    >
      <div
        style={{
          padding: "12px 14px",
          background: "#f8fafc",
          borderBottom: "1px solid #e5e7eb",
          color: "#374151",
          fontWeight: 700,
          fontSize: 14,
        }}
      >
        {title}
      </div>

      <div style={{ display: "grid", gap: 10, padding: 12, overflow: "visible" }}>
        {fields.length === 0 ? (
          <div style={{ color: "#6b7280", fontSize: 13 }}>(no labels)</div>
        ) : (
          fields.map((field) => {
            const obj = values?.[field] || { classification: false, description: "" };
            const active = isActiveClassification(field, obj);
            const displayValue =
              field === "execution_match_assessment"
                ? displayFlagged(obj.classification)
                : displayBooleanClassification(Boolean(obj.classification));

            return (
              <AnnotationFieldCard
                key={field}
                title={scope === "characterization" ? titleize(splitCharField(field).base) : titleize(field)}
                value={displayValue}
                description={obj.description}
                definition={getDefinitionForField(field, scope)}
                accent={accent}
                active={active}
                onEdit={() => onEdit(scope, field, instance)}
              />
            );
          })
        )}
      </div>
    </div>
  );
}

function CharacterizationRowGroup({ title, fields, values, onEdit, instance }) {
  return (
    <div
      style={{
        border: "1px solid #e5e7eb",
        borderRadius: 16,
        background: "white",
        padding: 14,
        overflow: "visible",
      }}
    >
      <div style={{ fontWeight: 700, marginBottom: 12 }}>{title}</div>
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(3, minmax(0, 1fr))",
          gap: 12,
          alignItems: "stretch",
        }}
      >
        {CHARACTERIZATION_GROUPS.map((group) => {
          const field = fields.find((entry) => entry.group === group)?.field;
          const obj = (field && values?.[field]) || { classification: false, description: "" };
          const accent = GROUP_COLORS[group];
          const active = isActiveClassification(field, obj);

          return (
            <AnnotationFieldCard
              key={`${title}-${group}`}
              title={titleize(group)}
              value={displayBooleanClassification(Boolean(obj.classification))}
              description={obj.description}
              definition={field ? getDefinitionForField(field, "characterization") : ""}
              accent={accent}
              active={active}
              onEdit={() => field && onEdit("characterization", field, instance)}
            />
          );
        })}
      </div>
    </div>
  );
}

function FullWidthAnnotationSection({ title, subtitle, children }) {
  return (
    <div style={{ ...cardStyle, padding: 16, overflow: "visible" }}>
      <div style={{ fontWeight: 700, fontSize: 18 }}>{title}</div>
      {subtitle ? (
        <div style={{ color: "#6b7280", marginTop: 4, marginBottom: 14 }}>{subtitle}</div>
      ) : (
        <div style={{ marginBottom: 14 }} />
      )}
      {children}
    </div>
  );
}

function pickNonEmptyColumns(...candidates) {
  for (const value of candidates) {
    if (Array.isArray(value) && value.length) return value;
  }
  return [];
}


function OverviewSectionCard({ title, subtitle, children, right }) {
  return (
    <div style={{ ...cardStyle, padding: 16, overflow: "visible", position: "relative", zIndex: 1 }}>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 16, alignItems: "flex-start", marginBottom: 14 }}>
        <div>
          <div style={{ fontWeight: 700 }}>{title}</div>
          {subtitle ? <div style={{ color: "#6b7280", marginTop: 4, fontSize: 13 }}>{subtitle}</div> : null}
        </div>
        {right}
      </div>
      {children}
    </div>
  );
}

function OverviewMetricCard({ label, value, total, definition, accent, onClick }) {
  const clickable = typeof onClick === "function";
  return (
    <button
      type="button"
      onClick={onClick}
      style={{
        ...cardStyle,
        padding: 16,
        textAlign: "left",
        cursor: clickable ? "pointer" : "default",
        border: `1px solid ${accent?.border || "#e5e7eb"}`,
        background: accent?.bg || "white",
        overflow: "visible",
        position: "relative",
        zIndex: 1,
      }}
    >
      <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
        <div style={{ fontSize: 12, color: accent?.text || "#6b7280", textTransform: "uppercase", letterSpacing: 0.5 }}>{label}</div>
        <InfoDot text={definition} />
      </div>
      <div style={{ fontSize: 28, fontWeight: 700, marginTop: 8, color: accent?.text || "#111827" }}>{value}</div>
      {typeof total === "number" ? <div style={{ fontSize: 12, color: "#6b7280", marginTop: 6 }}>{formatPercent(value, total)} of rows</div> : null}
    </button>
  );
}

function OverviewClickableRow({ item, total, accent, onClick }) {
  return (
    <button
      type="button"
      onClick={onClick}
      style={{
        width: "100%",
        textAlign: "left",
        border: "1px solid #e5e7eb",
        borderRadius: 12,
        background: "white",
        padding: 10,
        cursor: "pointer",
        overflow: "visible",
        position: "relative",
        zIndex: 1,
      }}
    >
      <div style={{ display: "grid", gridTemplateColumns: "minmax(0, 1fr) auto", gap: 8, alignItems: "start" }}>
        <div style={{ display: "flex", alignItems: "center", gap: 8, minWidth: 0 }}>
          <div style={{ fontWeight: 500, minWidth: 0, lineHeight: 1.35, overflow: "hidden", textOverflow: "ellipsis" }}>{item.label}</div>
          <InfoDot text={item.definition} />
        </div>
        <div style={{ color: "#6b7280", fontSize: 11, whiteSpace: "nowrap", flexShrink: 0, paddingTop: 3 }}>
          {formatCountPercent(item.value, total)}
        </div>
      </div>
      <div style={{ height: 8, background: "#e5e7eb", borderRadius: 999, marginTop: 8 }}>
        <div
          style={{
            width: `${total ? (100 * item.value) / total : 0}%`,
            height: "100%",
            borderRadius: 999,
            background: accent?.text || "#2563eb",
          }}
        />
      </div>
    </button>
  );
}

function OverviewTab({ overviewData, overviewMode, setOverviewMode, setTab, setInstanceFilter, setSelectedInstanceId, rows, model }) {
  const total = overviewData.total || 0;

  function getTargetRows(nextFilter) {
    let targetRows = [...(rows || [])];

    if ((nextFilter.flaggedStatus || "all") === "flagged") {
      targetRows = targetRows.filter((row) =>
        inferFlaggedValue(row.diagnoses?.[model]?.execution_match_assessment?.classification)
      );
    } else if ((nextFilter.flaggedStatus || "all") === "unflagged") {
      targetRows = targetRows.filter((row) =>
        !inferFlaggedValue(row.diagnoses?.[model]?.execution_match_assessment?.classification)
      );
    }

    if ((nextFilter.benchmarkIssueStatus || "all") === "has_issue") {
      targetRows = targetRows.filter((row) => hasAnyBenchmarkIssue(row));
    } else if ((nextFilter.benchmarkIssueStatus || "all") === "no_issue") {
      targetRows = targetRows.filter((row) => !hasAnyBenchmarkIssue(row));
    }

    if ((nextFilter.charFields || []).length) {
      targetRows = targetRows.filter((row) =>
        (nextFilter.charFields || []).every((charField) =>
          Boolean(row.characterization?.[charField]?.classification)
        )
      );
    }

    if ((nextFilter.diagnosisFields || []).length) {
      targetRows = targetRows.filter((row) =>
        (nextFilter.diagnosisFields || []).every((diagnosisField) => {
          const obj = row.diagnoses?.[model]?.[diagnosisField];
          if (!obj) return false;
          if (diagnosisField === "execution_match_assessment") {
            return inferFlaggedValue(obj.classification);
          }
          return Boolean(obj.classification);
        })
      );
    }

    if ((nextFilter.executionMatchStatus || "all") === "match") {
      targetRows = targetRows.filter((row) => row?.sql_eval_summary?.[model]?.execution_match === 1 || row?.sql_eval_summary?.[model]?.execution_match === true);
    } else if ((nextFilter.executionMatchStatus || "all") === "non-match") {
      targetRows = targetRows.filter((row) => !(row?.sql_eval_summary?.[model]?.execution_match === 1 || row?.sql_eval_summary?.[model]?.execution_match === true));
    }

    if ((nextFilter.resultShapeCategory || "all") !== "all") {
      targetRows = targetRows.filter((row) => getExecutionShapeCategoryForRow(row, model) === nextFilter.resultShapeCategory);
    }

    return targetRows;
  }

  function jumpToCharacterization(field, benchmarkIssueStatus = "all") {
    const nextFilter = {
      benchmarkIssueStatus,
      charFields: field ? [field] : [],
      coarseGroups: [],
      diagnosisFields: [],
      flaggedStatus: "all",
      executionMatchStatus: "all",
      resultShapeCategory: "all",
    };

    setTab("characterization");
    setInstanceFilter((prev) => ({
      ...prev,
      ...nextFilter,
    }));

    const targetRows = getTargetRows(nextFilter);
    if (targetRows.length) setSelectedInstanceId?.(targetRows[0].id);
  }

  function jumpToDiagnosis(field, flaggedStatus = "all", executionMatchStatus = "all") {
    const nextFilter = {
      benchmarkIssueStatus: "all",
      coarseGroups: [],
      charFields: [],
      diagnosisFields: field ? [field] : [],
      flaggedStatus,
      executionMatchStatus,
      resultShapeCategory: "all",
    };

    setTab("diagnosis");
    setInstanceFilter((prev) => ({
      ...prev,
      ...nextFilter,
    }));

    const targetRows = getTargetRows(nextFilter);
    if (targetRows.length) setSelectedInstanceId?.(targetRows[0].id);
  }

  const showChar = overviewMode === "characterization" || overviewMode === "both";
  const showDiagnosis = overviewMode === "diagnosis" || overviewMode === "both";


  return (
    <div style={{ display: "grid", gap: 18 }}>
      <SectionTitle
        title="Overview"
        subtitle="Use this page to frame the benchmark story first, then click into characterization or diagnosis slices."
        right={
          <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
            <FilterPill active={overviewMode === "characterization"} onClick={() => setOverviewMode("characterization")}>
              Characterization only
            </FilterPill>
            <FilterPill active={overviewMode === "diagnosis"} onClick={() => setOverviewMode("diagnosis")}>
              Prediction only
            </FilterPill>
            <FilterPill active={overviewMode === "both"} onClick={() => setOverviewMode("both")}>
              Side by side
            </FilterPill>
          </div>
        }
      />


      <div
        style={{
          display: "grid",
          gridTemplateColumns: showChar && showDiagnosis ? "1fr 1fr" : "1fr",
          gap: 16,
          alignItems: "start",
        }}
      >
        {showChar ? (
          <div style={{ display: "grid", gap: 16 }}>
            <OverviewSectionCard
              title="Benchmark overview"
              subtitle="Start by separating rows with benchmark errors from rows without benchmark errors, then drill into the coarse issue types."
            >
              <div style={{ display: "grid", gridTemplateColumns: "repeat(3, minmax(0, 1fr))", gap: 12, marginBottom: 14 }}>
                <OverviewMetricCard
                  label="Instances"
                  value={total}
                  total={total}
                  definition="Total number of rows in the current overview selection."
                  accent={{ bg: "#f8fafc", border: "#d1d5db", text: "#374151" }}
                  onClick={() => jumpToCharacterization(null, "all")}
                />
                <OverviewMetricCard
                  label="Benchmark errors"
                  value={overviewData.benchmarkIssues}
                  total={total}
                  definition="Rows where at least one benchmark error is present across the ambiguous, missing, or inaccurate categories."
                  accent={{ bg: "#faf5ff", border: "#d8b4fe", text: "#7c3aed" }}
                  onClick={() => jumpToCharacterization(null, "has_issue")}
                />
                <OverviewMetricCard
                  label="No benchmark errors"
                  value={overviewData.noBenchmarkIssues}
                  total={total}
                  definition="Rows with no active ambiguous, missing, or inaccurate benchmark-error labels."
                  accent={{ bg: "#f8fafc", border: "#d1d5db", text: "#374151" }}
                  onClick={() => jumpToCharacterization(null, "no_issue")}
                />
              </div>

              <div style={{ display: "grid", gridTemplateColumns: "repeat(3, minmax(0, 1fr))", gap: 12, alignItems: "start" }}>
                {overviewData.charGroups.map((group) => (
                  <div
                    key={group.key}
                    style={{
                      border: `1px solid ${group.accent.border}`,
                      borderRadius: 16,
                      overflow: "visible",
                      background: "white",
                    }}
                  >
                    <div style={{ padding: 12, background: group.accent.bg, borderBottom: `1px solid ${group.accent.border}` }}>
                      <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                        <div style={{ fontWeight: 700, color: group.accent.text }}>{group.label}</div>
                        <InfoDot text={group.definition} />
                      </div>
                      <div style={{ marginTop: 8, fontSize: 22, fontWeight: 800, color: group.accent.text }}>{group.value}</div>
                      <div style={{ marginTop: 4, fontSize: 12, color: "#6b7280" }}>{formatPercent(group.value, total)} of rows</div>
                    </div>

                    <div style={{ display: "grid", gap: 10, padding: 12 }}>
                      {group.items.length ? (
                        group.items.map((item) => (
                          <OverviewClickableRow
                            key={item.key}
                            item={item}
                            total={total}
                            accent={group.accent}
                            onClick={() => jumpToCharacterization(item.key, "all")}
                          />
                        ))
                      ) : (
                        <div style={{ color: "#6b7280", fontSize: 13 }}>(no labels)</div>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            </OverviewSectionCard>

            <OverviewSectionCard
              title="Common benchmark-issue co-occurrences"
              subtitle="These pairs help surface benchmark artifacts that tend to appear together."
            >
              <div style={{ display: "grid", gap: 10 }}>
                {overviewData.charPairs.length ? overviewData.charPairs.map((pair) => (
                  <OverviewClickableRow
                    key={pair.key}
                    item={pair}
                    total={total}
                    accent={{ text: "#7c3aed" }}
                    onClick={() => jumpToCharacterization(pair.left, "all")}
                  />
                )) : <div style={{ color: "#6b7280" }}>(not enough co-occurring labels)</div>}
              </div>
            </OverviewSectionCard>
          </div>
        ) : null}

        {showDiagnosis ? (
          <div style={{ display: "grid", gap: 16 }}>
            <OverviewSectionCard
              title="Prediction diagnosis"
              subtitle="Execution-aware diagnosis combines label frequencies, flagged outcomes, and execution-match behavior."
            >
              <div style={{ display: "grid", gridTemplateColumns: "repeat(4, minmax(0, 1fr))", gap: 12, marginBottom: 14 }}>
                <OverviewMetricCard
                  label="Instances"
                  value={total}
                  total={total}
                  definition="Total number of rows in the current overview selection."
                  accent={{ bg: "#f8fafc", border: "#d1d5db", text: "#374151" }}
                  onClick={() => jumpToDiagnosis(null, "all")}
                />
                <OverviewMetricCard
                  label="Flagged outcomes"
                  value={overviewData.flagged}
                  total={total}
                  definition={getDefinitionForField("execution_match_assessment", "diagnosis")}
                  accent={GROUP_COLORS.diagnosis}
                  onClick={() => jumpToDiagnosis(null, "flagged")}
                />
                <OverviewMetricCard
                  label="Execution matches"
                  value={overviewData.execMatches}
                  total={total}
                  definition="Rows whose predicted SQL has an execution match with the reference SQL."
                  accent={{ bg: "#eff6ff", border: "#93c5fd", text: "#1d4ed8" }}
                  onClick={() => jumpToDiagnosis(null, "all", "match")}
                />
                <OverviewMetricCard
                  label="Execution mismatches"
                  value={overviewData.execMismatches}
                  total={total}
                  definition="Rows whose predicted SQL does not execution-match the reference SQL."
                  accent={{ bg: "#f8fafc", border: "#d1d5db", text: "#374151" }}
                  onClick={() => jumpToDiagnosis(null, "all", "non-match")}
                />
              </div>

              <div style={{ display: "grid", gap: 10 }}>
                {overviewData.diagnosisItems.length ? overviewData.diagnosisItems.map((item) => (
                  <OverviewClickableRow
                    key={item.key}
                    item={item}
                    total={total}
                    accent={GROUP_COLORS.diagnosis}
                    onClick={() => jumpToDiagnosis(item.key, "all")}
                  />
                )) : <div style={{ color: "#6b7280" }}>(no diagnosis labels)</div>}
              </div>
            </OverviewSectionCard>

            <OverviewSectionCard
              title="Execution accuracy slices"
              subtitle="Execution match is most useful when paired with flagged versus unflagged status."
            >
              <div style={{ display: "grid", gridTemplateColumns: "repeat(2, minmax(0, 1fr))", gap: 12 }}>
                {overviewData.execBuckets.map((bucket) => (
                  <OverviewMetricCard
                    key={bucket.key}
                    label={bucket.label}
                    value={bucket.value}
                    total={total}
                    accent={bucket.flaggedStatus === "flagged" ? GROUP_COLORS.diagnosis : { bg: "#f8fafc", border: "#d1d5db", text: "#374151" }}
                    onClick={() => jumpToDiagnosis(null, bucket.flaggedStatus)}
                  />
                ))}
              </div>
            </OverviewSectionCard>

            <OverviewSectionCard
              title="Common diagnosis co-occurrences"
              subtitle="Useful for showing which error types often travel together once predictions fail."
            >
              <div style={{ display: "grid", gap: 10 }}>
                {overviewData.diagnosisPairs.length ? overviewData.diagnosisPairs.map((pair) => (
                  <OverviewClickableRow
                    key={pair.key}
                    item={pair}
                    total={total}
                    accent={GROUP_COLORS.diagnosis}
                    onClick={() => jumpToDiagnosis(pair.left, "all")}
                  />
                )) : <div style={{ color: "#6b7280" }}>(not enough co-occurring labels)</div>}
              </div>
            </OverviewSectionCard>
          </div>
        ) : null}
      </div>

      <div style={{ ...cardStyle, padding: 16 }}>
        <div style={{ fontWeight: 600, marginBottom: 8 }}>Demo guidance</div>
        <div style={{ color: "#4b5563", lineHeight: 1.6 }}>
          Start with the benchmark-error versus no-benchmark-error split, drill into the coarse issue types, then pivot to flagged execution outcomes and diagnosis co-occurrences to explain whether the apparent failure comes from the benchmark, the model, or the metric.
        </div>
      </div>
    </div>
  );
}


function CharacterizationTab({
  rows,
  selectedInstance,
  model,
  setSelectedInstanceId,
  instanceFilter,
  setInstanceFilter,
  onEdit,
}) {
  const charFields = useMemo(() => collectCharFields(rows), [rows]);
  const diagFields = useMemo(() => collectDiagnosisFields(rows, model).filter((field) => field !== "execution_match_assessment"), [rows, model]);
  const groupedByBase = getCharacterizationBaseGroups(charFields);

  const [setTopNode, topHeight] = useElementHeight([
    selectedInstance?.id,
    model,
    rows.length,
    instanceFilter.search,
    instanceFilter.coarseGroups,
    instanceFilter.charFields,
    instanceFilter.diagnosisFields,
    instanceFilter.flaggedStatus,
    instanceFilter.executionMatchStatus,
    instanceFilter.resultShapeCategory,
  ]);

  return (
    <div style={{ display: "grid", gap: 16 }}>
      <SectionTitle
        title="Benchmark Characterization"
        subtitle="Edit corrected benchmark issue labels and explanations. Click a chart in Overview to land here on a slice."
      />
      <FilterBar
        instanceFilter={instanceFilter}
        setInstanceFilter={setInstanceFilter}
        charOptions={charFields}
        diagnosisOptions={diagFields}
      />
      <CharacterizationInstanceSummary instance={selectedInstance} />

      <div style={{ display: "grid", gap: 16 }}>
        <div style={{ display: "grid", gridTemplateColumns: "320px 1fr", gap: 16, alignItems: "start" }}>
          <InstanceList
            rows={rows}
            selectedId={selectedInstance.id}
            setSelectedId={setSelectedInstanceId}
            rightLabel={() => "characterization"}
            height={topHeight}
          />

          <div style={{ display: "grid", gap: 14, minWidth: 0 }}>
            <div ref={setTopNode} style={{ display: "grid", gap: 14, minWidth: 0 }}>
              <InstanceHeader instance={selectedInstance} />
              <div style={{ display: "grid", gridTemplateColumns: "repeat(2, minmax(0, 1fr))", gap: 14 }}>
                <CodeBlock title="Gold SQL" value={formatSql(selectedInstance.gold_sql)} />
                <JsonBlock title="Schema" value={selectedInstance.schema_json} maxHeight={360} />
              </div>
            </div>
          </div>
        </div>

        <FullWidthAnnotationSection
          title="Editable characterization labels"
          subtitle="Issue types are shown as horizontal three-way blocks: ambiguous, missing, and inaccurate."
        >
          <div style={{ display: "grid", gap: 14, overflow: "visible" }}>
            {groupedByBase.map(({ base, title, fields }) => (
              <CharacterizationRowGroup
                key={base}
                title={title}
                fields={fields}
                values={selectedInstance.characterization}
                onEdit={onEdit}
                instance={selectedInstance}
              />
            ))}
          </div>
        </FullWidthAnnotationSection>
      </div>
    </div>
  );
}

function DiagnosisTab({
  rows,
  selectedInstance,
  model,
  setSelectedInstanceId,
  instanceFilter,
  setInstanceFilter,
  onEdit,
}) {
  const diagnosis = selectedInstance.diagnoses?.[model] || {};
  const prediction = selectedInstance.predictions?.[model] || {};
  const summary = selectedInstance.sql_eval_summary?.[model] || {};
  const details = selectedInstance.sql_execution_details?.[model] || {};
  const diagFields = useMemo(() => collectDiagnosisFields(rows, model), [rows, model]);
  const charFields = useMemo(() => collectCharFields(rows), [rows]);

  const orderedEditFields = [
    ...diagFields.filter((f) => f === "execution_match_assessment"),
    ...diagFields.filter((f) => f !== "execution_match_assessment"),
  ];

  const [setTopNode, topHeight] = useElementHeight([
    selectedInstance?.id,
    model,
    rows.length,
    instanceFilter.search,
    instanceFilter.coarseGroups,
    instanceFilter.charFields,
    instanceFilter.diagnosisFields,
    instanceFilter.flaggedStatus,
    instanceFilter.executionMatchStatus,
    instanceFilter.resultShapeCategory,
  ]);

  return (
    <div style={{ display: "grid", gap: 16 }}>
      <SectionTitle
        title="Prediction Diagnosis"
        subtitle="Inspect model-specific SQL, execution outputs, flagged outcomes, and editable diagnosis labels."
      />
      <FilterBar
        instanceFilter={instanceFilter}
        setInstanceFilter={setInstanceFilter}
        charOptions={charFields}
        diagnosisOptions={getDiagnosisFields(selectedInstance, model)}
      />
      <DiagnosisInstanceSummary instance={selectedInstance} model={model} rows={rows} />

      <div style={{ display: "grid", gap: 16 }}>
        <div style={{ display: "grid", gridTemplateColumns: "320px 1fr", gap: 16, alignItems: "start" }}>
          <InstanceList
            rows={rows}
            selectedId={selectedInstance.id}
            setSelectedId={setSelectedInstanceId}
            rightLabel={(row) => displayFlagged(row.diagnoses?.[model]?.execution_match_assessment?.classification)}
            height={topHeight}
          />

          <div style={{ display: "grid", gap: 14, minWidth: 0 }}>
            <div ref={setTopNode} style={{ display: "grid", gap: 14, minWidth: 0 }}>
              <InstanceHeader instance={selectedInstance} extra={`Model: ${model}`} />

              <div style={{ display: "grid", gridTemplateColumns: "repeat(2, minmax(0, 1fr))", gap: 14 }}>
                <CodeBlock title="Gold SQL" value={formatSql(selectedInstance.gold_sql)} maxHeight={260} />
                <CodeBlock title="Predicted SQL" value={formatSql(prediction.pred_sql)} maxHeight={260} />
              </div>

              <div style={{ display: "grid", gridTemplateColumns: "repeat(4, minmax(0,1fr))", gap: 10 }}>
                {metricCard("Execution match", summary.execution_match === 1 || summary.execution_match === true ? "match" : "non-match")}
                {metricCard("Exact match", summary.exact_match === 1 || summary.exact_match === true ? "match" : "non-match")}
                {metricCard("Flag status", displayFlagged(diagnosis.execution_match_assessment?.classification))}
                {metricCard("Pred execution flag", details.pred_execution_flag || summary.pred_execution_flag || "(none)")}
              </div>

              <div style={{ display: "grid", gridTemplateColumns: "repeat(2, minmax(0, 1fr))", gap: 14 }}>
                <JsonBlock
                  title="Gold execution metadata"
                  value={{
                    exec_error: (() => {
                      const v = details.gold_execution_flag ?? summary.gold_execution_flag;
                      if (!v || v === "result") return "none";
                      return v;
                    })(),
                    error_msg: (() => {
                      const v = details.gold_error ?? summary.gold_error;
                      if (!v) return "none";
                      return v;
                    })(),
                    shape: details.gold_shape || summary.gold_shape,
                  }}
                  maxHeight={220}
                />

                <JsonBlock
                  title="Pred execution metadata"
                  value={{
                    exec_error: (() => {
                      const v = details.pred_execution_flag ?? summary.pred_execution_flag;
                      if (!v || v === "result") return "none";
                      return v;
                    })(),
                    error_msg: (() => {
                      const v = details.pred_error ?? summary.pred_error;
                      if (!v) return "none";
                      return v;
                    })(),
                    shape: details.pred_shape || summary.pred_shape,
                  }}
                  maxHeight={220}
                />
              </div>

              <div style={{ display: "grid", gridTemplateColumns: "repeat(2, minmax(0, 1fr))", gap: 14 }}>
                <ExecutionResultsTable
                  title="Gold execution results"
                  columns={
                    Array.isArray(summary.gold_columns) && summary.gold_columns.length
                      ? summary.gold_columns
                      : Array.isArray(details.gold_columns) && details.gold_columns.length
                        ? details.gold_columns
                        : []
                  }
                  rows={details.gold_execution_results || summary.gold_execution_results || []}
                />

                <ExecutionResultsTable
                  title="Pred execution results"
                  columns={
                    Array.isArray(summary.pred_columns) && summary.pred_columns.length
                      ? summary.pred_columns
                      : Array.isArray(details.pred_columns) && details.pred_columns.length
                        ? details.pred_columns
                        : []
                  }
                  rows={details.pred_execution_results || summary.pred_execution_results || []}
                />
              </div>
            </div>
          </div>
        </div>

        <FullWidthAnnotationSection
          title="Editable diagnosis labels"
          subtitle="Flagged execution outcome is shown first, followed by the remaining prediction diagnosis labels."
        >
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(3, minmax(280px, 1fr))",
              gap: 14,
              alignItems: "start",
              overflow: "visible",
            }}
          >
            {orderedEditFields.map((field) => {
              const obj = diagnosis[field] || { classification: false, description: "" };
              const active = isActiveClassification(field, obj);
              const value =
                field === "execution_match_assessment"
                  ? displayFlagged(obj.classification)
                  : displayBooleanClassification(Boolean(obj.classification));

              return (
                <AnnotationFieldCard
                  key={field}
                  title={field === "execution_match_assessment" ? "Flagged Execution Outcome" : titleize(field)}
                  value={value}
                  description={obj.description}
                  definition={getDefinitionForField(field, "diagnosis")}
                  accent={GROUP_COLORS.diagnosis}
                  active={active}
                  onEdit={() => onEdit("diagnosis", field, selectedInstance)}
                />
              );
            })}
          </div>
        </FullWidthAnnotationSection>
      </div>
    </div>
  );
}

function ComparisonTab({ rows, primaryModel, secondaryModel, setSecondaryModel, modelOptions }) {
  const pairs = useMemo(() => {
    return rows.map((row) => {
      const leftDiagnosis = row.diagnoses?.[primaryModel] || {};
      const rightDiagnosis = row.diagnoses?.[secondaryModel] || {};

      const leftIssues = Object.entries(leftDiagnosis)
        .filter(([key, obj]) => key !== "execution_match_assessment" && obj && typeof obj === "object" && Boolean(obj.classification))
        .map(([key]) => key);

      const rightIssues = Object.entries(rightDiagnosis)
        .filter(([key, obj]) => key !== "execution_match_assessment" && obj && typeof obj === "object" && Boolean(obj.classification))
        .map(([key]) => key);

      const leftExec = row.sql_eval_summary?.[primaryModel]?.execution_match === 1 || row.sql_eval_summary?.[primaryModel]?.execution_match === true;
      const rightExec = row.sql_eval_summary?.[secondaryModel]?.execution_match === 1 || row.sql_eval_summary?.[secondaryModel]?.execution_match === true;

      return {
        id: row.id,
        nl: row.nl,
        leftFlagged: inferFlaggedValue(leftDiagnosis.execution_match_assessment?.classification),
        rightFlagged: inferFlaggedValue(rightDiagnosis.execution_match_assessment?.classification),
        leftExec,
        rightExec,
        leftIssues,
        rightIssues,
        issueUnion: Array.from(new Set([...leftIssues, ...rightIssues])),
      };
    });
  }, [rows, primaryModel, secondaryModel]);

  const stats = useMemo(() => {
    let bothFlagged = 0;
    let onlyLeft = 0;
    let onlyRight = 0;
    let neither = 0;

    pairs.forEach((p) => {
      if (p.leftFlagged && p.rightFlagged) bothFlagged += 1;
      else if (p.leftFlagged) onlyLeft += 1;
      else if (p.rightFlagged) onlyRight += 1;
      else neither += 1;
    });

    return { bothFlagged, onlyLeft, onlyRight, neither };
  }, [pairs]);

  const executionBuckets = useMemo(() => {
    const bucketDefs = [
      {
        key: "all",
        label: "All",
        rows: pairs,
      },
      {
        key: "unflagged",
        label: "Unflagged",
        rows: pairs.filter((p) => !p.leftFlagged && !p.rightFlagged),
      },
      {
        key: "flagged",
        label: "Flagged",
        rows: pairs.filter((p) => p.leftFlagged || p.rightFlagged),
      },
    ];

    return bucketDefs.map((bucket) => {
      const total = bucket.rows.length;
      const leftMatches = bucket.rows.filter((p) => p.leftExec).length;
      const rightMatches = bucket.rows.filter((p) => p.rightExec).length;

      return {
        ...bucket,
        total,
        leftMatches,
        rightMatches,
        leftRate: total ? (100 * leftMatches) / total : 0,
        rightRate: total ? (100 * rightMatches) / total : 0,
      };
    });
  }, [pairs]);

  const issueComparison = useMemo(() => {
    const issueMap = new Map();

    pairs.forEach((pair) => {
      pair.issueUnion.forEach((issue) => {
        const current = issueMap.get(issue) || {
          key: issue,
          label: titleize(issue),
          count: 0,
          leftMatches: 0,
          rightMatches: 0,
        };

        current.count += 1;
        if (pair.leftExec) current.leftMatches += 1;
        if (pair.rightExec) current.rightMatches += 1;

        issueMap.set(issue, current);
      });
    });

    return Array.from(issueMap.values())
      .map((item) => {
        const leftRate = item.count ? (100 * item.leftMatches) / item.count : 0;
        const rightRate = item.count ? (100 * item.rightMatches) / item.count : 0;
        const diff = leftRate - rightRate;

        return {
          ...item,
          leftRate,
          rightRate,
          diff,
        };
      })
      .sort((a, b) => Math.abs(b.diff) - Math.abs(a.diff));
  }, [pairs]);

  const improvements = issueComparison.filter((item) => item.diff > 0).slice(0, 8);
  const regressions = issueComparison.filter((item) => item.diff < 0).slice(0, 8);

  const alignedPairs = useMemo(() => {
    return [...pairs].sort((a, b) => {
      const aDisagree = Number(a.leftExec !== a.rightExec) + Number(a.leftFlagged !== a.rightFlagged);
      const bDisagree = Number(b.leftExec !== b.rightExec) + Number(b.leftFlagged !== b.rightFlagged);
      return bDisagree - aDisagree;
    });
  }, [pairs]);

  function renderExecutionBar(label, rate, color) {
    return (
      <div style={{ display: "grid", gap: 6 }}>
        <div style={{ display: "flex", justifyContent: "space-between", gap: 12, fontSize: 13 }}>
          <span>{label}</span>
          <span style={{ fontWeight: 600 }}>{rate.toFixed(1)}%</span>
        </div>
        <div style={{ height: 10, borderRadius: 999, background: "#e5e7eb", overflow: "hidden" }}>
          <div
            style={{
              width: `${Math.max(0, Math.min(100, rate))}%`,
              height: "100%",
              background: color,
              borderRadius: 999,
            }}
          />
        </div>
      </div>
    );
  }

  function renderIssueTable(title, items, tone, direction) {
    const maxAbsDiff = Math.max(1, ...items.map((item) => Math.abs(item.diff)));

    return (
      <div style={{ ...cardStyle, padding: 16 }}>
        <div style={{ fontWeight: 600, marginBottom: 12 }}>{title}</div>
        <div style={{ display: "grid", gap: 12 }}>
          {items.length === 0 ? (
            <div style={{ color: "#6b7280" }}>(no differences)</div>
          ) : items.map((item) => {
            const widthPct = (Math.abs(item.diff) / maxAbsDiff) * 50;
            const isPositive = item.diff >= 0;

            return (
              <div
                key={item.key}
                style={{
                  border: "1px solid #e5e7eb",
                  borderRadius: 12,
                  padding: 12,
                  background: "#fff",
                }}
              >
                <div style={{ display: "flex", justifyContent: "space-between", gap: 12, marginBottom: 8 }}>
                  <div style={{ fontWeight: 600 }}>{item.label}</div>
                  <div style={{ fontSize: 12, color: "#6b7280" }}>n={item.count}</div>
                </div>

                <div style={{ fontSize: 13, color: "#4b5563", marginBottom: 8 }}>
                  {primaryModel}: {item.leftRate.toFixed(1)}% · {secondaryModel}: {item.rightRate.toFixed(1)}%
                </div>

                <div
                  style={{
                    position: "relative",
                    height: 28,
                    borderRadius: 999,
                    background: "#f3f4f6",
                    overflow: "hidden",
                    marginBottom: 8,
                  }}
                >
                  <div
                    style={{
                      position: "absolute",
                      left: "50%",
                      top: 0,
                      bottom: 0,
                      width: 1,
                      background: "#9ca3af",
                      zIndex: 2,
                    }}
                  />

                  <div
                    style={{
                      position: "absolute",
                      top: 6,
                      bottom: 6,
                      left: isPositive ? "50%" : `calc(50% - ${widthPct}%)`,
                      width: `${widthPct}%`,
                      background: tone,
                      borderRadius: 999,
                      opacity: 0.9,
                    }}
                  />

                  <div
                    style={{
                      position: "absolute",
                      inset: 0,
                      display: "flex",
                      alignItems: "center",
                      justifyContent: "space-between",
                      padding: "0 10px",
                      fontSize: 11,
                      color: "#6b7280",
                    }}
                  >
                    <span>{direction === "positive" ? secondaryModel : primaryModel}</span>
                    <span>{direction === "positive" ? primaryModel : secondaryModel}</span>
                  </div>
                </div>

                <div style={{ fontSize: 13, color: tone, fontWeight: 600 }}>
                  Δ {item.diff > 0 ? "+" : ""}{item.diff.toFixed(1)} pts
                </div>
              </div>
            );
          })}
        </div>
      </div>
    );
  }

  return (
    <div style={{ display: "grid", gap: 16 }}>
      <SectionTitle
        title="Cross-Model Comparison"
        subtitle="Compare execution behavior across flag settings, then inspect where one model improves or regresses on specific issue types."
        right={<Selector label="Compare to" value={secondaryModel} onChange={setSecondaryModel} options={modelOptions.filter(Boolean)} />}
      />

      <div style={{ display: "grid", gridTemplateColumns: "repeat(4, minmax(0, 1fr))", gap: 14 }}>
        {metricCard("Both flagged", stats.bothFlagged)}
        {metricCard(`${primaryModel} only`, stats.onlyLeft)}
        {metricCard(`${secondaryModel} only`, stats.onlyRight)}
        {metricCard("Neither flagged", stats.neither)}
      </div>

      <div style={{ ...cardStyle, padding: 16 }}>
        <div style={{ fontWeight: 600, marginBottom: 12 }}>Execution match across flag settings</div>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(3, minmax(0, 1fr))", gap: 14 }}>
          {executionBuckets.map((bucket) => (
            <div
              key={bucket.key}
              style={{
                border: "1px solid #e5e7eb",
                borderRadius: 14,
                padding: 14,
                background: "#fff",
              }}
            >
              <div style={{ display: "flex", justifyContent: "space-between", gap: 12, marginBottom: 12 }}>
                <div style={{ fontWeight: 600 }}>{bucket.label}</div>
                <div style={{ fontSize: 12, color: "#6b7280" }}>n={bucket.total}</div>
              </div>
              <div style={{ display: "grid", gap: 12 }}>
                {renderExecutionBar(primaryModel, bucket.leftRate, "#2563eb")}
                {renderExecutionBar(secondaryModel, bucket.rightRate, "#7c3aed")}
              </div>
            </div>
          ))}
        </div>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "repeat(2, minmax(0, 1fr))", gap: 14 }}>
        {renderIssueTable(`${primaryModel} improvements by issue type`, improvements, "#10b981", "positive")}
        {renderIssueTable(`${primaryModel} regressions by issue type`, regressions, "#ef4444", "negative")}
      </div>

      <div style={{ ...cardStyle, padding: 16 }}>
        <div style={{ fontWeight: 600, marginBottom: 12 }}>Aligned instance comparison</div>
        <div style={{ display: "grid", gap: 10, maxHeight: 560, overflow: "auto" }}>
          {alignedPairs.map((pair) => (
            <div key={pair.id} style={{ border: "1px solid #e5e7eb", borderRadius: 12, padding: 12, background: "#fff" }}>
              <div style={{ display: "flex", justifyContent: "space-between", gap: 12, marginBottom: 6 }}>
                <div style={{ fontSize: 12, color: "#6b7280" }}>#{pair.id}</div>
                <div style={{ fontSize: 12, color: "#6b7280" }}>
                  {primaryModel}: {pair.leftFlagged ? "flagged" : "unflagged"} · {secondaryModel}: {pair.rightFlagged ? "flagged" : "unflagged"}
                </div>
              </div>
              <div style={{ fontWeight: 500, marginBottom: 8 }}>{pair.nl}</div>
              <div style={{ display: "grid", gridTemplateColumns: "repeat(2, minmax(0, 1fr))", gap: 10 }}>
                <div style={{ background: "#f8fafc", borderRadius: 10, padding: 10, border: "1px solid #e5e7eb" }}>
                  <div style={{ fontSize: 12, textTransform: "uppercase", color: "#6b7280" }}>{primaryModel}</div>
                  <div style={{ marginTop: 4 }}>Flag: {pair.leftFlagged ? "flagged" : "unflagged"}</div>
                  <div style={{ marginTop: 4 }}>Execution match: {pair.leftExec ? "match" : "unmatched"}</div>
                  <div style={{ marginTop: 4, color: "#6b7280" }}>
                    Issues: {pair.leftIssues.length ? pair.leftIssues.map((x) => titleize(x)).join(", ") : "none"}
                  </div>
                </div>
                <div style={{ background: "#f8fafc", borderRadius: 10, padding: 10, border: "1px solid #e5e7eb" }}>
                  <div style={{ fontSize: 12, textTransform: "uppercase", color: "#6b7280" }}>{secondaryModel}</div>
                  <div style={{ marginTop: 4 }}>Flag: {pair.rightFlagged ? "flagged" : "unflagged"}</div>
                  <div style={{ marginTop: 4 }}>Execution match: {pair.rightExec ? "match" : "unmatched"}</div>
                  <div style={{ marginTop: 4, color: "#6b7280" }}>
                    Issues: {pair.rightIssues.length ? pair.rightIssues.map((x) => titleize(x)).join(", ") : "none"}
                  </div>
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}


function getExecutionStatusBadge(value, kind = "neutral") {
  const normalized = String(value || "none");
  const successLike = ["match", "accepted", "success", "reference", "gold reference", "unflagged", "none"];
  const dangerLike = ["non-match", "flagged", "timeout", "exception", "error"];
  const isSuccess = successLike.includes(normalized.toLowerCase());
  const isDanger = dangerLike.includes(normalized.toLowerCase());

  return {
    label: normalized,
    background: isSuccess ? "#ecfdf5" : isDanger ? "#fef2f2" : "#f8fafc",
    border: isSuccess ? "#a7f3d0" : isDanger ? "#fecaca" : "#e5e7eb",
    color: isSuccess ? "#065f46" : isDanger ? "#b91c1c" : "#374151",
  };
}

function getExecutionPanelPayload(instance, modelName, role = "prediction") {
  const summary = instance?.sql_eval_summary?.[modelName] || {};
  const details = instance?.sql_execution_details?.[modelName] || {};
  const diagnosis = instance?.diagnoses?.[modelName] || {};
  const prediction = instance?.predictions?.[modelName] || {};

  const execMatch = summary.execution_match === 1 || summary.execution_match === true;
  const exactMatch = summary.exact_match === 1 || summary.exact_match === true;

  const execFlagRaw =
    role === "gold"
      ? details.gold_execution_flag ?? summary.gold_execution_flag
      : details.pred_execution_flag ?? summary.pred_execution_flag;

  const errorRaw =
    role === "gold"
      ? details.gold_error ?? summary.gold_error
      : details.pred_error ?? summary.pred_error;

  const shapeRaw =
    role === "gold"
      ? details.gold_shape ?? summary.gold_shape
      : details.pred_shape ?? summary.pred_shape;

  const columnsRaw =
    role === "gold"
      ? pickNonEmptyColumns(summary.gold_columns, details.gold_columns)
      : pickNonEmptyColumns(summary.pred_columns, details.pred_columns);

  const resultsRaw =
    role === "gold"
      ? details.gold_execution_results || summary.gold_execution_results || []
      : details.pred_execution_results || summary.pred_execution_results || [];

  const rowCount = getRowCountFromShape(shapeRaw);
  const displayExecFlag = !execFlagRaw || execFlagRaw === "result" ? "none" : execFlagRaw;
  const displayError = !errorRaw ? "none" : errorRaw;

  return {
    title: role === "gold" ? "Gold reference" : modelName,
    sql: role === "gold" ? instance?.gold_sql : prediction.pred_sql,
    executionMatchLabel: role === "gold" ? "reference" : execMatch ? "match" : "non-match",
    exactMatchLabel: role === "gold" ? "reference" : exactMatch ? "match" : "non-match",
    flagLabel: role === "gold" ? "reference" : displayFlagged(diagnosis.execution_match_assessment?.classification),
    predFlagLabel: displayExecFlag,
    metadata: {
      exec_error: displayExecFlag,
      error_msg: displayError,
      shape: shapeRaw || "none",
      columns: Array.isArray(columnsRaw) ? columnsRaw.length : 0,
      rows: rowCount == null ? "unknown" : rowCount,
    },
    columns: columnsRaw,
    results: resultsRaw,
  };
}

function StatusMiniCard({ label, value }) {
  const badge = getExecutionStatusBadge(value);
  return (
    <div
      style={{
        border: `1px solid ${badge.border}`,
        background: badge.background,
        borderRadius: 12,
        padding: "10px 12px",
        minHeight: 66,
        display: "grid",
        alignContent: "space-between",
        gap: 6,
        minWidth: 0,
        overflow: "hidden",
      }}
    >
      <div style={{ fontSize: 11, textTransform: "uppercase", color: "#6b7280", letterSpacing: 0.4 }}>{label}</div>
      <div style={{ fontSize: 14, fontWeight: 700, color: badge.color, lineHeight: 1.2, minWidth: 0, overflowWrap: "anywhere" }}>{badge.label}</div>
    </div>
  );
}

function ExecutionComparisonColumn({ panel, panelKey, syncedHeights = {}, registerSyncedNode }) {
  return (
    <div
      style={{
        ...cardStyle,
        padding: 14,
        display: "grid",
        gap: 12,
        alignContent: "start",
        height: "100%",
        minWidth: 0,
        overflow: "hidden",
      }}
    >
      <div style={{ fontSize: 18, fontWeight: 700 }}>{panel.title}</div>

      <div ref={registerSyncedNode ? registerSyncedNode("sql", panelKey) : null}>
        <CodeBlock
          title="SQL"
          value={formatSql(panel.sql)}
          maxHeight={180}
          containerMinHeight={syncedHeights.sql}
        />
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "repeat(2, minmax(0, 1fr))", gap: 10, minWidth: 0 }}>
        <StatusMiniCard label="Execution match" value={panel.executionMatchLabel} />
        <StatusMiniCard label="Exact match" value={panel.exactMatchLabel} />
        <StatusMiniCard label="Flag" value={panel.flagLabel} />
        <StatusMiniCard label="Pred flag" value={panel.predFlagLabel} />
      </div>

      <div ref={registerSyncedNode ? registerSyncedNode("metadata", panelKey) : null}>
        <JsonBlock
          title="Metadata"
          value={panel.metadata}
          maxHeight={190}
          containerMinHeight={syncedHeights.metadata}
        />
      </div>

      <ExecutionResultsTable
        title="Execution results"
        columns={panel.columns}
        rows={panel.results}
      />
    </div>
  );
}




function getCharacterizationProblemBucketForRow(row, group = null) {
  const bases = getActiveCharacterizationBases(row, group);

  if (group) {
    if (!hasCoarseBenchmarkIssue(row, group)) return "none";
    if (!bases.length) return "coarse_only";
    if (bases.length === 1) return bases[0];
    return "multiple_problem_types";
  }

  if (!bases.length) {
    return hasAnyBenchmarkIssue(row) ? "benchmark_issue_other" : "no_problem";
  }
  if (bases.length === 1) return bases[0];
  return "multiple_problem_types";
}

function collectCharacterizationProblemBuckets(rows = []) {
  const buckets = new Set(["no_problem", "benchmark_issue_other", "multiple_problem_types", "coarse_only"]);

  rows.forEach((row) => {
    getActiveCharacterizationBases(row).forEach((base) => buckets.add(base));
    CHARACTERIZATION_GROUPS.forEach((group) => {
      getActiveCharacterizationBases(row, group).forEach((base) => buckets.add(base));
    });
  });

  const orderedBases = USER_FIELDS
    .map((field) => field?.name)
    .filter(Boolean)
    .map((name) => splitCharField(name).base)
    .filter((base) => base && base !== "nl2sql_not_possible");

  const preferred = [
    "no_problem",
    ...orderedBases,
    "multiple_problem_types",
    "benchmark_issue_other",
    "coarse_only",
  ];

  return Array.from(new Set([...preferred, ...Array.from(buckets)])).filter(Boolean);
}

function getActiveDiagnosisProblemTypes(row, model = null) {
  const diagnosisSources = model ? [row?.diagnoses?.[model] || {}] : Object.values(row?.diagnoses || {});
  const active = [];

  diagnosisSources.forEach((diagnosis) => {
    Object.entries(diagnosis).forEach(([key, obj]) => {
      if (key === "execution_match_assessment") return;
      if (!obj || typeof obj !== "object") return;
      if (!Object.prototype.hasOwnProperty.call(obj, "classification")) return;
      if (!obj.classification) return;
      active.push(key);
    });
  });

  return Array.from(new Set(active));
}

function getDiagnosisProblemBucketForRow(row, model) {
  const problemTypes = getActiveDiagnosisProblemTypes(row, model);
  if (!problemTypes.length) return "no_problem";
  if (problemTypes.length === 1) return problemTypes[0];
  return "multiple_problem_types";
}

function collectDiagnosisProblemBuckets(rows = [], model = null) {
  const buckets = new Set(["no_problem", "multiple_problem_types"]);

  rows.forEach((row) => {
    getActiveDiagnosisProblemTypes(row, model).forEach((field) => buckets.add(field));
  });

  const orderedFields = DIAGNOSIS_FIELDS_ORDER.filter((field) => field !== "execution_match_assessment");
  const preferred = ["no_problem", ...orderedFields, "multiple_problem_types"];

  return Array.from(new Set([...preferred, ...Array.from(buckets)])).filter(Boolean);
}

function getProblemBucketLabel(bucket) {
  if (bucket === "no_problem") return "No problem";
  if (bucket === "multiple_problem_types") return "Multiple problem types";
  if (bucket === "benchmark_issue_other") return "Benchmark issue only";
  if (bucket === "coarse_only") return "Coarse only";
  return BASE_TITLE_OVERRIDES[bucket] || titleize(bucket);
}

function getProblemBucketColor(bucket, colorMap = {}) {
  return colorMap[bucket] || "#94a3b8";
}

function createProblemBucketColorMap(buckets = []) {
  const palette = [
    "#2563eb",
    "#7c3aed",
    "#10b981",
    "#f59e0b",
    "#ef4444",
    "#06b6d4",
    "#84cc16",
    "#ec4899",
    "#8b5cf6",
    "#14b8a6",
    "#f97316",
    "#0ea5e9",
    "#65a30d",
    "#6366f1",
    "#d946ef",
    "#22c55e",
    "#e11d48",
    "#0891b2",
  ];

  const fixed = {
    no_problem: "#94a3b8",
    multiple_problem_types: "#475569",
    benchmark_issue_other: "#64748b",
    coarse_only: "#a855f7",
  };

  const dynamic = buckets.filter((bucket) => !(bucket in fixed));
  const mapping = { ...fixed };
  dynamic.forEach((bucket, index) => {
    mapping[bucket] = palette[index % palette.length];
  });
  return mapping;
}

function countProblemBuckets(rows = [], group = null) {
  const counts = {};
  rows.forEach((row) => {
    const bucket = getCharacterizationProblemBucketForRow(row, group);
    counts[bucket] = (counts[bucket] || 0) + 1;
  });
  return counts;
}

function countDiagnosisProblemBuckets(rows = [], model) {
  const counts = {};
  rows.forEach((row) => {
    const bucket = getDiagnosisProblemBucketForRow(row, model);
    counts[bucket] = (counts[bucket] || 0) + 1;
  });
  return counts;
}

function buildProblemSegments(counts = {}, allBuckets = [], colorMap = {}, includeNone = false) {
  const buckets = allBuckets.filter((bucket) => includeNone || bucket !== "none");
  return buckets
    .map((bucket) => ({
      key: bucket,
      label: getProblemBucketLabel(bucket),
      value: counts[bucket] || 0,
      color: getProblemBucketColor(bucket, colorMap),
    }))
    .filter((item) => item.value > 0);
}

function buildCrossBenchmarkCharStats(rows = [], allBuckets = null, colorMap = null) {
  const total = rows.length;
  let ambiguous = 0;
  let missing = 0;
  let inaccurate = 0;
  let none = 0;

  rows.forEach((row) => {
    const hasAmbiguous = hasCoarseBenchmarkIssue(row, "ambiguous");
    const hasMissing = hasCoarseBenchmarkIssue(row, "missing");
    const hasInaccurate = hasCoarseBenchmarkIssue(row, "inaccurate");
    if (hasAmbiguous) ambiguous += 1;
    if (hasMissing) missing += 1;
    if (hasInaccurate) inaccurate += 1;
    if (!hasAmbiguous && !hasMissing && !hasInaccurate) none += 1;
  });

  const buckets = allBuckets || collectCharacterizationProblemBuckets(rows);
  const colors = colorMap || createProblemBucketColorMap(buckets);

  const fineGrainedByGroup = CHARACTERIZATION_GROUPS.map((group) => {
    const groupRows = rows.filter((row) => hasCoarseBenchmarkIssue(row, group));
    const groupTotal = groupRows.length;
    const counts = countProblemBuckets(groupRows, group);
    return {
      key: group,
      label: titleize(group),
      total: groupTotal,
      segments: buildProblemSegments(counts, buckets.filter((bucket) => bucket !== "no_problem" && bucket !== "benchmark_issue_other"), colors),
    };
  });

  return { total, ambiguous, missing, inaccurate, none, fineGrainedByGroup };
}

function buildMatchShapeStats(rows = [], model) {
  let emptyMatches = 0;
  let singleMatches = 0;
  let multiMatches = 0;

  rows.forEach((row) => {
    const execMatch = row?.sql_eval_summary?.[model]?.execution_match === 1 || row?.sql_eval_summary?.[model]?.execution_match === true;
    if (!execMatch) return;
    const shape = getExecutionShapeCategoryForRow(row, model);
    if (shape === "empty") emptyMatches += 1;
    else if (shape === "single") singleMatches += 1;
    else if (shape === "multi") multiMatches += 1;
  });

  return { emptyMatches, singleMatches, multiMatches };
}

function buildPredictionProblemBreakdown(rows = [], model, allBuckets = null, colorMap = null) {
  const total = rows.length;
  const buckets = allBuckets || collectDiagnosisProblemBuckets(rows, model);
  const colors = colorMap || createProblemBucketColorMap(buckets);

  const mismatchRows = rows.filter((row) => !(row?.sql_eval_summary?.[model]?.execution_match === 1 || row?.sql_eval_summary?.[model]?.execution_match === true));
  const flaggedMatchRows = rows.filter((row) => {
    const execMatch = row?.sql_eval_summary?.[model]?.execution_match === 1 || row?.sql_eval_summary?.[model]?.execution_match === true;
    const isFlagged = inferFlaggedValue(row?.diagnoses?.[model]?.execution_match_assessment?.classification);
    return execMatch && isFlagged;
  });

  return {
    problemSegments: buildProblemSegments(countDiagnosisProblemBuckets(rows, model), buckets, colors),
    mismatchProblemSegments: buildProblemSegments(countDiagnosisProblemBuckets(mismatchRows, model), buckets, colors),
    flaggedMatchProblemSegments: buildProblemSegments(countDiagnosisProblemBuckets(flaggedMatchRows, model), buckets, colors),
  };
}

function buildCrossBenchmarkPredictionStats(rows = [], model, allBuckets = null, colorMap = null) {
  const total = rows.length;
  let execMatches = 0;
  let flagged = 0;
  let benchmarkIssues = 0;
  let flaggedMatches = 0;

  rows.forEach((row) => {
    const execMatch = row?.sql_eval_summary?.[model]?.execution_match === 1 || row?.sql_eval_summary?.[model]?.execution_match === true;
    const isFlagged = inferFlaggedValue(row?.diagnoses?.[model]?.execution_match_assessment?.classification);
    if (execMatch) execMatches += 1;
    if (isFlagged) flagged += 1;
    if (hasAnyBenchmarkIssue(row)) benchmarkIssues += 1;
    if (execMatch && isFlagged) flaggedMatches += 1;
  });

  const shapeStats = buildMatchShapeStats(rows, model);
  const problemBreakdown = buildPredictionProblemBreakdown(rows, model, allBuckets, colorMap);

  return {
    total,
    execMatches,
    flagged,
    benchmarkIssues,
    flaggedMatches,
    ...shapeStats,
    ...problemBreakdown,
  };
}

function CrossBenchmarkStackedBar({ segments = [], total, height = 18 }) {
  return (
    <div style={{ height, background: "#e5e7eb", borderRadius: 999, overflow: "hidden", display: "flex" }}>
      {segments.map((segment) => {
        const width = total ? (100 * segment.value) / total : 0;
        if (width <= 0) return null;
        return <div key={segment.key} title={`${segment.label}: ${segment.value}`} style={{ width: `${width}%`, background: segment.color, minWidth: width > 0 ? 2 : 0 }} />;
      })}
    </div>
  );
}

function CrossBenchmarkLegend({ items = [] }) {
  return (
    <div style={{ display: "flex", gap: 12, flexWrap: "wrap" }}>
      {items.map((item) => (
        <div key={item.key} style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 12, color: "#4b5563" }}>
          <span style={{ width: 10, height: 10, borderRadius: 999, background: item.color, display: "inline-block" }} />
          <span>{item.label}</span>
        </div>
      ))}
    </div>
  );
}

function CrossBenchmarkMiniMetricRow({ label, value, total, color }) {
  return (
    <div style={{ display: "grid", gridTemplateColumns: "120px minmax(0, 1fr) 80px", gap: 10, alignItems: "center" }}>
      <div style={{ fontSize: 12, color: "#4b5563" }}>{label}</div>
      <div style={{ height: 10, borderRadius: 999, background: "#e5e7eb", overflow: "hidden" }}>
        <div style={{ width: `${total ? (100 * value) / total : 0}%`, height: "100%", background: color }} />
      </div>
      <div style={{ fontSize: 12, textAlign: "right" }}>{formatPercent(value, total)}</div>
    </div>
  );
}

function CrossBenchmarkProblemPanel({ title, subtitle, total, sections = [], legendItems = [] }) {
  return (
    <div style={{ ...cardStyle, padding: 16 }}>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 12, marginBottom: 12, flexWrap: "wrap" }}>
        <div>
          <div style={{ fontWeight: 600 }}>{title}</div>
          {subtitle ? <div style={{ color: "#6b7280", marginTop: 4 }}>{subtitle}</div> : null}
        </div>
        <CrossBenchmarkLegend items={legendItems} />
      </div>
      <div style={{ display: "grid", gap: 12 }}>
        {sections.map((section) => (
          <div key={section.key} style={{ display: "grid", gap: 6 }}>
            <div style={{ display: "flex", justifyContent: "space-between", gap: 12, fontSize: 12 }}>
              <span style={{ color: "#4b5563" }}>{section.label}</span>
              <span style={{ color: "#6b7280" }}>{section.caption}</span>
            </div>
            <CrossBenchmarkStackedBar total={total} segments={section.segments} />
          </div>
        ))}
      </div>
    </div>
  );
}

function CrossBenchmarkTab({ catalog }) {
  const [mode, setMode] = useState("characterization");
  const [charPicker, setCharPicker] = useState({ dataset: "", split: "", experiment: "" });
  const [predPicker, setPredPicker] = useState({ dataset: "", split: "", experiment: "", model: "" });
  const [selectedCharKeys, setSelectedCharKeys] = useState([]);
  const [selectedPredKeys, setSelectedPredKeys] = useState([]);
  const [charState, setCharState] = useState({ loading: false, error: null, views: [] });
  const [predState, setPredState] = useState({ loading: false, error: null, views: [] });

  const datasets = catalog?.datasets || [];

  useEffect(() => {
    if (!datasets.length) return;

    setCharPicker((prev) => {
      const dataset = prev.dataset && datasets.some((d) => d.name === prev.dataset) ? prev.dataset : datasets[0]?.name || "";
      const ds = datasets.find((d) => d.name === dataset) || datasets[0];
      const split = prev.split && ds?.splits?.some((s) => s.name === prev.split) ? prev.split : ds?.splits?.[0]?.name || "";
      const splitObj = ds?.splits?.find((s) => s.name === split) || ds?.splits?.[0];
      const experiment = prev.experiment && splitObj?.experiments?.some((exp) => exp.name === prev.experiment) ? prev.experiment : splitObj?.experiments?.[0]?.name || "";
      if (dataset === prev.dataset && split === prev.split && experiment === prev.experiment) return prev;
      return { dataset, split, experiment };
    });

    setPredPicker((prev) => {
      const dataset = prev.dataset && datasets.some((d) => d.name === prev.dataset) ? prev.dataset : datasets[0]?.name || "";
      const ds = datasets.find((d) => d.name === dataset) || datasets[0];
      const split = prev.split && ds?.splits?.some((s) => s.name === prev.split) ? prev.split : ds?.splits?.[0]?.name || "";
      const splitObj = ds?.splits?.find((s) => s.name === split) || ds?.splits?.[0];
      const experiment = prev.experiment && splitObj?.experiments?.some((exp) => exp.name === prev.experiment) ? prev.experiment : splitObj?.experiments?.[0]?.name || "";
      const experimentObj = splitObj?.experiments?.find((exp) => exp.name === experiment) || splitObj?.experiments?.[0];
      const modelOptions = experimentObj?.models || [];
      const model = prev.model && modelOptions.includes(prev.model) ? prev.model : modelOptions[0] || "";
      if (dataset === prev.dataset && split === prev.split && experiment === prev.experiment && model === prev.model) return prev;
      return { dataset, split, experiment, model };
    });
  }, [datasets]);

  const charDatasetObj = datasets.find((d) => d.name === charPicker.dataset);
  const charSplitOptions = (charDatasetObj?.splits || []).map((s) => s.name);
  const charSplitObj = charDatasetObj?.splits?.find((s) => s.name === charPicker.split) || charDatasetObj?.splits?.[0];
  const charExperimentOptions = (charSplitObj?.experiments || []).map((exp) => exp.name);

  const predDatasetObj = datasets.find((d) => d.name === predPicker.dataset);
  const predSplitObj = predDatasetObj?.splits?.find((s) => s.name === predPicker.split) || predDatasetObj?.splits?.[0];
  const predSplitOptions = (predDatasetObj?.splits || []).map((s) => s.name);
  const predExperimentOptions = (predSplitObj?.experiments || []).map((exp) => exp.name);
  const predExperimentObj = predSplitObj?.experiments?.find((exp) => exp.name === predPicker.experiment) || predSplitObj?.experiments?.[0];
  const predModelOptions = predExperimentObj?.models || [];

  function updateCharPicker(key, value) {
    setCharPicker((prev) => {
      const next = { ...prev, [key]: value };
      if (key === "dataset") {
        const ds = datasets.find((d) => d.name === value);
        const splitObj = ds?.splits?.[0];
        next.split = splitObj?.name || "";
        next.experiment = splitObj?.experiments?.[0]?.name || "";
      }
      if (key === "split") {
        const ds = datasets.find((d) => d.name === prev.dataset);
        const splitObj = ds?.splits?.find((s) => s.name === value) || ds?.splits?.[0];
        next.experiment = splitObj?.experiments?.[0]?.name || "";
      }
      return next;
    });
  }

  function updatePredPicker(key, value) {
    setPredPicker((prev) => {
      const next = { ...prev, [key]: value };
      if (key === "dataset") {
        const ds = datasets.find((d) => d.name === value);
        const splitObj = ds?.splits?.[0];
        const experimentObj = splitObj?.experiments?.[0];
        next.split = splitObj?.name || "";
        next.experiment = experimentObj?.name || "";
        next.model = experimentObj?.models?.[0] || "";
      }
      if (key === "split") {
        const ds = datasets.find((d) => d.name === prev.dataset);
        const splitObj = ds?.splits?.find((s) => s.name === value) || ds?.splits?.[0];
        const experimentObj = splitObj?.experiments?.[0];
        next.experiment = experimentObj?.name || "";
        next.model = experimentObj?.models?.[0] || "";
      }
      if (key === "experiment") {
        const ds = datasets.find((d) => d.name === prev.dataset);
        const splitObj = ds?.splits?.find((s) => s.name === prev.split) || ds?.splits?.[0];
        const experimentObj = splitObj?.experiments?.find((exp) => exp.name === value) || splitObj?.experiments?.[0];
        next.model = experimentObj?.models?.[0] || "";
      }
      return next;
    });
  }

  function addCharSelection() {
    const ds = datasets.find((d) => d.name === charPicker.dataset);
    const splitObj = ds?.splits?.find((s) => s.name === charPicker.split);
    const experiment = charPicker.experiment || splitObj?.experiments?.[0]?.name || "";
    if (!ds?.name || !splitObj?.name || !experiment) return;
    const key = [ds.name, splitObj.name, experiment].join("|||");
    setSelectedCharKeys((prev) => (prev.includes(key) ? prev : [...prev, key]));
  }

  function addPredSelection() {
    const ds = datasets.find((d) => d.name === predPicker.dataset);
    const splitObj = ds?.splits?.find((s) => s.name === predPicker.split);
    const experiment = predPicker.experiment || splitObj?.experiments?.[0]?.name || "";
    if (!ds?.name || !splitObj?.name || !experiment || !predPicker.model) return;
    const key = [ds.name, splitObj.name, experiment, predPicker.model].join("|||");
    setSelectedPredKeys((prev) => (prev.includes(key) ? prev : [...prev, key]));
  }

  useEffect(() => {
    let cancelled = false;
    async function load() {
      if (!selectedCharKeys.length) {
        setCharState({ loading: false, error: null, views: [] });
        return;
      }
      setCharState((prev) => ({ ...prev, loading: true, error: null }));
      try {
        const views = await Promise.all(selectedCharKeys.map(async (key) => {
          const [dataset, split, experiment] = key.split("|||");
          const params = new URLSearchParams({ dataset, split, experiment });
          const res = await fetch(`${API_BASE}/api/view?${params.toString()}`);
          if (!res.ok) throw new Error(await res.text());
          const json = await res.json();
          return { key, dataset, split, experiment, rows: json.rows || [], models: json.models || [] };
        }));
        if (!cancelled) setCharState({ loading: false, error: null, views });
      } catch (error) {
        if (!cancelled) setCharState({ loading: false, error: String(error), views: [] });
      }
    }
    load();
    return () => { cancelled = true; };
  }, [selectedCharKeys]);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      if (!selectedPredKeys.length) {
        setPredState({ loading: false, error: null, views: [] });
        return;
      }
      setPredState((prev) => ({ ...prev, loading: true, error: null }));
      try {
        const views = await Promise.all(selectedPredKeys.map(async (key) => {
          const [dataset, split, experiment, model] = key.split("|||");
          const params = new URLSearchParams({ dataset, split, experiment, model });
          const res = await fetch(`${API_BASE}/api/view?${params.toString()}`);
          if (!res.ok) throw new Error(await res.text());
          const json = await res.json();
          return { key, dataset, split, experiment, model, rows: json.rows || [] };
        }));
        if (!cancelled) setPredState({ loading: false, error: null, views });
      } catch (error) {
        if (!cancelled) setPredState({ loading: false, error: String(error), views: [] });
      }
    }
    load();
    return () => { cancelled = true; };
  }, [selectedPredKeys]);

  const allCharRows = useMemo(() => charState.views.flatMap((view) => view.rows || []), [charState.views]);
  const charProblemBuckets = useMemo(() => collectCharacterizationProblemBuckets(allCharRows), [allCharRows]);
  const charProblemColorMap = useMemo(() => createProblemBucketColorMap(charProblemBuckets), [charProblemBuckets]);

  const allPredRows = useMemo(() => predState.views.flatMap((view) => view.rows || []), [predState.views]);
  const predProblemBuckets = useMemo(() => collectDiagnosisProblemBuckets(allPredRows), [allPredRows]);
  const predProblemColorMap = useMemo(() => createProblemBucketColorMap(predProblemBuckets), [predProblemBuckets]);

  const charSelectionItems = useMemo(() => {
    return selectedCharKeys.map((key) => {
      const [dataset, split, experiment] = key.split("|||");
      return { key, label: `${titleize(dataset)} · ${titleize(split)} · ${titleize(experiment)}` };
    });
  }, [selectedCharKeys]);

  const predSelectionItems = useMemo(() => {
    return selectedPredKeys.map((key) => {
      const [dataset, split, experiment, model] = key.split("|||");
      return { key, label: `${titleize(dataset)} · ${titleize(split)} · ${titleize(experiment)} · ${model}` };
    });
  }, [selectedPredKeys]);

  const charBySlice = useMemo(() => {
    return charState.views.map((view) => ({
      key: view.key,
      label: `${titleize(view.dataset)} · ${titleize(view.split)} · ${titleize(view.experiment)}`,
      dataset: view.dataset,
      split: view.split,
      experiment: view.experiment,
      ...buildCrossBenchmarkCharStats(view.rows || [], charProblemBuckets, charProblemColorMap),
    }));
  }, [charState.views, charProblemBuckets, charProblemColorMap]);

  const charByDataset = useMemo(() => {
    const grouped = new Map();
    charState.views.forEach((view) => {
      const current = grouped.get(view.dataset) || { dataset: view.dataset, rows: [] };
      current.rows.push(...(view.rows || []));
      grouped.set(view.dataset, current);
    });
    return Array.from(grouped.values()).map((item) => ({
      key: item.dataset,
      label: titleize(item.dataset),
      dataset: item.dataset,
      ...buildCrossBenchmarkCharStats(item.rows, charProblemBuckets, charProblemColorMap),
    }));
  }, [charState.views, charProblemBuckets, charProblemColorMap]);

  const predBySelection = useMemo(() => {
    return predState.views.map((view) => ({
      key: view.key,
      label: `${titleize(view.dataset)} · ${titleize(view.split)} · ${titleize(view.experiment)} · ${view.model}`,
      dataset: view.dataset,
      split: view.split,
      experiment: view.experiment,
      model: view.model,
      ...buildCrossBenchmarkPredictionStats(view.rows || [], view.model, predProblemBuckets, predProblemColorMap),
    }));
  }, [predState.views, predProblemBuckets, predProblemColorMap]);

  const predByDatasetSplit = useMemo(() => {
    const grouped = new Map();
    predBySelection.forEach((item) => {
      const key = `${item.dataset}|||${item.split}`;
      const current = grouped.get(key) || { key, dataset: item.dataset, split: item.split, items: [] };
      current.items.push(item);
      grouped.set(key, current);
    });
    return Array.from(grouped.values()).map((group) => ({
      ...group,
      items: group.items.sort((a, b) => `${a.experiment}|||${a.model}`.localeCompare(`${b.experiment}|||${b.model}`)),
    }));
  }, [predBySelection]);

  const predByModel = useMemo(() => {
    const grouped = new Map();
    predState.views.forEach((view) => {
      const current = grouped.get(view.model) || { model: view.model, rows: [] };
      current.rows.push(...(view.rows || []));
      grouped.set(view.model, current);
    });
    return Array.from(grouped.values()).map((item) => ({
      model: item.model,
      ...buildCrossBenchmarkPredictionStats(item.rows, item.model, predProblemBuckets, predProblemColorMap),
    }));
  }, [predState.views, predProblemBuckets, predProblemColorMap]);

  const benchmarkLegend = [
    { key: "ambiguous", label: "Ambiguous", color: GROUP_COLORS.ambiguous.text },
    { key: "missing", label: "Missing", color: GROUP_COLORS.missing.text },
    { key: "inaccurate", label: "Inaccurate", color: GROUP_COLORS.inaccurate.text },
    { key: "none", label: "None", color: "#94a3b8" },
  ];

  const charProblemLegend = useMemo(() => {
    return charProblemBuckets
      .filter((bucket) => !["no_problem", "benchmark_issue_other"].includes(bucket))
      .map((bucket) => ({
        key: bucket,
        label: getProblemBucketLabel(bucket),
        color: getProblemBucketColor(bucket, charProblemColorMap),
      }));
  }, [charProblemBuckets, charProblemColorMap]);

  const predProblemLegend = useMemo(() => {
    return predProblemBuckets.map((bucket) => ({
      key: bucket,
      label: getProblemBucketLabel(bucket),
      color: getProblemBucketColor(bucket, predProblemColorMap),
    }));
  }, [predProblemBuckets, predProblemColorMap]);

  return (
    <div style={{ display: "grid", gap: 16 }}>
      <SectionTitle
        title="Cross-Benchmark Analysis"
        subtitle="Select multiple benchmark slices to compare characterization patterns or prediction behavior across datasets, splits, experiments, and models."
        right={
          <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
            <FilterPill active={mode === "characterization"} onClick={() => setMode("characterization")}>
              Characterization
            </FilterPill>
            <FilterPill active={mode === "prediction"} onClick={() => setMode("prediction")}>
              Prediction
            </FilterPill>
          </div>
        }
      />

      {mode === "characterization" ? (
        <div style={{ display: "grid", gap: 16 }}>
          <div style={{ ...cardStyle, padding: 16 }}>
            <div style={{ fontWeight: 600, marginBottom: 12 }}>Characterization selection</div>
            <div style={{ display: "grid", gridTemplateColumns: "repeat(3, minmax(0, 1fr)) auto", gap: 12, alignItems: "end" }}>
              <Selector label="Dataset" value={charPicker.dataset} onChange={(v) => updateCharPicker("dataset", v)} options={datasets.map((d) => d.name)} />
              <Selector label="Split" value={charPicker.split} onChange={(v) => updateCharPicker("split", v)} options={charSplitOptions} />
              <Selector label="Experiment" value={charPicker.experiment} onChange={(v) => updateCharPicker("experiment", v)} options={charExperimentOptions} />
              <button type="button" onClick={addCharSelection} style={{ height: FILTER_CONTROL_HEIGHT, borderRadius: 12, border: "1px solid #2563eb", background: "#2563eb", color: "white", fontWeight: 600, padding: "0 16px", cursor: "pointer" }}>Add</button>
            </div>
            <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginTop: 14 }}>
              {charSelectionItems.length ? charSelectionItems.map((item) => (
                <button key={item.key} type="button" onClick={() => setSelectedCharKeys((prev) => prev.filter((x) => x !== item.key))} style={{ border: "1px solid #d1d5db", background: "white", borderRadius: 999, padding: "7px 12px", cursor: "pointer" }}>
                  {item.label} ×
                </button>
              )) : <div style={{ color: "#6b7280" }}>(no dataset+split+experiment selections yet)</div>}
            </div>
          </div>

          {charState.loading ? <div>Loading cross-benchmark characterization…</div> : null}
          {charState.error ? <div style={{ color: "#b91c1c" }}>{charState.error}</div> : null}

          {charBySlice.length ? (
            <>
              <div style={{ display: "grid", gridTemplateColumns: "repeat(3, minmax(0, 1fr))", gap: 14 }}>
                {metricCard("Selected views", charBySlice.length)}
                {metricCard("Selected datasets", charByDataset.length)}
                {metricCard("Total rows", charBySlice.reduce((sum, item) => sum + item.total, 0))}
              </div>

              <div style={{ ...cardStyle, padding: 16 }}>
                <div style={{ display: "flex", justifyContent: "space-between", gap: 12, marginBottom: 12, flexWrap: "wrap" }}>
                  <div>
                    <div style={{ fontWeight: 600 }}>Benchmark error composition by selected benchmark slice</div>
                    <div style={{ color: "#6b7280", marginTop: 4 }}>Each bar shows ambiguous, missing, inaccurate, and no-issue percentages for one selected dataset/split/experiment slice.</div>
                  </div>
                  <CrossBenchmarkLegend items={benchmarkLegend} />
                </div>
                <div style={{ display: "grid", gap: 12 }}>
                  {charBySlice.map((item) => (
                    <div key={item.key} style={{ display: "grid", gridTemplateColumns: "280px minmax(0, 1fr) 140px", gap: 12, alignItems: "center" }}>
                      <div>
                        <div style={{ fontWeight: 600 }}>{item.label}</div>
                        <div style={{ fontSize: 12, color: "#6b7280" }}>n={item.total}</div>
                      </div>
                      <CrossBenchmarkStackedBar
                        total={item.total}
                        segments={[
                          { key: "ambiguous", label: "Ambiguous", value: item.ambiguous, color: GROUP_COLORS.ambiguous.text },
                          { key: "missing", label: "Missing", value: item.missing, color: GROUP_COLORS.missing.text },
                          { key: "inaccurate", label: "Inaccurate", value: item.inaccurate, color: GROUP_COLORS.inaccurate.text },
                          { key: "none", label: "None", value: item.none, color: "#94a3b8" },
                        ]}
                      />
                      <div style={{ fontSize: 12, color: "#4b5563", textAlign: "right" }}>
                        Error rate {formatPercent(item.total - item.none, item.total)}
                      </div>
                    </div>
                  ))}
                </div>
              </div>

              <div style={{ ...cardStyle, padding: 16 }}>
                <div style={{ display: "flex", justifyContent: "space-between", gap: 12, marginBottom: 12, flexWrap: "wrap" }}>
                  <div>
                    <div style={{ fontWeight: 600 }}>Fine-grained problem types aligned with each coarse group</div>
                    <div style={{ color: "#6b7280", marginTop: 4 }}>For each selected slice, the fine-grained bars break down the rows inside ambiguous, missing, and inaccurate. The none column stays gray.</div>
                  </div>
                  <CrossBenchmarkLegend items={charProblemLegend} />
                </div>
                <div style={{ display: "grid", gap: 16 }}>
                  {charBySlice.map((item) => (
                    <div key={`${item.key}-fine`} style={{ border: "1px solid #e5e7eb", borderRadius: 14, padding: 14 }}>
                      <div style={{ fontWeight: 600, marginBottom: 12 }}>{item.label}</div>
                      <div style={{ display: "grid", gridTemplateColumns: "repeat(4, minmax(0, 1fr))", gap: 12 }}>
                        {item.fineGrainedByGroup.map((groupInfo) => (
                          <div key={groupInfo.key} style={{ display: "grid", gap: 8 }}>
                            <div style={{ fontSize: 12, fontWeight: 600, color: GROUP_COLORS[groupInfo.key]?.text || "#374151" }}>
                              {groupInfo.label}
                            </div>
                            <CrossBenchmarkStackedBar total={groupInfo.total || 1} segments={groupInfo.segments} />
                            <div style={{ fontSize: 12, color: "#6b7280" }}>{groupInfo.total ? `n=${groupInfo.total}` : "n=0"}</div>
                          </div>
                        ))}
                        <div style={{ display: "grid", gap: 8 }}>
                          <div style={{ fontSize: 12, fontWeight: 600, color: "#475569" }}>None</div>
                          <CrossBenchmarkStackedBar total={item.total || 1} segments={[{ key: "none", label: "None", value: item.none, color: "#94a3b8" }]} />
                          <div style={{ fontSize: 12, color: "#6b7280" }}>{item.total ? `n=${item.none}` : "n=0"}</div>
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              </div>

              <div style={{ ...cardStyle, padding: 16 }}>
                <div style={{ fontWeight: 600, marginBottom: 12 }}>Aggregated dataset view</div>
                <div style={{ display: "grid", gap: 14 }}>
                  {charByDataset.map((item) => (
                    <div key={item.key} style={{ border: "1px solid #e5e7eb", borderRadius: 14, padding: 14 }}>
                      <div style={{ display: "flex", justifyContent: "space-between", gap: 12, marginBottom: 10 }}>
                        <div style={{ fontWeight: 600 }}>{item.label}</div>
                        <div style={{ fontSize: 12, color: "#6b7280" }}>aggregated across selected split + experiment views · n={item.total}</div>
                      </div>
                      <CrossBenchmarkStackedBar
                        total={item.total}
                        height={22}
                        segments={[
                          { key: "ambiguous", label: "Ambiguous", value: item.ambiguous, color: GROUP_COLORS.ambiguous.text },
                          { key: "missing", label: "Missing", value: item.missing, color: GROUP_COLORS.missing.text },
                          { key: "inaccurate", label: "Inaccurate", value: item.inaccurate, color: GROUP_COLORS.inaccurate.text },
                          { key: "none", label: "None", value: item.none, color: "#94a3b8" },
                        ]}
                      />
                      <div style={{ display: "grid", gridTemplateColumns: "repeat(4, minmax(0, 1fr))", gap: 10, marginTop: 12, fontSize: 12, color: "#4b5563" }}>
                        <div>Ambiguous: {formatCountPercent(item.ambiguous, item.total)}</div>
                        <div>Missing: {formatCountPercent(item.missing, item.total)}</div>
                        <div>Inaccurate: {formatCountPercent(item.inaccurate, item.total)}</div>
                        <div>None: {formatCountPercent(item.none, item.total)}</div>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </>
          ) : null}
        </div>
      ) : (
        <div style={{ display: "grid", gap: 16 }}>
          <div style={{ ...cardStyle, padding: 16 }}>
            <div style={{ fontWeight: 600, marginBottom: 12 }}>Prediction selection</div>
            <div style={{ display: "grid", gridTemplateColumns: "repeat(4, minmax(0, 1fr)) auto", gap: 12, alignItems: "end" }}>
              <Selector label="Dataset" value={predPicker.dataset} onChange={(v) => updatePredPicker("dataset", v)} options={datasets.map((d) => d.name)} />
              <Selector label="Split" value={predPicker.split} onChange={(v) => updatePredPicker("split", v)} options={predSplitOptions} />
              <Selector label="Experiment" value={predPicker.experiment} onChange={(v) => updatePredPicker("experiment", v)} options={predExperimentOptions} />
              <Selector label="Model" value={predPicker.model} onChange={(v) => updatePredPicker("model", v)} options={predModelOptions} />
              <button type="button" onClick={addPredSelection} style={{ height: FILTER_CONTROL_HEIGHT, borderRadius: 12, border: "1px solid #2563eb", background: "#2563eb", color: "white", fontWeight: 600, padding: "0 16px", cursor: "pointer" }}>Add</button>
            </div>
            <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginTop: 14 }}>
              {predSelectionItems.length ? predSelectionItems.map((item) => (
                <button key={item.key} type="button" onClick={() => setSelectedPredKeys((prev) => prev.filter((x) => x !== item.key))} style={{ border: "1px solid #d1d5db", background: "white", borderRadius: 999, padding: "7px 12px", cursor: "pointer" }}>
                  {item.label} ×
                </button>
              )) : <div style={{ color: "#6b7280" }}>(no dataset+split+experiment+model selections yet)</div>}
            </div>
          </div>

          {predState.loading ? <div>Loading cross-benchmark prediction analysis…</div> : null}
          {predState.error ? <div style={{ color: "#b91c1c" }}>{predState.error}</div> : null}

          {predBySelection.length ? (
            <>
              <div style={{ display: "grid", gridTemplateColumns: "repeat(4, minmax(0, 1fr))", gap: 14 }}>
                {metricCard("Selected views", predBySelection.length)}
                {metricCard("Dataset/split groups", predByDatasetSplit.length)}
                {metricCard("Unique models", predByModel.length)}
                {metricCard("Total rows", predBySelection.reduce((sum, item) => sum + item.total, 0))}
              </div>

              <div style={{ ...cardStyle, padding: 16 }}>
                <div style={{ fontWeight: 600, marginBottom: 12 }}>Execution match by dataset/split, aligned by experiment and model</div>
                <div style={{ display: "grid", gap: 14 }}>
                  {predByDatasetSplit.map((group) => (
                    <div key={group.key} style={{ border: "1px solid #e5e7eb", borderRadius: 14, padding: 14 }}>
                      <div style={{ fontWeight: 600, marginBottom: 10 }}>{titleize(group.dataset)} · {titleize(group.split)}</div>
                      <div style={{ display: "grid", gap: 12 }}>
                        {group.items.map((item) => (
                          <div key={item.key} style={{ display: "grid", gridTemplateColumns: "240px minmax(0, 1fr) 110px 110px", gap: 12, alignItems: "center" }}>
                            <div>
                              <div style={{ fontWeight: 600 }}>{titleize(item.experiment)} · {item.model}</div>
                              <div style={{ fontSize: 12, color: "#6b7280" }}>n={item.total}</div>
                            </div>
                            <div style={{ height: 12, borderRadius: 999, background: "#e5e7eb", overflow: "hidden" }}>
                              <div style={{ width: `${item.total ? (100 * item.execMatches) / item.total : 0}%`, height: "100%", background: "#2563eb", borderRadius: 999 }} />
                            </div>
                            <div style={{ fontSize: 12, color: "#4b5563" }}>Exec {formatPercent(item.execMatches, item.total)}</div>
                            <div style={{ fontSize: 12, color: "#4b5563" }}>Flagged {formatPercent(item.flagged, item.total)}</div>
                          </div>
                        ))}
                      </div>
                    </div>
                  ))}
                </div>
              </div>

              <div style={{ display: "grid", gridTemplateColumns: "repeat(2, minmax(0, 1fr))", gap: 14 }}>
                <div style={{ display: "grid", gap: 14 }}>
                  <div style={{ ...cardStyle, padding: 16 }}>
                    <div style={{ fontWeight: 600, marginBottom: 12 }}>Model-level summary across selected benchmarks</div>
                    <div style={{ display: "grid", gap: 12 }}>
                      {predByModel.map((item) => (
                        <div key={item.model} style={{ border: "1px solid #e5e7eb", borderRadius: 12, padding: 12 }}>
                          <div style={{ display: "flex", justifyContent: "space-between", gap: 12, marginBottom: 8 }}>
                            <div style={{ fontWeight: 600 }}>{item.model}</div>
                            <div style={{ fontSize: 12, color: "#6b7280" }}>n={item.total}</div>
                          </div>
                          <div style={{ display: "grid", gap: 8 }}>
                            <CrossBenchmarkMiniMetricRow label="Execution match" value={item.execMatches} total={item.total} color="#2563eb" />
                            <CrossBenchmarkMiniMetricRow label="Flagged" value={item.flagged} total={item.total} color="#7c3aed" />
                            <CrossBenchmarkMiniMetricRow label="Flagged + match" value={item.flaggedMatches} total={item.total} color="#10b981" />
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>

                  <CrossBenchmarkProblemPanel
                    title="Problem-type summary by model"
                    subtitle="Each bar spans the full evaluation set for that model. Gray background indicates rows outside the selected subset."
                    total={1}
                    legendItems={predProblemLegend}
                    sections={[]}
                  />
                </div>

                <div style={{ display: "grid", gap: 14 }}>
                  {predByModel.map((item) => (
                    <CrossBenchmarkProblemPanel
                      key={`model-problems-${item.model}`}
                      title={`${item.model} · problem-type breakdown`}
                      subtitle={`n=${item.total}`}
                      total={item.total || 1}
                      legendItems={predProblemLegend}
                      sections={[
                        {
                          key: "all",
                          label: "Problem types",
                          caption: formatCountPercent(item.total, item.total),
                          segments: item.problemSegments,
                        },
                        {
                          key: "mismatch",
                          label: "Problem types + mismatch",
                          caption: formatCountPercent(item.total - item.execMatches, item.total),
                          segments: item.mismatchProblemSegments,
                        },
                        {
                          key: "flagged_match",
                          label: "Problem types + flagged + match",
                          caption: formatCountPercent(item.flaggedMatches, item.total),
                          segments: item.flaggedMatchProblemSegments,
                        },
                      ]}
                    />
                  ))}
                </div>
              </div>

              <div style={{ ...cardStyle, padding: 16 }}>
                <div style={{ fontWeight: 600, marginBottom: 12 }}>Selected benchmark slices</div>
                <div style={{ display: "grid", gap: 12, maxHeight: 900, overflow: "auto" }}>
                  {predBySelection.map((item) => (
                    <div key={item.key} style={{ border: "1px solid #e5e7eb", borderRadius: 12, padding: 12 }}>
                      <div style={{ display: "flex", justifyContent: "space-between", gap: 12, marginBottom: 8 }}>
                        <div style={{ fontWeight: 600 }}>{item.label}</div>
                        <div style={{ fontSize: 12, color: "#6b7280" }}>n={item.total}</div>
                      </div>
                      <div style={{ display: "grid", gridTemplateColumns: "repeat(4, minmax(0, 1fr))", gap: 10, fontSize: 12, color: "#4b5563", marginBottom: 12 }}>
                        <div>Execution match: {formatCountPercent(item.execMatches, item.total)}</div>
                        <div>Flagged: {formatCountPercent(item.flagged, item.total)}</div>
                        <div>Benchmark error: {formatCountPercent(item.benchmarkIssues, item.total)}</div>
                        <div>Flagged + match: {formatCountPercent(item.flaggedMatches, item.total)}</div>
                      </div>

                      <div style={{ display: "grid", gap: 12 }}>
                        <CrossBenchmarkProblemPanel
                          title="Execution match and match shape"
                          subtitle="The second bar only fills the execution-match portion; mismatches remain gray."
                          total={item.total || 1}
                          legendItems={[
                            { key: "match", label: "Execution match", color: "#2563eb" },
                            { key: "empty", label: "Empty result", color: "#7c3aed" },
                            { key: "single", label: "Single-row result", color: "#06b6d4" },
                            { key: "multi", label: "Multi-row result", color: "#10b981" },
                          ]}
                          sections={[
                            {
                              key: "exec",
                              label: "Execution match",
                              caption: formatCountPercent(item.execMatches, item.total),
                              segments: [{ key: "match", label: "Execution match", value: item.execMatches, color: "#2563eb" }],
                            },
                            {
                              key: "shape",
                              label: "Match type",
                              caption: formatCountPercent(item.emptyMatches + item.singleMatches + item.multiMatches, item.total),
                              segments: [
                                { key: "empty", label: "Empty result", value: item.emptyMatches, color: "#7c3aed" },
                                { key: "single", label: "Single-row result", value: item.singleMatches, color: "#06b6d4" },
                                { key: "multi", label: "Multi-row result", value: item.multiMatches, color: "#10b981" },
                              ],
                            },
                          ]}
                        />

                        <CrossBenchmarkProblemPanel
                          title="Problem-type slices"
                          subtitle="Includes no-problem rows and keeps the full-set baseline so the gray remainder shows rows outside each subset."
                          total={item.total || 1}
                          legendItems={predProblemLegend}
                          sections={[
                            {
                              key: "all",
                              label: "Problem types",
                              caption: formatCountPercent(item.total, item.total),
                              segments: item.problemSegments,
                            },
                            {
                              key: "mismatch",
                              label: "Problem types + mismatch",
                              caption: formatCountPercent(item.total - item.execMatches, item.total),
                              segments: item.mismatchProblemSegments,
                            },
                            {
                              key: "flagged_match",
                              label: "Problem types + flagged + match",
                              caption: formatCountPercent(item.flaggedMatches, item.total),
                              segments: item.flaggedMatchProblemSegments,
                            },
                          ]}
                        />
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </>
          ) : null}
        </div>
      )}
    </div>
  );
}

function SliceBuilderFilterPanel({ filter, setFilter, rows, model, onApplyToExplorer }) {
  const charOptions = useMemo(() => collectCharFields(rows), [rows]);
  const diagnosisOptions = useMemo(() => collectDiagnosisFields(rows, model).filter((field) => field !== "execution_match_assessment"), [rows, model]);

  function update(patch) {
    setFilter((prev) => ({ ...prev, ...patch }));
  }

  return (
    <div style={{ ...cardStyle, padding: 16 }}>
      <div style={{ fontWeight: 600, marginBottom: 12 }}>Slice filters</div>
      <div style={{ display: "grid", gap: 14 }}>
        <div style={{ display: "grid", gridTemplateColumns: "minmax(260px, 1.4fr) repeat(4, minmax(180px, 1fr))", gap: 12, alignItems: "end" }}>
          <label style={{ display: "grid", gap: 6 }}>
            <span style={{ fontSize: 12, color: "#6b7280", textTransform: "uppercase" }}>Search</span>
            <input
              value={filter.search || ""}
              onChange={(e) => update({ search: e.target.value })}
              placeholder="Question, db, SQL, prediction…"
              style={{ ...filterControlShellStyle, padding: "0 12px", borderRadius: 12, outline: "none" }}
            />
          </label>
          <Selector label="Benchmark issue" value={filter.benchmarkIssueStatus || "all"} onChange={(v) => update({ benchmarkIssueStatus: v })} options={["all", "has_issue", "no_issue"]} />
          <Selector label="Flag status" value={filter.flaggedStatus || "all"} onChange={(v) => update({ flaggedStatus: v })} options={["all", "flagged", "unflagged"]} />
          <Selector label="Execution match" value={filter.executionMatchStatus || "all"} onChange={(v) => update({ executionMatchStatus: v })} options={["all", "match", "non-match"]} />
          <Selector label="Match type" value={filter.resultShapeCategory || "all"} onChange={(v) => update({ resultShapeCategory: v })} options={["all", "empty", "single", "multi", "unknown"]} />
        </div>

        <div style={{ display: "grid", gridTemplateColumns: "minmax(320px, 1.2fr) minmax(280px, 1fr) minmax(280px, 1fr)", gap: 12, alignItems: "start" }}>
          <HierarchicalCharFilter selected={filter.charFields || []} onChange={(values) => update({ charFields: values })} charOptions={charOptions} />
          <MultiSelectFilter label="Diagnosis labels" selected={filter.diagnosisFields || []} onChange={(values) => update({ diagnosisFields: values })} options={diagnosisOptions} />
          <div style={{ display: "grid", gap: 12 }}>
            <FilterPanel label="Coarse benchmark error" summary={formatSelectionSummary((filter.coarseGroups || []).map((group) => titleize(group)), "All coarse benchmark errors")}>
              <div style={{ display: "grid", gap: 8, maxHeight: 360, overflow: "auto" }}>
                {CHARACTERIZATION_GROUPS.map((group) => {
                  const checked = (filter.coarseGroups || []).includes(group);
                  const accent = GROUP_COLORS[group];
                  return (
                    <label
                      key={group}
                      style={{
                        display: "flex",
                        alignItems: "center",
                        gap: 10,
                        padding: "8px 10px",
                        borderRadius: 10,
                        border: `1px solid ${checked ? accent.border : "#e5e7eb"}`,
                        background: checked ? accent.bg : "white",
                        cursor: "pointer",
                      }}
                    >
                      <input
                        type="checkbox"
                        checked={checked}
                        onChange={() => update({ coarseGroups: checked ? (filter.coarseGroups || []).filter((x) => x !== group) : [...(filter.coarseGroups || []), group] })}
                      />
                      <span style={{ fontWeight: 500 }}>{titleize(group)}</span>
                    </label>
                  );
                })}
              </div>
            </FilterPanel>

            <div style={{ display: "flex", gap: 8, flexWrap: "wrap", justifyContent: "flex-end" }}>
              <button type="button" onClick={() => setFilter({ search: "", benchmarkIssueStatus: "all", coarseGroups: [], charFields: [], diagnosisFields: [], flaggedStatus: "all", executionMatchStatus: "all", resultShapeCategory: "all" })} style={{ border: "1px solid #d1d5db", background: "white", borderRadius: 10, padding: "8px 10px", cursor: "pointer", fontWeight: 600 }}>
                Reset filters
              </button>
              <button type="button" onClick={onApplyToExplorer} style={{ border: "1px solid #2563eb", background: "#eff6ff", color: "#1d4ed8", borderRadius: 10, padding: "8px 10px", cursor: "pointer", fontWeight: 600 }}>
                Open in instance explorer
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

function SliceBuilderNode({ title, rows, totalRows, levelConfig = [], levelIndex = 0, model }) {
  if (!levelConfig[levelIndex]) return null;
  const dimension = levelConfig[levelIndex];
  const levelPalette = [
    { fill: "#2563eb", bg: "#eff6ff", border: "#93c5fd", text: "#1d4ed8" },
    { fill: "#7c3aed", bg: "#f5f3ff", border: "#c4b5fd", text: "#6d28d9" },
    { fill: "#0f766e", bg: "#f0fdfa", border: "#99f6e4", text: "#115e59" },
    { fill: "#ea580c", bg: "#fff7ed", border: "#fdba74", text: "#c2410c" },
    { fill: "#dc2626", bg: "#fef2f2", border: "#fca5a5", text: "#b91c1c" },
    { fill: "#4f46e5", bg: "#eef2ff", border: "#a5b4fc", text: "#4338ca" },
  ];
  const accent = levelPalette[levelIndex % levelPalette.length];
  const items = dimension.values.map((value) => {
    const subset = rows.filter((row) => dimension.match(row, model).includes(value));
    return { value, label: dimension.labels?.[value] || titleize(value), subset };
  }).filter((item) => item.subset.length > 0);

  if (!items.length) return null;

  return (
    <div style={{ display: "grid", gap: 12 }}>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 12, flexWrap: "wrap", alignItems: "center" }}>
        <div style={{ fontWeight: 700, color: accent.text }}>{title}</div>
        <div
          style={{
            fontSize: 12,
            color: accent.text,
            background: accent.bg,
            border: `1px solid ${accent.border}`,
            borderRadius: 999,
            padding: "6px 10px",
            fontWeight: 600,
          }}
        >
          Level {levelIndex + 1} · {titleize(dimension.label)} split · n={rows.length}
        </div>
      </div>
      <div style={{ display: "grid", gap: 10 }}>
        {items.map((item) => (
          <div
            key={`${dimension.key}-${item.value}`}
            style={{
              border: `1px solid ${accent.border}`,
              borderRadius: 12,
              padding: 12,
              background: accent.bg,
            }}
          >
            <div style={{ display: "grid", gap: 8 }}>
              <div style={{ display: "flex", justifyContent: "space-between", gap: 12, fontSize: 13 }}>
                <span style={{ fontWeight: 600, color: accent.text }}>{item.label}</span>
                <span style={{ color: "#6b7280" }}>{item.subset.length}/{rows.length} in parent · {formatPercent(item.subset.length, rows.length, 1)}</span>
              </div>
              <div style={{ height: 10, borderRadius: 999, background: "#e5e7eb", overflow: "hidden" }}>
                <div style={{ width: `${(100 * item.subset.length) / Math.max(1, rows.length)}%`, height: "100%", background: accent.fill, borderRadius: 999 }} />
              </div>
              <div style={{ fontSize: 12, color: "#6b7280" }}>Share of full filtered set: {formatPercent(item.subset.length, totalRows, 1)}</div>
            </div>
            {levelConfig[levelIndex + 1] ? (
              <div style={{ marginTop: 12, paddingLeft: 14, borderLeft: `3px solid ${accent.border}` }}>
                <SliceBuilderNode title={item.label} rows={item.subset} totalRows={totalRows} levelConfig={levelConfig} levelIndex={levelIndex + 1} model={model} />
              </div>
            ) : null}
          </div>
        ))}
      </div>
    </div>
  );
}

function SliceBuilderTab({ rows, model, instanceFilter, setInstanceFilter, setTab, setSelectedInstanceId }) {
  const [builderFilter, setBuilderFilter] = useState(instanceFilter);
  const [levelSelections, setLevelSelections] = useState(["coarse_benchmark_error", "diagnosis_type", "execution_match", "", "", ""]);

  useEffect(() => {
    setBuilderFilter(instanceFilter);
  }, [instanceFilter, model, rows.length]);

  const filteredRows = useMemo(() => filterRowsByPayload(rows, builderFilter, model), [rows, builderFilter, model]);

  const diagnosisOptions = useMemo(() => collectDiagnosisFields(rows, model).filter((field) => field !== "execution_match_assessment"), [rows, model]);
  const charBases = useMemo(() => Array.from(new Set(collectCharFields(rows).map((field) => splitCharField(field).base).filter((base) => base !== "nl2sql_not_possible"))), [rows]);

  const diagnosisLegend = useMemo(() => [
    { key: "none", label: "No problem", color: "#94a3b8" },
    ...diagnosisOptions.map((field, index) => ({ key: field, label: titleize(field), color: ["#2563eb", "#7c3aed", "#06b6d4", "#10b981", "#f59e0b", "#ef4444", "#8b5cf6", "#14b8a6"][index % 8] })),
  ], [diagnosisOptions]);
  const charLegend = useMemo(() => [
    { key: "none", label: "No fine-grained problem", color: "#94a3b8" },
    ...charBases.map((base, index) => ({ key: base, label: titleize(base), color: ["#7c3aed", "#f59e0b", "#ef4444", "#2563eb", "#06b6d4", "#10b981", "#8b5cf6", "#14b8a6"][index % 8] })),
  ], [charBases]);

  const diagnosisSegments = useMemo(() => {
    const totals = new Map();
    filteredRows.forEach((row) => {
      getDiagnosisProblemTypesForRow(row, model).forEach((field) => totals.set(field, (totals.get(field) || 0) + 1));
    });
    return diagnosisLegend.map((item) => ({ ...item, value: totals.get(item.key) || 0 })).filter((item) => item.value > 0);
  }, [filteredRows, diagnosisLegend, model]);

  const charSegments = useMemo(() => {
    const totals = new Map();
    filteredRows.forEach((row) => {
      getActiveCharacterizationBases(row).forEach((base) => totals.set(base, (totals.get(base) || 0) + 1));
    });
    return charLegend.map((item) => ({ ...item, value: totals.get(item.key) || 0 })).filter((item) => item.value > 0);
  }, [filteredRows, charLegend]);

  const summary = useMemo(() => {
    const total = filteredRows.length;
    let execMatches = 0;
    let flagged = 0;
    let benchmarkIssues = 0;
    let empty = 0;
    let single = 0;
    let multi = 0;
    filteredRows.forEach((row) => {
      const execMatch = row?.sql_eval_summary?.[model]?.execution_match === 1 || row?.sql_eval_summary?.[model]?.execution_match === true;
      if (execMatch) execMatches += 1;
      if (inferFlaggedValue(row?.diagnoses?.[model]?.execution_match_assessment?.classification)) flagged += 1;
      if (hasAnyBenchmarkIssue(row)) benchmarkIssues += 1;
      if (execMatch) {
        const shape = getExecutionShapeCategoryForRow(row, model);
        if (shape === "empty") empty += 1;
        else if (shape === "single") single += 1;
        else if (shape === "multi") multi += 1;
      }
    });
    return { total, execMatches, flagged, benchmarkIssues, empty, single, multi };
  }, [filteredRows, model]);

  const levelDefinitions = useMemo(() => ({
    coarse_benchmark_error: {
      key: "coarse_benchmark_error",
      label: "benchmark error",
      values: ["ambiguous", "missing", "inaccurate", "none"],
      labels: { ambiguous: "Ambiguous", missing: "Missing", inaccurate: "Inaccurate", none: "None" },
      match: (row) => {
        const values = CHARACTERIZATION_GROUPS.filter((group) => hasCoarseBenchmarkIssue(row, group));
        return values.length ? values : ["none"];
      },
    },
    characterization_type: {
      key: "characterization_type",
      label: "benchmark problem type",
      values: ["none", ...charBases],
      labels: { none: "None" },
      match: (row) => getActiveCharacterizationBases(row),
    },
    flag_group: {
      key: "flag_group",
      label: "flag status",
      values: ["flagged", "unflagged"],
      labels: { flagged: "Flagged", unflagged: "Unflagged" },
      match: (row, currentModel) => [inferFlaggedValue(row?.diagnoses?.[currentModel]?.execution_match_assessment?.classification) ? "flagged" : "unflagged"],
    },
    execution_match: {
      key: "execution_match",
      label: "execution match",
      values: ["match", "mismatch"],
      labels: { match: "Match", mismatch: "Mismatch" },
      match: (row, currentModel) => [row?.sql_eval_summary?.[currentModel]?.execution_match === 1 || row?.sql_eval_summary?.[currentModel]?.execution_match === true ? "match" : "mismatch"],
    },
    match_type: {
      key: "match_type",
      label: "match type",
      values: ["empty", "single", "multi", "none"],
      labels: { empty: "Empty", single: "Single-row", multi: "Multi-row", none: "None" },
      match: (row, currentModel) => {
        const execMatch = row?.sql_eval_summary?.[currentModel]?.execution_match === 1 || row?.sql_eval_summary?.[currentModel]?.execution_match === true;
        if (!execMatch) return ["none"];
        const category = getExecutionShapeCategoryForRow(row, currentModel);
        if (category === "empty" || category === "single" || category === "multi") return [category];
        return ["none"];
      },
    },
    diagnosis_type: {
      key: "diagnosis_type",
      label: "prediction diagnosis type",
      values: ["none", ...diagnosisOptions],
      labels: { none: "None" },
      match: (row, currentModel) => getDiagnosisProblemTypesForRow(row, currentModel),
    },
  }), [charBases, diagnosisOptions]);

  const levelOptions = [
    { key: "coarse_benchmark_error", label: "Benchmark error (coarse)" },
    { key: "characterization_type", label: "Benchmark problem type" },
    { key: "flag_group", label: "Flag status" },
    { key: "execution_match", label: "Execution match" },
    { key: "match_type", label: "Match type" },
    { key: "diagnosis_type", label: "Prediction diagnosis type" },
  ];

  const activeLevelKeys = levelSelections.filter(Boolean);
  const activeLevelDefs = activeLevelKeys.map((key) => levelDefinitions[key]).filter(Boolean);

  function updateLevel(index, value) {
    setLevelSelections((prev) => {
      const next = [...prev];
      next[index] = value;
      for (let i = index + 1; i < next.length; i += 1) {
        if (!next[index] && next[i]) next[i] = "";
      }
      for (let i = 1; i < next.length; i += 1) {
        if (!next[i - 1]) next[i] = "";
      }
      return next;
    });
  }

  function applyToExplorer() {
    setInstanceFilter(builderFilter);
    setTab("explorer");
    if (filteredRows.length) setSelectedInstanceId(filteredRows[0].id);
    if (typeof window !== "undefined") {
      requestAnimationFrame(() => window.scrollTo({ top: 0, behavior: "smooth" }));
    }
  }

  return (
    <div style={{ display: "grid", gap: 16 }}>
      <SectionTitle
        title="Slice Builder"
        subtitle="Build a custom slice with all filtering controls, inspect its composition, and then progressively group it layer by layer."
      />

      <SliceBuilderFilterPanel filter={builderFilter} setFilter={setBuilderFilter} rows={rows} model={model} onApplyToExplorer={applyToExplorer} />

      <div style={{ display: "grid", gridTemplateColumns: "repeat(4, minmax(0, 1fr))", gap: 12 }}>
        {metricCard("Rows in slice", summary.total, `${rows.length} total rows in current view`)}
        {metricCard("Execution match", formatCountPercent(summary.execMatches, summary.total), `${summary.total ? formatPercent(summary.execMatches, summary.total, 1) : "0%"} of filtered rows`)}
        {metricCard("Flagged", formatCountPercent(summary.flagged, summary.total), `${summary.total ? formatPercent(summary.flagged, summary.total, 1) : "0%"} of filtered rows`)}
        {metricCard("Benchmark issues", formatCountPercent(summary.benchmarkIssues, summary.total), `${summary.total ? formatPercent(summary.benchmarkIssues, summary.total, 1) : "0%"} of filtered rows`)}
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "minmax(0, 1fr) minmax(0, 1fr)", gap: 14, alignItems: "start" }}>
        <CrossBenchmarkProblemPanel
          title="Slice overview"
          subtitle="High-level coverage of benchmark issues, flagged rows, execution matches, and match-result shapes."
          total={summary.total || 1}
          legendItems={[
            { key: "issues", label: "Benchmark issue", color: "#7c3aed" },
            { key: "flagged", label: "Flagged", color: "#ef4444" },
            { key: "match", label: "Execution match", color: "#2563eb" },
            { key: "empty", label: "Empty", color: "#8b5cf6" },
            { key: "single", label: "Single-row", color: "#06b6d4" },
            { key: "multi", label: "Multi-row", color: "#10b981" },
          ]}
          sections={[
            { key: "issue", label: "Benchmark issues", caption: formatCountPercent(summary.benchmarkIssues, summary.total), segments: [{ key: "issues", label: "Benchmark issue", value: summary.benchmarkIssues, color: "#7c3aed" }] },
            { key: "flagged", label: "Flagged rows", caption: formatCountPercent(summary.flagged, summary.total), segments: [{ key: "flagged", label: "Flagged", value: summary.flagged, color: "#ef4444" }] },
            { key: "match", label: "Execution match", caption: formatCountPercent(summary.execMatches, summary.total), segments: [{ key: "match", label: "Execution match", value: summary.execMatches, color: "#2563eb" }] },
            { key: "shape", label: "Matched result type", caption: formatCountPercent(summary.empty + summary.single + summary.multi, summary.total), segments: [{ key: "empty", label: "Empty", value: summary.empty, color: "#8b5cf6" }, { key: "single", label: "Single-row", value: summary.single, color: "#06b6d4" }, { key: "multi", label: "Multi-row", value: summary.multi, color: "#10b981" }] },
          ]}
        />

        <div style={{ display: "grid", gap: 14 }}>
          <CrossBenchmarkProblemPanel title="Benchmark characterization problem types" subtitle="Fine-grained benchmark problem types within the current custom slice." total={summary.total || 1} sections={[{ key: "char", label: "Characterization problem types", caption: `${charSegments.reduce((acc, item) => acc + item.value, 0)} tagged counts`, segments: charSegments }]} legendItems={charLegend} />
          <CrossBenchmarkProblemPanel title="Prediction diagnosis problem types" subtitle="Diagnosis problem types for the current model within the filtered slice." total={summary.total || 1} sections={[{ key: "diag", label: "Diagnosis problem types", caption: `${diagnosisSegments.reduce((acc, item) => acc + item.value, 0)} tagged counts`, segments: diagnosisSegments }]} legendItems={diagnosisLegend} />
        </div>
      </div>

      <div style={{ ...cardStyle, padding: 16, display: "grid", gap: 14 }}>
        <div>
          <div style={{ fontWeight: 600 }}>Hierarchical graph builder</div>
          <div style={{ color: "#6b7280", marginTop: 4 }}>Choose progressively deeper grouping levels. You can leave trailing levels blank, but intermediate blanks are automatically cleared.</div>
        </div>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(6, minmax(0, 1fr))", gap: 10 }}>
          {levelSelections.map((value, index) => {
            const priorFilled = index === 0 || Boolean(levelSelections[index - 1]);
            const available = levelOptions.filter((opt) => opt.key === value || !levelSelections.includes(opt.key));
            return (
              <Selector
                key={`level-${index}`}
                label={`Level ${index + 1}`}
                value={value}
                onChange={(nextValue) => updateLevel(index, nextValue)}
                options={["", ...available.map((opt) => opt.key)]}
              />
            );
          })}
        </div>
        <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
          {activeLevelKeys.map((key, idx) => (
            <span key={key} style={{ border: "1px solid #d1d5db", background: "#f8fafc", borderRadius: 999, padding: "6px 10px", fontSize: 12, color: "#4b5563" }}>
              Level {idx + 1}: {levelOptions.find((opt) => opt.key === key)?.label || titleize(key)}
            </span>
          ))}
        </div>
        {filteredRows.length && activeLevelDefs.length ? (
          <SliceBuilderNode title="Filtered slice" rows={filteredRows} totalRows={filteredRows.length} levelConfig={activeLevelDefs} levelIndex={0} model={model} />
        ) : (
          <div style={{ color: "#6b7280" }}>Add at least one grouping level and keep some rows in the current slice to render the hierarchy.</div>
        )}
      </div>
    </div>
  );
}

function ExplorerTab({ rows, selectedInstance, model, secondaryModel, setSelectedInstanceId, instanceFilter, setInstanceFilter }) {
  const [setTopNode, topHeight] = useElementHeight([
    selectedInstance?.id,
    model,
    secondaryModel,
    rows.length,
    instanceFilter.search,
    instanceFilter.coarseGroups,
    instanceFilter.charFields,
    instanceFilter.diagnosisFields,
    instanceFilter.flaggedStatus,
    instanceFilter.executionMatchStatus,
    instanceFilter.resultShapeCategory,
  ]);

  const comparisonModel = secondaryModel && secondaryModel !== model
    ? secondaryModel
    : rows.length
      ? Object.keys(rows[0]?.predictions || {}).find((name) => name && name !== model) || model
      : model;

  const primaryPanel = getExecutionPanelPayload(selectedInstance, model, "prediction");
  const secondaryPanel = getExecutionPanelPayload(selectedInstance, comparisonModel, "prediction");
  const goldPanel = getExecutionPanelPayload(selectedInstance, model, "gold");
  const { registerNode: registerComparisonNode, heights: comparisonHeights } = useSyncedBlockHeights([
    selectedInstance?.id,
    model,
    comparisonModel,
    primaryPanel?.sql,
    secondaryPanel?.sql,
    goldPanel?.sql,
    JSON.stringify(primaryPanel?.metadata || {}),
    JSON.stringify(secondaryPanel?.metadata || {}),
    JSON.stringify(goldPanel?.metadata || {}),
  ]);

  return (
    <div style={{ display: "grid", gap: 16 }}>
      <SectionTitle
        title="Instance Explorer"
        subtitle="Unified drill-down across question, schema, gold SQL, and side-by-side execution behavior for the gold query and both model predictions."
      />
      <FilterBar
        instanceFilter={instanceFilter}
        setInstanceFilter={setInstanceFilter}
        charOptions={collectCharFields(rows)}
        diagnosisOptions={collectDiagnosisFields(rows, model).filter((field) => field !== "execution_match_assessment")}
      />

      <div style={{ display: "grid", gap: 16 }}>
        <div style={{ display: "grid", gridTemplateColumns: "320px 1fr", gap: 16, alignItems: "start" }}>
          <InstanceList
            rows={rows}
            selectedId={selectedInstance.id}
            setSelectedId={setSelectedInstanceId}
            rightLabel={() => "instance"}
            height={topHeight}
          />

          <div style={{ display: "grid", gap: 14, minWidth: 0 }}>
            <div ref={setTopNode} style={{ display: "grid", gap: 14, minWidth: 0 }}>
              <InstanceHeader
                instance={selectedInstance}
                extra={`Model A: ${model}${comparisonModel ? ` · Model B: ${comparisonModel}` : ""}`}
              />
              <div style={{ display: "grid", gridTemplateColumns: "repeat(2, minmax(0, 1fr))", gap: 14 }}>
                <CodeBlock title="Gold SQL" value={formatSql(selectedInstance.gold_sql)} maxHeight={260} />
                <JsonBlock title="Schema" value={selectedInstance.schema_json} maxHeight={360} />
              </div>
            </div>
          </div>
        </div>

        <FullWidthAnnotationSection
          title="Execution and prediction comparison"
          subtitle="Gold, Model A, and Model B are aligned side by side to compare match status, flagging behavior, execution metadata, and result tables."
        >
          <div style={{ display: "grid", gridTemplateColumns: "repeat(3, minmax(0, 1fr))", gap: 14, alignItems: "stretch" }}>
            <ExecutionComparisonColumn
              panel={goldPanel}
              panelKey="gold"
              syncedHeights={comparisonHeights}
              registerSyncedNode={registerComparisonNode}
            />
            <ExecutionComparisonColumn
              panel={primaryPanel}
              panelKey="primary"
              syncedHeights={comparisonHeights}
              registerSyncedNode={registerComparisonNode}
            />
            <ExecutionComparisonColumn
              panel={secondaryPanel}
              panelKey="secondary"
              syncedHeights={comparisonHeights}
              registerSyncedNode={registerComparisonNode}
            />
          </div>
        </FullWidthAnnotationSection>
      </div>
    </div>
  );
}

function AgentsTab({ selection }) {
  const [form, setForm] = useState({ agent: "benchmark_characterization", commandPreview: "" });
  const [runResult, setRunResult] = useState(null);

  useEffect(() => {
    const agent = form.agent;
    const preview = [
      "python",
      agent === "benchmark_characterization" ? "agents/characterization/characterization.py" : agent === "prediction_diagnosis" ? "agents/prediction_diagnosis/run_prediction_diagnosis.py" : "agents/nl2sql_system/predict_query.py",
      `--dataset ${selection.dataset}`,
      `--split ${selection.split}`,
      `--experiment-name ${selection.experiment}`,
      selection.model ? `--solution-name ${selection.model}` : "",
    ].filter(Boolean).join(" ");
    setForm((f) => ({ ...f, commandPreview: preview }));
  }, [form.agent, selection]);

  async function runAgent() {
    const res = await fetch(`${API_BASE}/api/run-agent`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ...selection, agent: form.agent }),
    });
    const json = await res.json();
    setRunResult(json);
  }

  return (
    <div style={{ display: "grid", gap: 16 }}>
      <SectionTitle title="Run Agents" subtitle="Optional utility area. Not essential to the live demo, but useful for loading new results offline." />
      <div style={{ ...cardStyle, padding: 16, display: "grid", gap: 12, maxWidth: 900 }}>
        <Selector label="Agent" value={form.agent} onChange={(v) => setForm((f) => ({ ...f, agent: v }))} options={["benchmark_characterization", "nl2sql_predictions", "prediction_diagnosis"]} />
        <CodeBlock title="Command preview" value={form.commandPreview} maxHeight={140} />
        <div>
          <button type="button" onClick={runAgent} style={{ border: "1px solid #2563eb", background: "#2563eb", color: "white", padding: "10px 14px", borderRadius: 12, cursor: "pointer" }}>Run agent</button>
        </div>
        {runResult ? <JsonBlock title="Run result" value={runResult} maxHeight={240} /> : null}
      </div>
    </div>
  );
}

function InstanceHeader({ instance, extra }) {
  return (
    <div style={{ ...cardStyle, padding: 16, display: "grid", gap: 12 }}>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 16, flexWrap: "wrap" }}>
        <div>
          <div style={{ fontSize: 12, color: "#6b7280" }}>#{instance.id} · {instance.db_id}</div>
          <div style={{ fontSize: 20, fontWeight: 700, marginTop: 4 }}>{instance.nl}</div>
        </div>
        {extra ? <div style={{ fontSize: 13, color: "#6b7280" }}>{extra}</div> : null}
      </div>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(2, minmax(0, 1fr))", gap: 12 }}>
        <LabeledText label="Context" value={instance.context} />
        <LabeledText label="Additional context" value={instance.additional_context} />
      </div>
    </div>
  );
}

function EditModal({ editingField, editBuffer, setEditBuffer, onClose, onSave }) {
  const isFlagField = editingField.field === "execution_match_assessment";
  const definition = getDefinitionForField(editingField.field, editingField.scope);

  return (
    <div style={{ position: "fixed", inset: 0, background: "rgba(15, 23, 42, 0.48)", display: "grid", placeItems: "center", zIndex: 40 }}>
      <div style={{ width: 760, maxWidth: "calc(100vw - 40px)", ...cardStyle, padding: 18 }}>
        <div style={{ display: "flex", justifyContent: "space-between", gap: 16, alignItems: "center", marginBottom: 14 }}>
          <div>
            <div style={{ fontSize: 12, color: "#6b7280", textTransform: "uppercase" }}>{editingField.scope}</div>
            <div style={{ fontSize: 22, fontWeight: 700 }}>{editingField.field === "execution_match_assessment" ? "Flagged Execution Outcome" : titleize(editingField.field)}</div>
          </div>
          <button type="button" onClick={onClose} style={{ border: "1px solid #d1d5db", background: "white", borderRadius: 10, padding: "8px 10px", cursor: "pointer" }}>Close</button>
        </div>

        {definition ? (
          <div style={{ marginBottom: 14, padding: 12, borderRadius: 12, background: "#f8fafc", border: "1px solid #e5e7eb", whiteSpace: "pre-wrap", lineHeight: 1.5 }}>
            {definition}
          </div>
        ) : null}

        <div style={{ display: "grid", gap: 12 }}>
          <div>
            <div style={{ fontSize: 12, color: "#6b7280", textTransform: "uppercase", marginBottom: 6 }}>Classification</div>
            {isFlagField ? (
              <div style={{ display: "flex", gap: 12 }}>
                <label><input type="radio" checked={editBuffer.classification === "unflagged"} onChange={() => setEditBuffer((b) => ({ ...b, classification: "unflagged" }))} /> Unflagged</label>
                <label><input type="radio" checked={editBuffer.classification === "flagged"} onChange={() => setEditBuffer((b) => ({ ...b, classification: "flagged" }))} /> Flagged</label>
              </div>
            ) : (
              <label style={{ display: "flex", alignItems: "center", gap: 8 }}>
                <input type="checkbox" checked={Boolean(editBuffer.classification)} onChange={(e) => setEditBuffer((b) => ({ ...b, classification: e.target.checked }))} />
                Mark as TRUE
              </label>
            )}
          </div>
          <div>
            <div style={{ fontSize: 12, color: "#6b7280", textTransform: "uppercase", marginBottom: 6 }}>Description</div>
            <textarea value={editBuffer.description} onChange={(e) => setEditBuffer((b) => ({ ...b, description: e.target.value }))} style={{ width: "100%", minHeight: 180, border: "1px solid #d1d5db", borderRadius: 12, padding: 12 }} />
          </div>
          <div style={{ display: "flex", justifyContent: "flex-end", gap: 10 }}>
            <button type="button" onClick={onClose} style={{ border: "1px solid #d1d5db", background: "white", borderRadius: 10, padding: "10px 12px", cursor: "pointer" }}>Cancel</button>
            <button type="button" onClick={onSave} style={{ border: "1px solid #2563eb", background: "#2563eb", color: "white", borderRadius: 10, padding: "10px 12px", cursor: "pointer" }}>Save correction</button>
          </div>
        </div>
      </div>
    </div>
  );
}

export default App;
