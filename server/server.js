"use strict";

/*
 * chatty backend — Phase 2
 * -------------------------------------------------------------
 * Phase 2 adds:
 *   - POST /api/agent — streaming agent loop with read-only tools
 *     (list_dir, read_file, search), SSE typed event protocol,
 *     robust streamed tool-call accumulation, and JSON parse-retry.
 */

const express = require("express");
const path = require("path");
const fs = require("fs");
const { TOOL_DEFINITIONS, READ_TOOL_NAMES, executeTool, listWorkspaceFiles, buildIgnore } = require("./tools");

// ---- tiny .env loader (no dependency) --------------------------------------
(function loadEnv() {
  const envPath = path.join(__dirname, ".env");
  if (!fs.existsSync(envPath)) return;
  const text = fs.readFileSync(envPath, "utf8");
  for (const rawLine of text.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith("#")) continue;
    const eq = line.indexOf("=");
    if (eq === -1) continue;
    const key = line.slice(0, eq).trim();
    let val = line.slice(eq + 1).trim();
    if ((val.startsWith('"') && val.endsWith('"')) || (val.startsWith("'") && val.endsWith("'"))) {
      val = val.slice(1, -1);
    }
    if (key && !(key in process.env)) process.env[key] = val;
  }
})();

const OPENROUTER_API_KEY = (process.env.OPENROUTER_API_KEY || "").trim();
const PORT = Number(process.env.PORT) || 3000;
const OPENROUTER_BASE = "https://openrouter.ai/api/v1";

// Resolved once at startup; individual requests re-resolve per-call so a
// runtime .env change or WORKSPACE_DIR override still works.
function getWorkspaceDir() {
  return path.resolve(__dirname, process.env.WORKSPACE_DIR || "./workspace");
}

// ---- on-disk data (tasks + checkpoints) ------------------------------------
const DATA_DIR  = path.join(__dirname, ".data");
const TASKS_DIR = path.join(DATA_DIR, "tasks");
const CKPT_DIR  = path.join(DATA_DIR, "checkpoints");
function ensureDir(d) { if (!fs.existsSync(d)) fs.mkdirSync(d, { recursive: true }); }

// Phase 8 — always-on workspace rules file (first that exists wins)
function readWorkspaceRules(workspaceDir) {
  for (const name of ["AGENTS.md", ".chattyrules", ".cursorrules"]) {
    try {
      const p = path.join(workspaceDir, name);
      if (fs.existsSync(p)) return fs.readFileSync(p, "utf8").slice(0, 8000);
    } catch (_) {}
  }
  return "";
}

// Phase 7 — snapshot a file's pre-write state so a step can be rolled back.
function snapshotFile(taskId, step, relPath, absPath, res) {
  try {
    ensureDir(path.join(CKPT_DIR, taskId));
    const id = "ck_" + Date.now() + "_" + Math.random().toString(36).slice(2, 6);
    const existed = fs.existsSync(absPath);
    const meta = { id, taskId, step, path: relPath, existed, time: Date.now() };
    const base = path.join(CKPT_DIR, taskId, id);
    if (existed) fs.copyFileSync(absPath, base + ".content");
    fs.writeFileSync(base + ".json", JSON.stringify(meta));
    if (res) sseWrite(res, { type: "checkpoint", id, step, path: relPath, existed });
  } catch (_) {}
}

const app = express();
app.use(express.json({ limit: "8mb" }));

// ---- frontend --------------------------------------------------------------
const INDEX_HTML = path.join(__dirname, "..", "index.html");
app.get("/", (_req, res) => res.sendFile(INDEX_HTML));

// ---- status ----------------------------------------------------------------
app.get("/api/health", (_req, res) => {
  res.json({ ok: true, hasKey: Boolean(OPENROUTER_API_KEY), workspace: getWorkspaceDir() });
});

// ---- confirm gate ----------------------------------------------------------
// When the agent needs to run_command or overwrite a file, it pauses the loop,
// emits a confirm_request SSE event, and awaits a POST to /api/confirm/:id.
// The frontend shows Approve / Deny; the result resolves the promise.
const pendingConfirms = new Map();

app.post("/api/confirm/:id", (req, res) => {
  const cb = pendingConfirms.get(req.params.id);
  if (!cb) return res.status(404).json({ error: "No pending confirmation found." });
  pendingConfirms.delete(req.params.id);
  cb(Boolean(req.body?.approved));
  res.json({ ok: true });
});

// ---- live model list -------------------------------------------------------
app.get("/api/models", async (_req, res) => {
  try {
    const upstream = await fetch(OPENROUTER_BASE + "/models");
    const body = await upstream.text();
    res.status(upstream.status).type("application/json").send(body);
  } catch (_e) {
    res.status(502).json({ error: { message: "Could not reach OpenRouter to load the model list." } });
  }
});

// ---- plain chat completion (CHAT panel) ------------------------------------
app.post("/api/chat", async (req, res) => {
  if (!OPENROUTER_API_KEY) {
    return res.status(401).json({
      error: { message: "No OPENROUTER_API_KEY set on the server. Add it to server/.env and restart." }
    });
  }
  const { model, messages } = req.body || {};
  if (!model || !Array.isArray(messages) || messages.length === 0) {
    return res.status(400).json({ error: { message: "Request must include a model and a non-empty messages array." } });
  }

  const controller = new AbortController();
  res.on("close", () => controller.abort());

  let upstream;
  try {
    upstream = await fetch(OPENROUTER_BASE + "/chat/completions", {
      method: "POST",
      headers: {
        "Authorization": "Bearer " + OPENROUTER_API_KEY,
        "Content-Type": "application/json",
        "HTTP-Referer": "http://localhost:" + PORT,
        "X-Title": "chatty"
      },
      body: JSON.stringify({ model, messages, stream: true }),
      signal: controller.signal
    });
  } catch (_e) {
    console.error('OpenRouter fetch error:', _e);
    return res.status(502).json({ error: { message: "Could not reach OpenRouter. Check the server's connection." } });
  }

  if (!upstream.ok) {
    const errText = await upstream.text();
    let errJson;
    try { errJson = JSON.parse(errText); }
    catch (_e) { errJson = { error: { message: errText || ("Upstream error " + upstream.status) } }; }
    return res.status(upstream.status).json(errJson);
  }

  res.setHeader("Content-Type", "text/event-stream; charset=utf-8");
  res.setHeader("Cache-Control", "no-cache, no-transform");
  res.setHeader("Connection", "keep-alive");
  res.setHeader("X-Accel-Buffering", "no");
  if (res.flushHeaders) res.flushHeaders();

  try {
    const reader = upstream.body.getReader();
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      res.write(Buffer.from(value));
    }
  } catch (_e) { /* client aborted */ } finally {
    res.end();
  }
});

// ===========================================================================
// AGENT LOOP — Phase 2
// ===========================================================================

// ---- SSE helpers -----------------------------------------------------------
function sseWrite(res, obj) {
  res.write("data: " + JSON.stringify(obj) + "\n\n");
}

// ---- streamed tool-call accumulation ----------------------------------------
// OpenRouter streams tool calls as fragments across many SSE chunks, e.g.:
//   chunk 1: {index:0, id:"call_x", function:{name:"read_file", arguments:""}}
//   chunk 2: {index:0, function:{arguments:'{"pa'}}
//   chunk 3: {index:0, function:{arguments:'th":"src/main.js"}'}}
// We accumulate by index, concatenating argument fragments.

function makeToolCallAcc() { return new Map(); }

function accumulateToolCalls(acc, deltaToolCalls) {
  if (!Array.isArray(deltaToolCalls)) return;
  for (const tc of deltaToolCalls) {
    const idx = typeof tc.index === "number" ? tc.index : 0;
    if (!acc.has(idx)) acc.set(idx, { id: "", name: "", arguments: "" });
    const entry = acc.get(idx);
    if (tc.id) entry.id += tc.id;
    if (tc.function) {
      if (tc.function.name) entry.name += tc.function.name;
      if (tc.function.arguments) entry.arguments += tc.function.arguments;
    }
  }
}

function finalizeToolCalls(acc) {
  return Array.from(acc.entries())
    .sort(([a], [b]) => a - b)
    .map(([idx, tc]) => ({
      id: tc.id || ("tc_" + idx + "_" + Date.now()),
      type: "function",
      function: { name: tc.name, arguments: tc.arguments }
    }));
}

// ---- JSON parse with retry on common model mangling -----------------------
function parseToolArgs(raw) {
  if (!raw || raw.trim() === "") return {};
  // Pass 1: straight parse
  try { return JSON.parse(raw); } catch (_) {}
  // Pass 2: strip trailing commas before } or ], then retry
  const cleaned = raw
    .replace(/,(\s*[}\]])/g, "$1")      // trailing commas
    .replace(/([{,]\s*)(\w+)(\s*):/g, '$1"$2"$3:')  // unquoted keys
    .trim();
  try { return JSON.parse(cleaned); } catch (_) {}
  // Pass 3: try to close an unclosed object
  try { return JSON.parse(cleaned + "}"); } catch (_) {}
  return null; // signal failure; caller sends error back to model
}

// ---- one streaming completion round-trip ----------------------------------
// Makes one call to OpenRouter with the current message list + tool definitions.
// Streams text tokens to the client via sseWrite as they arrive.
// Accumulates tool_calls across all SSE chunks, returns them when streaming ends.
//
// Returns: { text, toolCalls, stopReason }  — or throws on network/upstream errors.
async function streamOneCompletion({ model, messages, tools }, res, signal) {
  const upstream = await fetch(OPENROUTER_BASE + "/chat/completions", {
    method: "POST",
    headers: {
      "Authorization": "Bearer " + OPENROUTER_API_KEY,
      "Content-Type": "application/json",
      "HTTP-Referer": "http://localhost:" + PORT,
      "X-Title": "chatty"
    },
    body: JSON.stringify({ model, messages, tools, stream: true, usage: { include: true } }),
    signal
  });

  if (!upstream.ok) {
    const errText = await upstream.text();
    let detail = errText;
    try { detail = JSON.parse(errText).error?.message || errText; } catch (_) {}
    const e = new Error("OpenRouter " + upstream.status + ": " + detail);
    e.status = upstream.status;
    throw e;
  }

  const toolCallAcc = makeToolCallAcc();
  let textAcc = "";
  let stopReason = null;
  let usage = null;

  const reader = upstream.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });

    const lines = buffer.split("\n");
    buffer = lines.pop(); // keep incomplete tail

    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed || !trimmed.startsWith("data:")) continue;
      const data = trimmed.slice(5).trim();
      if (data === "[DONE]") { buffer = ""; break; }

      let json;
      try { json = JSON.parse(data); } catch (_) { continue; }

      // Usage arrives in a trailing chunk (often with empty choices)
      if (json.usage) usage = json.usage;

      const choice = json.choices?.[0];
      if (!choice) continue;

      const delta = choice.delta || {};

      // Text content — stream directly to client
      if (delta.content) {
        textAcc += delta.content;
        sseWrite(res, { type: "text", content: delta.content });
      }

      // Tool call fragments — accumulate across chunks
      if (delta.tool_calls) {
        accumulateToolCalls(toolCallAcc, delta.tool_calls);
      }

      if (choice.finish_reason) stopReason = choice.finish_reason;
    }
  }

  const toolCalls = finalizeToolCalls(toolCallAcc);
  return { text: textAcc, toolCalls, stopReason, usage };
}

// ---- agent loop ------------------------------------------------------------
// ---- permission gating -----------------------------------------------------
// A "gate" decides whether a tool action runs. If the relevant auto-approve
// flag (or bypass) is set, it resolves immediately; otherwise it emits a
// confirm_request SSE event and awaits a POST to /api/confirm/:id.
function autoApproved(perms, kind) {
  if (!perms) return false;
  if (perms.bypass) return true;
  if (kind === "read") return perms.autoReads;
  if (kind === "write_file") return perms.autoWrites;
  if (kind === "command") return perms.autoCommands;
  return false;
}

function makeGate(perms, res, gateSet) {
  return (payload) => {
    if (autoApproved(perms, payload.kind)) {
      sseWrite(res, { type: "auto_approved", kind: payload.kind });
      return Promise.resolve(true);
    }
    return new Promise((resolve) => {
      const gateId = "gate_" + Date.now() + "_" + Math.random().toString(36).slice(2, 6);
      gateSet.add(gateId);
      sseWrite(res, { type: "confirm_request", id: gateId, ...payload });
      pendingConfirms.set(gateId, (val) => { gateSet.delete(gateId); resolve(val); });
      setTimeout(() => {
        if (pendingConfirms.has(gateId)) {
          pendingConfirms.delete(gateId);
          gateSet.delete(gateId);
          sseWrite(res, { type: "confirm_expired", id: gateId });
          resolve(false);
        }
      }, 5 * 60 * 1000);
    });
  };
}

// Runs until the model produces a response with no tool_calls, the iteration
// cap is hit, or the client aborts.
async function runAgentLoop(ctx, res, signal) {
  const { model, messages, tools, workspaceDir, perms, gateSet, taskId } = ctx;
  const maxIterations = Math.min(Math.max(1, Number(ctx.maxIterations) || 20), 50);
  const requestConfirm = makeGate(perms, res, gateSet);
  let totalUsage = { prompt: 0, completion: 0, cost: 0 };

  for (let iter = 1; iter <= maxIterations; iter++) {
    if (signal.aborted) { sseWrite(res, { type: "aborted" }); return; }
    sseWrite(res, { type: "iteration", n: iter });

    let result;
    try {
      result = await streamOneCompletion({ model, messages, tools }, res, signal);
    } catch (err) {
      if (err.name === "AbortError") { sseWrite(res, { type: "aborted" }); return; }
      sseWrite(res, { type: "error", message: err.message || String(err) });
      return;
    }

    const { text, toolCalls, usage } = result;
    if (usage) {
      totalUsage.prompt += usage.prompt_tokens || 0;
      totalUsage.completion += usage.completion_tokens || 0;
      totalUsage.cost += usage.cost || 0;
      sseWrite(res, {
        type: "usage",
        prompt: usage.prompt_tokens || 0,
        completion: usage.completion_tokens || 0,
        cost: usage.cost || 0,
        total: totalUsage
      });
    }

    if (toolCalls.length === 0) {
      sseWrite(res, { type: "done", usage: totalUsage });
      return;
    }

    messages.push({
      role: "assistant",
      content: text || null,
      tool_calls: toolCalls.map(tc => ({
        id: tc.id, type: "function",
        function: { name: tc.function.name, arguments: tc.function.arguments }
      }))
    });

    for (const tc of toolCalls) {
      if (signal.aborted) { sseWrite(res, { type: "aborted" }); return; }

      const name = tc.function.name;
      const parsedArgs = parseToolArgs(tc.function.arguments);

      sseWrite(res, { type: "tool_start", id: tc.id, name, input: parsedArgs || {} });

      let resultContent;
      if (parsedArgs === null) {
        resultContent =
          "Error: could not parse tool arguments as JSON after multiple attempts.\n" +
          "Raw arguments string: " + tc.function.arguments;
      } else {
        const callbacks = {
          signal,
          requestConfirm,
          onOutput: (t) => sseWrite(res, { type: "command_output", id: tc.id, text: t }),
          // Snapshot a file before it is overwritten (Phase 7 checkpoints)
          onBeforeWrite: (relPath, absPath) => snapshotFile(taskId, iter, relPath, absPath, res)
        };

        // Read tools gate at loop level (writes/commands gate inside the tool).
        if (READ_TOOL_NAMES.has(name)) {
          const ok = await requestConfirm({ kind: "read", name, input: parsedArgs });
          if (!ok) {
            resultContent = "Read denied by user.";
            sseWrite(res, { type: "tool_result", id: tc.id, content: resultContent });
            messages.push({ role: "tool", tool_call_id: tc.id, content: resultContent });
            continue;
          }
        }

        try {
          resultContent = await executeTool(name, parsedArgs, workspaceDir, callbacks);
        } catch (err) {
          resultContent = "Error executing tool: " + err.message;
        }
      }

      sseWrite(res, { type: "tool_result", id: tc.id, content: resultContent });
      messages.push({ role: "tool", tool_call_id: tc.id, content: resultContent });
    }
  }

  sseWrite(res, {
    type: "error",
    message: "Reached the maximum of " + maxIterations + " iterations without a final answer. Stopping."
  });
}

// ---- /api/agent endpoint ---------------------------------------------------
// The CODE panel sends: { model, messages, systemMessage, mode }
// mode = "plan" (read-only, propose plan) | "act" (execute — write tools added in Phase 4)
//
// SSE event types: iteration, text, tool_start, tool_result, done, aborted, error

const AGENT_BASE =
  "You are an expert coding assistant with access to a local workspace.\n" +
  "Tools available: list_dir, read_file, search (all workspace-scoped; paths are relative to workspace root).\n";

const MODE_PREFIX = {
  plan:
    "MODE: PLAN — read-only investigation.\n" +
    "Explore the workspace thoroughly, then output a clear written plan. Do NOT modify any files.\n",
  act:
    "MODE: ACT — execute the task.\n" +
    "Tools: list_dir, read_file, search (read), write_file (create/overwrite), run_command (shell).\n" +
    "Write files carefully. All file writes and shell commands require explicit user approval.\n" +
    "If a command or write is denied, self-correct and try a different approach.\n"
};

app.post("/api/agent", async (req, res) => {
  if (!OPENROUTER_API_KEY) {
    return res.status(401).json({
      error: { message: "No OPENROUTER_API_KEY set on the server. Add it to server/.env and restart." }
    });
  }

  const {
    model, messages, systemMessage, mode = "plan",
    permissions, maxIterations, taskId, workspaceRules
  } = req.body || {};
  if (!model || !Array.isArray(messages)) {
    return res.status(400).json({ error: { message: "'model' and 'messages' are required." } });
  }

  const workspaceDir = getWorkspaceDir();
  if (!fs.existsSync(workspaceDir)) fs.mkdirSync(workspaceDir, { recursive: true });

  // Permissions (default: reads auto-approved, writes/commands gated, no bypass)
  const perms = {
    autoReads:    permissions?.autoReads    !== false,
    autoWrites:   Boolean(permissions?.autoWrites),
    autoCommands: Boolean(permissions?.autoCommands),
    bypass:       Boolean(permissions?.bypass)
  };

  // Read the always-on workspace rules file, if present (Phase 8)
  const wsRules = readWorkspaceRules(workspaceDir);

  const userSys = (systemMessage || "").trim();
  const modeLine = MODE_PREFIX[mode] || MODE_PREFIX.plan;
  const effectiveSys = AGENT_BASE + modeLine
    + (wsRules ? "\n\nWorkspace rules (always apply):\n" + wsRules : "")
    + (workspaceRules ? "" : "")  // reserved
    + (userSys ? "\n\n" + userSys : "");

  const fullMessages = [{ role: "system", content: effectiveSys }, ...messages];

  res.setHeader("Content-Type", "text/event-stream; charset=utf-8");
  res.setHeader("Cache-Control", "no-cache, no-transform");
  res.setHeader("Connection", "keep-alive");
  res.setHeader("X-Accel-Buffering", "no");
  if (res.flushHeaders) res.flushHeaders();

  const controller = new AbortController();
  const gateSet = new Set();
  // On disconnect: abort the loop and resolve any open confirmations as denied
  // so a blocked await (e.g. modal open when Stop is hit) unblocks immediately.
  res.on("close", () => {
    controller.abort();
    for (const id of gateSet) {
      const cb = pendingConfirms.get(id);
      if (cb) { pendingConfirms.delete(id); cb(false); }
    }
  });

  const tools = mode === "act"
    ? TOOL_DEFINITIONS
    : TOOL_DEFINITIONS.filter(t => READ_TOOL_NAMES.has(t.function.name));

  sseWrite(res, { type: "mode", mode, permissions: perms });

  await runAgentLoop(
    { model, messages: fullMessages, tools, workspaceDir, perms, gateSet,
      maxIterations, taskId: taskId || "default" },
    res,
    controller.signal
  );

  res.end();
});

// ===========================================================================
// Phase 6 — @-context file access
// ===========================================================================
app.get("/api/files", (req, res) => {
  const ws = getWorkspaceDir();
  res.json({ files: listWorkspaceFiles(ws, req.query.q || "", 40) });
});

app.get("/api/file", (req, res) => {
  const ws = getWorkspaceDir();
  const rel = String(req.query.path || "");
  try {
    const base = path.resolve(ws);
    const target = path.resolve(base, rel);
    if (target !== base && !target.startsWith(base + path.sep))
      return res.status(403).json({ error: "Path outside workspace." });
    if (buildIgnore(ws)(rel)) return res.status(403).json({ error: "File is ignored." });
    const content = fs.readFileSync(target, "utf8").slice(0, 100 * 1024);
    res.json({ path: rel, content });
  } catch (e) {
    res.status(404).json({ error: "Could not read file: " + e.message });
  }
});

// ===========================================================================
// Phase 7 — task history + checkpoints
// ===========================================================================

// Save / update a task (CODE session) to disk.
app.put("/api/tasks/:id", (req, res) => {
  try {
    ensureDir(TASKS_DIR);
    const id = req.params.id.replace(/[^\w.-]/g, "_");
    const task = { id, ...req.body, updated: Date.now() };
    fs.writeFileSync(path.join(TASKS_DIR, id + ".json"), JSON.stringify(task));
    res.json({ ok: true, id });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// List saved tasks (metadata only).
app.get("/api/tasks", (_req, res) => {
  try {
    ensureDir(TASKS_DIR);
    const tasks = fs.readdirSync(TASKS_DIR)
      .filter(f => f.endsWith(".json"))
      .map(f => { try { return JSON.parse(fs.readFileSync(path.join(TASKS_DIR, f), "utf8")); } catch { return null; } })
      .filter(Boolean)
      .map(t => ({ id: t.id, title: t.title || "Untitled task", updated: t.updated, mode: t.mode,
                   messageCount: (t.messages || []).length }))
      .sort((a, b) => b.updated - a.updated);
    res.json({ tasks });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Load a full task.
app.get("/api/tasks/:id", (req, res) => {
  try {
    const id = req.params.id.replace(/[^\w.-]/g, "_");
    const data = fs.readFileSync(path.join(TASKS_DIR, id + ".json"), "utf8");
    res.json(JSON.parse(data));
  } catch (e) { res.status(404).json({ error: "Task not found." }); }
});

app.delete("/api/tasks/:id", (req, res) => {
  try {
    const id = req.params.id.replace(/[^\w.-]/g, "_");
    fs.rmSync(path.join(TASKS_DIR, id + ".json"), { force: true });
    fs.rmSync(path.join(CKPT_DIR, id), { recursive: true, force: true });
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// List checkpoints for a task.
app.get("/api/checkpoints/:taskId", (req, res) => {
  try {
    const taskId = req.params.taskId.replace(/[^\w.-]/g, "_");
    const dir = path.join(CKPT_DIR, taskId);
    if (!fs.existsSync(dir)) return res.json({ checkpoints: [] });
    const cps = fs.readdirSync(dir)
      .filter(f => f.endsWith(".json"))
      .map(f => { try { return JSON.parse(fs.readFileSync(path.join(dir, f), "utf8")); } catch { return null; } })
      .filter(Boolean)
      .sort((a, b) => b.time - a.time);
    res.json({ checkpoints: cps });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Roll a single checkpoint back: restore (or delete) the file to its snapshot.
app.post("/api/checkpoints/:taskId/:id/rollback", (req, res) => {
  try {
    const taskId = req.params.taskId.replace(/[^\w.-]/g, "_");
    const id = req.params.id.replace(/[^\w.-]/g, "_");
    const dir = path.join(CKPT_DIR, taskId);
    const meta = JSON.parse(fs.readFileSync(path.join(dir, id + ".json"), "utf8"));
    const ws = getWorkspaceDir();
    const base = path.resolve(ws);
    const target = path.resolve(base, meta.path);
    if (target !== base && !target.startsWith(base + path.sep))
      return res.status(403).json({ error: "Path outside workspace." });
    if (meta.existed) fs.copyFileSync(path.join(dir, id + ".content"), target);
    else fs.rmSync(target, { force: true });   // file didn't exist before — remove it
    res.json({ ok: true, path: meta.path, action: meta.existed ? "restored" : "deleted" });
  } catch (e) { res.status(500).json({ error: "Rollback failed: " + e.message }); }
});

// ---------------------------------------------------------------------------
app.listen(PORT, () => {
  const ws = getWorkspaceDir();
  console.log("\n  chatty backend  →  http://localhost:" + PORT);
  console.log("  workspace       →  " + ws);
  if (!OPENROUTER_API_KEY) {
    console.log("  ⚠  No OPENROUTER_API_KEY found. Copy server/.env.example to server/.env and add your key.\n");
  } else {
    console.log("  ✓  OpenRouter key loaded.\n");
  }
});
