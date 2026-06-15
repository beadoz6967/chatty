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
const { TOOL_DEFINITIONS, executeTool } = require("./tools");

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

const app = express();
app.use(express.json({ limit: "8mb" }));

// ---- frontend --------------------------------------------------------------
const INDEX_HTML = path.join(__dirname, "..", "index.html");
app.get("/", (_req, res) => res.sendFile(INDEX_HTML));

// ---- status ----------------------------------------------------------------
app.get("/api/health", (_req, res) => {
  res.json({ ok: true, hasKey: Boolean(OPENROUTER_API_KEY), workspace: getWorkspaceDir() });
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
  req.on("close", () => controller.abort());

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
    body: JSON.stringify({ model, messages, tools, stream: true }),
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
  return { text: textAcc, toolCalls, stopReason };
}

// ---- agent loop ------------------------------------------------------------
// Runs until the model produces a response with no tool_calls, or until the
// iteration cap is hit, or until the client aborts.
async function runAgentLoop({ model, messages, tools, maxIterations = 20 }, workspaceDir, res, signal) {
  for (let iter = 1; iter <= maxIterations; iter++) {
    // Notify the frontend which loop iteration we're starting (used to render
    // the AI text bubble and iteration badge correctly).
    sseWrite(res, { type: "iteration", n: iter });

    let result;
    try {
      result = await streamOneCompletion({ model, messages, tools }, res, signal);
    } catch (err) {
      if (err.name === "AbortError") {
        sseWrite(res, { type: "aborted" });
        return;
      }
      sseWrite(res, { type: "error", message: err.message || String(err) });
      return;
    }

    const { text, toolCalls } = result;

    if (toolCalls.length === 0) {
      // Model produced a final answer with no tool calls — we're done.
      sseWrite(res, { type: "done" });
      return;
    }

    // Append assistant turn (may have both text and tool_calls).
    messages.push({
      role: "assistant",
      content: text || null,
      tool_calls: toolCalls.map(tc => ({
        id: tc.id,
        type: "function",
        function: { name: tc.function.name, arguments: tc.function.arguments }
      }))
    });

    // Execute each tool call; stream start/result events to the frontend.
    for (const tc of toolCalls) {
      const parsedArgs = parseToolArgs(tc.function.arguments);

      sseWrite(res, {
        type: "tool_start",
        id: tc.id,
        name: tc.function.name,
        input: parsedArgs || {}
      });

      let resultContent;
      if (parsedArgs === null) {
        resultContent =
          "Error: could not parse tool arguments as JSON after multiple attempts.\n" +
          "Raw arguments string: " + tc.function.arguments;
      } else {
        try {
          resultContent = executeTool(tc.function.name, parsedArgs, workspaceDir);
        } catch (err) {
          resultContent = "Error executing tool: " + err.message;
        }
      }

      sseWrite(res, { type: "tool_result", id: tc.id, content: resultContent });

      // Feed the result back so the model can use it in the next iteration.
      messages.push({
        role: "tool",
        tool_call_id: tc.id,
        content: resultContent
      });
    }
    // Loop — model sees tool results and continues.
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
    "Use tools methodically. Be precise. (Write/command tools unlock in the next phase.)\n"
};

app.post("/api/agent", async (req, res) => {
  if (!OPENROUTER_API_KEY) {
    return res.status(401).json({
      error: { message: "No OPENROUTER_API_KEY set on the server. Add it to server/.env and restart." }
    });
  }

  const { model, messages, systemMessage, mode = "plan" } = req.body || {};
  if (!model || !Array.isArray(messages)) {
    return res.status(400).json({ error: { message: "'model' and 'messages' are required." } });
  }

  const workspaceDir = getWorkspaceDir();
  if (!fs.existsSync(workspaceDir)) fs.mkdirSync(workspaceDir, { recursive: true });

  const userSys = (systemMessage || "").trim();
  const modeLine = MODE_PREFIX[mode] || MODE_PREFIX.plan;
  const effectiveSys = AGENT_BASE + modeLine + (userSys ? "\n" + userSys : "");

  const fullMessages = [
    { role: "system", content: effectiveSys },
    ...messages
  ];

  // Set up SSE before starting the loop
  res.setHeader("Content-Type", "text/event-stream; charset=utf-8");
  res.setHeader("Cache-Control", "no-cache, no-transform");
  res.setHeader("Connection", "keep-alive");
  res.setHeader("X-Accel-Buffering", "no");
  if (res.flushHeaders) res.flushHeaders();

  const controller = new AbortController();
  req.on("close", () => controller.abort());

  // Send the resolved mode back to the frontend so it can label the UI correctly
  sseWrite(res, { type: "mode", mode });

  await runAgentLoop(
    { model, messages: fullMessages, tools: TOOL_DEFINITIONS },
    workspaceDir,
    res,
    controller.signal
  );

  res.end();
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
