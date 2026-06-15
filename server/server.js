"use strict";

/*
 * chatty backend — Phase 1
 * -------------------------------------------------------------
 * Localhost-only privileged brain for the chatty app.
 *
 * Phase 1 responsibilities (intentionally small):
 *   - Serve the single-file frontend (../index.html).
 *   - Hold the OpenRouter API key (from .env) OFF the browser.
 *   - Proxy the live model list:           GET  /api/models
 *   - Stream plain chat completions (SSE):  POST /api/chat
 *   - Report status to the UI:             GET  /api/health
 *
 * The agent loop, tools, filesystem and terminal arrive in later phases.
 * Only dependency: express. Upstream calls use Node's built-in fetch (Node 18+).
 */

const express = require("express");
const path = require("path");
const fs = require("fs");

// ---- tiny .env loader (no dependency) --------------------------------------
// Reads server/.env if present and populates process.env without overwriting
// anything already set in the real environment.
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

const app = express();
app.use(express.json({ limit: "8mb" }));

// ---- frontend --------------------------------------------------------------
// Serve ONLY index.html. We deliberately do not static-serve a directory so
// that server/.env and other backend files can never leak over HTTP.
const INDEX_HTML = path.join(__dirname, "..", "index.html");
app.get("/", (_req, res) => res.sendFile(INDEX_HTML));

// ---- status ----------------------------------------------------------------
app.get("/api/health", (_req, res) => {
  res.json({ ok: true, hasKey: Boolean(OPENROUTER_API_KEY) });
});

// ---- live model list (proxied so all OpenRouter calls stay server-side) ----
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
// No tools, no workspace awareness — just streams OpenRouter's SSE straight
// through to the browser so the existing frontend parser works unchanged.
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

  // Abort the upstream request if the browser disconnects mid-stream.
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

  // Upstream errors arrive before streaming begins — forward status + body so
  // the frontend's existing error handling (401/402/429/5xx) still works.
  if (!upstream.ok) {
    const errText = await upstream.text();
    let errJson;
    try { errJson = JSON.parse(errText); }
    catch (_e) { errJson = { error: { message: errText || ("Upstream error " + upstream.status) } }; }
    return res.status(upstream.status).json(errJson);
  }

  // Pipe the SSE bytes through unchanged.
  res.status(200);
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
  } catch (_e) {
    // client aborted or the stream broke — nothing more to do
  } finally {
    res.end();
  }
});

app.listen(PORT, () => {
  console.log("\n  chatty backend  →  http://localhost:" + PORT);
  if (!OPENROUTER_API_KEY) {
    console.log("  ⚠  No OPENROUTER_API_KEY found. Copy server/.env.example to server/.env and add your key.\n");
  } else {
    console.log("  ✓  OpenRouter key loaded.\n");
  }
});
