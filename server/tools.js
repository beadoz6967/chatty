"use strict";

const fs = require("fs");
const path = require("path");

const READ_LIMIT = 50 * 1024;   // 50 KB hard cap on read_file
const LIST_LIMIT = 300;          // max entries for list_dir
const SEARCH_LIMIT = 100;        // max matching lines for search

// ---- .agentignore ----------------------------------------------------------
// Patterns the agent may NOT read or write. Always-on defaults + workspace file.
const DEFAULT_IGNORES = ["node_modules", ".git", ".env", ".env.*", ".data"];

function buildIgnore(workspaceDir) {
  let patterns = DEFAULT_IGNORES.slice();
  try {
    const f = path.join(workspaceDir, ".agentignore");
    if (fs.existsSync(f)) {
      const lines = fs.readFileSync(f, "utf8").split(/\r?\n/)
        .map(l => l.trim()).filter(l => l && !l.startsWith("#"));
      patterns = patterns.concat(lines);
    }
  } catch (_) {}

  // Convert a gitignore-ish glob to a RegExp that matches a single path segment
  // or a full relative path.
  const regexes = patterns.map(p => {
    const clean = p.replace(/\/+$/, "");
    const rx = clean
      .replace(/[.+^${}()|[\]\\]/g, "\\$&")
      .replace(/\*/g, "[^/]*")
      .replace(/\?/g, "[^/]");
    return new RegExp("^" + rx + "$");
  });

  // relPath uses forward slashes; matches if any segment or the whole path hits
  return function isIgnored(relPath) {
    const norm = relPath.replace(/\\/g, "/").replace(/^\.\//, "");
    if (!norm || norm === ".") return false;
    const segments = norm.split("/");
    return regexes.some(rx => rx.test(norm) || segments.some(seg => rx.test(seg)));
  };
}

// ---- workspace guard -------------------------------------------------------
// Every tool path must stay inside WORKSPACE_DIR. Throws on escape attempt.
function resolveSafe(workspaceDir, relPath) {
  const base = path.resolve(workspaceDir);
  const target = path.resolve(base, relPath || ".");
  // Ensure the resolved path starts with the base (with trailing sep to block
  // "workspace_sibling" style escapes).
  if (target !== base && !target.startsWith(base + path.sep)) {
    throw new Error("Path escape rejected: '" + relPath + "' resolves outside the workspace.");
  }
  return target;
}

// ---- list_dir --------------------------------------------------------------
function listDir(workspaceDir, args) {
  let target;
  try { target = resolveSafe(workspaceDir, args.path || "."); }
  catch (e) { return "Error: " + e.message; }

  const isIgnored = buildIgnore(workspaceDir);
  const baseRel = (args.path || ".").replace(/\\/g, "/").replace(/^\.\//, "").replace(/\/$/, "");

  let entries;
  try { entries = fs.readdirSync(target, { withFileTypes: true }); }
  catch (e) { return "Error reading directory: " + e.message; }

  entries = entries.filter(e => !isIgnored((baseRel && baseRel !== "." ? baseRel + "/" : "") + e.name));
  if (entries.length === 0) return "(empty directory, or all entries ignored)";

  const lines = entries.slice(0, LIST_LIMIT).map(e => {
    const isDir = e.isDirectory();
    const suffix = isDir ? "/" : "";
    let size = "";
    if (!isDir) {
      try { size = " (" + fs.statSync(path.join(target, e.name)).size + " B)"; } catch (_) {}
    }
    return (isDir ? "d " : "f ") + e.name + suffix + size;
  });

  if (entries.length > LIST_LIMIT) lines.push("[truncated — " + entries.length + " total entries]");
  return lines.join("\n");
}

// ---- read_file -------------------------------------------------------------
function readFile(workspaceDir, args) {
  if (!args.path) return "Error: 'path' argument is required.";

  let target;
  try { target = resolveSafe(workspaceDir, args.path); }
  catch (e) { return "Error: " + e.message; }

  if (buildIgnore(workspaceDir)(args.path)) {
    return "Error: '" + args.path + "' is excluded by .agentignore and cannot be read.";
  }

  let stat;
  try { stat = fs.statSync(target); }
  catch (e) { return "Error: " + e.message; }

  if (stat.isDirectory()) return "Error: '" + args.path + "' is a directory. Use list_dir to browse it.";

  try {
    const buf = Buffer.allocUnsafe(READ_LIMIT + 1);
    const fd = fs.openSync(target, "r");
    const bytesRead = fs.readSync(fd, buf, 0, READ_LIMIT + 1, 0);
    fs.closeSync(fd);
    const content = buf.slice(0, bytesRead).toString("utf8");
    if (bytesRead > READ_LIMIT) {
      return content.slice(0, READ_LIMIT) + "\n\n[File truncated at 50 KB — " + stat.size + " bytes total]";
    }
    return content || "(empty file)";
  } catch (e) {
    return "Error reading file: " + e.message;
  }
}

// ---- search ----------------------------------------------------------------
// Pure-JS recursive grep so the tool works everywhere without system deps.
function search(workspaceDir, args) {
  if (!args.pattern) return "Error: 'pattern' argument is required.";

  let target;
  try { target = resolveSafe(workspaceDir, args.path || "."); }
  catch (e) { return "Error: " + e.message; }

  let regex;
  try { regex = new RegExp(args.pattern, "m"); }
  catch (e) { return "Error: invalid regular expression — " + e.message; }

  // Build a simple file-glob filter if file_pattern was given
  let fileFilter = null;
  if (args.file_pattern) {
    const escaped = args.file_pattern
      .replace(/[.+^${}()|[\]\\]/g, "\\$&")
      .replace(/\*/g, ".*")
      .replace(/\?/g, ".");
    try { fileFilter = new RegExp("^" + escaped + "$"); }
    catch (e) { return "Error: invalid file_pattern — " + e.message; }
  }

  const isIgnored = buildIgnore(workspaceDir);
  const results = [];

  function searchFile(filePath, relPath) {
    let content;
    try { content = fs.readFileSync(filePath, "utf8"); }
    catch (_e) { return; } // binary or unreadable — skip

    const lines = content.split("\n");
    for (let i = 0; i < lines.length && results.length < SEARCH_LIMIT; i++) {
      if (regex.test(lines[i])) {
        results.push(relPath + ":" + (i + 1) + ": " + lines[i].slice(0, 200));
      }
    }
  }

  function walk(dir, base) {
    if (results.length >= SEARCH_LIMIT) return;
    let entries;
    try { entries = fs.readdirSync(dir, { withFileTypes: true }); }
    catch (_e) { return; }

    for (const e of entries) {
      if (results.length >= SEARCH_LIMIT) break;
      const full = path.join(dir, e.name);
      const rel = path.join(base, e.name).replace(/\\/g, "/").replace(/^\.\//, "");
      if (isIgnored(rel)) continue;
      if (e.isDirectory()) {
        walk(full, rel);
      } else if (e.isFile()) {
        if (fileFilter && !fileFilter.test(e.name)) continue;
        searchFile(full, rel);
      }
    }
  }

  try {
    const stat = fs.statSync(target);
    if (stat.isFile()) {
      searchFile(target, args.path || ".");
    } else {
      walk(target, ".");
    }
  } catch (e) {
    return "Error: " + e.message;
  }

  if (results.length === 0) return "No matches found for pattern '" + args.pattern + "'.";
  const out = results.join("\n");
  return results.length >= SEARCH_LIMIT
    ? out + "\n\n[Results truncated at " + SEARCH_LIMIT + " matches]"
    : out;
}

// ---- simpleDiff ------------------------------------------------------------
// LCS-based unified diff. Caps at 500 lines each to stay memory-safe.
function simpleDiff(oldText, newText) {
  const a = (oldText || "").split("\n");
  const b = (newText || "").split("\n");
  if (a.length > 500 || b.length > 500) {
    return "[File too large to diff inline — showing new content first 60 lines]\n" +
      b.slice(0, 60).map(l => "+" + l).join("\n") + (b.length > 60 ? "\n..." : "");
  }
  const n = a.length, m = b.length;
  // Build DP table
  const dp = Array.from({ length: n + 1 }, () => new Uint16Array(m + 1));
  for (let i = 1; i <= n; i++)
    for (let j = 1; j <= m; j++)
      dp[i][j] = a[i-1] === b[j-1] ? dp[i-1][j-1] + 1 : Math.max(dp[i-1][j], dp[i][j-1]);
  // Traceback
  const trace = [];
  let i = n, j = m;
  while (i > 0 || j > 0) {
    if (i > 0 && j > 0 && a[i-1] === b[j-1]) { trace.push(" " + a[i-1]); i--; j--; }
    else if (j > 0 && (i === 0 || dp[i][j-1] >= dp[i-1][j])) { trace.push("+" + b[j-1]); j--; }
    else { trace.push("-" + a[i-1]); i--; }
  }
  trace.reverse();
  // Collapse unchanged runs to context-only (3 lines around changes)
  const CONTEXT = 3;
  const changed = trace.map(l => l[0] !== " ");
  const out = [];
  let skipping = false;
  for (let k = 0; k < trace.length; k++) {
    const near = changed.slice(Math.max(0, k - CONTEXT), k + CONTEXT + 1).some(Boolean);
    if (near) { skipping = false; out.push(trace[k]); }
    else if (!skipping) { skipping = true; out.push("@@ ... @@"); }
  }
  return out.join("\n");
}

// ---- write_file ------------------------------------------------------------
async function writeFile(workspaceDir, args, requestConfirm, onBeforeWrite) {
  if (!args.path) return "Error: 'path' argument is required.";
  if (args.content == null) return "Error: 'content' argument is required.";

  let target;
  try { target = resolveSafe(workspaceDir, args.path); }
  catch (e) { return "Error: " + e.message; }

  if (buildIgnore(workspaceDir)(args.path)) {
    return "Error: '" + args.path + "' is excluded by .agentignore and cannot be written.";
  }

  const exists = fs.existsSync(target);
  if (exists) {
    let oldContent = "";
    try { oldContent = fs.readFileSync(target, "utf8"); } catch (_) {}
    const diff = simpleDiff(oldContent, args.content);
    const approved = await requestConfirm({ kind: "write_file", path: args.path, diff });
    if (!approved) return "File write denied by user. The file was not modified.";
  }

  try {
    // Checkpoint the file's prior state (for one-click rollback) before writing.
    if (onBeforeWrite) { try { onBeforeWrite(args.path, target); } catch (_) {} }
    fs.mkdirSync(path.dirname(target), { recursive: true });
    fs.writeFileSync(target, args.content, "utf8");
    return (exists ? "Updated" : "Created") + " " + args.path + " (" + Buffer.byteLength(args.content) + " bytes).";
  } catch (e) {
    return "Error writing file: " + e.message;
  }
}

// ---- run_command -----------------------------------------------------------
const { spawn } = require("child_process");
const CMD_TIMEOUT_DEFAULT = 30;   // seconds
const CMD_TIMEOUT_MAX     = 300;

async function runCommand(workspaceDir, args, requestConfirm, onOutput, signal) {
  if (!args.command) return "Error: 'command' argument is required.";

  const approved = await requestConfirm({ kind: "command", command: args.command });
  if (!approved) return "Command denied by user.";

  const timeoutSec = Math.min(Math.max(1, Number(args.timeout) || CMD_TIMEOUT_DEFAULT), CMD_TIMEOUT_MAX);
  const isWin = process.platform === "win32";

  return new Promise((resolve) => {
    let proc;
    try {
      proc = spawn(isWin ? "cmd" : "sh", [isWin ? "/c" : "-c", args.command], {
        cwd: workspaceDir,
        stdio: ["ignore", "pipe", "pipe"]
      });
    } catch (e) { return resolve("Error spawning process: " + e.message); }

    let output = "", finished = false;

    const done = (code, err) => {
      if (finished) return;
      finished = true;
      clearTimeout(timer);
      resolve(err ? "Error: " + err.message
        : output + "\n[Exit " + code + (code !== 0 ? " — command failed" : "") + "]");
    };

    const timer = setTimeout(() => {
      onOutput("\n[Timed out after " + timeoutSec + "s]\n");
      proc.kill("SIGTERM");
      setTimeout(() => { if (!finished) proc.kill("SIGKILL"); }, 2000);
      done(null, new Error("Command timed out after " + timeoutSec + "s"));
    }, timeoutSec * 1000);

    signal?.addEventListener("abort", () => {
      if (!finished) { proc.kill("SIGTERM"); setTimeout(() => { if (!finished) proc.kill("SIGKILL"); }, 2000); }
    });

    const onData = chunk => { const t = chunk.toString(); output += t; onOutput(t); };
    proc.stdout.on("data", onData);
    proc.stderr.on("data", onData);
    proc.on("close", code => done(code ?? 1));
    proc.on("error", err => done(null, err));
  });
}

// ---- dispatch --------------------------------------------------------------
// Async — write_file and run_command need to await user confirmation.
// callbacks: { requestConfirm, onOutput, signal }
async function executeTool(name, args, workspaceDir, callbacks = {}) {
  const { requestConfirm = async () => true, onOutput = () => {}, signal, onBeforeWrite } = callbacks;
  switch (name) {
    case "list_dir":    return listDir(workspaceDir, args || {});
    case "read_file":   return readFile(workspaceDir, args || {});
    case "search":      return search(workspaceDir, args || {});
    case "write_file":  return writeFile(workspaceDir, args || {}, requestConfirm, onBeforeWrite);
    case "run_command": return runCommand(workspaceDir, args || {}, requestConfirm, onOutput, signal);
    default:            return "Error: unknown tool '" + name + "'.";
  }
}

// ---- OpenRouter tool definitions -------------------------------------------
// READ_TOOL_NAMES — exposed in Plan mode only.
// ALL tools — exposed in Act mode.
const READ_TOOL_NAMES = new Set(["list_dir", "read_file", "search"]);

const TOOL_DEFINITIONS = [
  {
    type: "function",
    function: {
      name: "list_dir",
      description:
        "List the files and subdirectories at a path inside the workspace. " +
        "Shows type (f=file, d=directory), name, and file size. Defaults to the workspace root.",
      parameters: {
        type: "object",
        properties: {
          path: {
            type: "string",
            description: "Relative path within the workspace to list. Defaults to '.' (workspace root)."
          }
        }
      }
    }
  },
  {
    type: "function",
    function: {
      name: "read_file",
      description:
        "Read the full text content of a file in the workspace. " +
        "Returns the content as a string. Files larger than 50 KB are truncated.",
      parameters: {
        type: "object",
        required: ["path"],
        properties: {
          path: {
            type: "string",
            description: "Relative path to the file within the workspace."
          }
        }
      }
    }
  },
  {
    type: "function",
    function: {
      name: "search",
      description:
        "Search for a regular-expression pattern across files in the workspace. " +
        "Returns matching lines with file path and line number. Skips node_modules, .git, dist, and build directories automatically.",
      parameters: {
        type: "object",
        required: ["pattern"],
        properties: {
          pattern: {
            type: "string",
            description: "Regular expression to search for."
          },
          path: {
            type: "string",
            description: "Directory or file to search within. Defaults to the workspace root."
          },
          file_pattern: {
            type: "string",
            description: "Optional glob pattern to filter which files are searched, e.g. '*.js' or '*.py'."
          }
        }
      }
    }
  },
  {
    type: "function",
    function: {
      name: "write_file",
      description:
        "Create or overwrite a file in the workspace with the given content. " +
        "Requires user approval before overwriting an existing file (a diff is shown). " +
        "Parent directories are created automatically.",
      parameters: {
        type: "object",
        required: ["path", "content"],
        properties: {
          path: { type: "string", description: "Relative path within the workspace." },
          content: { type: "string", description: "Full UTF-8 content to write to the file." }
        }
      }
    }
  },
  {
    type: "function",
    function: {
      name: "run_command",
      description:
        "Run a shell command inside the workspace directory. Requires user approval before execution. " +
        "stdout and stderr are streamed in real time and fed back to you when done. " +
        "Use for: running tests, linters, builds, package installs, etc.",
      parameters: {
        type: "object",
        required: ["command"],
        properties: {
          command: {
            type: "string",
            description: "Shell command to execute (sh -c on Unix, cmd /c on Windows)."
          },
          timeout: {
            type: "number",
            description: "Timeout in seconds before the process is killed (default 30, max 300)."
          }
        }
      }
    }
  }
];

// ---- workspace file listing (Phase 6 @-context) ----------------------------
// Returns up to `limit` relative file paths matching `query`, respecting ignore.
function listWorkspaceFiles(workspaceDir, query = "", limit = 40) {
  const isIgnored = buildIgnore(workspaceDir);
  const q = (query || "").toLowerCase();
  const out = [];
  function walk(dir, base) {
    if (out.length >= limit) return;
    let entries;
    try { entries = fs.readdirSync(dir, { withFileTypes: true }); }
    catch (_) { return; }
    for (const e of entries) {
      if (out.length >= limit) break;
      const rel = (base ? base + "/" : "") + e.name;
      if (isIgnored(rel)) continue;
      if (e.isDirectory()) walk(path.join(dir, e.name), rel);
      else if (e.isFile() && (!q || rel.toLowerCase().includes(q))) out.push(rel);
    }
  }
  try { walk(path.resolve(workspaceDir), ""); } catch (_) {}
  return out;
}

module.exports = { TOOL_DEFINITIONS, READ_TOOL_NAMES, executeTool, listWorkspaceFiles, buildIgnore };
