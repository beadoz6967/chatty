"use strict";

const fs = require("fs");
const path = require("path");

const READ_LIMIT = 50 * 1024;   // 50 KB hard cap on read_file
const LIST_LIMIT = 300;          // max entries for list_dir
const SEARCH_LIMIT = 100;        // max matching lines for search

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

  let entries;
  try { entries = fs.readdirSync(target, { withFileTypes: true }); }
  catch (e) { return "Error reading directory: " + e.message; }

  if (entries.length === 0) return "(empty directory)";

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

  const SKIP_DIRS = new Set(["node_modules", ".git", ".next", "dist", "build", "__pycache__"]);
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
      const rel = path.join(base, e.name);
      if (e.isDirectory()) {
        if (SKIP_DIRS.has(e.name)) continue;
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

// ---- dispatch --------------------------------------------------------------
function executeTool(name, args, workspaceDir) {
  switch (name) {
    case "list_dir":  return listDir(workspaceDir, args || {});
    case "read_file": return readFile(workspaceDir, args || {});
    case "search":    return search(workspaceDir, args || {});
    default:          return "Error: unknown tool '" + name + "'.";
  }
}

// ---- OpenRouter tool definitions -------------------------------------------
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
  }
];

module.exports = { TOOL_DEFINITIONS, executeTool };
