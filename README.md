# chatty

A personal, localhost-only AI workbench built on OpenRouter. Two panels in one frosted-glass UI:

- **CHAT** — plain streaming conversation with any OpenRouter model.
- **CODE** — a full agentic coding assistant (Plan/Act modes, tools, terminal, approvals, checkpoints, cost) scoped to a single workspace directory.

Frontend is a single self-contained `index.html`. Backend is a small Node/Express server that holds your API key, runs the agent loop, and owns all filesystem/terminal access.

---

## Run it

```bash
cd server
npm install
cp .env.example .env        # then paste your OpenRouter key into OPENROUTER_API_KEY
npm start                   # → http://localhost:3000
```

Open **http://localhost:3000** in your browser.

### `.env`

```
OPENROUTER_API_KEY=sk-or-v1-...   # your key — stays on the server, never sent to the browser
PORT=3000
WORKSPACE_DIR=./workspace          # the ONLY directory the CODE agent may touch
```

Point `WORKSPACE_DIR` at any project on your machine to work on it.

---

## Features

### CHAT panel
- Live model selector (fetched from OpenRouter, searchable), real-time streaming.
- Global system rules (all chats) + per-chat rules + Skills, layered into one system message.
- Copy, clear, graceful errors.

### CODE panel (the agent)
- **Plan mode** — read-only investigation → a written plan. **Act mode** — executes. Each mode has its own model selector.
- **Tools** (all workspace-scoped): `list_dir`, `read_file`, `search`, `write_file`, `run_command`.
- **Streaming agent loop** with robust tool-call accumulation + JSON repair for models that mangle arguments.
- **Confirm gate** — overwriting a file shows a diff; running a command shows the command. Approve/Deny. Commands stream live output and have a timeout.
- **Permissions** (🛡) — independent auto-approve toggles for reads / writes / commands, a **Bypass (YOLO)** mode, and a max-iterations cap. Always workspace-scoped.
- **Stop** — halts the loop and kills any running command mid-task.
- **`.agentignore`** — exclude files/dirs from everything the agent can see or touch (defaults: `node_modules`, `.git`, `.env`, `.data`).
- **@-context** — type `@` to attach a workspace file; pinned files inject into context.
- **Context meter** — live token/context-window % with auto-trim near the limit.
- **Task history** — every session autosaves and is reopenable.
- **Checkpoints** — every file write is snapshotted; one-click rollback.
- **Cost tracking** — real token usage + cost per task from OpenRouter.

### Skills
Reusable instruction modules you toggle on per session (✦ Skills in the sidebar). Plus an always-on **workspace rules** file — drop `AGENTS.md` (or `.chattyrules` / `.cursorrules`) in your workspace and the agent always reads it.

---

## Architecture

```
browser (index.html)  ──HTTP/SSE──►  server/ (Express, localhost)  ──►  OpenRouter
                                          │
                                          └── tools.js → WORKSPACE_DIR (read/write/shell)
```

- Only dependency: `express`. Upstream calls use Node 18+ built-in `fetch`.
- No database — tasks and checkpoints are flat files under `server/.data/` (git-ignored).
- The key lives in `server/.env` and is never exposed to the browser.

## Security notes
- The agent can only read/write/run inside `WORKSPACE_DIR`; path escapes are rejected.
- Writes and commands are gated by default — nothing destructive runs without your click (unless you enable auto-approve or Bypass).
- This is a personal localhost tool with no auth. Don't expose the port publicly.
