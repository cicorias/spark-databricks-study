# Study Q&A web app

A simple chat UI that answers questions about **this repository** (Spark / Delta Lake /
Databricks / Python study notes and code). It uses the
[GitHub Copilot SDK](https://github.com/github/copilot-sdk), so it relies on your existing
Copilot CLI authentication — no separate API key is required.

The assistant runs in **read-only** mode: it can read and search the repo's files to ground
its answers (with file citations) but cannot modify, create, or delete anything, and cannot
run shell commands.

## Prerequisites

- Node.js 20+ (tested on 24) and [pnpm](https://pnpm.io) — both provided via mise (`mise.toml`).
- GitHub Copilot CLI installed on your `PATH` and authenticated (`copilot --version`). The
  server drives this CLI via the Copilot SDK.

## Run

```bash
cd webapp
pnpm install
pnpm start
```

Or, from the repo root, use the mise task (installs deps and starts the server):

```bash
mise run webapp
```

Then open <http://localhost:4173>.

## Configuration

Set via environment variables:

| Variable           | Default                         | Purpose                                              |
| ------------------ | ------------------------------- | ---------------------------------------------------- |
| `PORT`             | `4173`                          | HTTP port                                            |
| `COPILOT_MODEL`    | `claude-sonnet-4.6`             | Model name (must be one your Copilot plan offers)    |
| `STUDY_REPO_ROOT`  | repo root (parent of `webapp/`) | Directory whose docs/code the assistant answers from |
| `COPILOT_CLI_PATH` | auto-detected on `PATH`         | Explicit path to the `copilot` CLI binary            |

The available models for your account are printed to the console on startup.

## How it works

- `src/server.ts` — Express server. Starts one `CopilotClient` pointed at the repo root via
  `workingDirectory`. `POST /api/chat` opens (or reuses) a streaming Copilot session and
  relays `assistant.message_delta` events to the browser as Server-Sent Events. A custom
  `onPermissionRequest` handler approves only read operations.
- `public/index.html` — single-page chat UI (no build step) that consumes the SSE stream.
  Answers are rendered as rich Markdown — including GFM **tables**, headings, and lists — via
  [`marked`](https://marked.js.org), sanitized with [`DOMPurify`](https://github.com/cure53/DOMPurify).
  Both library browser builds are served by the backend under `/vendor/`.

### Clickable file citations

When an answer mentions a repository file, the UI turns it into a clickable 📄 link:

- `GET /api/files` lists citable files (via `git ls-files`, filtered to docs/code extensions).
  The browser linkifies any of those filenames (full paths and unambiguous basenames) that
  appear in answers.
- Clicking a link calls `POST /api/open { path }`, which validates the path stays inside the
  repository and then opens the file with the host's default application. The open logic
  mirrors `scripts/teach.py:open_file` so it behaves the same on **WSL2** (`wslview`, falling
  back to `explorer.exe` + `wslpath`) and **macOS** (`open`), with `xdg-open` on plain Linux.

### Copilot CLI resolution (pnpm note)

pnpm's isolated `node_modules` layout breaks the SDK's built-in lookup of its bundled CLI, so
the server resolves the CLI explicitly: `COPILOT_CLI_PATH` if set, otherwise the `copilot`
binary found on `PATH`. `pnpm-workspace.yaml` (`allowBuilds: esbuild`) and `.npmrc`
(`node-linker=hoisted`) are required for the dependencies to install and run correctly.

## Notes

- Conversation continuity: the browser keeps the returned `sessionId` and sends it back so
  follow-up questions share context.
- `GET /health` returns status, model, repo path, and active session count.
