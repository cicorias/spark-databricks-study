import { execFileSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { delimiter, dirname, join, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import express from "express";
import {
  CopilotClient,
  RuntimeConnection,
  type CopilotSession,
  type PermissionRequest,
  type PermissionRequestResult,
} from "@github/copilot-sdk";

const __dirname = dirname(fileURLToPath(import.meta.url));

const PORT = Number(process.env.PORT ?? 4173);
const MODEL = process.env.COPILOT_MODEL ?? "claude-sonnet-4.6";
// The repository whose docs/code the assistant answers questions about.
const REPO_ROOT = resolve(process.env.STUDY_REPO_ROOT ?? resolve(__dirname, "..", ".."));

// File extensions whose files can be cited and opened from the UI.
const OPENABLE_EXTENSIONS = new Set([
  ".md", ".py", ".ipynb", ".sql", ".sh", ".toml", ".yaml", ".yml", ".json", ".html",
]);

const SYSTEM_MESSAGE = `
<role>
You are a study assistant for the repository located at the current working directory.
The repository is a mix of Markdown study notes, rendered HTML lesson pages (under
teach/lessons/*.html), and code (PySpark, Databricks, notebooks) covering Spark, Delta Lake,
Databricks, and Python data-engineering topics.
</role>

<answering_rules>
- Answer ONLY from the content of files in this repository. Use your read/search tools
  (grep, glob, view) to find relevant material before answering.
- Treat the HTML lesson pages under teach/lessons/ as first-class sources, equal to the
  Markdown notes. When a question maps to a lesson, read and cite the lesson .html file.
- If the repository does not cover the topic, say so plainly instead of guessing.
- Always cite the specific file(s) you drew the answer from, e.g.
  "see 04-Spark-Optimization-Playbook.md" or "see teach/lessons/12-delta-lake-fundamentals.html".
- Prefer concise, well-structured answers. Use short code blocks when quoting code.
- Never modify, create, or delete files. You are strictly read-only.
</answering_rules>
`;

// Read-only guard: allow the agent to read/search the repo, deny anything that mutates
// the filesystem or runs shell/MCP/network side effects.
function readOnlyPermissionHandler(
  request: PermissionRequest,
): PermissionRequestResult {
  switch (request.kind) {
    case "read":
    case "memory":
      return { kind: "approve-once" };
    default:
      return {
        kind: "reject",
        feedback: "This assistant is read-only; only reading repository files is permitted.",
      };
  }
}

// ── Cross-platform helpers ──────────────────────────────────────────────────

function whichExe(name: string): string | undefined {
  const dirs = (process.env.PATH ?? "").split(delimiter).filter(Boolean);
  for (const dir of dirs) {
    const candidate = join(dir, name);
    if (existsSync(candidate)) return candidate;
  }
  return undefined;
}

function isWsl(): boolean {
  if (process.platform !== "linux") return false;
  try {
    return readFileSync("/proc/version", "utf8").toLowerCase().includes("microsoft");
  } catch {
    return false;
  }
}

// Locate the Copilot CLI the SDK should drive. Under pnpm the SDK's bundled-CLI
// lookup fails, so prefer an explicit path / the CLI on PATH and fall back to
// the SDK default.
function resolveCopilotCli(): string | undefined {
  if (process.env.COPILOT_CLI_PATH && existsSync(process.env.COPILOT_CLI_PATH)) {
    return process.env.COPILOT_CLI_PATH;
  }
  return whichExe("copilot");
}

// Open a file with the host's default handler. Mirrors scripts/teach.py:open_file
// so behavior is identical on WSL2 and macOS.
function openOnHost(absPath: string): void {
  if (isWsl()) {
    const wslview = whichExe("wslview");
    if (wslview) {
      execFileSync(wslview, [absPath]);
    } else {
      const winPath = execFileSync("wslpath", ["-w", absPath]).toString().trim();
      execFileSync("explorer.exe", [winPath]);
    }
  } else if (process.platform === "darwin") {
    execFileSync("open", [absPath]);
  } else {
    const xdg = whichExe("xdg-open");
    if (!xdg) throw new Error("No opener found (need xdg-open on Linux).");
    execFileSync(xdg, [absPath]);
  }
}

// List repository files that can be cited/opened, as paths relative to REPO_ROOT.
// Uses `git ls-files` so .gitignore'd and build artifacts are excluded.
function listRepoFiles(): string[] {
  try {
    const out = execFileSync("git", ["ls-files"], { cwd: REPO_ROOT }).toString();
    return out
      .split("\n")
      .map((l) => l.trim())
      .filter((l) => l && OPENABLE_EXTENSIONS.has(l.slice(l.lastIndexOf(".")).toLowerCase()));
  } catch {
    return [];
  }
}

const app = express();
app.use(express.json());
app.use(express.static(resolve(__dirname, "..", "public")));

// Serve the browser builds of the Markdown renderer + sanitizer.
const NODE_MODULES = resolve(__dirname, "..", "node_modules");
app.use("/vendor/marked", express.static(join(NODE_MODULES, "marked", "lib")));
app.use("/vendor/dompurify", express.static(join(NODE_MODULES, "dompurify", "dist")));

const cliPath = resolveCopilotCli();
const client = new CopilotClient({
  workingDirectory: REPO_ROOT,
  ...(cliPath ? { connection: RuntimeConnection.forStdio({ path: cliPath }) } : {}),
});
const sessions = new Map<string, CopilotSession>();

async function getSession(sessionId?: string): Promise<CopilotSession> {
  if (sessionId) {
    const existing = sessions.get(sessionId);
    if (existing) return existing;
  }
  const session = await client.createSession({
    model: MODEL,
    streaming: true,
    systemMessage: { content: SYSTEM_MESSAGE },
    onPermissionRequest: readOnlyPermissionHandler,
  });
  sessions.set(session.sessionId, session);
  return session;
}

app.get("/health", (_req, res) => {
  res.json({ status: "ok", model: MODEL, repo: REPO_ROOT, sessions: sessions.size });
});

// List of citable/openable repo files, so the UI can linkify filenames in answers.
app.get("/api/files", (_req, res) => {
  res.json({ files: listRepoFiles() });
});

// Open a repository file with the host's default app (WSL2 / macOS / Linux).
app.post("/api/open", (req, res) => {
  const requested = String(req.body?.path ?? "").trim();
  if (!requested) {
    res.status(400).json({ error: "path is required" });
    return;
  }
  // Resolve and confine strictly within the repository.
  const abs = resolve(REPO_ROOT, requested);
  const rel = relative(REPO_ROOT, abs);
  if (rel.startsWith("..") || rel.startsWith(sep) || resolve(REPO_ROOT, rel) !== abs) {
    res.status(403).json({ error: "path escapes repository" });
    return;
  }
  if (!existsSync(abs)) {
    res.status(404).json({ error: "file not found" });
    return;
  }
  try {
    openOnHost(abs);
    res.json({ ok: true, opened: rel });
  } catch (err) {
    res.status(500).json({ error: (err as Error).message });
  }
});

app.post("/api/chat", async (req, res) => {
  const message = String(req.body?.message ?? "").trim();
  const sessionId = req.body?.sessionId ? String(req.body.sessionId) : undefined;

  if (!message) {
    res.status(400).json({ error: "message is required" });
    return;
  }

  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders();

  const send = (event: string, data: unknown) => {
    res.write(`event: ${event}\n`);
    res.write(`data: ${JSON.stringify(data)}\n\n`);
  };

  let session: CopilotSession;
  try {
    session = await getSession(sessionId);
  } catch (err) {
    send("error", { message: `Failed to start session: ${(err as Error).message}` });
    res.end();
    return;
  }

  send("session", { sessionId: session.sessionId });

  const unsubscribers: Array<() => void> = [];
  const cleanup = () => unsubscribers.forEach((fn) => fn());

  unsubscribers.push(
    session.on("assistant.message_delta", (event) => {
      send("delta", { text: event.data.deltaContent });
    }),
  );
  unsubscribers.push(
    session.on("session.idle", () => {
      send("done", {});
      cleanup();
      res.end();
    }),
  );

  req.on("close", () => {
    cleanup();
    session.abort().catch(() => {});
  });

  try {
    await session.send({ prompt: message });
  } catch (err) {
    send("error", { message: (err as Error).message });
    cleanup();
    res.end();
  }
});

async function main() {
  await client.start();
  try {
    const models = await client.listModels();
    const names = models.map((m: { id?: string; name?: string }) => m.id ?? m.name).filter(Boolean);
    console.log(`Available models: ${names.join(", ")}`);
    if (names.length && !names.includes(MODEL)) {
      console.warn(`⚠  Configured COPILOT_MODEL="${MODEL}" not in available list; requests may fail.`);
    }
  } catch {
    // listModels is best-effort; ignore if unavailable.
  }

  app.listen(PORT, () => {
    console.log(`Study Q&A app on http://localhost:${PORT}`);
    console.log(`Answering questions about: ${REPO_ROOT}`);
    console.log(`Model: ${MODEL}`);
    console.log(`Copilot CLI: ${cliPath ?? "(SDK bundled default)"}`);
  });
}

async function shutdown() {
  console.log("\nShutting down...");
  await client.stop().catch(() => {});
  process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
