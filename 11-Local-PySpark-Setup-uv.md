---
title: Local Dev Setup — mise + uv + Databricks CLI
tags: [setup, mise, uv, pyspark, databricks-cli, local]
---

# 11 — Local Dev Setup (mise + uv + Databricks CLI)

[← Hands-On Lab Index](10-Hands-On-Lab-Index.md) · Next: [Delta Lake Deep Dive →](12-Delta-Lake-Deep-Dive.md)

> One command (`cd`) gives you the right Python, Java, uv, Databricks CLI, an auto-activated `.venv`, and loaded `.env`. Lets you practice the labs locally **and** push them to Free Edition without leaving the terminal.

## The stack and who owns what

| Tool | Owns |
|------|------|
| **mise** | Tool versions (Python, Java, uv, Databricks CLI), env vars, `.env` loading, venv activation, task runner |
| **uv** | Python packages (PySpark, Delta, Jupyter, jupytext) into `.venv/` |
| **Databricks CLI** | Auth to Free Edition + workspace import/export of notebooks |
| **`.env`** (gitignored) | Per-machine secrets: workspace host, profile, notebook destination path |

> **Versions are pinned to match Databricks Serverless Environment v4** (current as of 2026): **Python 3.12.3, JDK 17, PySpark 3.5.x client APIs, Delta Lake**. Whatever runs locally with this setup will run on Free Edition. See the [official environment version reference](https://docs.databricks.com/aws/en/release-notes/serverless/environment-version/).

The repo files that make this work: [`mise.toml`](../mise.toml), [`pyproject.toml`](../pyproject.toml), [`.env.example`](../.env.example), [`scripts/init-env.sh`](../scripts/init-env.sh).

---

## 1. Install mise (once per machine)

```bash
curl https://mise.run | sh
# then add this to your shell rc (~/.zshrc, ~/.bashrc, etc.):
eval "$(mise activate bash)"     # or zsh / fish
```

Reopen the terminal. `mise --version` should print a version.

> mise auto-activates per-directory: when you `cd` into a repo that has a `mise.toml`, the tools, env vars, and `.env` for **that repo** become live; when you `cd` out, they're gone. No global pollution.

## 2. Clone and `cd` in

```bash
git clone <this repo>
cd spark-databricks-study
```

The first time, mise will prompt you to trust the `mise.toml` (security gate against malicious repo configs):

```bash
mise trust
```

Then it will fetch and install everything pinned in `mise.toml`:

```bash
mise install        # installs Python 3.11, Temurin JDK 17, uv, databricks-cli
```

> If `mise install` can't resolve `databricks-cli`, your mise registry is older. Update mise (`mise self-update`) or fall back to `"ubi:databricks/cli" = "latest"` in `mise.toml`.

Verify:

```bash
mise ls            # shows installed versions, all green
python --version   # Python 3.11.x
java -version      # 17.x temurin
uv --version
databricks --version
```

## 3. Install Python deps

```bash
mise run setup     # equivalent to `uv sync` — creates .venv and installs pyspark+delta+jupyter+jupytext
```

`mise.toml` auto-activates `.venv` on `cd`, so `python` and `pip` already point inside it. You generally don't need to run `source .venv/bin/activate` manually.

## 4. Local PySpark smoke test

```bash
mise run smoke
```

Expected:

```
Spark 3.5.3
+---+
| id|
+---+
|  0|
…
|  4|
+---+
```

A wall of `WARN NativeCodeLoader…` is normal on Linux/macOS without Hadoop natives — ignore.

## 5. Configure Databricks CLI auth

Run the interactive initializer — it prompts for each value, validates the host format, and writes `.env` with mode `0600`:

```bash
mise run init-env         # prompts for HOST, NOTEBOOK_DEST, CATALOG, SCHEMA, …
```

mise reloads `.env` automatically (it's listed in `mise.toml`'s `_.file`). Then:

```bash
mise run db:login         # opens a browser → Free Edition login → writes ~/.databrickscfg
mise run db:whoami        # confirms you're authenticated
```

`db:login` runs `databricks auth login --host $DATABRICKS_HOST` under the hood and stores an OAuth token under the profile name in `$DATABRICKS_PROFILE` (default `DEFAULT`).

> **Where to find your host:** open Free Edition in a browser; the URL is `https://dbc-xxxxxxx-yyyy.cloud.databricks.com`. That whole thing (no trailing slash) is `DATABRICKS_HOST`.
>
> Prefer to edit by hand? `cp .env.example .env && $EDITOR .env` also works — `mise run init-env` is just a friendlier wrapper.

## 6. Push the notebooks into your workspace

```bash
mise run db:import        # uploads notebooks/databricks/ → $DATABRICKS_NOTEBOOK_DEST
```

Open Free Edition; the notebooks appear in the workspace tree at the destination you set. Attach to serverless and run.

To pull edits back (e.g., you fixed something in the workspace UI and want it back in git):

```bash
mise run db:export        # ⚠️ overwrites your local notebooks/databricks/ — review the git diff before committing
```

## 7. Running the local notebooks

The files under `notebooks/local/` are written in **jupytext percent format** (`# %%` for cells, `# %% [markdown]` for prose). Three ways to run them:

**A. VS Code (zero config)** — open a `.py` file. The Jupyter extension renders `# %%` blocks as cells and gives you a per-cell Run button. Pick the `.venv` interpreter when prompted.

**B. Convert to `.ipynb` first** —
```bash
mise run to-ipynb         # writes notebooks/local/*.ipynb next to the .py files
mise run lab              # opens Jupyter Lab in notebooks/local/
```

**C. Run end-to-end as a plain script** —
```bash
uv run python notebooks/local/01_local_warmup.py
```

## 8. Tear down between labs

```bash
mise run clean            # removes spark-warehouse, metastore_db, derby.log, checkpoints
```

On Databricks: `spark.sql("DROP TABLE IF EXISTS workspace.default.<tablename>")`.

---

## All mise tasks at a glance

```bash
mise tasks                 # list them
mise run <name>            # run one
```

| Task | What it does |
|------|--------------|
| `setup`        | `uv sync` — install Python deps |
| `init-env`     | Interactive `.env` setup (prompts for host, profile, notebook dest, catalog, schema) |
| `smoke`        | Verify local PySpark works |
| `lab`          | Launch Jupyter Lab on `notebooks/local/` |
| `to-ipynb`     | Convert percent-format `.py` → `.ipynb` |
| `clean`        | Remove local Spark scratch dirs |
| `db:version`   | Show Databricks CLI version |
| `db:login`     | OAuth login to your `$DATABRICKS_HOST` |
| `db:profiles`  | List configured auth profiles |
| `db:whoami`    | Show the authenticated user |
| `db:import`    | Push `notebooks/databricks/` → workspace |
| `db:export`    | Pull workspace edits back into the repo |

---

## Without mise (fallback)

If you can't or won't install mise, the moving pieces are:

1. Install Python 3.11, JDK 17, uv, and `databricks` CLI manually (see each tool's docs).
2. Activate the venv yourself: `uv sync && source .venv/bin/activate`.
3. Export the same env vars from `.env` yourself: `set -a && source .env && set +a`.
4. Run the underlying commands directly — open `mise.toml`'s `[tasks.*]` blocks to see what each shortcut maps to.

You lose auto-activation and the task shortcuts, but nothing else.

---

## Differences vs Databricks Free Edition (know these so you don't get false confidence)

| Topic | Local PySpark 3.5 | Free Edition (serverless) |
|-------|-------------------|---------------------------|
| `df.cache()` | **Works** | **Restricted** — materialize to Delta instead |
| `spark.sparkContext`, RDDs | **Work** | **Not available** |
| Spark UI | **Available** at `http://localhost:4040` while a job runs | **Not available**; use **Query Profile** |
| Genie Code AI | n/a | Available in the workspace UI |
| Photon engine | n/a (open-source Spark only) | Auto-enabled on serverless |
| AQE / auto-broadcast | On by default | On by default |
| Unity Catalog | n/a | Yes (`catalog.schema.table`) |
| Delta `MERGE` | Needs `delta-spark` configured (notebooks do this for you) | Built-in |

> **Takeaway:** local is fine for vocabulary and muscle memory. Any answer that involves *"on serverless you can't do X"* — do that lap on Free Edition once so you've felt it.

---

## Enabling Delta locally (if you need it outside the notebooks)

The local notebooks build the Spark session for you. If you're at an ad-hoc REPL:

```python
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip   # from delta-spark

builder = (SparkSession.builder
    .appName("local-delta")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.warehouse.dir", "spark-warehouse"))

spark = configure_spark_with_delta_pip(builder).getOrCreate()
```

After that, `CREATE TABLE … USING delta` and `MERGE INTO` work locally. `spark-warehouse/` and `metastore_db/` are gitignored.

---

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `mise: command not found` | Add `eval "$(mise activate bash)"` to your shell rc and reopen the terminal |
| `mise trust` warning when entering the repo | Run `mise trust` (one-time, security feature) |
| `JAVA_HOME is not set` | Should be set by mise automatically. Confirm with `mise env`. If empty, run `mise install java` again |
| `Python worker failed to connect back` | Mismatch between driver and worker Python. mise sets `PYSPARK_PYTHON` to the venv; confirm with `echo $PYSPARK_PYTHON` |
| `databricks auth login` opens browser but token never lands | Check `$DATABRICKS_HOST` has no trailing slash; rerun `mise run db:login` |
| `Permission denied` on `db:import` | Your workspace path in `$DATABRICKS_NOTEBOOK_DEST` must be under `/Workspace/Users/<your-login>/...` |
| Long wall of `WARN`s on local startup | Cosmetic. To silence at runtime: `spark.sparkContext.setLogLevel("ERROR")` (local only) |
