---
title: Hands-On Lab Index
tags: [databricks, spark, labs, hands-on]
---

# 10 — Hands-On Lab Index

[← PySpark Cheatsheet](09-Cheatsheet-PySpark.md) · Next: [Local PySpark Setup →](11-Local-PySpark-Setup-uv.md)

> Everything in this section is **runnable code**, not just reading. Two parallel tracks:
> 1. **Databricks track** — Databricks-format `.py` notebooks under `notebooks/databricks/`. Import them into your Free Edition workspace (Workspace → ⋮ → Import → File). They run **as-is** on serverless.
> 2. **Local track** — jupytext percent-format `.py` files under `notebooks/local/`. Open in VS Code (Jupyter extension) or convert to `.ipynb` with `./scripts/to_ipynb.sh`, then run with a local pip-installed PySpark. Use these when you can't or don't want to spin up Free Edition.

## Why a local track at all?

Free Edition is great but it spins up/down, has quotas, and you have to log in. For **muscle-memory drilling** — typing `from pyspark.sql import functions as F` 50 times until it's reflex — local is faster. Save Free Edition for the **dress-rehearsal** lap where you need serverless semantics (no `.cache()`, Query Profile UI, Genie Code).

## The labs

| # | Lab | Track | Maps to note | Time |
|---|-----|-------|--------------|------|
| 01 | Environment check (`print(spark.version)`, SQL cell, table write) | Databricks | [02](02-Databricks-Free-Edition-Serverless-Gotchas.md) | 5 min |
| 02 | Spark fundamentals — transformations vs actions, narrow vs wide, partition counts | Both | [03](03-Spark-Mental-Models.md) | 15 min |
| 03 | Optimization challenge (slow app — find the 6 issues) | Both | [05](05-Spark-Optimization-Challenge.md) | **25 min timed** |
| 04 | Optimization challenge — reference solution walkthrough | Both | [05](05-Spark-Optimization-Challenge.md) | 10 min |
| 05 | Broadcast joins & skew handling deep dive | Databricks | [04 Lever 1, 5](04-Spark-Optimization-Playbook.md) | 20 min |
| 06 | Delta `MERGE` + SCD2 + `OPTIMIZE` + time travel | Databricks | [12](12-Delta-Lake-Deep-Dive.md) | 20 min |
| 07 | Window functions — top-N per group, running totals, gap-and-island | Databricks | [13](13-Spark-SQL-Drills.md) | 15 min |
| 08 | Python data-quality engine — feature challenge | Both | [06](06-Python-Feature-Dev-Challenge.md) | **25 min timed** |

## Suggested order (matches the 7-day plan in the README)

1. **First time on the repo:** clone, run `uv sync`, open `notebooks/local/01_local_warmup.py` in VS Code → "Run All". Confirms local Spark works in <60 seconds. *Then* sign up for Free Edition and import `notebooks/databricks/01_environment_check.py`.
2. **Day 4 (Spark hands-on):** start a 25-minute timer, open `notebooks/databricks/03_optimization_challenge_start.py`, find as many of the 6 issues as you can. When the timer expires, open `04_optimization_challenge_solution.py` and diff.
3. **Day 5 (Python hands-on):** 25-minute timer, open `notebooks/databricks/08_dq_engine_python.py`, do the Tier-1 task; if time remains, Tier 2.
4. **Day 6+ (drills):** rotate through `02`, `05`, `06`, `07` to build vocabulary breadth.

## Repo layout

```
notebooks/
├── databricks/                  # Databricks-format .py — import to Free Edition
│   ├── 01_environment_check.py
│   ├── 02_spark_fundamentals.py
│   ├── 03_optimization_challenge_start.py
│   ├── 04_optimization_challenge_solution.py
│   ├── 05_broadcast_and_skew.py
│   ├── 06_delta_merge_scd.py
│   ├── 07_window_functions.py
│   └── 08_dq_engine_python.py
└── local/                       # Jupytext percent-format — local PySpark
    ├── 01_local_warmup.py
    ├── 02_local_optimization.py
    └── 03_local_dq_engine.py
scripts/
└── to_ipynb.sh                  # Convert local/*.py → *.ipynb
pyproject.toml                   # uv-managed deps
```

## Importing into Databricks Free Edition

1. Workspace pane → your user folder → **⋮** → **Import**.
2. Choose **File**, drop in `notebooks/databricks/*.py`. Databricks reads the `# Databricks notebook source` header and the `# COMMAND ----------` / `# MAGIC %md` markers automatically — you get a normal notebook with cells.
3. Attach to serverless compute. Run. The data-gen cells use `spark.range()` so there's no upload step.

> Tip: the `workspace.default` schema is writable by default on Free Edition. All notebooks write tables there. If your workspace uses a different catalog/schema, search-and-replace at the top of each notebook.
