# Setup — Databricks FDE Interview Prep

## 1. Databricks Free Edition

1. Sign up at https://www.databricks.com/learn/free-edition (free, fully serverless).
2. Log in **before** interview day and get comfortable with the notebook UI, attaching compute, and running Python + SQL cells.
3. Free Edition is serverless — you don't size a cluster; some features are rate-limited/throttled. Absolute timings and some query plans (e.g. AQE auto-broadcasting a small dimension) will differ from any hardcoded numbers in the solution notebook. Noticing and narrating that difference is a good interview signal, not a problem.

## 2. Import the notebooks

For each file in `notebooks/`:

1. In the workspace sidebar: **Workspace → (your folder) → Import**.
2. Choose **File**, select `slow_spark_app_BROKEN.py` (and again for `slow_spark_app_SOLUTION.py`).
3. The `# Databricks notebook source` header and `# COMMAND ----------` separators make these import as native notebooks with cells — no conversion needed.
4. Open the notebook, click **Connect** (top right) to attach serverless compute.

> Alternative: clone the repo into Databricks via **Repos → Add Repo** and point it at your GitHub URL, then open the notebooks in place.

## 3. How to practice

1. Open `slow_spark_app_BROKEN.py`. Run top to bottom once — it works, just slowly.
2. For each numbered section: run the cell, note the wall-clock time, then open the **Query Profile** (SQL/notebook cell output → *View → Query Profile*, or the **Spark UI** via the compute page).
3. Find the expensive stage: shuffle read/write size, a straggler task, or the join strategy in the plan.
4. State one hypothesis out loud, make **one** change, re-run, compare the time.
5. End each fix with a one-sentence customer justification.
6. Check yourself against `slow_spark_app_SOLUTION.py` — but only after attempting it cold.

## 4. Reading the plan / UI — quick reference

- `df.explain("formatted")` — look for `BroadcastHashJoin` (good for large+small) vs `SortMergeJoin` + `Exchange` (shuffle), and `PushedFilters` (predicate pushdown working).
- **Query Profile** — per-stage time, rows, and bytes; the fastest way to spot skew (one task dwarfs the rest) and oversized shuffles.
- Confirm config in a cell: `spark.conf.get("spark.sql.adaptive.enabled")`, `spark.conf.get("spark.sql.autoBroadcastJoinThreshold")`, `spark.conf.get("spark.sql.shuffle.partitions")`.

## 5. Databricks Assistant

The built-in **Databricks Assistant** is permitted in the interview (other external AI tools are not). Practice with it now:

- Give it context, not just a verb: schema, expected output, constraints.
- Audit every suggestion before accepting — does it compile, handle nulls/dupes/empty input, and avoid hidden `groupByKey` / `collect()` / cross joins / Python UDFs?
- Rehearse the "bail-out": if stuck >2 min on syntax, let the Assistant fill it while you keep the architecture moving.

## 6. More reps

[jrlasak/databricks-code-practice](https://github.com/jrlasak/databricks-code-practice) — 104 exercises (ELT + Delta Lake). Do each yourself **before** asking the Assistant, then diff — that builds the audit instinct that's graded.

## Day-of checklist

- **Optimization framework:** read less data → move less data → handle skew → reuse work → right-size parallelism → avoid Python UDFs (always verify in the UI).
- **"What breaks at 1 TB?"** collect-to-driver, skew, shuffle blow-up, tiny files, Python UDF cost.
- Narrate everything · audit the Assistant · end each fix with a customer one-liner.