---
title: Free Edition / Serverless Gotchas
tags: [databricks, serverless, spark-connect, setup]
---

# 02 — Databricks Free Edition / Serverless Gotchas

[← Overview](01-Interview-Overview-and-Strategy.md) · Next: [Spark Mental Models →](03-Spark-Mental-Models.md)

> **Why this note exists:** the interview runs on **Databricks Free Edition**, which is **serverless-only** and uses **Spark Connect** under the hood. Several "classic Spark" habits *throw exceptions* here. Knowing this cold is a genuine differentiator — most candidates suggest `.cache()` and then look surprised. You won't.

## The big mental shift: Spark Connect, not classic Spark

Free Edition gives you **serverless compute** only. Serverless uses **Spark Connect**, a thin client that sends an unresolved logical plan to a remote server. Two consequences matter live:

1. **No JVM access from the client.** Anything that reaches into the JVM — `SparkContext`, RDDs, broadcast *variables*, accumulators — is unavailable.
2. **Analysis is deferred to execution.** Name resolution and analysis happen at execute time, so some errors surface later than they would on classic Spark. Don't be thrown if a typo in a column name only blows up when you call an action.

## Things that DON'T work on serverless (memorize the short list)

| You might reach for… | What happens | Do this instead |
|----------------------|--------------|-----------------|
| `spark.sparkContext`, `sc`, `sqlContext` | **Not supported** (JVM attribute) | Use the `spark` (SparkSession) DataFrame/SQL APIs only |
| `df.rdd...`, `sc.parallelize(...)`, RDD APIs | **Not supported** | Use DataFrame APIs; build data with `spark.range()` |
| `df.cache()` / `df.persist()` / `df.unpersist()` | **Restricted on serverless** — may raise an exception | **Materialize** to a Delta table or temp view, then read it back |
| `CACHE TABLE` / `UNCACHE TABLE` / `CLEAR CACHE` | Restricted on serverless | Same — write a Delta table |
| `df.rdd.getNumPartitions()` | RDD path not supported | `df.rdd` is JVM-backed; instead use `spark.sql("SELECT spark_partition_id() ...")` or check the **query profile** |
| The **Spark UI** (stages/DAG tab) | **Not available** on serverless | Use the **Query Profile** (open it from the cell's query output / the SQL editor) |
| Setting arbitrary `spark.conf.set(...)` | Only a **supported subset** is allowed | Stick to known-supported confs (e.g. `spark.sql.shuffle.partitions`); don't assume cluster-level knobs |
| `sc.broadcast(my_dict)` (broadcast *variable*) | RDD/JVM, not supported | For *join* broadcasting use `from pyspark.sql.functions import broadcast; broadcast(df)` — **this DataFrame hint works fine** |
| Maven coordinates / compute-scoped libs | Not supported | Use notebook-scoped `%pip install` |
| `spark.createDataFrame(localdata)` with a huge object | Row size capped (~128 MB) | Generate data server-side with `spark.range()` |

> ⚠️ **The two that bite people most: `.cache()` and the Spark UI.** If the interviewer says "make this faster" and your instinct is "I'll cache the reused DataFrame," catch yourself: on serverless, *materialize to Delta* instead, and *diagnose with the query profile, not the Spark UI*. Saying this out loud is worth real points.

## Caching, the honest version

On classic Databricks compute, `df.cache()` is a normal optimization. On **serverless**, the DataFrame/SQL cache APIs are documented as **not supported** and can throw. Behavior can vary slightly by runtime, so:

- **Don't blindly suggest `.cache()`.** Mention the constraint.
- If you genuinely reuse a DataFrame several times, **write it once to a Delta table** (or a temp view backed by a `CREATE TABLE`) and read it back. That's the serverless-friendly equivalent of caching and it also survives across cells.
- If you want to *test* whether cache works in the actual interview workspace, run a tiny `spark.range(10).cache().count()` early during setup so there are no surprises.

```python
# Serverless-friendly "caching": materialize a reused result to Delta
(reused_df
   .write.mode("overwrite")
   .saveAsTable("workspace.default.reused_step"))      # written once

reused = spark.table("workspace.default.reused_step")   # cheap reads afterward
```

## What DOES work (your reliable toolkit)

- `spark.range(n)` to generate data without RDDs.
- The whole **DataFrame API**: `select`, `filter`/`where`, `groupBy`, `agg`, `join`, `withColumn`, window functions.
- **Spark SQL** via `spark.sql("...")`.
- `from pyspark.sql import functions as F` (all the built-ins — prefer these over Python UDFs).
- `broadcast(df)` join hint from `pyspark.sql.functions`.
- `df.explain(True)` / `df.explain("formatted")` to read the plan (works and is your best friend live).
- `repartition(...)`, `coalesce(...)`, `repartition(col)`.
- **Delta** tables (`saveAsTable`, `MERGE`, `OPTIMIZE`, `ZORDER` / Liquid Clustering).
- **AQE is on by default** — adaptive query execution coalesces shuffle partitions and handles skewed joins automatically; you can lean on it and mention it.

## Diagnosing performance without the Spark UI

```python
# 1) Read the physical plan — look for Exchange (shuffle) and BroadcastHashJoin vs SortMergeJoin
df.explain("formatted")

# 2) See how rows are distributed across partitions (skew detection) without RDD APIs
from pyspark.sql import functions as F
(df.groupBy(F.spark_partition_id().alias("pid"))
   .count()
   .orderBy(F.desc("count"))
   .show(20, truncate=False))

# 3) After running a query, open the QUERY PROFILE from the cell output to see
#    per-stage time, rows, spill, and where the wall-clock actually went.
```

In the physical plan, the words to hunt for: **`Exchange`** (a shuffle — expensive), **`SortMergeJoin`** (big-to-big join, shuffles both sides), **`BroadcastHashJoin`** (one side broadcast — cheap), **`Filter`/`PushedFilters`** (predicate pushdown working), and **column lists** under the scan (projection pruning working).

## Pre-interview setup checklist

- [ ] Sign up at **databricks.com/learn/free-edition** and log in *before* the interview.
- [ ] Open a notebook; run a Python cell (`print(spark.version)`) and a SQL cell (`%sql SELECT 1`).
- [ ] Confirm you can create a table: `spark.range(5).write.saveAsTable("workspace.default.smoke_test")`.
- [ ] Open the **Genie Code** pane (top-right icon) and ask it one thing to see the UX. (See [07](07-AI-Stewardship-Genie-Code.md).)
- [ ] Run `spark.range(10).cache().count()` once to learn whether cache errors in this workspace.
- [ ] Find the **Query Profile** for a query you just ran so you know where the button is *before* the clock is running.
- [ ] Know how to `%pip install` a package in case you need one.
- [ ] Note the catalog/schema you can write to (commonly `workspace.default`).

> Free Edition is quota-limited and serverless auto-scales to zero. If compute seems to "pause," it's spinning up. Don't panic mid-interview; mention it and keep narrating.
