---
title: Spark Mental Models
tags: [spark, pyspark, internals, distributed]
---

# 03 — Spark Mental Models (read this until it's reflex)

[← Free Edition Gotchas](02-Databricks-Free-Edition-Serverless-Gotchas.md) · Next: [Optimization Playbook →](04-Spark-Optimization-Playbook.md)

The interview explicitly says: *"We will discuss how your code executes. Be prepared to talk about how your code scales."* This note is the vocabulary for that conversation. Aim to explain each idea in two sentences without notes.

## 1. Lazy evaluation: transformations vs actions

Spark builds a **plan**, it doesn't run anything, until you call an **action**.

- **Transformations** (lazy, return a new DataFrame): `select`, `filter`, `withColumn`, `join`, `groupBy().agg()`, `repartition`. Nothing executes; Spark just extends the logical plan.
- **Actions** (eager, trigger a job): `count`, `collect`, `show`, `write`, `take`, `toPandas`.

**Why it matters:** the optimizer (Catalyst) sees the *whole* chain before executing, so it can reorder filters, prune columns, and pick join strategies. It also means a slow cell might be slow because of work *defined many cells earlier*. And it means calling `count()` "just to check" actually executes the whole pipeline — don't do it casually in a loop.

> One-liner for the interview: *"Transformations are lazy and just build the plan; the cost only lands when I call an action like `write` or `count`."*

## 2. Partitions: the unit of parallelism

A DataFrame is split into **partitions**; each partition is processed by one **task** on one core. Parallelism ≈ number of partitions you can run at once.

- **Too few partitions** → idle cores, no parallelism, possible memory pressure per task.
- **Too many tiny partitions** → scheduling overhead dominates (the "small files / tiny tasks" problem).
- Rule of thumb: aim for partitions in the low **hundreds of MB** each. `spark.sql.shuffle.partitions` (default 200) controls how many partitions a shuffle produces; AQE will coalesce these down adaptively.

## 3. Narrow vs wide transformations (the single most important distinction)

- **Narrow:** each output partition depends on **one** input partition. No data moves between machines. Cheap. (`select`, `filter`, `withColumn`, `union`.)
- **Wide:** output partitions depend on **many** input partitions, so Spark must **shuffle** — redistribute data across the network by key. Expensive. (`groupBy`, `join` on non-broadcast, `distinct`, `orderBy`, `repartition`.)

**The shuffle is where time and money go.** It writes intermediate data to disk, sends it over the network, and re-reads it. Almost every Spark optimization is "do fewer/smaller shuffles."

> One-liner: *"Narrow transforms stay on the same machine; wide transforms shuffle data across the network, and the shuffle is usually the bottleneck."*

## 4. Jobs → Stages → Tasks

- A **job** is triggered by one action.
- A job is split into **stages** at **shuffle boundaries** (each `Exchange` in the plan = a new stage).
- A stage runs as many **tasks** as there are partitions, in parallel across cores.

So "how many stages does my code have?" ≈ "how many shuffles?" Fewer shuffles = fewer stages = usually faster.

## 5. The Catalyst optimizer + AQE

- **Catalyst** rewrites your logical plan: pushes filters down to the scan (predicate pushdown), prunes unused columns (projection pruning), reorders operations, and chooses join strategies. This is why declarative DataFrame/SQL code beats hand-rolled Python loops.
- **AQE (Adaptive Query Execution)** — **on by default** in modern Spark/Databricks — re-optimizes *during* execution using real runtime statistics. It:
  - **coalesces** shuffle partitions (fixes the "200 partitions but tiny data" problem automatically),
  - converts sort-merge joins to **broadcast** joins when a side turns out small,
  - **splits skewed** partitions in joins.

> Talking point: *"I can lean on AQE for partition coalescing and skew handling, but it can't fix bad logic — e.g. it won't avoid a shuffle I didn't need, or fix a Python UDF that blocks vectorization."*

## 6. Tungsten & Photon (the execution engine)

- **Tungsten** is Spark's engine for compact binary in-memory formats and whole-stage code generation (it compiles a chain of operators into tight JVM bytecode instead of interpreting row-by-row).
- **Photon** is Databricks' native (C++) vectorized engine that accelerates many SQL/DataFrame operations. You don't write Photon code; you *enable* it. **Python UDFs break out of Photon/Tungsten** (data must serialize to a Python process), which is a key reason to prefer built-in functions.

## 7. Joins: pick the right strategy

| Strategy | When Spark uses it | Cost |
|----------|-------------------|------|
| **Broadcast hash join** | One side is small (≤ `spark.sql.autoBroadcastJoinThreshold`, ~10 MB default; AQE can trigger it at runtime) | Cheap — small side shipped to every executor, **no shuffle of the big side** |
| **Sort-merge join** | Both sides large | Expensive — **shuffles both sides** by join key, then sorts |
| **Shuffle hash join** | Mid-size, specific conditions | Shuffles, builds hash table |

The big lever: if one side is a small dimension table, **force a broadcast** with `broadcast(dim_df)` so the giant fact table never shuffles. (See [04](04-Spark-Optimization-Playbook.md).)

## 8. Data skew

If one join/group key has vastly more rows than others, one task does most of the work while the rest finish and sit idle — the stage is as slow as its slowest task. Symptoms: one task runs 10× longer; one huge partition in your `spark_partition_id` count. Fixes: let **AQE skew handling** do it, **salt** the hot key, or broadcast the small side to avoid the shuffle entirely.

## 9. The driver vs the executors (don't crash the driver)

- The **driver** runs your notebook code and the plan; **executors** do the distributed work.
- `collect()` and `toPandas()` pull **all** result data into the **driver's** memory. On a big DataFrame this OOMs the driver or chokes the notebook. Use them only on already-tiny results. Prefer `write` (distributed) or `show(n)` / `limit(n)` for a peek.

## 10. File layout & Delta (storage side of performance)

- **Columnar formats (Parquet/Delta)** enable column pruning and predicate pushdown — Spark reads only the columns and row-groups it needs.
- **Partition pruning:** if a table is partitioned by `date`, a filter on `date` skips whole folders.
- **Small-files problem:** thousands of tiny files = thousands of tiny tasks = overhead. Fix with `OPTIMIZE` (Delta) / writing fewer, larger files.
- **Data skipping / Z-ordering / Liquid Clustering** co-locate related data so filters skip more files. Liquid Clustering is the newer, maintenance-free alternative to fixed partitioning + Z-order.

---

### Self-test (say the answers out loud)

1. Why is `filter` cheap but `groupBy` expensive?
2. What creates a new stage?
3. Your job has 200 tasks but the data is tiny — what's happening and what fixes it?
4. When would you force a broadcast join, and what's the risk if the "small" side isn't small?
5. Why can a Python UDF make an otherwise-fast query slow?
6. Why is `df.toPandas()` dangerous on a large DataFrame?

*(Answers live throughout this note and in [08 Mock Q&A](08-Mock-QA-and-Talking-Points.md).)*
