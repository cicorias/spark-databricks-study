---
title: Mock Q&A & Talking Points
tags: [interview, spark, qa, drills]
---

# 08 — Mock Q&A & Talking Points

[← AI Stewardship](07-AI-Stewardship-Genie-Code.md) · Next: [PySpark Cheatsheet →](09-Cheatsheet-PySpark.md)

> Rehearse these **out loud**. The goal isn't word-perfect recall; it's being able to reason through each one fluently while someone watches. Cover the answer and try first.

## A. Spark internals (the "how does it execute" questions)

**Q1. Transformation vs action — and why does it matter?**
<details><summary>Answer</summary>Transformations (`select`, `filter`, `join`, `groupBy`) are lazy — they build a plan. Actions (`count`, `collect`, `write`, `show`) trigger execution. It matters because the optimizer sees the whole chain before running it, and because cost only lands at the action — so a "slow cell" may reflect work defined earlier, and calling `count()` casually re-runs the whole pipeline.</details>

**Q2. Narrow vs wide transformation?**
<details><summary>Answer</summary>Narrow: each output partition comes from one input partition; no data moves (`select`, `filter`). Wide: output depends on many input partitions, forcing a **shuffle** across the network (`groupBy`, `join`, `distinct`, `orderBy`). Shuffles are the usual bottleneck — they write to disk, cross the network, and re-read.</details>

**Q3. What's a shuffle and why is it expensive?**
<details><summary>Answer</summary>Redistributing data across the cluster by key so related rows land together. It serializes data, writes shuffle files to local disk, sends them over the network, and re-reads them — disk + network + serialization, plus it creates a stage boundary. Minimizing shuffles is the core of Spark tuning.</details>

**Q4. Jobs, stages, tasks?**
<details><summary>Answer</summary>An action triggers a **job**; the job splits into **stages** at shuffle boundaries; each stage runs one **task** per partition in parallel. More shuffles → more stages. Tasks are the unit of parallel work, one per core.</details>

**Q5. What does AQE do, and is it on?**
<details><summary>Answer</summary>Adaptive Query Execution (on by default in modern Spark/Databricks) re-optimizes at runtime: coalesces shuffle partitions to the right size, converts joins to broadcast when a side turns out small, and splits skewed partitions. It fixes *statistical* surprises, not *logical* mistakes — it won't remove a shuffle you didn't need.</details>

**Q6. Broadcast join vs sort-merge join — when each?**
<details><summary>Answer</summary>Sort-merge: both sides large → shuffle both by key, then merge. Broadcast: one side small (≤ ~10 MB default threshold, or AQE decides at runtime) → ship it to every executor, the big side never shuffles. Force it with `broadcast(small_df)` when you *know* a side is a small dimension. Risk: broadcasting something too big OOMs executors.</details>

**Q7. `repartition` vs `coalesce`?**
<details><summary>Answer</summary>`repartition(n)` does a full shuffle to get exactly `n` partitions (or hash-partition by a column) — use to *increase* parallelism or redistribute. `coalesce(n)` only *reduces* partitions by merging, no shuffle — use before a write to avoid tiny files.</details>

**Q8. Why avoid Python UDFs?**
<details><summary>Answer</summary>They serialize each row from the JVM to a Python process and back, can't be optimized by Catalyst, and break whole-stage codegen and Photon — often 10–100× slower than a built-in. Prefer `pyspark.sql.functions`; if custom logic is unavoidable, use a vectorized (pandas) UDF that works on Arrow batches.</details>

**Q9. What's data skew and how do you fix it?**
<details><summary>Answer</summary>One key has far more rows than others, so one task runs much longer while the rest idle; the stage is as slow as its slowest task. Fixes: rely on AQE skew-join handling, broadcast the small side to avoid the shuffle, or salt the hot key to spread it across partitions. Detect it by a single long task in the query profile or a lopsided `spark_partition_id` count.</details>

**Q10. Why is `collect()` / `toPandas()` dangerous?**
<details><summary>Answer</summary>They pull all result data into the **driver's** memory, which OOMs on large results and serializes through one machine. Use them only on already-small results; otherwise `write` (distributed) or `show(n)`/`limit(n)` to peek.</details>

## B. Optimization scenarios (the "make this faster" questions)

**Q11. A nightly job got slow as data grew. Where do you start?**
<details><summary>Answer</summary>Don't guess — open the **query profile** (Spark UI isn't available on serverless) and read `explain("formatted")`. Find where wall-clock goes: the biggest `Exchange`/shuffle, a `SortMergeJoin` that could broadcast, scans without `PushedFilters`, or one long-running task (skew). Fix biggest-cost first, re-measure.</details>

**Q12. The output is 10,000 tiny files. Problem?**
<details><summary>Answer</summary>Yes — the small-files problem. Every future read spawns a tiny task per file; overhead dominates. `coalesce` before writing, and on Delta run `OPTIMIZE` to compact. Right-size files to roughly hundreds of MB.</details>

**Q13. You need a reused intermediate result several times. On serverless, what do you do?**
<details><summary>Answer</summary>On classic compute I'd `cache()`/`persist()`. On serverless those are restricted, so I **materialize to a Delta table** once and read it back — same benefit, and it persists across cells. Calling out the serverless constraint is the point.</details>

**Q14. How would this scale to 100× the data?**
<details><summary>Answer</summary>Filter-early and column-pruning savings scale with the data; the broadcast join keeps the fact table from shuffling at any size. Make sources Delta and partitioned/clustered on the filter column for partition pruning. Watch `countDistinct` (shuffles) — consider `approx_count_distinct` if exactness isn't required (ask the customer). Never `collect()`; write to a table.</details>

## C. AI stewardship

**Q15. How do you use the assistant without getting burned?**
<details><summary>Answer</summary>Prompt with specific goals + constraints (e.g. "serverless, no cache"), ask it to explain its reasoning, iterate in small auditable steps, then run my audit checklist: does it run, is it correct, is it efficient, is it serverless-legal, are the APIs real, did it keep the contract. I'm the pilot; I own every line.</details>

**Q16. Give an example of catching a hallucination.**
<details><summary>Answer</summary>"It suggested `spark.conf.set('spark.sql.turboJoin.enabled', true)` — I didn't recognize that config, checked, and it doesn't exist. And it used `.cache()`, which is restricted on serverless, so I replaced it with a Delta materialization." (See [07](07-AI-Stewardship-Genie-Code.md).)</details>

## D. Customer-translation drills (turn each fix into business value)

Practice converting tech → business in one breath:

| Technical fix | Business translation |
|---------------|---------------------|
| Broadcast the dimension table | "We stop reshuffling tens of millions of rows; the nightly run finishes before the team logs in." |
| Filter/prune early | "We only read the data we report on, so it's faster and cheaper per run." |
| Replace UDF with built-in | "The engine processes batches in native code instead of one row at a time." |
| Materialize a reused step | "We compute the shared step once instead of rebuilding it for every dashboard." |
| Fix skew (salt/broadcast) | "We balance the load so one overloaded machine isn't holding up the whole job." |
| `OPTIMIZE` / compact files | "Every future query against this table gets faster, not just today's job." |

## E. Behavioral / FDE-fit (be ready, briefly)

- *"Tell me about debugging something you didn't write."* → Structure: how you built a mental model, formed a hypothesis, tested it, and what you learned. Emphasize narration and collaboration.
- *"How do you explain a technical tradeoff to a non-technical stakeholder?"* → Give a concrete before/after in time or money, not jargon.
- *"You disagree with a customer's approach. What do you do?"* → Understand their constraint first, show the tradeoff with data, propose an option, let them decide. FDEs influence, they don't lecture.

## The phrases worth memorizing

- "Let me check the **query profile** before I optimize — I don't want to guess."
- "That's a wide transformation, so it shuffles — that's where I'll look first."
- "We're on serverless, so instead of `.cache()` I'll materialize to Delta."
- "Let me restate what this code does before I change it."
- "Genie drafted this; let me audit it before I trust it."
- "In business terms, this takes the job from ~40 minutes to a few minutes."
