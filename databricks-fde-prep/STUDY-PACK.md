# Databricks FDE Study Pack — Drills · Flash Cards · Scenarios

Companion to `databricks-spark-interview-drills.md` (priorities) and `slow_spark_app_practice.py` (runnable code + output examples). This file holds the three recall/verbal components. Everything is tuned to the Forward Deployed Engineer brief: pair-programming, think out loud, audit the Assistant, optimize and justify.

---

# 1) DRILLS (active motions — *do*, don't just read)

Each drill is a thing to perform, in a notebook or out loud, until it's automatic. Time-box them.

**DR1 — Narrate the DAG (2 min each).** Open any code cell. Out loud, label every line as *transformation (lazy)* or *action (triggers a job)*, point to where shuffles happen (groupBy, join, repartition, distinct), and predict the number of stages. Then run and check against the Query Profile / Spark UI.

**DR2 — "What breaks at 1 TB?" (1 min each).** For any working snippet, name the first thing that fails at scale: `collect()`/`toPandas()` to driver, skew, shuffle blow-up, tiny-files on write, or a Python UDF. This is the most predictable verbal question in this format.

**DR3 — Before/after timing (10 min).** In the practice notebook, run a slow cell, record wall-clock, apply one fix, re-run, state the speedup and *why*. Never claim a fix without measuring it.

**DR4 — Read-the-plan (5 min).** Run `df.explain("formatted")` on a join. Find: is it `BroadcastHashJoin` or `SortMergeJoin`? Where's the `Exchange` (shuffle)? Are filters pushed down (`PushedFilters`)? Practice saying it in one breath.

**DR5 — One-hypothesis debugging (per bug).** When an error hits: read the real exception line, state one hypothesis aloud, change one thing, re-run. No shotgun edits. The brief grades *how* you navigate the unknown (Resilience).

**DR6 — Prompt-then-audit (per Assistant call).** Before accepting Assistant output, say: does it compile, is it right on nulls/dupes/empty input, and is it efficient (any hidden groupByKey, collect, cross join, UDF)? Catching the inefficiency out loud is the AI-Stewardship signal.

**DR7 — Customer one-liner (per solution).** End every solution with a business-language sentence: "We stopped moving 2 TB across the network by shipping the 30 MB lookup to each worker." No jargon.

**DR8 — Window-function reps (15 min).** Without looking: write `row_number`, `rank`, `dense_rank`, a 7-row moving average, and "latest record per key" using a window. These show up constantly in Phase 1.

**DR9 — MERGE rep (10 min).** Write a Delta `MERGE` upsert from scratch (matched → update, not matched → insert). Then add a delete-on-condition clause. This is the data-engineering spike bread-and-butter.

**DR10 — Skew fix two ways (10 min).** Take a skewed groupBy and fix it (a) by trusting AQE and (b) by manual salting. Be able to explain when salting still helps over AQE.

**DR11 — Bail-out rehearsal.** Practice the sentence for when you're stuck >2 min on syntax: "I know the shape is X — let me have the Assistant fill syntax while I keep the architecture moving." The session typically offers this; using it well is a positive signal, not a failure.

**DR12 — Defend-a-random-line.** Point at any line of Assistant-generated code and justify it as if you wrote it. If you can't, you don't own it yet.

---

# 2) FLASH CARDS (cover the answer, recall, check)

Mark each ✅ / 🔁 / ❌ and re-run the ❌ pile daily.

### Execution model
**F1.** Transformation vs. action? → Transformation builds the plan and is lazy; action (`count`, `collect`, `write`, `show`) triggers execution of the recorded DAG.
**F2.** Narrow vs. wide transformation? → Narrow (`map`, `filter`, `select`) needs no data movement; wide (`groupBy`, `join`, `repartition`, `distinct`) forces a shuffle.
**F3.** What is a shuffle and why care? → All-to-all redistribution across partitions over the network; the usual performance bottleneck. Minimize and pre-aggregate.
**F4.** Driver vs. executor? → Driver runs `main`, builds the DAG, schedules; executors run tasks and hold cached data. `collect()`/`toPandas()` pulls all data into the driver → OOM risk.
**F5.** What is a partition? → The unit of parallelism; one task processes one partition. Too few = giant tasks/spill; too many = scheduling overhead and tiny files.

### Joins & shuffles
**F6.** Broadcast vs. sort-merge join? → Broadcast ships a small table to every executor (no shuffle of the big side); sort-merge shuffles and sorts both sides. Use broadcast when one side is small.
**F7.** `autoBroadcastJoinThreshold`? → Default ~10 MB; tables estimated under it are auto-broadcast. Force with `broadcast(df)`.
**F8.** `reduceByKey`/`groupBy().agg()` vs. `groupByKey`? → The former pre-aggregates map-side before the shuffle (less network, less OOM); `groupByKey` ships every value.
**F9.** What is AQE and what does it do? → Adaptive Query Execution (on by default in modern DBR): coalesces shuffle partitions at runtime, handles skewed joins, and can switch sort-merge → broadcast using real stats.

### Skew & partitioning
**F10.** Signs of skew? → A few straggler tasks far slower than the rest, lopsided shuffle-read sizes in the UI.
**F11.** Fixes for skew? → Broadcast (if a join), AQE skew handling, salt the hot key, or isolate/handle the hot key separately.
**F12.** `repartition` vs. `coalesce`? → `repartition` (full shuffle, up or down, even sizes); `coalesce` (no full shuffle, only down, can be uneven). Coalesce before writing to cut small files.
**F13.** `spark.sql.shuffle.partitions` default? → 200. Often wrong for tiny or huge jobs; tune it or let AQE coalesce.

### Caching & lineage
**F14.** `cache()` vs. `persist()`? → `persist` lets you pick a storage level; `cache` = `MEMORY_AND_DISK` for DataFrames. Cache only what's read multiple times; `unpersist()` when done.
**F15.** Is cache eager or lazy? → Lazy — it materializes on the first action.
**F16.** What is lineage and why does it matter? → The recorded chain of transformations; enables recomputation of lost partitions (fault tolerance without data replication). Very long lineage → checkpoint or write to Delta.

### Python/UDF performance
**F17.** Why are Python UDFs slow? → Rows are serialized out of the JVM to a Python process and back, and Catalyst can't optimize through them. Prefer built-in `pyspark.sql.functions`.
**F18.** Faster UDF alternative? → Vectorized `pandas_udf` (Arrow-based, batch-wise) when a native function truly doesn't exist.

### Delta Lake (data-engineering spike)
**F19.** What does Delta add over Parquet? → ACID transactions, schema enforcement/evolution, time travel, `MERGE`/upserts, `OPTIMIZE` compaction, change data feed.
**F20.** `MERGE` use case? → Upserts/SCD: matched → update, not matched → insert; can also delete on condition.
**F21.** `OPTIMIZE` and clustering? → `OPTIMIZE` compacts small files; ZORDER / liquid clustering co-locate data on common filter columns for faster reads.
**F22.** Time travel? → Query an older version with `versionAsOf` / `timestampAsOf`; useful for audits and rollback.

### SQL shaping (Phase 1)
**F23.** `row_number` vs. `rank` vs. `dense_rank`? → `row_number` always unique; `rank` leaves gaps after ties; `dense_rank` no gaps.
**F24.** "Latest record per key" pattern? → Window partitioned by key, ordered by timestamp desc, keep `row_number() == 1`.
**F25.** Default join type and a footgun? → Inner join is default and silently drops non-matching rows; check counts and consider `left`. Null keys never match.

### File formats & reads
**F26.** Why columnar (Parquet/Delta)? → Reads only needed columns, compresses well, supports predicate/column pushdown.
**F27.** The small-files problem? → Many tiny files = per-file overhead + tiny partitions + heavy shuffle. Fix upstream compaction, `coalesce` on write, or Delta `OPTIMIZE`.

### Meta / interview
**F28.** The 6-step optimization framework? → 1) read less data, 2) move less data, 3) handle skew, 4) reuse work (cache), 5) right-size parallelism, 6) avoid Python UDFs — always verify in the UI.

---

# 3) SCENARIOS (think-out-loud rehearsals)

Practice *speaking* a structured answer. Each has a prompt, the clarifying questions to ask, a model walkthrough, and the customer one-liner. The walkthrough is what you'd narrate — not a script to memorize.

### SC1 — Phase 2: "This nightly job took 20 minutes, now it takes 3 hours. Nothing changed in the code."
**Ask first:** Did data volume grow? Did one source skew? Any new join? Look at the Spark UI — which stage regressed?
**Walkthrough:** "I'd open the Query Profile and find the slow stage. A code-stable job that suddenly degrades usually means data changed — most often skew (one key grew) or a join that flipped from broadcast to sort-merge because a 'small' table crossed the broadcast threshold. I'd confirm with task-time distribution: a few stragglers = skew; uniform-but-slow = the broadcast flipped or shuffle partitions are now mis-sized. Fix accordingly: re-enable/force `broadcast()` on the dim, turn on AQE skew handling or salt the hot key. Then re-run and compare wall-clock."
**Customer line:** "A lookup table grew just past the auto-broadcast limit, so Spark started shuffling the whole fact table; we pinned it back to a broadcast and we're back to minutes."

### SC2 — Phase 1: "Here's a customer notebook that produces the wrong revenue total. Find the bug."
**Ask first:** What's the expected number vs. actual? Higher or lower than truth?
**Walkthrough:** "Wrong totals usually trace to the join or the grain. Too high → a one-to-many join fanned out rows (duplicates) before the `sum`; I'd check row counts before/after the join and dedup or aggregate to the right grain first. Too low → an inner join dropped unmatched rows or null keys; I'd switch to `left` and inspect nulls. I'd also check for double-counting from a `union` or a window that isn't partitioned as intended. One hypothesis at a time, verify with counts."
**Customer line:** "Orders were joined to a table that had multiple address rows per customer, which quietly duplicated revenue; we aggregated addresses to one row per customer before joining."

### SC3 — Phase 2: "The job runs but spills to disk constantly and occasionally OOMs."
**Ask first:** Where — a join, an aggregation, a sort? Is anyone calling `collect()`?
**Walkthrough:** "Spill/OOM points to too much data per task or pulling to the driver. First I'd kill any `collect()`/`toPandas()` on large data — that's a driver OOM. For executor spill, the partitions are too big: increase shuffle partitions (or let AQE coalesce), and check for skew concentrating data on one task. If it's a wide aggregation, pre-aggregate earlier. If a Python UDF is materializing big objects, replace it with a native function."
**Customer line:** "The report was collecting the full dataset onto a single machine to format it; we did the formatting in Spark and wrote out directly, so no single node has to hold everything."

### SC4 — Phase 1 + AI: "Use the Assistant to write a dedup, then convince me it's correct."
**Walkthrough:** "I'd prompt with the schema, the dedup key, and the tie-breaker: 'Given table X with columns …, keep the most recent row per customer_id by updated_at.' Then I audit: does it use a window with `row_number` (deterministic) rather than `dropDuplicates` (non-deterministic on which row survives)? Does the order-by handle null timestamps? I'd test on a tiny frame with a known duplicate and a null. If the Assistant used `dropDuplicates(['customer_id'])`, I'd flag that it doesn't guarantee *which* row you keep and rewrite with the window."
**Customer line:** "We keep the newest record per customer deterministically, so the result is the same every run — important for auditing."

### SC5 — Data-engineering spike: "Design an incremental daily load into a bronze→silver→gold medallion."
**Ask first:** Append-only or updates/late data? Volume? SLA?
**Walkthrough:** "Bronze: land raw as-is (Auto Loader for incremental file ingestion, schema captured). Silver: clean, enforce schema, dedup, and `MERGE` to handle updates/late-arriving data — that gives idempotent reruns. Gold: business aggregates for reporting. I'd partition/cluster on common filter columns, `OPTIMIZE` to manage file sizes, and use Delta time travel for auditability. The `MERGE` in silver is what makes the daily load safely re-runnable."
**Customer line:** "Each layer has one job — raw capture, clean/conform, business metrics — so when something's wrong we know exactly which layer to fix, and reruns don't double-count."

### SC6 — Customer translation under pressure: "Explain to a non-technical stakeholder why this fix matters."
**Walkthrough:** "I drop the Spark vocabulary. 'The job was copying the entire sales history between computers every night just to attach a small product list. We now hand that small list to each computer once, so the heavy data stays put. Same numbers, a fraction of the time and cost.' Then tie it to their metric: report ready by 6 a.m. instead of 9, lower compute spend."

---

## Day-of checklist (say these out loud before you start)
1. The 6-step optimization framework (F28).
2. The "what breaks at 1 TB?" list (DR2).
3. "I narrate everything, I audit the Assistant, I end each fix with a customer sentence."
