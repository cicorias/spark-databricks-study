# Databricks FDE Interview Drills (re-prioritized)

Rebuilt around the actual interview brief: a 60-minute **Forward Deployed Engineer** pair-programming session, open-book, using Databricks Free Edition + the Databricks Assistant. It is explicitly **not** a syntax or typing test. Two graded phases:

- **Phase 1 — Python:** digest and debug an unfamiliar *customer* PySpark codebase; reason about how it scales; use the Assistant as a co-pilot you audit.
- **Phase 2 — Spark Optimization:** a running-but-slow Spark app you must speed up and *justify*.

Graded on four signals: **Computational Thinking, Code Stewardship** (explain + read others' code), **AI Stewardship** (prompt well, catch the Assistant's mistakes), **Resilience** (how you navigate a bug). Plus two throughlines: **think out loud** and **customer translation**.

The priority tiers below reflect how much interview weight each area carries. Spend your prep time top-down.

---

## TIER 1 — Highest weight (do these first)

### A. Spark Optimization drills (this IS Phase 2)

For each: state the symptom you'd see in the Spark UI, the root cause, the fix, and the one-sentence customer justification.

**O1. A stage has 199 fast tasks and 1 task that runs 50x longer.**
> Symptom: one straggler task, huge shuffle-read on it. Cause: data skew — one key dominates. Fixes (in order of preference): broadcast the small side if it's a join; salt the hot key; enable AQE skew join (`spark.sql.adaptive.skewJoin.enabled`); filter/handle the hot key separately. Customer line: "One customer ID had 40% of the rows, so one machine did almost all the work — we spread that key out."

**O2. A join between a 2 TB fact table and a 30 MB dim table shuffles both sides.**
> Cause: Spark chose sort-merge join and shuffled the big table. Fix: broadcast the dim (`broadcast(df)` or let AQE auto-broadcast; check `spark.sql.autoBroadcastJoinThreshold`). Result: no shuffle of the 2 TB side. Customer line: "The lookup table is tiny — we ship a copy to every worker instead of moving terabytes across the network."

**O3. Job does `df.count()` then `df.filter(...).count()` then `df.write(...)` and re-reads source each time.**
> Cause: no caching — the DAG recomputes from source on every action. Fix: `df.cache()` / `persist()` the reused DataFrame (pick level by memory), or restructure to a single pass. Caveat: only cache what's reused; caching everything wastes memory.

**O4. `groupByKey().mapValues(sum)` on a large RDD/DF.**
> Cause: `groupByKey` shuffles every value. Fix: `reduceByKey` / `groupBy().agg()` so partial aggregation happens map-side before the shuffle. Far less network traffic, less OOM risk.

**O5. Thousands of tiny output files after a write (the "small files problem").**
> Cause: too many partitions, each writing a sliver. Fix: `coalesce`/`repartition` before write, set a sane partition count, or use Delta `OPTIMIZE` / auto-compaction. Downstream reads get faster too.

**O6. `select *` then one column used; or filter applied late.**
> Cause: reading/serializing columns you don't need; filtering after expensive work. Fix: column pruning (select needed columns), predicate pushdown (filter early, push to source/Parquet/Delta). Catalyst does a lot of this for DataFrames but not if you fight it with UDFs.

**O7. A Python UDF in the hot path.**
> Cause: Python UDFs serialize rows out of the JVM, killing Catalyst optimization. Fix: replace with built-in `pyspark.sql.functions`; if unavoidable, use a pandas/vectorized UDF. Customer line: "We swapped a row-by-row Python function for Spark's native one — same logic, runs in the engine."

**O8. Default 200 shuffle partitions on a tiny job, or too few on a huge job.**
> Cause: `spark.sql.shuffle.partitions` mismatch — too many = scheduling overhead/small files; too few = giant tasks/spill. Fix: tune it, or rely on AQE coalescing (`spark.sql.adaptive.enabled`). Know that AQE is on by default in modern DBR and auto-coalesces post-shuffle.

**O9. Repeated `withColumn` in a long loop, or repeated `union` building a DataFrame.**
> Cause: ballooning logical plan / lineage. Fix: `select` with all expressions at once; for many unions, `reduce(DataFrame.unionByName, list)` then a single write, or read all paths at once.

**O10. Wide transformation chain with no checkpoint in a long iterative job.**
> Cause: lineage grows unboundedly, recovery and planning get slow. Fix: `checkpoint()` (or write to Delta) to truncate lineage at a stable point.

**Optimization framework to say out loud (memorize this order):**
> 1) Read less data (partition pruning, column pruning, predicate pushdown, file format). 2) Move less data (broadcast joins, reduce shuffles, pre-aggregate). 3) Handle skew (salt/AQE). 4) Reuse work (cache what's read multiple times). 5) Right-size parallelism (shuffle partitions / AQE). 6) Avoid Python UDFs. *Always confirm with the Spark UI before and after.*

### B. Read & Debug Unfamiliar PySpark (this IS Phase 1)

Practice the *motion*, not memorized answers. Open any notebook from the jrlasak repo (below), have the Assistant generate a solution, then run these drills on it.

**D1. Narrate-the-DAG drill.** Given a code cell you didn't write, say aloud: which lines are transformations (lazy) vs. actions (trigger execution), where the shuffles are, and how many jobs/stages you'd expect. Verify against the Spark UI.

**D2. Find-the-bug drill.** Common PySpark bugs to train your eye on: chained ops on the wrong DataFrame; `==` vs. `&`/`|` with missing parens in filters; integer/null handling in joins (inner dropping rows you wanted); column name collisions after join; using a Python `for` loop where a join/groupBy belongs; forgetting `.cache()` causes recompute (not a correctness bug but a perf one to flag).

**D3. Reproduce-then-fix drill.** When you hit an error, narrate: read the actual exception, form one hypothesis, make one change, re-run. Resist shotgun debugging — the interview grades *how* you navigate the unknown.

**D4. Scale-it question.** For any working snippet, answer: "This works on 1 GB. What breaks at 1 TB?" (collect to driver, skew, shuffle explosion, small files, UDF cost). This is the single most predictable verbal question given the "Distributed Reasoning" signal.

### C. Hands-on reps — primary practice resource

**jrlasak/databricks-code-practice** — 104 exercises, import into Databricks Free Edition, write code, run assertions. This is the closest thing to the real environment and the recommended self-practice.

Map to the phases:
- **For Phase 1 fluency:** the **ELT** folder (53 exercises) — Spark SQL joins, **window functions**, PySpark transformations, batch ingestion, **medallion architecture**, complex/nested data types.
- **For data-engineering depth:** the **Delta Lake** folder (51 exercises) — **MERGE** (upserts), time travel, schema enforcement, **OPTIMIZE**, liquid clustering, change data feed.
- Workflow per exercise: read the problem → write your own first → *then* ask the Assistant → diff the two → run assertions. Doing it before the Assistant builds the audit instinct they grade.

> Companion (theory/Q-bank), optional: the author's Databricks interview cheat sheets at dataengineer.wiki.

---

## TIER 2 — High weight (the throughlines)

### AI Stewardship drills (you're the pilot)

**AI1. Prompt-quality rep.** Practice giving the Assistant context, not just a verb: schema, expected output, constraints. Bad: "join these." Good: "left join orders to customers on customer_id, keep all orders, flag unmatched, expect ~1M rows."

**AI2. Audit rep.** After every Assistant output, state out loud: does it compile, is the logic right on an edge case (nulls, dupes, empty input), and is it *efficient* (any hidden `groupByKey`, `collect`, Python UDF, or cross join)? The brief explicitly wants you to catch hallucinations and inefficiency.

**AI3. Bail-out rep.** If you're stuck >2 min on syntax, you can ask for a bail-out. Practice saying: "I know the shape of the answer is X; let me have the Assistant fill the syntax while I keep the architecture moving."

### Distributed reasoning (asked throughout)

Be fluent on: lazy evaluation & the DAG; narrow vs. wide transformations (which cause shuffles); driver vs. executor (and why `collect()`/`toPandas()` on big data kills the driver); partitions as the unit of parallelism; broadcast vs. shuffle joins; why in-memory + lineage gives fault tolerance without replication.

### Communication drills

**T1. Think-out-loud:** narrate trade-offs continuously — silence reads as being stuck. **T2. Customer translation:** after each solution, give the one-line business justification (see the "customer line" in the O-drills). **T3. Defend-every-line:** whether you wrote it or the Assistant did, be ready to explain any line. Pick a random line of a generated cell and justify it.

---

## TIER 3 — Supporting fluency (warm-ups, get these automatic)

Quick Spark concept recall — keep, but you no longer need to grind all 20. The optimization-relevant ones (compressed):

- **Transformation vs. action / lazy eval / DAG** — see Distributed Reasoning above.
- **Shuffle triggers:** repartition, coalesce(↑), groupByKey, reduceByKey, join, cogroup.
- **cache vs persist:** persist lets you choose the storage level; cache = MEMORY_ONLY.
- **RDD vs DataFrame vs Dataset:** prefer DataFrame/SQL in PySpark — Catalyst-optimized; RDDs lose that.
- **Broadcast variable vs accumulator:** read-only shared lookup vs. additive counter.
- **Partitioning / parquet+delta columnar / predicate & column pushdown / schema-on-write.**
- **Window functions** (rank, row_number, lag/lead, running aggregates) — extremely likely in Phase 1; drill in the ELT folder.
- **Delta MERGE** for upserts — likely in the data-engineering spike; drill in the Delta folder.

---

## TIER 4 — De-prioritized (warm-up only, not the main event)

The earlier LeetCode/DSA list (Two Sum, LRU Cache, topological sort, etc.) is **largely off-target** for this specific interview: it isn't testing algorithm puzzles or syntax recall, and the format is pair-programming on customer *data* problems in a notebook. Keep at most a couple as a mental warm-up the morning of (Top-K, word-frequency — they at least rehearse aggregation thinking that maps to groupBy). Don't spend real prep hours here. The format describes data engineering / full-stack spikes rather than a hidden algorithm round.

---

## Suggested prep plan (if you have ~1 week)

- **Days 1–2:** jrlasak **ELT** folder — window functions, joins, transformations. Do each *before* asking the Assistant. (Tier 1C + Tier 3)
- **Day 3:** jrlasak **Delta Lake** folder — MERGE, OPTIMIZE, schema enforcement. (Tier 1C)
- **Day 4:** Optimization drills O1–O10; for each, find or build a slow version in a notebook and fix it while watching the Spark UI. (Tier 1A)
- **Day 5:** Read/debug + scale-it drills D1–D4 on code you didn't write (use AI-generated cells as the "customer code"). (Tier 1B)
- **Day 6:** AI stewardship + communication reps; practice narrating and giving customer one-liners. (Tier 2)
- **Day 7 / T-1:** Spin up Free Edition, run a cluster, do one 50–60 min end-to-end exercise with the Assistant as recommended. Rehearse the bail-out and the optimization framework out loud.

**The morning of:** re-read the 6-step optimization framework and the "what breaks at 1 TB?" checklist. Those are the two things most likely to come out of your mouth and most heavily rewarded.
