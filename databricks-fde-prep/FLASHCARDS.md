# Spark / Databricks Flash Cards

Click a question to reveal the answer. 28 cards.

## Execution model

<details><summary><b>F1.</b> Transformation vs. action?</summary>

Transformation builds the plan and is lazy. An action (`count`, `collect`, `write`, `show`) triggers execution of the recorded DAG.
</details>

<details><summary><b>F2.</b> Narrow vs. wide transformation?</summary>

Narrow (`map`, `filter`, `select`) needs no data movement. Wide (`groupBy`, `join`, `repartition`, `distinct`) forces a shuffle.
</details>

<details><summary><b>F3.</b> What is a shuffle, and why care?</summary>

All-to-all redistribution across partitions over the network — the usual performance bottleneck. Minimize it and pre-aggregate.
</details>

<details><summary><b>F4.</b> Driver vs. executor?</summary>

Driver runs `main`, builds the DAG, schedules. Executors run tasks and hold cached data. `collect()`/`toPandas()` pulls all data into the driver → OOM risk.
</details>

<details><summary><b>F5.</b> What is a partition?</summary>

The unit of parallelism — one task processes one partition. Too few = giant tasks/spill; too many = scheduling overhead and tiny files.
</details>

## Joins & shuffles

<details><summary><b>F6.</b> Broadcast vs. sort-merge join?</summary>

Broadcast ships a small table to every executor (no shuffle of the big side). Sort-merge shuffles and sorts both sides. Use broadcast when one side is small.
</details>

<details><summary><b>F7.</b> What is <code>autoBroadcastJoinThreshold</code>?</summary>

Default ~10 MB. Tables estimated under it are auto-broadcast. Force it with `broadcast(df)`.
</details>

<details><summary><b>F8.</b> <code>reduceByKey</code> / <code>groupBy().agg()</code> vs. <code>groupByKey</code>?</summary>

The former pre-aggregates map-side before the shuffle (less network, less OOM). `groupByKey` ships every value across the network.
</details>

<details><summary><b>F9.</b> What is AQE and what does it do?</summary>

Adaptive Query Execution (on by default in modern DBR): coalesces shuffle partitions at runtime, handles skewed joins, and can switch sort-merge → broadcast using real stats.
</details>

## Skew & partitioning

<details><summary><b>F10.</b> Signs of skew?</summary>

A few straggler tasks far slower than the rest, and lopsided shuffle-read sizes in the Spark UI.
</details>

<details><summary><b>F11.</b> Fixes for skew?</summary>

Broadcast (if it's a join), AQE skew handling, salt the hot key, or isolate/handle the hot key separately.
</details>

<details><summary><b>F12.</b> <code>repartition</code> vs. <code>coalesce</code>?</summary>

`repartition`: full shuffle, up or down, even sizes. `coalesce`: no full shuffle, only down, can be uneven. Coalesce before writing to cut small files.
</details>

<details><summary><b>F13.</b> <code>spark.sql.shuffle.partitions</code> default?</summary>

200. Often wrong for tiny or huge jobs — tune it, or let AQE coalesce.
</details>

## Caching & lineage

<details><summary><b>F14.</b> <code>cache()</code> vs. <code>persist()</code>?</summary>

`persist` lets you pick a storage level; `cache` = MEMORY_AND_DISK for DataFrames. Cache only what's read multiple times; `unpersist()` when done.
</details>

<details><summary><b>F15.</b> Is cache eager or lazy?</summary>

Lazy — it materializes on the first action.
</details>

<details><summary><b>F16.</b> What is lineage and why does it matter?</summary>

The recorded chain of transformations. It lets Spark recompute lost partitions (fault tolerance without replication). Very long lineage → checkpoint or write to Delta.
</details>

## Python / UDF

<details><summary><b>F17.</b> Why are Python UDFs slow?</summary>

Rows are serialized out of the JVM to a Python process and back, and Catalyst can't optimize through them. Prefer built-in `pyspark.sql.functions`.
</details>

<details><summary><b>F18.</b> Faster UDF alternative?</summary>

A vectorized `pandas_udf` (Arrow-based, batch-wise) when a native function truly doesn't exist.
</details>

## Delta Lake

<details><summary><b>F19.</b> What does Delta add over Parquet?</summary>

ACID transactions, schema enforcement/evolution, time travel, `MERGE`/upserts, `OPTIMIZE` compaction, and change data feed.
</details>

<details><summary><b>F20.</b> <code>MERGE</code> use case?</summary>

Upserts / SCD: matched → update, not matched → insert; can also delete on a condition.
</details>

<details><summary><b>F21.</b> <code>OPTIMIZE</code> and clustering?</summary>

`OPTIMIZE` compacts small files; ZORDER / liquid clustering co-locate data on common filter columns for faster reads.
</details>

<details><summary><b>F22.</b> Time travel?</summary>

Query an older version with `versionAsOf` / `timestampAsOf` — useful for audits and rollback.
</details>

## SQL shaping

<details><summary><b>F23.</b> <code>row_number</code> vs. <code>rank</code> vs. <code>dense_rank</code>?</summary>

`row_number` is always unique; `rank` leaves gaps after ties; `dense_rank` has no gaps.
</details>

<details><summary><b>F24.</b> "Latest record per key" pattern?</summary>

Window partitioned by key, ordered by timestamp desc, keep `row_number() == 1`.
</details>

<details><summary><b>F25.</b> Default join type and a footgun?</summary>

Inner join is the default and silently drops non-matching rows; check counts and consider `left`. Null keys never match.
</details>

## File formats

<details><summary><b>F26.</b> Why columnar (Parquet/Delta)?</summary>

Reads only the needed columns, compresses well, and supports predicate/column pushdown.
</details>

<details><summary><b>F27.</b> The small-files problem?</summary>

Many tiny files = per-file overhead + tiny partitions + heavy shuffle. Fix with upstream compaction, `coalesce` on write, or Delta `OPTIMIZE`.
</details>

## Meta

<details><summary><b>F28.</b> The 6-step optimization framework?</summary>

1. Read less data
2. Move less data
3. Handle skew
4. Reuse work (cache)
5. Right-size parallelism
6. Avoid Python UDFs

— and always verify in the UI.
</details>