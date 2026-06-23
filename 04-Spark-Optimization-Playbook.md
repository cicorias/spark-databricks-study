---
title: Spark Optimization Playbook
tags: [spark, pyspark, optimization, performance]
---

# 04 — Spark Optimization Playbook (the 6 levers)

[← Spark Mental Models](03-Spark-Mental-Models.md) · Next: [Optimization Challenge →](05-Spark-Optimization-Challenge.md)

Your Phase-2 task is to find **4–6** improvable areas in a running app. This is that menu, ordered roughly by typical impact. For each lever: **the symptom**, **the fix (before → after)**, and **the customer translation** (say this out loud).

> **Diagnose before you optimize.** Run `df.explain("formatted")` and open the **query profile**. Hunt for `Exchange` (shuffles), `SortMergeJoin`, large scans without `PushedFilters`, and skewed task times. Fix the biggest cost first.

---

## Lever 1 — Replace shuffles with broadcast joins

**Symptom:** plan shows `SortMergeJoin` with an `Exchange` on both sides; one side is actually a small lookup/dimension table.

**Why it's slow:** a sort-merge join shuffles *both* sides by key across the network. If one side is small, that whole shuffle of the big side is wasted.

```python
# BEFORE: both sides shuffled (sort-merge join)
result = fact.join(dim, "product_id")        # fact = 100M rows, dim = 5k rows

# AFTER: broadcast the small side — big side never shuffles
from pyspark.sql.functions import broadcast
result = fact.join(broadcast(dim), "product_id")
```

**Watch out:** only broadcast something that comfortably fits in executor memory (think ≤ ~10–100 MB). Broadcasting a large table will OOM executors. AQE may auto-broadcast at runtime, but an explicit hint is reliable when you *know* it's small.

**Customer translation:** *"The lookup table is tiny, so instead of reshuffling 100 million rows across the cluster we send a 5,000-row copy to every machine. The big table stays put — that's the difference between minutes and seconds."*

---

## Lever 2 — Filter and project early (predicate & projection pushdown)

**Symptom:** code reads `select("*")`, does heavy joins/aggregations, *then* filters at the end. Scans read far more data than needed.

**Why it's slow:** every byte read, shuffled, and joined that you later throw away is wasted work.

```python
# BEFORE: read everything, filter last
df = spark.table("events")                       # 200 columns, all dates
out = (df.join(other, "id")
         .groupBy("country").agg(F.sum("amount"))
         .where(F.col("year") == 2025))          # filter applied way too late

# AFTER: prune columns + push the filter to the scan
df = (spark.table("events")
        .where(F.col("year") == 2025)            # partition/predicate pushdown
        .select("id", "country", "amount"))      # projection pruning
out = (df.join(other.select("id", "region"), "id")
         .groupBy("country").agg(F.sum("amount")))
```

On Parquet/Delta these become `PushedFilters` and a short column list in the scan — Spark reads only what it needs, and partition pruning can skip whole folders.

**Customer translation:** *"We were loading the entire table and 200 columns just to use three of them for one year. By filtering and selecting up front, we read a fraction of the data — less I/O, less shuffle, faster job, lower cost."*

---

## Lever 3 — Fix partitioning (`repartition` vs `coalesce`, shuffle partitions)

**Symptoms:** thousands of tiny tasks (over-partitioned), or a handful of giant tasks (under-partitioned), or `spark.sql.shuffle.partitions` left at 200 for a tiny dataset.

```python
# Reduce partitions WITHOUT a shuffle (e.g., before writing output) — narrow, cheap
df.coalesce(8).write.format("delta").save(path)

# Redistribute / increase partitions WITH a shuffle (e.g., to parallelize or de-skew)
df.repartition(200, "customer_id")

# Right-size shuffle output for a small dataset (AQE also coalesces automatically)
spark.conf.set("spark.sql.shuffle.partitions", 64)
```

- **`coalesce(n)`** only *reduces* partitions, no shuffle (merges existing ones). Great right before a write to avoid tiny output files.
- **`repartition(n)`** / `repartition(col)` does a full shuffle to get exactly `n` partitions or to hash-partition by a column. Use to *increase* parallelism or co-locate by key.
- Lean on **AQE** to coalesce post-shuffle partitions automatically; mention it.

**Customer translation:** *"We were writing 2,000 tiny files, which makes every future read slow. Coalescing to a handful of right-sized files speeds up everything downstream."*

---

## Lever 4 — Stop using Python UDFs where a built-in exists

**Symptom:** a `udf(lambda ...)` doing something a built-in function could do.

**Why it's slow:** a Python UDF serializes every row out of the JVM to a Python process and back. It blocks Catalyst optimizations, whole-stage codegen, and Photon. It's often 10–100× slower than the equivalent built-in.

```python
# BEFORE: Python UDF — row-by-row serialization, opaque to the optimizer
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
upper_udf = udf(lambda s: s.upper() if s else None, StringType())
df = df.withColumn("name_u", upper_udf("name"))

# AFTER: built-in — vectorized, optimizable, Photon-friendly
from pyspark.sql import functions as F
df = df.withColumn("name_u", F.upper("name"))
```

If you truly need custom logic, prefer a **vectorized (pandas) UDF** over a plain Python UDF — it operates on batches via Arrow instead of row-by-row.

**Customer translation:** *"That custom function forces Spark to hand every row to Python one at a time. Swapping in the built-in lets the engine process whole batches in native code — same result, a fraction of the time."*

---

## Lever 5 — Handle data skew

**Symptom:** one task in a stage runs many times longer than the rest; one partition holds most of the rows (check with `spark_partition_id`). Common with nulls or a dominant key.

```python
from pyspark.sql import functions as F

# Quick diagnosis (serverless-safe, no RDD):
(df.groupBy(F.spark_partition_id().alias("pid")).count()
   .orderBy(F.desc("count")).show())

# Fix A: let AQE handle it (often enough) — on by default:
#   spark.sql.adaptive.enabled = true, spark.sql.adaptive.skewJoin.enabled = true

# Fix B: broadcast the small side to avoid the skewed shuffle entirely (best when possible)
big.join(F.broadcast(small), "key")

# Fix C: salt the hot key so it spreads across partitions
salted = big.withColumn("salt", (F.rand() * 16).cast("int"))
dim_salted = (dim.withColumn("salt", F.explode(F.array([F.lit(i) for i in range(16)]))))
big.join(dim_salted, ["key", "salt"])
```

**Customer translation:** *"One customer accounts for half the rows, so one machine was doing half the work while the rest sat idle. Spreading that hot key across the cluster balances the load so the whole job finishes when the slowest machine does — and now they all finish together."*

---

## Lever 6 — Don't recompute; materialize (and don't pull to the driver)

**Symptoms:** the same DataFrame is used several times (each use re-runs the whole lineage); or `collect()`/`toPandas()` pulls a big result to the driver.

```python
# BEFORE: base recomputed 3x (Spark re-runs the lineage each action) + driver pull
base = expensive_pipeline(raw)
a = base.where("region='EU'").count()
b = base.where("region='US'").count()
rows = base.collect()                 # pulls everything to the driver — danger

# AFTER (serverless-friendly): materialize once to Delta, read back; avoid collect
(base.write.mode("overwrite").saveAsTable("workspace.default.base_step"))
base = spark.table("workspace.default.base_step")
a = base.where("region='EU'").count()
b = base.where("region='US'").count()
# need a peek? use show()/limit(), not collect()/toPandas()
base.limit(20).show()
```

> On **classic** compute you'd `base.cache()`. On **serverless** the cache APIs are restricted — **materialize to Delta** instead (see [02](02-Databricks-Free-Edition-Serverless-Gotchas.md)). Calling this out *is* the point that scores.

**Customer translation:** *"We were rebuilding the same intermediate result three times. Computing it once and saving it means the later steps are near-instant — and we never risk crashing the driver by pulling millions of rows into one machine."*

---

## Two more, if you have time

- **Lever 7 — Use Delta + file maintenance.** Convert CSV/JSON inputs to **Delta**, run `OPTIMIZE` to compact small files, and use partitioning or **Liquid Clustering** so filters skip files. Columnar + statistics = less I/O.
- **Lever 8 — Cut redundant wide ops.** Replace `distinct()` you don't need, collapse multiple `groupBy`s into one, and avoid `orderBy` on the full dataset when a partial/top-N (`limit` after `orderBy`, which AQE optimizes) is enough.

---

## The 30-second ranking you say at the start of Phase 2

> *"Let me rank by impact. Shuffles cost the most, so first I'll check the joins — is anything a sort-merge that should be a broadcast? Then I'll make sure we filter and select columns before the heavy work. Then partitioning and any Python UDFs. Skew and recomputation last, if the profile points there. I'll confirm each guess in the query profile rather than assume."*

That sentence demonstrates computational thinking *and* gives you a checklist to work through.
