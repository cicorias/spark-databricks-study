# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Broadcast Joins & Skew Handling (deep dive)
# MAGIC
# MAGIC Two of the highest-impact levers in Spark performance. This notebook *shows* both in the physical plan and on a real skewed dataset.
# MAGIC
# MAGIC See: [04 — Spark Optimization Playbook](../../04-Spark-Optimization-Playbook.md), levers 1 & 5.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

CATALOG, SCHEMA = "workspace", "default"

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Broadcast vs sort-merge — the physical plan tells the story

# COMMAND ----------

big   = spark.range(0, 10_000_000).withColumn("key", F.col("id") % 1000).drop("id")
small = spark.range(0, 1000).withColumnRenamed("id", "key").withColumn("dim_val", F.col("key") * 10)

# 1) Default — Spark may or may not broadcast depending on size estimate
big.join(small, "key").explain("formatted")

# COMMAND ----------

# 2) Force a SortMergeJoin by disabling auto-broadcast
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
big.join(small, "key").explain("formatted")     # SortMergeJoin with Exchange on BOTH sides

# COMMAND ----------

# 3) Force a BroadcastHashJoin with the hint
big.join(broadcast(small), "key").explain("formatted")   # No Exchange on the big side

# COMMAND ----------

# Restore the default
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)   # 10 MB

# COMMAND ----------

# MAGIC %md
# MAGIC ### When NOT to broadcast
# MAGIC The "small" side has to *actually* be small. Rough rule: ≤ 10–100 MB serialized. If you broadcast a 5 GB table, executors OOM.
# MAGIC
# MAGIC Quick size check before deciding:

# COMMAND ----------

# Approximate size by sampling + scaling — cheap heuristic, not exact
def estimate_mb(df, sample_frac=0.01):
    sample_rows = df.sample(sample_frac).toPandas()
    if sample_rows.empty:
        return 0.0
    return sample_rows.memory_usage(deep=True).sum() / sample_frac / (1024 * 1024)

print(f"small ≈ {estimate_mb(small):0.2f} MB → broadcast")
# print(f"big   ≈ {estimate_mb(big):0.2f} MB → do NOT broadcast")   # don't sample 10M for a demo

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Skew — symptoms and fixes
# MAGIC We'll build a skewed dataset where one key has 50% of all rows, then see the symptom in `spark_partition_id` and try three fixes.

# COMMAND ----------

# Skewed fact table: key=0 holds half the rows
skewed = (spark.range(0, 10_000_000)
    .withColumn("key", F.when(F.col("id") < 5_000_000, F.lit(0)).otherwise(F.col("id") % 100))
    .withColumn("val", F.rand(seed=7)))

dim = spark.range(0, 100).withColumnRenamed("id", "key").withColumn("label", F.concat(F.lit("k_"), F.col("key")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Symptom — one partition dwarfs the rest after a hash-shuffle on `key`

# COMMAND ----------

repartitioned = skewed.repartition(20, "key")
(repartitioned.groupBy(F.spark_partition_id().alias("partition_id"))
   .count()
   .orderBy(F.desc("count"))
   .show(20, truncate=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fix A — broadcast the small side (the cheapest, when possible)
# MAGIC No shuffle on the skewed side ⇒ skew can't bite you.

# COMMAND ----------

skewed.join(broadcast(dim), "key").explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fix B — let AQE handle it
# MAGIC AQE skew-join splits oversized partitions during the shuffle. Confirm it's on:

# COMMAND ----------

print("AQE enabled       :", spark.conf.get("spark.sql.adaptive.enabled"))
print("AQE skew join     :", spark.conf.get("spark.sql.adaptive.skewJoin.enabled"))
# After running a query, the Query Profile will show "Adaptive Plan" + per-stage stats.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fix C — salt the hot key (manual; use when AQE + broadcast aren't enough)
# MAGIC We split key=0 across N buckets on both sides so the load spreads.

# COMMAND ----------

N = 16  # salt buckets

skewed_salted = skewed.withColumn(
    "salt",
    F.when(F.col("key") == 0, (F.rand(seed=42) * N).cast("int")).otherwise(F.lit(0)),
)

# Explode the dim row for key=0 into N copies, one per salt bucket; everything else gets salt=0
dim_salted = dim.withColumn(
    "salt",
    F.when(F.col("key") == 0,
           F.explode(F.array([F.lit(i) for i in range(N)])))
     .otherwise(F.lit(0)),
)

salted_join = skewed_salted.join(dim_salted, ["key", "salt"])

# Now look at partition counts AFTER salting — should be much flatter
salted_join.repartition(20, "key", "salt") \
   .groupBy(F.spark_partition_id().alias("partition_id")).count() \
   .orderBy(F.desc("count")).show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### What you say out loud
# MAGIC > *"Half the rows live on a single key, so the shuffle dumps them all on one task — the stage is as slow as that one machine. I'd try broadcast first because the dim is tiny; if I had to keep the shuffle I'd let AQE skew-handling do it, and salt the hot key as a last resort."*

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Anti-pattern recap (so you can name them out loud)
# MAGIC - **Broadcasting something not actually small** → executor OOM. Estimate first.
# MAGIC - **Skipping `broadcast(...)` and hoping AQE figures it out** → AQE needs runtime stats; an explicit hint is reliable when you *know* a side is small.
# MAGIC - **Using `repartition(col)` to "fix skew"** → that's the *shuffle that creates the skew*; salting or broadcasting are the actual fixes.
