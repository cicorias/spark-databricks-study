# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Spark Fundamentals (transformations vs actions, narrow vs wide, partitions)
# MAGIC
# MAGIC **Goal:** be able to point at every concept in note 03 with a concrete cell. Run these top-to-bottom, **predicting** what each will do *before* you run it.
# MAGIC
# MAGIC See [03 — Spark Mental Models](../../03-Spark-Mental-Models.md).

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Lazy evaluation — defining the plan vs running it

# COMMAND ----------

# Nothing runs yet — these are all transformations (lazy)
df = (spark.range(0, 10_000_000)
      .withColumn("grp", F.col("id") % 100)
      .withColumn("val", F.rand(seed=42)))

filtered = df.where(F.col("grp") < 5)
projected = filtered.select("id", "val")

print("No job has run yet. The plan is built but nothing is executed.")

# COMMAND ----------

# THIS triggers a job (action)
projected.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read the physical plan
# MAGIC Look for: `Exchange` (shuffle), `BroadcastHashJoin` vs `SortMergeJoin`, `PushedFilters`, the column list in the scan.

# COMMAND ----------

projected.explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Narrow vs wide — count the shuffles
# MAGIC `select` / `filter` / `withColumn` are **narrow** (no shuffle). `groupBy` / `join` / `distinct` / `orderBy` are **wide** (shuffle).

# COMMAND ----------

# NARROW chain — no Exchange in the plan
narrow = df.select("id", "val").where(F.col("val") > 0.5).withColumn("two_x", F.col("val") * 2)
narrow.explain("formatted")

# COMMAND ----------

# WIDE — groupBy forces an Exchange (= a new stage)
wide = df.groupBy("grp").agg(F.sum("val").alias("total"))
wide.explain("formatted")
wide.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Partitions — the unit of parallelism
# MAGIC On serverless we can't call `df.rdd.getNumPartitions()`. Use `F.spark_partition_id()` instead.

# COMMAND ----------

(df.groupBy(F.spark_partition_id().alias("partition"))
   .count()
   .orderBy("partition")
   .show(truncate=False))

# COMMAND ----------

# Shuffle output is controlled by spark.sql.shuffle.partitions (default 200).
# AQE coalesces these adaptively at runtime so you usually don't have to babysit.
print("Configured shuffle partitions:", spark.conf.get("spark.sql.shuffle.partitions"))
print("AQE enabled:", spark.conf.get("spark.sql.adaptive.enabled"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. `repartition` vs `coalesce`

# COMMAND ----------

# repartition(n) = full shuffle to get exactly n partitions — use to increase parallelism / redistribute by key
rep = df.repartition(8, "grp")
(rep.groupBy(F.spark_partition_id().alias("partition"))
    .count().orderBy("partition").show(truncate=False))

# COMMAND ----------

# coalesce(n) = reduce only, no shuffle — use right before writing to avoid tiny output files
coa = df.coalesce(4)
(coa.groupBy(F.spark_partition_id().alias("partition"))
    .count().orderBy("partition").show(truncate=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Actions to know — and the ones to fear
# MAGIC `show`/`take`/`limit`/`write`/`count` are safe. `collect`/`toPandas` pull everything to the **driver** — danger on big data.

# COMMAND ----------

# Safe — only ships 5 rows back
df.limit(5).show()

# Safe — only ships 5 rows back, materialized as pandas
df.limit(5).toPandas()

# DON'T do this on a real dataset:
# df.collect()    # ← would try to pull 10M rows to the driver

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Self-test (say the answers out loud)
# MAGIC
# MAGIC 1. Why is `filter` cheap but `groupBy` expensive?
# MAGIC 2. What does `Exchange` mean in `explain` output?
# MAGIC 3. Your job has 200 tasks but the data is tiny — what's happening and what fixes it?
# MAGIC 4. When would you `coalesce(8)` vs `repartition(8)`?
# MAGIC 5. Why is `df.toPandas()` dangerous on a 1B-row DataFrame?
