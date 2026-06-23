---
title: PySpark Cheatsheet
tags: [pyspark, reference, cheatsheet, serverless]
---

# 09 — PySpark Cheatsheet (serverless-safe)

[← Mock Q&A](08-Mock-QA-and-Talking-Points.md) · [Back to README](README.md)

Everything here works on **Free Edition / serverless**. Snippets that *don't* work there are flagged ⛔ with the alternative.

## Setup & imports

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import broadcast
print(spark.version)
```

## Make data without RDDs

```python
df = spark.range(0, 1_000_000)                       # column "id"
df = df.withColumn("grp", (F.col("id") % 10))
# ⛔ sc.parallelize([...])  -> not on serverless. Use spark.range / spark.createDataFrame (small).
```

## Read / write (Delta is the default sweet spot)

```python
df = spark.table("workspace.default.orders")
df = spark.read.format("delta").load("/Volumes/.../path")     # or .csv/.parquet/.json
df = (spark.read.option("header", True).option("inferSchema", True)
        .csv("/Volumes/catalog/schema/vol/file.csv"))

df.write.mode("overwrite").saveAsTable("workspace.default.out")        # managed table
df.write.format("delta").mode("append").save("/Volumes/.../path")
df.createOrReplaceTempView("v")                       # NOT registerTempTable (deprecated)
```

## Core transformations

```python
df.select("a", "b")                                   # projection (prune!)
df.selectExpr("a", "b * 2 as b2")
df.where(F.col("year") == 2025)                       # == df.filter(...)
df.withColumn("rev", F.col("qty") * F.col("price"))
df.withColumnRenamed("old", "new")
df.drop("c")
df.distinct()                                         # wide — use only if needed
df.dropDuplicates(["id"])
df.orderBy(F.desc("rev"))                             # wide
df.limit(100)
```

## Aggregations

```python
(df.groupBy("category")
   .agg(F.sum("rev").alias("total"),
        F.avg("price").alias("avg_price"),
        F.count("*").alias("n"),
        F.countDistinct("customer_id").alias("customers"),     # expensive at scale
        F.approx_count_distinct("customer_id").alias("approx"))) # cheaper
```

## Joins (mind the strategy)

```python
big.join(small, "key")                                # default; may be sort-merge
big.join(broadcast(small), "key")                     # force broadcast (small dim)
a.join(b, on=["k1", "k2"], how="left")                # how: inner|left|right|outer|left_semi|left_anti
a.join(b, a.id == b.fk, "inner")                      # expression join
```

## Common column functions

```python
F.lower, F.upper, F.trim, F.length, F.concat, F.concat_ws, F.substring
F.when(cond, x).otherwise(y)
F.coalesce("a", "b")                                  # first non-null
F.cast / F.col("x").cast("int")
F.to_date, F.to_timestamp, F.date_format, F.year, F.month, F.datediff
F.round, F.floor, F.ceil, F.abs
F.regexp_replace, F.regexp_extract, F.split
F.col("x").isNull(), F.col("x").isin(1,2,3), F.col("x").between(0,120)
F.explode, F.array, F.struct, F.collect_list, F.collect_set
```

## Window functions

```python
w = Window.partitionBy("customer_id").orderBy(F.desc("ts"))
df.withColumn("rn", F.row_number().over(w))           # latest per customer where rn == 1
df.withColumn("run_total", F.sum("rev").over(
        Window.partitionBy("cat").orderBy("ts")
              .rowsBetween(Window.unboundedPreceding, Window.currentRow)))
df.withColumn("prev", F.lag("rev", 1).over(w))
```

## Partitioning

```python
df.repartition(200)                                   # full shuffle, exact count
df.repartition("customer_id")                         # hash-partition by col
df.coalesce(8)                                        # reduce only, no shuffle (pre-write)
spark.conf.set("spark.sql.shuffle.partitions", 64)    # supported subset only on serverless
```

## Diagnostics (serverless way)

```python
df.explain("formatted")            # read the physical plan
df.printSchema()
df.count()                         # an action — triggers execution

# skew / partition distribution WITHOUT RDDs:
(df.groupBy(F.spark_partition_id().alias("pid")).count()
   .orderBy(F.desc("count")).show())

# ⛔ df.rdd.getNumPartitions()  -> RDD path not on serverless
# ⛔ Spark UI stages tab        -> use the QUERY PROFILE on the cell output instead
```

In `explain` output, look for: `Exchange` (shuffle, costly) · `BroadcastHashJoin` (good) · `SortMergeJoin` (shuffles both sides) · `PushedFilters` (predicate pushdown working) · the scan's column list (projection pruning working).

## "Caching" on serverless

```python
# ⛔ df.cache() / df.persist() / df.unpersist()  -> restricted on serverless
# ✅ materialize instead:
df.write.mode("overwrite").saveAsTable("workspace.default.step1")
step1 = spark.table("workspace.default.step1")
```

## Useful Spark SQL / Delta

```python
spark.sql("SELECT category, SUM(rev) FROM workspace.default.orders GROUP BY category").show()
spark.sql("OPTIMIZE workspace.default.orders")                 # compact small files (Delta)
spark.sql("OPTIMIZE workspace.default.orders ZORDER BY (product_id)")
spark.sql("DESCRIBE DETAIL workspace.default.orders").show()   # size, numFiles
# MERGE (upsert)
spark.sql("""
  MERGE INTO target t USING source s ON t.id = s.id
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *
""")
```

## Config keys that actually exist (don't trust invented ones)

```text
spark.sql.shuffle.partitions            # post-shuffle partition count (default 200)
spark.sql.autoBroadcastJoinThreshold    # auto-broadcast size cutoff (~10MB; -1 disables)
spark.sql.adaptive.enabled              # AQE (default true)
spark.sql.adaptive.coalescePartitions.enabled
spark.sql.adaptive.skewJoin.enabled     # AQE skew handling
spark.sql.ansi.enabled                  # ANSI mode (default true on serverless)
```

> If the assistant suggests a config you don't recognize, treat it as a hallucination until verified. (See [07](07-AI-Stewardship-Genie-Code.md).)

## Peeking safely (never `collect()` a big result)

```python
df.show(20, truncate=False)
df.limit(50).toPandas()        # OK only because of limit(50)
# ⛔ df.collect() / df.toPandas() on a large df  -> driver OOM
```
