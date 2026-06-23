---
applyTo: "**/*.py,**/*.sql,**/*.scala,**/*.ipynb"
---

# Databricks Serverless Compute — Coding Instructions

All code and examples **must** be compatible with **Databricks Serverless compute**
(Spark Connect architecture, DBR 14+). Apply every rule below before generating or
suggesting any code.

---

## ❌ BLOCKED — Never generate these

### RDD / SparkContext APIs (Blocker — will throw at runtime)

| Do NOT use | Use instead |
|---|---|
| `sc.parallelize(...)` | `spark.createDataFrame([...], schema)` or `spark.range(n)` |
| `sc.textFile(path)` | `spark.read.text(path)` |
| `sc.wholeTextFiles(path)` | `spark.read.format("binaryFile").load(path)` |
| `sc.broadcast(obj)` | `from pyspark.sql.functions import broadcast; broadcast(df)` |
| `sc.accumulator(...)` | `df.agg(F.sum("col"))` / `df.count()` |
| `spark.sparkContext` / `sc` | Use `spark` (SparkSession) DataFrame/SQL API only |
| `sqlContext.*` | `spark.sql(...)` or `spark.table(...)` |
| `sc.hadoopConfiguration.set(...)` | Use Unity Catalog external locations — no credential config needed |
| `SparkContext.getOrCreate()` | `spark.createDataFrame(...)` or `spark.range(n)` |
| `df.rdd`, `rdd.map`, `rdd.filter`, `rdd.reduce` | DataFrame API equivalents (see table below) |
| `rdd.flatMap(fn)` | `df.select(F.explode(F.split(col, " ")))` |
| `rdd.groupByKey()` | `df.groupBy("key").agg(F.collect_list("value"))` |
| `rdd.reduceByKey(fn)` | `df.groupBy("key").agg(F.sum("value"))` |
| `rdd.mapPartitions(fn)` | `df.groupBy(F.spark_partition_id()).applyInPandas(fn, schema)` |
| `rdd.foreachPartition(fn)` | Rewrite with `applyInPandas` or DataFrame sink |
| `rdd.toDF()` | `spark.createDataFrame(data, schema)` |
| `rdd.getNumPartitions()` | `spark.sql("SELECT COUNT(DISTINCT spark_partition_id()) FROM t")` |

### Caching (Warning — raises on serverless)

| Do NOT use | Use instead |
|---|---|
| `df.cache()` | Materialize to a Delta table: `df.write.mode("overwrite").saveAsTable("cat.schema.tbl")` |
| `df.persist()` / `df.unpersist()` | Remove — write expensive intermediates to Delta |
| `df.checkpoint()` | Write to Delta table |
| `CACHE TABLE` / `UNCACHE TABLE` | Remove |
| `spark.catalog.cacheTable(...)` | Remove |

### Languages (Blocker)

- Never generate `%scala` notebook cells — port to PySpark or SQL
- Never generate `%r` notebook cells — port to PySpark
- Never generate `CREATE GLOBAL TEMPORARY VIEW` — use `CREATE OR REPLACE TEMPORARY VIEW`
- Never reference `global_temp.` database prefix — global temp views don't exist on serverless

### Data Paths (Blocker — DBFS is not available)

| Do NOT use | Use instead |
|---|---|
| `dbfs:/path` or `"/dbfs/path"` | `/Volumes/<catalog>/<schema>/<volume>/path` |
| `dbfs:/mnt/...` | UC external location + UC Volume |
| `dbutils.fs.mount(...)` | Create UC external location |
| `file:///dbfs/...` | `/Volumes/...` (persistent) or `/tmp/` (temp) |

Checkpoint locations must use `/Volumes/...`, not DBFS.

### Streaming Triggers (Blocker)

| Do NOT use | Use instead |
|---|---|
| `.trigger(processingTime="10 seconds")` | `.trigger(availableNow=True)` |
| `.trigger(continuous="1 second")` | Migrate to Spark Declarative Pipelines (SDP) |
| `.writeStream` **without** `.trigger(...)` | Always add `.trigger(availableNow=True)` — the Spark default `ProcessingTime("0s")` is blocked |

### Libraries (Blocker)

| Do NOT use | Use instead |
|---|---|
| `dbutils.library.install(jarPath)` | Compile as JAR job task (Scala 2.13 / JDK 17, environment v4+) |
| Maven coordinates in cluster/job config | PyPI packages via `%pip install` |
| `%pip install pyspark` | Remove — installing PySpark breaks the serverless session |

### Spark Configuration (Blocker / Warning)

Only **6** `spark.conf.set(...)` calls are permitted. Do not generate any other:

spark.sql.shuffle.partitions
spark.sql.session.timeZone
spark.sql.ansi.enabled
spark.sql.files.maxPartitionBytes
spark.sql.legacy.timeParserPolicy
spark.databricks.execution.timeout



Do NOT generate (auto-tuned, will be ignored or error):
- `spark.sql.adaptive.*`
- `spark.executor.*`
- `spark.driver.memory`
- `spark.databricks.delta.autoCompact.enabled`
- `spark.databricks.delta.optimizeWrite.enabled`
- `spark.default.parallelism`

### Hive Compatibility (Blocker)

| Do NOT use | Use instead |
|---|---|
| `${hivevar:name}` variable syntax | SQL session variables: `DECLARE OR REPLACE VARIABLE name = value` |
| `SET hivevar:name = value` | `DECLARE OR REPLACE VARIABLE name = value` (DBR 14.1+) |
| `hive_metastore.schema.table` | UC three-level namespace: `catalog.schema.table` |
| `CREATE DATABASE/SCHEMA` without `USE CATALOG` first | Prepend `USE CATALOG <catalog>` or qualify fully |
| Two-level table references `schema.table` | Three-level: `catalog.schema.table` |

### Debugging / Monitoring

| Do NOT reference | Use instead |
|---|---|
| Spark UI, Spark History Server, `spark.ui.port` | **Query Profile** — click "See performance" under cell output |
| `df.rdd.getNumPartitions()` | `df.groupBy(F.spark_partition_id()).count()` or `.explain("formatted")` |

---

## ✅ ALWAYS use these patterns on serverless

```python
# Generate data without RDDs
df = spark.range(1_000_000)
df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])

# All DataFrame API: select, filter/where, groupBy, agg, join, withColumn, window functions
from pyspark.sql import functions as F

# Broadcast join hint (this works — it is NOT sc.broadcast)
from pyspark.sql.functions import broadcast
big_df.join(broadcast(small_df), "key")

# Read physical plan — your primary diagnostic tool
df.explain("formatted")

# Detect partition skew without RDD
df.groupBy(F.spark_partition_id().alias("pid")).count().orderBy(F.desc("count")).show()

# Repartitioning — fine on serverless
df.repartition(200)
df.coalesce(10)
df.repartition(F.col("date"))

# Materialize expensive intermediate (serverless "cache" equivalent)
df.write.mode("overwrite").saveAsTable("catalog.schema.reused_step")
reused = spark.table("catalog.schema.reused_step")

# Delta operations
df.write.format("delta").mode("overwrite").saveAsTable("catalog.schema.tbl")
spark.sql("OPTIMIZE catalog.schema.tbl ZORDER BY (col)")

# Safe streaming with AvailableNow
(df.writeStream
   .format("delta")
   .outputMode("append")
   .trigger(availableNow=True)
   .option("checkpointLocation", "/Volumes/main/data/checkpoints/s1")
   .start("/Volumes/main/data/output/s1")
   .awaitTermination())

# Libraries: notebook-scoped only
# %pip install numpy==2.2.2 pandas==2.2.3  (pin versions)

# ANSI mode is ON by default — use safe casts
df.select(F.expr("try_cast(col AS INT)"))
df.select(F.expr("try_divide(a, b)"))


Quick Compatibility Checklist (apply before finalizing any code)


 No sc.*, SparkContext, sqlContext, or .rdd usage

 No .cache(), .persist(), CACHE TABLE

 No dbfs:/ or /dbfs/ paths — use /Volumes/...

 No %scala or %r cells

 No CREATE GLOBAL TEMPORARY VIEW

 Streaming: .trigger(availableNow=True) on every .writeStream

 %pip install pyspark is absent

 Only the 6 permitted spark.conf.set(...) keys are used

 Table references use 3-level catalog.schema.table namespace

 No Hive variable syntax ${hivevar:...}

 AQE / executor / driver configs removed (auto-managed)

 spark.sql.ansi.enabled behavior accounted for (default true)



Sources: 02-Databricks-Free-Edition-Serverless-Gotchas.md,
.agents/skills/databricks-serverless-migration/references/compatibility-checks.md,
.agents/skills/databricks-serverless-migration/references/configuration-guide.md,
.agents/skills/databricks-serverless-migration/references/streaming-migration.md



---

**Summary of what this file enforces:**

| Category | Key Restrictions |
|---|---|
| **RDD/SparkContext** | All `sc.*`, `.rdd.*`, `SparkContext` — blocked, use DataFrame API |
| **Caching** | `.cache()`, `.persist()`, `CACHE TABLE` — remove, materialize to Delta |
| **Language** | No `%scala`, `%r`, no global temp views |
| **Data paths** | No `dbfs:/` — use `/Volumes/catalog/schema/vol/path` |
| **Streaming** | Only `availableNow=True` trigger; missing trigger = failure |
| **Libraries** | No JARs/Maven in notebooks; no `%pip install pyspark`; pin versions |
| **Configuration** | Only 6 allowed `spark.conf.set` keys; AQE/executor configs are auto-managed |
| **Hive compat** | No `${hivevar:...}`, no 2-level table names, no DBFS mounts |
| **Debugging** | Use Query Profile instead of Spark UI; use DataFrame plan instead of RDD |
| **ANSI SQL** | On by default — use `try_cast`/`try_divide` for safe conversions |