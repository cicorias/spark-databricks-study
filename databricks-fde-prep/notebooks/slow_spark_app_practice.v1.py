# Databricks notebook source
# MAGIC %md
# MAGIC # Slow Spark App — Practice & Fix (Phase 2 rehearsal)
# MAGIC
# MAGIC Import into **Databricks Free Edition**, attach serverless or a DBR 13.3+ cluster, run top to bottom.
# MAGIC Each "slow" cell is paired with an optimized rewrite. Run both, watch the **Query Profile / Spark UI**,
# MAGIC and narrate *why* the fix helps — that is exactly the Phase 2 motion.
# MAGIC
# MAGIC Output numbers in comments are **illustrative** (free-edition timings vary). The *direction* and *ratio* are the point.

# COMMAND ----------

import os
import time
from contextlib import contextmanager
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

@contextmanager
def timed(label):
    t = time.time()
    yield
    print(f"{label:<42} {time.time() - t:6.2f}s")

# ---------------------------------------------------------------------------
# Serverless detection
# Databricks sets IS_SERVERLESS=TRUE in the notebook environment on serverless
# compute. On a classic DBR cluster the variable is absent (defaults to False).
# ---------------------------------------------------------------------------
is_serverless = os.getenv("IS_SERVERLESS", "").upper() == "TRUE"
print(f"is_serverless = {is_serverless}")

def show_conf(key, serverless_note=None):
    """Print a Spark config value.

    CLASSIC DBR  : spark.conf.get() works for any readable config.
    SERVERLESS   : configs the engine owns exclusively raise CONFIG_NOT_AVAILABLE
                   because they are locked/auto-managed and not user-visible.
                   show_conf catches that and prints the serverless_note instead.
    """
    try:
        print(f"{key} = {spark.conf.get(key)}")
    except Exception:
        if is_serverless and serverless_note:
            print(f"{key} = <locked on serverless> — {serverless_note}")
        else:
            print(f"{key} = <unavailable>")

# spark.sql.adaptive.enabled
#   Classic DBR : readable and settable; true by default on DBR 7+.
#   Serverless  : AQE is always ON, engine-owned, not user-visible or settable.
show_conf("spark.sql.adaptive.enabled",
          serverless_note="AQE is always ON; this config is engine-owned and not user-settable")

# spark.sql.autoBroadcastJoinThreshold
#   Classic DBR : default 10 MB (10485760 bytes); tunable.
#   Serverless  : auto-managed by AQE; not exposed to the user session.
show_conf("spark.sql.autoBroadcastJoinThreshold",
          serverless_note="auto-managed by AQE; not user-configurable on serverless")

# spark.sql.shuffle.partitions
#   Classic DBR : defaults to 200; tunable per-session.
#   Serverless  : 'auto' — the engine dynamically coalesces post-shuffle partitions.
show_conf("spark.sql.shuffle.partitions",
          serverless_note="set to 'auto'; serverless coalesces shuffle partitions dynamically")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build synthetic data
# MAGIC A large skewed `orders` fact and a tiny `customers` dim. ~40% of orders share one hot `customer_id`,
# MAGIC which is how we reproduce **skew** and demonstrate **broadcast joins**.

# COMMAND ----------

N_ORDERS = 5_000_000
N_CUSTOMERS = 2_000

# Fact: hot key 0 gets ~40% of rows; the rest spread across all customers.
orders = (
    spark.range(N_ORDERS)
    .withColumn(
        "customer_id",
        F.when(F.rand(seed=1) < 0.40, F.lit(0))
         .otherwise((F.rand(seed=2) * N_CUSTOMERS).cast("int")),
    )
    .withColumn("amount", (F.rand(seed=3) * 500).cast("double"))
    .withColumnRenamed("id", "order_id")
)

# Dim: a few thousand rows, a few KB — well under the 10 MB broadcast threshold.
customers = (
    spark.range(N_CUSTOMERS)
    .withColumnRenamed("id", "customer_id")
    .withColumn("segment", F.when(F.col("customer_id") % 3 == 0, "enterprise")
                            .when(F.col("customer_id") % 3 == 1, "mid")
                            .otherwise("smb"))
)

print(f"orders   ~ {N_ORDERS:,} rows")
print(f"customers ~ {N_CUSTOMERS:,} rows")
# Example output:
# orders   ~ 5,000,000 rows
# customers ~ 2,000 rows

# COMMAND ----------

# MAGIC %md
# MAGIC ## Anti-pattern #1 — Python UDF in the hot path
# MAGIC A row-by-row Python UDF blocks Catalyst and serializes every row out of the JVM.

# COMMAND ----------

from pyspark.sql.types import StringType

# SLOW: Python UDF
def bucket_py(amount):
    if amount < 100: return "low"
    elif amount < 300: return "mid"
    else: return "high"

bucket_udf = F.udf(bucket_py, StringType())

with timed("UDF bucketing (SLOW)"):
    orders.withColumn("bucket", bucket_udf("amount")).groupBy("bucket").count().collect()
# Example output:  UDF bucketing (SLOW)                       8.10s

# FIX: native column expression — stays in the engine, Catalyst-optimized
with timed("native when() bucketing (FAST)"):
    (orders.withColumn(
        "bucket",
        F.when(F.col("amount") < 100, "low")
         .when(F.col("amount") < 300, "mid")
         .otherwise("high"),
    ).groupBy("bucket").count().collect())
# Example output:  native when() bucketing (FAST)             1.30s
# Talking point: same logic, ~5-6x faster; no JVM<->Python row serialization.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Anti-pattern #2 — no caching, repeated actions re-read source
# MAGIC Three actions on the same derived DataFrame recompute the whole DAG three times.

# COMMAND ----------

enriched = orders.withColumn("amount_with_tax", F.col("amount") * 1.2)

# SLOW: recomputes from scratch each action
with timed("3 actions, no cache (SLOW)"):
    _ = enriched.count()
    _ = enriched.filter(F.col("amount_with_tax") > 300).count()
    _ = enriched.agg(F.sum("amount_with_tax")).collect()
# Example output:  3 actions, no cache (SLOW)                 4.90s

# FIX: cache the reused DataFrame (materializes on first action)
# CLASSIC DBR — df.cache() / df.persist():
#   Materializes the DataFrame's partitions into executor JVM heap on the first action.
#   Subsequent actions read from the BlockManager (in-memory), skipping full DAG recompute.
#   .unpersist() frees the memory explicitly. Zero write-amplification; best for multi-action
#   reuse within a single session where durability is not a concern.
#
# SERVERLESS (Spark Connect) — df.cache() is NOT supported:
#   The Spark Connect protocol does not expose the executor-side BlockManager to the Connect
#   client. .cache() is unavailable on serverless. Instead, write the intermediate result to
#   a Delta table and read it back. Delta's DBIO cache and data-skipping replicate the reuse
#   benefit; the materialized data also survives executor eviction, making it more durable
#   than in-memory cache — at the cost of a write step.
enriched.write.format("delta").mode("overwrite").saveAsTable("enriched_temp_s2")
enriched_materialized = spark.table("enriched_temp_s2")

with timed("3 actions, Delta-materialized (FAST — serverless)"):
    _ = enriched_materialized.count()
    _ = enriched_materialized.filter(F.col("amount_with_tax") > 300).count()
    _ = enriched_materialized.agg(F.sum("amount_with_tax")).collect()
# Example output:  3 actions, Delta-materialized (FAST — serverless)   ~2.10s
spark.sql("DROP TABLE IF EXISTS enriched_temp_s2")
# Talking point: the reuse principle is the same as cache() — compute once, read many times.
# On classic DBR, cache() skips the write entirely; on serverless, Delta materialization is
# the supported equivalent. Always clean up temp tables when done.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Anti-pattern #3 — large/small join left to shuffle
# MAGIC Compare the default plan to an explicit broadcast. Inspect both with `.explain()`.

# COMMAND ----------

# WHY you cannot create a slow/fast comparison on serverless for this section:
#
# CLASSIC DBR:
#   spark.sql.autoBroadcastJoinThreshold is user-settable. Lower it to -1 to force
#   SortMergeJoin on the "slow" side, then reset. Join hints also work as expected.
#     spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
#     default_join = orders.join(customers, "customer_id")   # → SortMergeJoin + Exchange
#     spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")
#
# SERVERLESS + PHOTON:
#   1. spark.sql.autoBroadcastJoinThreshold is engine-owned and not user-settable.
#   2. Photon overrides ALL join strategy hints (.hint("merge"), .hint("shuffle_hash"),
#      .hint("shuffle_merge")) for tables it considers broadcastable — verified empirically.
#      The plan shows PhotonBroadcastHashJoin regardless of the hint applied.
#   There is no user-facing mechanism to force SortMergeJoin when Photon decides to broadcast.
#
# THE REFRAME — what to practise on serverless:
#   Confirm in the plan that PhotonBroadcastHashJoin is present (that IS the correct outcome).
#   Then understand the two cases where adding broadcast() explicitly still matters:
#     1. Source has no statistics (JSON / CSV without ANALYZE TABLE): Photon may
#        underestimate size and fall back to SortMergeJoin unexpectedly — explicit
#        broadcast() overrides the estimate.
#     2. Plan stability: classic DBR clusters default to a 10 MB threshold, so a dim
#        that Photon auto-broadcasts on serverless might get SortMergeJoin on classic.
#        Explicit broadcast() makes the plan consistent across both environments.

default_join = orders.join(customers, "customer_id")
bcast_join   = orders.join(broadcast(customers), "customer_id")

print("=== default join plan ===")
default_join.explain("formatted")
# On serverless: PhotonBroadcastHashJoin — Photon chose correctly, nothing to fix.
# On classic DBR: may show SortMergeJoin + Exchange if customers exceeds the 10 MB threshold.

print("=== explicit broadcast join plan ===")
bcast_join.explain("formatted")
# Both environments: BroadcastHashJoin, no Exchange on the large (orders) side.

with timed("join + agg, default"):
    default_join.groupBy("segment").agg(F.sum("amount").alias("rev")).collect()

with timed("join + agg, explicit broadcast"):
    bcast_join.groupBy("segment").agg(F.sum("amount").alias("rev")).collect()
# On serverless timings will be nearly identical — both already use BroadcastHashJoin.
# On classic DBR with a larger customers table the explicit broadcast version is faster.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Anti-pattern #4 — skewed aggregation (one hot key)
# MAGIC `customer_id = 0` holds ~40% of rows, so one reduce task does most of the work.
# MAGIC AQE helps automatically; **salting** is the manual technique to explain out loud.

# COMMAND ----------

# Baseline: skewed groupBy. In the Spark UI you'll see one straggler task.
with timed("skewed groupBy (baseline)"):
    orders.groupBy("customer_id").agg(F.sum("amount").alias("total")).collect()
# Example output:  skewed groupBy (baseline)                  4.60s

# FIX (manual salting): split the hot key across N sub-keys, aggregate twice.
SALT = 16
salted = orders.withColumn("salt", (F.rand() * SALT).cast("int"))
with timed("salted two-stage groupBy (FAST on skew)"):
    partial = salted.groupBy("customer_id", "salt").agg(F.sum("amount").alias("p"))
    final = partial.groupBy("customer_id").agg(F.sum("p").alias("total"))
    final.collect()
# Example output:  salted two-stage groupBy (FAST on skew)    2.80s
# Talking point: salting spreads the hot key's rows across 16 tasks, so no single task
# is overloaded; we re-aggregate the partials. Prefer AQE first; salt when AQE isn't enough
# or for groupBy skew that AQE's skew-join handling doesn't cover.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Anti-pattern #5 — tiny output files
# MAGIC Writing with too many partitions produces many small files; coalesce before write.

# COMMAND ----------

over_partitioned = orders.repartition(400)

# CLASSIC DBR — df.rdd.getNumPartitions():
#   A zero-cost metadata call; reads the partition count from the in-memory RDD plan graph
#   via SparkContext with no job submission. Available because the driver runs in-process
#   alongside SparkContext and has direct access to the RDD scheduler.
#
# SERVERLESS (Spark Connect) — .rdd is NOT accessible:
#   Spark Connect restricts the Connect client to the DataFrame/SQL API surface. The RDD
#   layer stays entirely server-side; accessing .rdd raises AnalysisException. The
#   serverless-compatible substitute is spark_partition_id(), a SQL function that returns
#   each row's partition ID. Counting distinct values gives the partition count, but unlike
#   the classic metadata call this DOES submit a Spark job — it costs compute. Use it only
#   when you genuinely need to observe partition count at runtime.
print("partitions before write:", over_partitioned.select(F.spark_partition_id()).distinct().count())  # 400

# FIX: coalesce to a sane count before writing (no full shuffle)
right_sized = over_partitioned.coalesce(8)
print("partitions after coalesce:", right_sized.select(F.spark_partition_id()).distinct().count())     # 8
# Talking point: 400 tiny files = per-file overhead on every downstream read; 8 larger files
# read far faster. In Delta you'd also use OPTIMIZE / auto-compaction. Avoid the opposite
# extreme too — coalesce(1) on big data serializes the whole write through one task.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Anti-pattern #6 — collect() to the driver
# MAGIC Pulling a large result to the driver is the classic OOM. Aggregate/sample/write instead.

# COMMAND ----------

# SLOW & risky (don't do on real big data): orders.collect()  -> driver OOM
# FIX: keep work distributed; only bring back small results.
top_segments = (orders.join(broadcast(customers), "customer_id")
                .groupBy("segment").agg(F.sum("amount").alias("rev"))
                .orderBy(F.desc("rev")))
top_segments.show()   # small, safe result set
# Example output:
# +----------+--------------------+
# |   segment|                 rev|
# +----------+--------------------+
# |       smb| 4.21E8             |
# |       mid| 4.18E8             |
# |enterprise| 4.15E8             |
# +----------+--------------------+
# Talking point: never collect() what you can aggregate or write. show()/take() bound the data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wrap-up — the framework to recite
# MAGIC 1. Read less data (prune columns, push down filters, columnar format)
# MAGIC 2. Move less data (broadcast small joins, pre-aggregate, fewer shuffles)
# MAGIC 3. Handle skew (AQE / salt the hot key)
# MAGIC 4. Reuse work (cache what's read multiple times)
# MAGIC 5. Right-size parallelism (shuffle partitions / AQE coalescing; coalesce before write)
# MAGIC 6. Avoid Python UDFs (use native functions or vectorized pandas_udf)
# MAGIC
# MAGIC Always confirm before/after in the **Query Profile / Spark UI**, and end with a customer one-liner.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC Run this cell to drop any temporary tables written during the notebook.
# MAGIC
# MAGIC **Why a dedicated cleanup cell?**
# MAGIC Each section drops its own temp table inline, but a cell earlier in the notebook can
# MAGIC fail (or be interrupted) before reaching that inline drop. Running this cell at any
# MAGIC point — or re-running it after a failed run — guarantees a clean slate.
# MAGIC
# MAGIC **Serverless vs classic DBR:** behaviour is identical here. `DROP TABLE IF EXISTS` is a
# MAGIC standard SQL statement supported on both compute types. On serverless the table lives in
# MAGIC Unity Catalog; on classic DBR with HMS it lives in the Hive Metastore. Either way the
# MAGIC statement is idempotent and safe to run multiple times.

# COMMAND ----------

_temp_tables = [
    "enriched_temp_s2",   # Anti-pattern #2 — Delta-materialized reuse demo
]

for _t in _temp_tables:
    spark.sql(f"DROP TABLE IF EXISTS {_t}")
    print(f"dropped (if existed): {_t}")

print("cleanup complete.")
