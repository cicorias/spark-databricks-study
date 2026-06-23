# Databricks notebook source
# MAGIC %md
# MAGIC # Slow Spark App — SOLUTION (fixes + talking points)
# MAGIC
# MAGIC Each section pairs the slow version with the optimized rewrite, the example timing, and the
# MAGIC **out-loud explanation** + **customer one-liner** to deliver. Output numbers are illustrative
# MAGIC (free-edition timings vary); the direction and ratio are the point.

# COMMAND ----------

import os
import time
from contextlib import contextmanager
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from pyspark.sql.types import StringType

@contextmanager
def timed(label):
    t = time.time()
    yield
    print(f"{label:<42} {time.time() - t:6.2f}s")

# ---------------------------------------------------------------------------
# Serverless detection
# Databricks sets IS_SERVERLESS=TRUE in the notebook environment on serverless
# compute. On a classic DBR cluster the variable is absent (defaults to False).
# Use this flag anywhere behaviour differs between the two compute types.
# ---------------------------------------------------------------------------
is_serverless = os.getenv("IS_SERVERLESS", "").upper() == "TRUE"
print(f"is_serverless = {is_serverless}")

def show_conf(key, serverless_note=None):
    """Print a Spark config value.

    CLASSIC DBR  : spark.conf.get() works for any readable config.
    SERVERLESS   : configs that the engine owns exclusively raise
                   CONFIG_NOT_AVAILABLE — they are locked/auto-managed and not
                   user-visible. show_conf catches that and prints the
                   serverless_note instead of a raw exception.
    """
    try:
        print(f"{key} = {spark.conf.get(key)}")
    except Exception:
        if is_serverless and serverless_note:
            print(f"{key} = <locked on serverless> — {serverless_note}")
        else:
            # On classic DBR this path means the config genuinely doesn't exist.
            print(f"{key} = <unavailable>")

# spark.sql.adaptive.enabled
#   Classic DBR : readable and settable; defaults to true on DBR 7+.
#   Serverless  : AQE is always ON and cannot be changed — the config is owned
#                 by the engine and not exposed to the user session at all.
show_conf("spark.sql.adaptive.enabled",
          serverless_note="AQE is always ON; this config is engine-owned and not user-settable")

# spark.sql.autoBroadcastJoinThreshold
#   Classic DBR : readable/settable; default 10 MB (10485760 bytes).
#   Serverless  : auto-managed by AQE; not configurable by the user session.
show_conf("spark.sql.autoBroadcastJoinThreshold",
          serverless_note="auto-managed by AQE; not user-configurable on serverless")

# spark.sql.shuffle.partitions
#   Classic DBR : defaults to 200; tunable per-session.
#   Serverless  : reports 'auto' — the engine coalesces post-shuffle partitions
#                 dynamically via AQE and ignores a fixed setting.
show_conf("spark.sql.shuffle.partitions",
          serverless_note="set to 'auto'; serverless coalesces shuffle partitions dynamically")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data setup

# COMMAND ----------

N_ORDERS = 5_000_000
N_CUSTOMERS = 2_000

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

customers = (
    spark.range(N_CUSTOMERS)
    .withColumnRenamed("id", "customer_id")
    .withColumn("segment", F.when(F.col("customer_id") % 3 == 0, "enterprise")
                            .when(F.col("customer_id") % 3 == 1, "mid")
                            .otherwise("smb"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1 — Python UDF → native expression

# COMMAND ----------

def bucket_py(amount):
    if amount < 100: return "low"
    elif amount < 300: return "mid"
    else: return "high"
bucket_udf = F.udf(bucket_py, StringType())

with timed("S1 UDF (SLOW)"):
    orders.withColumn("bucket", bucket_udf("amount")).groupBy("bucket").count().collect()
# Example:  S1 UDF (SLOW)                                8.10s

with timed("S1 native when() (FAST)"):
    (orders.withColumn("bucket",
        F.when(F.col("amount") < 100, "low")
         .when(F.col("amount") < 300, "mid")
         .otherwise("high"))
     .groupBy("bucket").count().collect())
# Example:  S1 native when() (FAST)                      1.30s
# Explain: a Python UDF serializes every row out of the JVM to a Python worker and back, and Catalyst
#          can't optimize through it. The native when() stays in the engine. ~5-6x here.
# Customer: "Same business logic, but it now runs inside the engine instead of row-by-row in Python."

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2 — cache the reused DataFrame

# COMMAND ----------

enriched = orders.withColumn("amount_with_tax", F.col("amount") * 1.2)

with timed("S2 no cache (SLOW)"):
    _ = enriched.count()
    _ = enriched.filter(F.col("amount_with_tax") > 300).count()
    _ = enriched.agg(F.sum("amount_with_tax")).collect()
# Example:  S2 no cache (SLOW)                           4.90s

# CLASSIC DBR — df.cache() / df.persist():
#   Pins the DataFrame's computed partitions in executor JVM heap (MEMORY_AND_DISK by default).
#   Lazy: no work happens until the first action fires; subsequent actions read from the
#   BlockManager instead of recomputing the full DAG. .unpersist() releases memory when done.
#   Zero write-amplification; the fastest path for multi-action reuse within a single session.
#
# SERVERLESS (Spark Connect) — df.cache() is NOT supported:
#   Serverless uses the Spark Connect RPC protocol. The client only has access to the
#   DataFrame/SQL API surface; the executor-side BlockManager that backs cache() is not
#   reachable from the Spark Connect driver. Calling .cache() will either silently no-op or
#   raise an error depending on the runtime version.
#   The serverless-compatible equivalent is to materialize the intermediate result to a Delta
#   table and read it back. Delta's local DBIO cache and data-skipping provide similar reuse
#   benefits; the result also survives executor eviction and session restarts — a durability
#   improvement over in-memory cache, at the cost of an explicit write step.

enriched.write.format("delta").mode("overwrite").saveAsTable("enriched_temp_s2")
enriched_materialized = spark.table("enriched_temp_s2")

with timed("S2 Delta-materialized (FAST — serverless)"):
    _ = enriched_materialized.count()
    _ = enriched_materialized.filter(F.col("amount_with_tax") > 300).count()
    _ = enriched_materialized.agg(F.sum("amount_with_tax")).collect()
spark.sql("DROP TABLE IF EXISTS enriched_temp_s2")
# Example:  S2 Delta-materialized (FAST — serverless)      ~2.10s
# Explain: the Delta write materializes the full DAG once; the three reads that follow scan
#          the already-written Delta table instead of recomputing from source. On classic DBR,
#          in-memory cache() is faster still (no write overhead) — but on serverless, Delta
#          materialization is the only supported pattern for avoiding DAG recomputation.
# Customer: "We compute the enriched dataset once and reuse it, instead of rebuilding it three times."

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3 — broadcast the small dimension

# COMMAND ----------

# WHY you cannot create a slow/fast comparison on serverless for this section:
#
# CLASSIC DBR:
#   spark.sql.autoBroadcastJoinThreshold is user-settable. Set it to -1 to disable
#   auto-broadcast and the default join falls through to SortMergeJoin + Exchange.
#   Join strategy hints (.hint("merge"), .hint("shuffle_hash")) also work as expected.
#     spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
#     slow = orders.join(customers, "customer_id")          # → SortMergeJoin + Exchange
#     fast = orders.join(broadcast(customers), "customer_id")  # → BroadcastHashJoin
#     spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")
#
# SERVERLESS + PHOTON:
#   1. spark.sql.autoBroadcastJoinThreshold is engine-owned — reads raise CONFIG_NOT_AVAILABLE,
#      writes are silently ignored or rejected.
#   2. Photon's optimizer overrides ALL join strategy hints (.hint("merge"),
#      .hint("shuffle_hash"), .hint("shuffle_merge")) for tables it judges broadcastable.
#      The plan always shows PhotonBroadcastHashJoin regardless of the hint.
#   There is no user-facing mechanism to force SortMergeJoin on serverless when Photon
#   has decided to broadcast.
#
# THE REFRAME — what the teaching point becomes on serverless:
#   AQE + Photon auto-selecting BroadcastHashJoin IS the correct and optimal outcome.
#   Your job is to READ the plan and confirm it happened, and to know when to add
#   broadcast() DEFENSIVELY:
#     - Source has no row-count statistics (JSON, CSV without ANALYZE TABLE): Photon may
#       underestimate the table size and fall back to SortMergeJoin unexpectedly.
#     - Dim is just above AQE's internal threshold but you know it is safe to broadcast.
#     - You need plan stability across environments (serverless AND classic DBR clusters
#       where the default threshold is only 10 MB).

default_join = orders.join(customers, "customer_id")
bcast_join   = orders.join(broadcast(customers), "customer_id")

print("=== default plan ==="); default_join.explain("formatted")
print("=== explicit broadcast plan ==="); bcast_join.explain("formatted")
# On serverless: both plans show PhotonBroadcastHashJoin — Photon chose correctly.
# On classic DBR: the default plan may show SortMergeJoin + Exchange if customers exceeds
# the 10 MB autoBroadcastJoinThreshold. The broadcast plan always shows BroadcastHashJoin.
# Key things to find in a BroadcastHashJoin plan:
#   - No Exchange (shuffle) on the large (orders) side — that's the win.
#   - The small side (customers) is collected to driver and broadcast to all executors.

with timed("S3 default join+agg"):
    default_join.groupBy("segment").agg(F.sum("amount").alias("rev")).collect()
with timed("S3 explicit broadcast join+agg"):
    bcast_join.groupBy("segment").agg(F.sum("amount").alias("rev")).collect()
# On serverless timings will be nearly identical — both are already BroadcastHashJoin.
# On classic DBR with a larger customers table the broadcast version will be meaningfully faster.
# Customer: "The lookup table is tiny, so we ship one copy to every worker instead of moving
#            the multi-million-row table across the network."

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4 — fix skew (AQE first, salting as the manual technique)

# COMMAND ----------

with timed("S4 skewed groupBy (baseline)"):
    orders.groupBy("customer_id").agg(F.sum("amount").alias("total")).collect()
# Example:  S4 skewed groupBy (baseline)                 4.60s
# In the UI: one straggler task (customer_id = 0 holds ~40% of rows).

SALT = 16
salted = orders.withColumn("salt", (F.rand() * SALT).cast("int"))
with timed("S4 salted two-stage (FAST on skew)"):
    partial = salted.groupBy("customer_id", "salt").agg(F.sum("amount").alias("p"))
    final = partial.groupBy("customer_id").agg(F.sum("p").alias("total"))
    final.collect()
# Example:  S4 salted two-stage (FAST on skew)           2.80s
# Explain: salting splits the hot key across 16 sub-groups so no single task is overloaded, then
#          re-aggregates the partials. Prefer AQE skew handling first; salt when AQE isn't enough or
#          for groupBy skew that the AQE skew-*join* path doesn't cover.
# Customer: "One customer had 40% of the rows, so one machine did almost all the work; we spread that
#            key across many machines and combined the results."

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5 — avoid tiny files (coalesce before write)

# COMMAND ----------

over_partitioned = orders.repartition(400)

# CLASSIC DBR — df.rdd.getNumPartitions():
#   A zero-cost metadata call on the DAG scheduler. No Spark job is submitted; the partition
#   count is read directly from the in-memory RDD plan graph via SparkContext. Available
#   because the driver has direct, in-process access to the RDD scheduler.
#
# SERVERLESS (Spark Connect) — .rdd is NOT accessible:
#   Spark Connect exposes only the DataFrame/SQL API over an RPC channel. The RDD layer
#   remains entirely server-side and cannot be reached from the Connect client. Accessing
#   .rdd raises: AnalysisException: 'RDD operations are not supported on the remote Spark
#   Connect client.'
#   The serverless-compatible alternative is to query spark_partition_id() across the
#   DataFrame. Unlike the classic metadata call, this DOES trigger a Spark job (a
#   distinct-count scan), so it carries actual compute cost — but it is the only supported
#   way to observe the live partition count on serverless.
print("before:", over_partitioned.select(F.spark_partition_id()).distinct().count())   # 400
right_sized = over_partitioned.coalesce(8)
print("after :", right_sized.select(F.spark_partition_id()).distinct().count())        # 8
# Explain: 400 partitions => 400 tiny files => per-file overhead on every downstream read and heavy
#          metadata. coalesce reduces partitions without a full shuffle. In Delta, also use OPTIMIZE /
#          auto-compaction. Don't over-correct to coalesce(1) on big data — that funnels the whole
#          write through one task.
# Customer: "We write a handful of right-sized files instead of hundreds of tiny ones, so every later
#            read is faster and cheaper."

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6 — never collect() big data to the driver

# COMMAND ----------

# Risky: orders.collect()  -> pulls all rows to the driver -> OOM
top_segments = (orders.join(broadcast(customers), "customer_id")
                .groupBy("segment").agg(F.sum("amount").alias("rev"))
                .orderBy(F.desc("rev")))
top_segments.show()   # small, safe result
# Example:
# +----------+-------+
# |   segment|    rev|
# +----------+-------+
# |       smb| 4.21E8|
# |       mid| 4.18E8|
# |enterprise| 4.15E8|
# +----------+-------+
# Explain: keep the heavy work distributed; only bring back small aggregates. Use show()/take() to bound
#          what reaches the driver; write() for large outputs.
# Customer: "We summarize on the cluster and return only the few rows we need, so no single machine has
#            to hold the entire dataset."

# COMMAND ----------

# MAGIC %md
# MAGIC ## The framework to recite
# MAGIC 1. Read less data  2. Move less data  3. Handle skew  4. Reuse work
# MAGIC 5. Right-size parallelism  6. Avoid Python UDFs — and always verify before/after in the UI.

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
    "enriched_temp_s2",   # Section 2 — Delta-materialized reuse demo
]

for _t in _temp_tables:
    spark.sql(f"DROP TABLE IF EXISTS {_t}")
    print(f"dropped (if existed): {_t}")

print("cleanup complete.")
