# Databricks notebook source
# MAGIC %md
# MAGIC # Slow Spark App — BROKEN (practice version)
# MAGIC
# MAGIC This notebook **runs but is slow**. It mirrors the Phase 2 task: a working-but-poor Spark app you must
# MAGIC speed up and *justify*. Each section has one or more performance anti-patterns and a `# TODO`.
# MAGIC
# MAGIC **How to practice (do this cold, no solution open):**
# MAGIC 1. Run the cell, note the wall-clock time.
# MAGIC 2. Open the **Query Profile / Spark UI** and find the expensive stage (shuffle size, straggler tasks, plan).
# MAGIC 3. State a hypothesis out loud, make **one** change, re-run, and compare.
# MAGIC 4. End with a one-sentence customer justification.
# MAGIC
# MAGIC Then diff against `slow_spark_app_SOLUTION.py`. The skill being graded is finding and explaining the fix —
# MAGIC not memorizing it.

# COMMAND ----------

import time
from contextlib import contextmanager
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

@contextmanager
def timed(label):
    t = time.time()
    yield
    print(f"{label:<42} {time.time() - t:6.2f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data setup (leave as-is)
# MAGIC Large skewed `orders` fact + tiny `customers` dim. ~40% of orders share one hot `customer_id`.

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
# MAGIC ## Section 1
# MAGIC TODO: This bucketing is slow. Why? What is the cost of this approach, and what's the faster equivalent?

# COMMAND ----------

def bucket_py(amount):
    if amount < 100: return "low"
    elif amount < 300: return "mid"
    else: return "high"

bucket_udf = F.udf(bucket_py, StringType())

with timed("Section 1"):
    orders.withColumn("bucket", bucket_udf("amount")).groupBy("bucket").count().collect()

# TODO: rewrite so the logic runs inside the engine. Confirm the speedup by re-timing.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2
# MAGIC TODO: Three actions on the same derived DataFrame. What work is repeated, and how do you avoid it?

# COMMAND ----------

enriched = orders.withColumn("amount_with_tax", F.col("amount") * 1.2)

with timed("Section 2"):
    _ = enriched.count()
    _ = enriched.filter(F.col("amount_with_tax") > 300).count()
    _ = enriched.agg(F.sum("amount_with_tax")).collect()

# TODO: what is recomputed each action? Apply the fix, and note the tradeoff of that fix.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3
# MAGIC TODO: A large-with-small join. Inspect the plan with `.explain("formatted")`.
# MAGIC Is the big table being shuffled? Should it be? How do you guarantee the better plan?

# COMMAND ----------

joined = orders.join(customers, "customer_id")

# TODO: run joined.explain("formatted") and read it out loud before changing anything.

with timed("Section 3"):
    joined.groupBy("segment").agg(F.sum("amount").alias("rev")).collect()

# TODO: force the better join strategy and re-time.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4
# MAGIC TODO: This groupBy is skewed. In the Spark UI you'll see one task run far longer than the rest.
# MAGIC Identify the hot key and fix the skew two different ways. Be ready to say when each is appropriate.

# COMMAND ----------

with timed("Section 4"):
    orders.groupBy("customer_id").agg(F.sum("amount").alias("total")).collect()

# TODO: which customer_id dominates? Fix the skew. Hint: there are both an automatic and a manual approach.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5
# MAGIC TODO: This write would produce many small files. Why is that bad downstream, and what do you change?

# COMMAND ----------

over_partitioned = orders.repartition(400)
# CLASSIC DBR — df.rdd.getNumPartitions():
#   Zero-cost metadata call. The driver has in-process access to SparkContext, so it reads
#   the partition count straight from the RDD plan graph — no Spark job submitted.
#
# SERVERLESS (Spark Connect) — .rdd is NOT accessible:
#   The Connect client only sees the DataFrame/SQL API. .rdd raises AnalysisException.
#   Use spark_partition_id() instead: it is a SQL function that runs a scan to count
#   distinct partition IDs, so it does submit a job — but it is the only serverless-
#   compatible way to observe the live partition count.
print("partitions:", over_partitioned.select(F.spark_partition_id()).distinct().count())

# TODO: right-size the partition count before a write without triggering a full shuffle.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6
# MAGIC TODO: A report wants the per-segment revenue back in the notebook. The tempting move is `.collect()`
# MAGIC on a big DataFrame. Why is that dangerous, and what's the safe pattern?

# COMMAND ----------

# TODO: produce the small per-segment result safely (no collect() of a large DataFrame).
