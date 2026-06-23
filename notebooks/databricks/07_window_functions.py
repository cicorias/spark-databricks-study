# Databricks notebook source
# MAGIC %md
# MAGIC # 07 — Window Functions (top-N per group, running totals, gap-and-island)
# MAGIC
# MAGIC Window functions show up constantly in real customer pipelines: latest record per customer, 7-day rolling revenue, sessionization. Practice these until the syntax is automatic.
# MAGIC
# MAGIC See [13 — Spark SQL Drills](../../13-Spark-SQL-Drills.md) for SQL versions of the same problems.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dataset — order events per customer

# COMMAND ----------

events = spark.createDataFrame(
    [
        (1, "2026-06-01", 30.0),
        (1, "2026-06-02", 10.0),
        (1, "2026-06-05", 50.0),
        (2, "2026-06-01", 20.0),
        (2, "2026-06-03",  5.0),
        (2, "2026-06-04", 15.0),
        (3, "2026-06-02", 40.0),
    ],
    "customer_id INT, ts DATE, amount DOUBLE",
)
events.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Latest event per customer (the most common window pattern)

# COMMAND ----------

w_latest = Window.partitionBy("customer_id").orderBy(F.desc("ts"))

(events
   .withColumn("rn", F.row_number().over(w_latest))
   .where(F.col("rn") == 1)
   .drop("rn")
   .orderBy("customer_id")
   .show())

# COMMAND ----------

# MAGIC %md
# MAGIC ### `row_number` vs `rank` vs `dense_rank` — know the difference
# MAGIC
# MAGIC - `row_number`: 1,2,3,4 — unique even on ties (good for "pick one")
# MAGIC - `rank`: 1,2,2,4 — same on ties, with gaps (Olympic ranking)
# MAGIC - `dense_rank`: 1,2,2,3 — same on ties, no gaps

# COMMAND ----------

ties = spark.createDataFrame(
    [("A", 100), ("B", 100), ("C", 90), ("D", 80)], "name STRING, score INT"
)
w = Window.orderBy(F.desc("score"))
(ties
   .withColumn("row_number", F.row_number().over(w))
   .withColumn("rank",       F.rank().over(w))
   .withColumn("dense_rank", F.dense_rank().over(w))
   .show())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Running total per customer

# COMMAND ----------

w_running = (Window.partitionBy("customer_id")
                   .orderBy("ts")
                   .rowsBetween(Window.unboundedPreceding, Window.currentRow))

(events
   .withColumn("running_total", F.sum("amount").over(w_running))
   .orderBy("customer_id", "ts")
   .show())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Rolling 7-day average (a frame defined by *values*, not row count)

# COMMAND ----------

# rangeBetween uses the orderBy column's units. For a date column the unit is days.
w_7day = (Window.partitionBy("customer_id")
                .orderBy(F.col("ts").cast("timestamp").cast("long"))   # epoch seconds
                .rangeBetween(-7 * 86_400, 0))                          # 7 days back .. now

(events
   .withColumn("avg_7d", F.avg("amount").over(w_7day))
   .orderBy("customer_id", "ts")
   .show())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Lag / lead — previous and next event

# COMMAND ----------

w_per_cust = Window.partitionBy("customer_id").orderBy("ts")
(events
   .withColumn("prev_amount", F.lag("amount", 1).over(w_per_cust))
   .withColumn("next_amount", F.lead("amount", 1).over(w_per_cust))
   .withColumn("delta_vs_prev", F.col("amount") - F.col("prev_amount"))
   .orderBy("customer_id", "ts")
   .show())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Sessionization — gap-and-island pattern
# MAGIC Common request: "group consecutive events into sessions where a gap > N minutes starts a new session."
# MAGIC
# MAGIC The trick: a session id = running sum of "is this event the start of a new session?"

# COMMAND ----------

clicks = spark.createDataFrame(
    [
        (1, "2026-06-01 09:00:00"),
        (1, "2026-06-01 09:02:00"),       # 2 min → same session
        (1, "2026-06-01 10:00:00"),       # 58 min → new session
        (1, "2026-06-01 10:05:00"),       # 5 min → same session
        (2, "2026-06-01 09:00:00"),
        (2, "2026-06-01 09:45:00"),       # 45 min → new session
    ],
    "user_id INT, ts STRING",
).withColumn("ts", F.to_timestamp("ts"))

GAP_MIN = 30
w_user = Window.partitionBy("user_id").orderBy("ts")

with_gap = (clicks
    .withColumn("prev_ts", F.lag("ts").over(w_user))
    .withColumn("gap_min", (F.col("ts").cast("long") - F.col("prev_ts").cast("long")) / 60)
    .withColumn("is_new_session", F.when(F.col("gap_min").isNull() | (F.col("gap_min") > GAP_MIN), 1).otherwise(0)))

sessioned = (with_gap
    .withColumn("session_id", F.sum("is_new_session").over(w_user))
    .select("user_id", "ts", "gap_min", "session_id")
    .orderBy("user_id", "ts"))

sessioned.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. The performance gotcha
# MAGIC A window with **no `partitionBy`** sends ALL the data to a single executor for ordering. Always partition by the largest reasonable key:
# MAGIC
# MAGIC ```python
# MAGIC # ⛔ all data through one task
# MAGIC Window.orderBy("ts")
# MAGIC
# MAGIC # ✅ parallel per customer
# MAGIC Window.partitionBy("customer_id").orderBy("ts")
# MAGIC ```
# MAGIC
# MAGIC If you genuinely need a global ranking, do it on an already-aggregated, small DataFrame.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Self-test (say the answers out loud)
# MAGIC
# MAGIC 1. Pick `row_number` vs `rank` for: "deduplicate to one row per customer (latest wins)"
# MAGIC 2. Why does an unbounded window without `partitionBy` kill performance?
# MAGIC 3. What's the difference between `rowsBetween` and `rangeBetween`?
# MAGIC 4. How would you find users whose 7-day-rolling spend doubled week over week?
