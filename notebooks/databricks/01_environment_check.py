# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Environment Check
# MAGIC
# MAGIC **Goal:** in <5 minutes, prove your Free Edition workspace can do the four things every later lab assumes:
# MAGIC
# MAGIC 1. Run Python and SQL cells
# MAGIC 2. Create and read a managed Delta table
# MAGIC 3. Diagnose without the Spark UI (use `explain` + `spark_partition_id`)
# MAGIC 4. Find out *now* whether `.cache()` errors in this workspace (it sometimes does on serverless)
# MAGIC
# MAGIC Run every cell. If any cell errors, **note the exact error** — that's the constraint the interviewer expects you to know cold.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Python cell

# COMMAND ----------

print("spark.version =", spark.version)
print("python", __import__("sys").version)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. SQL cell

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_catalog() AS catalog, current_schema() AS schema, current_user() AS user

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Write + read a managed Delta table
# MAGIC Default-edition default is `workspace.default`. Change here if your workspace is different.

# COMMAND ----------

CATALOG = "workspace"
SCHEMA  = "default"
TABLE   = f"{CATALOG}.{SCHEMA}.smoke_test"

(spark.range(0, 100)
      .withColumnRenamed("id", "n")
      .write.mode("overwrite")
      .saveAsTable(TABLE))

spark.table(TABLE).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Diagnose without the Spark UI
# MAGIC The Spark UI is **not** available on serverless. Your two tools are `df.explain("formatted")` and `F.spark_partition_id()`.

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.table(TABLE).withColumn("bucket", F.col("n") % 4)

# Physical plan — look for Exchange (shuffle) / BroadcastHashJoin / SortMergeJoin / PushedFilters
df.where(F.col("bucket") == 0).explain("formatted")

# COMMAND ----------

# Row distribution across partitions, RDD-free (serverless-safe)
(df.groupBy(F.spark_partition_id().alias("partition_id"))
   .count()
   .orderBy("partition_id")
   .show(truncate=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Does `.cache()` work in *this* workspace?
# MAGIC Free Edition documentation says cache APIs are restricted on serverless, but exact behavior can vary by runtime. **Run this and write down what happens** — if it raises, you've just confirmed you must materialize to Delta instead in every later lab.

# COMMAND ----------

try:
    spark.range(10).cache().count()
    print("✅ cache() succeeded — treat this as best-effort; still prefer materialize-to-Delta in interviews")
except Exception as e:
    print("⛔ cache() failed — exactly as expected on serverless:")
    print(f"   {type(e).__name__}: {str(e)[:200]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Genie Code sanity check (manual)
# MAGIC Open the Genie Code pane (top-right ✨ icon). Ask it: *"Explain what this notebook does."* Confirm:
# MAGIC
# MAGIC - The pane opens
# MAGIC - It returns *something*
# MAGIC - You can see the **Run** / **Insert** buttons for any code it produces
# MAGIC
# MAGIC You don't have to use Genie for everything — but knowing it works **before** the interview clock starts is what we want.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Cleanup

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {TABLE}")
print("done")
