# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Delta Lake: MERGE, SCD2, OPTIMIZE, Time Travel
# MAGIC
# MAGIC The four Delta features an FDE is most likely to be asked about. See [12 — Delta Lake Deep Dive](../../12-Delta-Lake-Deep-Dive.md).

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG, SCHEMA = "workspace", "default"
TARGET = f"{CATALOG}.{SCHEMA}.customers_scd2"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Seed an SCD2 target table

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {TARGET}")

seed = spark.createDataFrame(
    [
        (1, "Alice",  "Seattle",  "2026-01-01"),
        (2, "Bob",    "Boston",   "2026-01-01"),
        (3, "Carol",  "Chicago",  "2026-01-01"),
    ],
    "customer_id INT, name STRING, city STRING, effective_from DATE",
)

(seed
   .withColumn("effective_to", F.lit(None).cast("date"))
   .withColumn("is_current", F.lit(True))
   .write.mode("overwrite")
   .saveAsTable(TARGET))

spark.table(TARGET).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The daily CDC batch (incoming changes)

# COMMAND ----------

cdc = spark.createDataFrame(
    [
        (1, "Alice",   "Portland", "2026-06-09"),    # city changed → close old row, insert new
        (2, "Bob",     "Boston",   "2026-06-09"),    # unchanged → ignore
        (4, "Dave",    "Denver",   "2026-06-09"),    # new customer → insert
    ],
    "customer_id INT, name STRING, city STRING, effective_from DATE",
)
cdc.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. SCD2 MERGE in two passes
# MAGIC Standard SCD2 needs two SQL steps because MERGE can't do "close existing row AND insert new row" in one match:
# MAGIC
# MAGIC **Pass A** — close the currently-open rows whose business attributes changed (set `is_current=false`, `effective_to=today`).
# MAGIC
# MAGIC **Pass B** — insert the new versions (and brand-new customer_ids).

# COMMAND ----------

cdc.createOrReplaceTempView("cdc_batch")

# Pass A — expire the rows that have a changed version coming in
spark.sql(f"""
MERGE INTO {TARGET} t
USING (
    SELECT c.customer_id, c.city AS new_city, c.effective_from
    FROM cdc_batch c
    JOIN {TARGET} t ON t.customer_id = c.customer_id
    WHERE t.is_current = true AND t.city <> c.city
) src
ON t.customer_id = src.customer_id AND t.is_current = true
WHEN MATCHED THEN UPDATE SET
    t.is_current   = false,
    t.effective_to = src.effective_from
""")

# Pass B — insert new versions + brand-new customers
spark.sql(f"""
MERGE INTO {TARGET} t
USING (
    -- new customer (no existing row)
    SELECT c.customer_id, c.name, c.city, c.effective_from
    FROM cdc_batch c
    LEFT JOIN {TARGET} t ON t.customer_id = c.customer_id
    WHERE t.customer_id IS NULL

    UNION ALL

    -- updated customer (the matching current row was just expired)
    SELECT c.customer_id, c.name, c.city, c.effective_from
    FROM cdc_batch c
    JOIN {TARGET} t ON t.customer_id = c.customer_id
    WHERE t.is_current = false AND t.effective_to = c.effective_from
) src
ON 1=0   -- we always want NOT MATCHED → INSERT
WHEN NOT MATCHED THEN INSERT
    (customer_id, name, city, effective_from, effective_to, is_current)
VALUES
    (src.customer_id, src.name, src.city, src.effective_from, NULL, true)
""")

spark.table(TARGET).orderBy("customer_id", "effective_from").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Time travel — yesterday's view of the table
# MAGIC Every Delta operation creates a version. You can query any prior version by number or timestamp.

# COMMAND ----------

# How many versions are there now?
spark.sql(f"DESCRIBE HISTORY {TARGET}").select("version", "timestamp", "operation").show(truncate=False)

# Read the table as it looked at version 0 (right after the seed)
spark.sql(f"SELECT * FROM {TARGET} VERSION AS OF 0 ORDER BY customer_id").show()

# Or with the DataFrame API
spark.read.format("delta").option("versionAsOf", 0).table(TARGET).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. OPTIMIZE and ZORDER (small-files + data skipping)
# MAGIC `OPTIMIZE` compacts many small files into right-sized ones (~128 MB default). `ZORDER BY` co-locates rows that share values in a column so range filters skip more files.

# COMMAND ----------

spark.sql(f"OPTIMIZE {TARGET}").show(truncate=False)

# COMMAND ----------

spark.sql(f"OPTIMIZE {TARGET} ZORDER BY (customer_id)").show(truncate=False)

# COMMAND ----------

# DESCRIBE DETAIL shows numFiles, sizeInBytes, partitionColumns, …
spark.sql(f"DESCRIBE DETAIL {TARGET}").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Liquid Clustering (the modern alternative)
# MAGIC Newer Delta tables can use Liquid Clustering instead of fixed partitioning + ZORDER — automatic, no maintenance window.
# MAGIC
# MAGIC ```sql
# MAGIC CREATE TABLE foo (id INT, region STRING, amount DECIMAL(10,2))
# MAGIC USING DELTA
# MAGIC CLUSTER BY (region);
# MAGIC ```
# MAGIC
# MAGIC Add/change clustering keys later: `ALTER TABLE foo CLUSTER BY (region, dt);`. Then `OPTIMIZE foo;` to apply.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. The customer-translation lines
# MAGIC
# MAGIC - **MERGE / SCD2:** *"We track every change in customer state, not just the latest — so reports can answer 'what did the table look like on March 5?'"*
# MAGIC - **OPTIMIZE:** *"Every future query gets faster — not just today's job — because reads now hit a handful of right-sized files instead of thousands of tiny ones."*
# MAGIC - **ZORDER / Liquid Clustering:** *"We physically co-locate rows that get filtered together so the engine skips most files for a typical query."*
# MAGIC - **Time travel:** *"If a bad batch corrupts the table, we can roll the read back to the prior version while we investigate — no restore-from-backup window."*
