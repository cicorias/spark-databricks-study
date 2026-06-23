# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Optimization Challenge (SOLUTION)
# MAGIC
# MAGIC The 6 problems and their fixes from [05](../../05-Spark-Optimization-Challenge.md), turned into runnable cells you can A/B vs the slow version in `03_optimization_challenge_start`.
# MAGIC
# MAGIC Run `03_…_start` first (or at least its data-gen cell) so the tables exist.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

CATALOG, SCHEMA = "workspace", "default"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fix 1 — kill the Python UDF (problem **B**)
# MAGIC `strip().lower()` has a built-in equivalent. Built-ins are vectorized and Photon-friendly.
# MAGIC
# MAGIC **Customer translation:** *"That custom function processes rows one at a time in Python; the built-in does it in native vectorized code — same output, far faster."*

# COMMAND ----------

# BEFORE (don't run, just for diff):
# .withColumn("category", normalize_udf("raw_category"))

# AFTER
# .withColumn("category", F.lower(F.trim("raw_category")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fix 2 — broadcast the small dimension (problem **A**)
# MAGIC `products` is 5k rows — broadcast it, the 20M `orders` rows never shuffle.
# MAGIC
# MAGIC **Customer translation:** *"The product list is tiny, so we copy it to every machine instead of reshuffling 20 million order rows."*

# COMMAND ----------

# Confirm the strategy switched: SortMergeJoin -> BroadcastHashJoin in the plan
orders   = spark.table(f"{CATALOG}.{SCHEMA}.orders")
products = spark.table(f"{CATALOG}.{SCHEMA}.products")
orders.join(broadcast(products), "product_id").explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fix 3 — push the filter to the source (problem **D**)
# MAGIC We were aggregating 2024 *and* 2025, then throwing 2024 away. Filter at the source so it never touches the join/aggregation.
# MAGIC
# MAGIC **Customer translation:** *"We were crunching a full year of data we immediately threw away — filtering up front means we only touch the rows we report on."*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fix 4 — projection pruning, *and* notice the join is unnecessary (problem **(E) projection / join**)
# MAGIC The final report uses only `raw_category`, `customer_id`, `qty`, `unit_price` — all on `orders`. The whole `products` join is **dead overhead** for *this* output. **Recognizing the unnecessary join is the senior move.**
# MAGIC
# MAGIC If the interviewer insists product columns ARE in the output: keep the join, but **broadcast + prune `products`** to only the needed columns.
# MAGIC
# MAGIC **Customer translation:** *"The report doesn't actually use any product details, so the join was pure overhead — removing it eliminates a whole stage."*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fix 5 — never `collect()` (problem **F**)
# MAGIC Pulls everything to the driver. Survivable when the result is small (here), landmine in production. Use `show` / `write` / `limit().toPandas()` instead.
# MAGIC
# MAGIC **Customer translation:** *"Pulling the whole result into one machine is how you crash the driver in production; we keep the work spread out and only ship back the small summary."*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fix 6 — materialize reused intermediates (the scaling story)
# MAGIC If the same intermediate is read by several downstream reports, **compute once, persist to Delta**. On classic compute you'd `cache()`; on serverless that's restricted, so you materialize.
# MAGIC
# MAGIC **Customer translation:** *"If five dashboards read this intermediate step, we compute it once and reuse it instead of rebuilding it five times."*

# COMMAND ----------

# MAGIC %md
# MAGIC ## The clean final version

# COMMAND ----------

orders = spark.table(f"{CATALOG}.{SCHEMA}.orders")

revenue_by_category = (orders
    .where(F.col("year") == 2025)                                  # Fix 3 — filter early
    .select("raw_category", "customer_id", "qty", "unit_price")    # Fix 4 — prune
    .withColumn("category", F.lower(F.trim("raw_category")))       # Fix 1 — built-in not UDF
    .withColumn("revenue", F.col("qty") * F.col("unit_price"))
    .groupBy("category")
    .agg(F.sum("revenue").alias("total_revenue"),
         F.countDistinct("customer_id").alias("customers"))
    .orderBy(F.desc("total_revenue")))

revenue_by_category.show(truncate=False)                           # Fix 5 — no collect

# COMMAND ----------

# If product/supplier attributes ARE required in the output:
products_slim = (spark.table(f"{CATALOG}.{SCHEMA}.products")
                       .select("product_id", "product_name"))      # prune the small side too

with_products = (orders
    .where(F.col("year") == 2025)
    .select("product_id", "raw_category", "customer_id", "qty", "unit_price")
    .join(broadcast(products_slim), "product_id")                  # Fix 2 — broadcast
    .withColumn("category", F.lower(F.trim("raw_category")))
    .withColumn("revenue", F.col("qty") * F.col("unit_price"))
    .groupBy("category", "product_name")
    .agg(F.sum("revenue").alias("total_revenue")))

with_products.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Materialize for reuse (Fix 6 in practice)

# COMMAND ----------

(revenue_by_category
   .write.mode("overwrite")
   .saveAsTable(f"{CATALOG}.{SCHEMA}.revenue_by_category_2025"))

spark.table(f"{CATALOG}.{SCHEMA}.revenue_by_category_2025").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## What you say out loud at the end of the segment
# MAGIC
# MAGIC > *"To recap — the biggest wins were broadcasting the small dimension and filtering 2024 out at the source. I also dropped the join because the report doesn't use product columns. I replaced the Python UDF with a built-in for vectorization, replaced `collect` with `show` so we don't pull to the driver, and noted that on serverless I'd materialize this to Delta instead of `cache()` if other reports reuse it. In business terms, the nightly job goes from minutes to seconds and we never risk crashing the driver."*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional — measure the win
# MAGIC Time the slow version vs the clean version side by side. Don't trust your gut, trust the wall clock.

# COMMAND ----------

import time

def time_it(label, fn):
    t0 = time.time()
    fn()
    print(f"{label}: {time.time() - t0:0.2f}s")

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
normalize_udf = udf(lambda s: s.strip().lower() if s else None, StringType())

def slow():
    o = spark.table(f"{CATALOG}.{SCHEMA}.orders")
    p = spark.table(f"{CATALOG}.{SCHEMA}.products")
    enriched = o.join(p, "product_id").withColumn("category", normalize_udf("raw_category"))
    enriched = enriched.withColumn("revenue", F.col("qty") * F.col("unit_price"))
    by_cat = (enriched.groupBy("category", "year")
              .agg(F.sum("revenue").alias("total_revenue"),
                   F.countDistinct("customer_id").alias("customers")))
    by_cat_2025 = by_cat.where(F.col("year") == 2025).orderBy(F.desc("total_revenue"))
    by_cat_2025.collect()    # the original pulled to the driver

def fast():
    o = spark.table(f"{CATALOG}.{SCHEMA}.orders")
    result = (o.where(F.col("year") == 2025)
               .select("raw_category", "customer_id", "qty", "unit_price")
               .withColumn("category", F.lower(F.trim("raw_category")))
               .withColumn("revenue", F.col("qty") * F.col("unit_price"))
               .groupBy("category")
               .agg(F.sum("revenue").alias("total_revenue"),
                    F.countDistinct("customer_id").alias("customers")))
    result.write.mode("overwrite").format("noop").save()    # action without driver pull

time_it("slow", slow)
time_it("fast", fast)
