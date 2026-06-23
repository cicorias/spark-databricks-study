# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Optimization Challenge (START — set a 25-minute timer)
# MAGIC
# MAGIC This is the same hands-on challenge described in [05](../../05-Spark-Optimization-Challenge.md). It mirrors Phase 2 of the interview: a **running but slow PySpark app** with **4–6 deliberate improvable areas**.
# MAGIC
# MAGIC **Rules:**
# MAGIC 1. **25 minutes on the clock.**
# MAGIC 2. **Narrate out loud** as if a teammate is watching. The grading rubric explicitly rewards thinking aloud.
# MAGIC 3. Find issues by **diagnosing first** (`explain("formatted")` + query profile), not by guessing.
# MAGIC 4. When done — or when the timer rings — open `04_optimization_challenge_solution` and diff.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0 — generate the data (run once)

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG, SCHEMA = "workspace", "default"

orders = (spark.range(0, 20_000_000)
    .withColumn("product_id",  (F.col("id") % 5000).cast("int"))
    .withColumn("customer_id", (F.col("id") % 250000).cast("int"))
    .withColumn("qty",         (F.col("id") % 7 + 1).cast("int"))
    .withColumn("unit_price",   F.round(F.rand(seed=1) * 100 + 1, 2))
    .withColumn("year",         F.when(F.col("id") % 10 < 3, 2024).otherwise(2025))
    .withColumn("raw_category",
        F.when(F.col("product_id") % 3 == 0, F.lit(" Electronics "))
         .when(F.col("product_id") % 3 == 1, F.lit("home goods"))
         .otherwise(F.lit("TOYS")))
    .drop("id"))

products = (spark.range(0, 5000)
    .withColumnRenamed("id", "product_id")
    .withColumn("product_id",   F.col("product_id").cast("int"))
    .withColumn("product_name", F.concat(F.lit("Product_"), F.col("product_id")))
    .withColumn("supplier",     F.concat(F.lit("Supplier_"), (F.col("product_id") % 50))))

orders.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.orders")
products.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.products")

print("Data ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — the slow app (this is what you're handed)
# MAGIC
# MAGIC It runs. It's slow. **Find the issues.** Markers (A)–(F) are referenced in the solution notebook.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

orders   = spark.table(f"{CATALOG}.{SCHEMA}.orders")
products = spark.table(f"{CATALOG}.{SCHEMA}.products")

# A Python UDF to normalize the category text
def normalize_category(s):
    if s is None:
        return None
    return s.strip().lower()

normalize_udf = udf(normalize_category, StringType())

# Join everything first, keep all columns
enriched = orders.join(products, "product_id")                                  # (A)
enriched = enriched.withColumn("category", normalize_udf("raw_category"))       # (B)
enriched = enriched.withColumn("revenue", F.col("qty") * F.col("unit_price"))

# Aggregate, then filter to 2025 at the very end
by_cat = (enriched
    .groupBy("category", "year")
    .agg(F.sum("revenue").alias("total_revenue"),
         F.countDistinct("customer_id").alias("customers")))                    # (C)

by_cat_2025 = by_cat.where(F.col("year") == 2025)                               # (D)

# Sort the entire result and pull it all to the driver to "look at it"
final = by_cat_2025.orderBy(F.desc("total_revenue"))                            # (E)
rows = final.collect()                                                          # (F)
for r in rows:
    print(r["category"], r["total_revenue"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — diagnose before you change anything
# MAGIC
# MAGIC 1. Read `enriched.explain("formatted")` — what's the join strategy?
# MAGIC 2. Read `by_cat_2025.explain("formatted")` — count the `Exchange` nodes.
# MAGIC 3. Open the **Query Profile** from the last cell's output. Where did wall-clock actually go?

# COMMAND ----------

enriched.explain("formatted")

# COMMAND ----------

by_cat_2025.explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — fix
# MAGIC
# MAGIC Edit the slow app above. Fix the biggest-cost issue first. After each fix, say the **business translation** in one sentence.
# MAGIC
# MAGIC When done (or when the timer rings) open **`04_optimization_challenge_solution`**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — be ready to answer
# MAGIC
# MAGIC *"How does this scale if `orders` is 20 **billion** rows instead of 20 million?"*
# MAGIC
# MAGIC (Answer template lives in [05](../../05-Spark-Optimization-Challenge.md) at the bottom.)
