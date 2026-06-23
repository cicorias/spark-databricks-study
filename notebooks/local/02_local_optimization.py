# ---
# jupyter:
#   jupytext:
#     formats: py:percent,ipynb
#     text_representation:
#       extension: .py
#       format_name: percent
# ---

# %% [markdown]
# # 02 — Local Optimization Challenge
#
# Local-friendly version of the Phase-2 challenge. Smaller data (2M rows instead of 20M) so it
# finishes in a couple of minutes on a laptop, but the **same 6 levers** apply.
#
# Companion to `notebooks/databricks/03_optimization_challenge_start.py` — use this one for daily
# reps without spinning up Free Edition. Set a **15-minute timer** (less than full because the
# data is smaller) and find the issues before reading the SOLUTION section below.

# %% [markdown]
# ## Setup

# %%
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast, udf
from pyspark.sql.types import StringType
import time

try:
    from delta import configure_spark_with_delta_pip
    builder = (SparkSession.builder.appName("local-opt").master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", "spark-warehouse")
        .config("spark.ui.showConsoleProgress", "false"))
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
except ImportError:
    spark = SparkSession.builder.appName("local-opt").master("local[*]").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# %% [markdown]
# ## Data generation (idempotent)

# %%
orders = (spark.range(0, 2_000_000)
    .withColumn("product_id",  (F.col("id") % 5000).cast("int"))
    .withColumn("customer_id", (F.col("id") % 100000).cast("int"))
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
    .withColumn("product_id", F.col("product_id").cast("int"))
    .withColumn("product_name", F.concat(F.lit("Product_"), F.col("product_id")))
    .withColumn("supplier", F.concat(F.lit("Supplier_"), (F.col("product_id") % 50))))

orders.write.mode("overwrite").saveAsTable("default.orders")
products.write.mode("overwrite").saveAsTable("default.products")
print("data ready")

# %% [markdown]
# ## The slow app — find the issues

# %%
def slow_version():
    o = spark.table("default.orders")
    p = spark.table("default.products")

    normalize = udf(lambda s: s.strip().lower() if s else None, StringType())

    enriched = o.join(p, "product_id")
    enriched = enriched.withColumn("category", normalize("raw_category"))
    enriched = enriched.withColumn("revenue", F.col("qty") * F.col("unit_price"))

    by_cat = (enriched.groupBy("category", "year")
              .agg(F.sum("revenue").alias("total_revenue"),
                   F.countDistinct("customer_id").alias("customers")))
    by_cat_2025 = by_cat.where(F.col("year") == 2025)
    final = by_cat_2025.orderBy(F.desc("total_revenue"))
    return final.collect()    # driver pull

t0 = time.time()
result_slow = slow_version()
slow_t = time.time() - t0
print(f"slow: {slow_t:0.2f}s, rows={len(result_slow)}")
for r in result_slow:
    print(r["category"], r["total_revenue"])

# %% [markdown]
# ---
# ## Try first. SOLUTION below.
#
# ### The 6 issues
# 1. Python UDF for `strip().lower()` — use `F.lower(F.trim(...))`
# 2. Sort-merge join of 2M × 5k — `broadcast(products)` or skip the join entirely (output doesn't use product cols)
# 3. Filter applied AFTER aggregation — push `where(year=2025)` to the source
# 4. Projection bloat — `select(...)` only the columns the report needs
# 5. `collect()` to the driver — use `show`/`write` instead
# 6. Recomputation on reuse — materialize intermediates to Delta if reused

# %%
def fast_version():
    o = spark.table("default.orders")
    result = (o.where(F.col("year") == 2025)                                    # fix 3
                .select("raw_category", "customer_id", "qty", "unit_price")     # fix 4
                .withColumn("category", F.lower(F.trim("raw_category")))        # fix 1
                .withColumn("revenue", F.col("qty") * F.col("unit_price"))
                .groupBy("category")
                .agg(F.sum("revenue").alias("total_revenue"),
                     F.countDistinct("customer_id").alias("customers"))
                .orderBy(F.desc("total_revenue")))
    rows = result.collect()   # safe — already aggregated to ~3 rows
    return rows

t0 = time.time()
result_fast = fast_version()
fast_t = time.time() - t0
print(f"fast: {fast_t:0.2f}s, rows={len(result_fast)}")
for r in result_fast:
    print(r["category"], r["total_revenue"])

# %%
print(f"\nspeedup: {slow_t / fast_t:0.1f}x")

# %% [markdown]
# ## Cleanup

# %%
spark.sql("DROP TABLE IF EXISTS default.orders")
spark.sql("DROP TABLE IF EXISTS default.products")
spark.stop()
print("done")
