# ---
# jupyter:
#   jupytext:
#     formats: py:percent,ipynb
#     text_representation:
#       extension: .py
#       format_name: percent
# ---

# %% [markdown]
# # 01 — Local Warmup (mise + uv + local PySpark)
#
# Confirms your local environment is wired correctly and walks through Spark fundamentals
# end-to-end with a tiny dataset. ~10 minutes.
#
# **Prereqs:** `mise install && mise run setup` already run. See `11-Local-PySpark-Setup-uv.md`.

# %% [markdown]
# ## 1. Build a local SparkSession (Delta-enabled)

# %%
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Build a Delta-enabled local session. The helper from delta-spark pulls the right Maven jars.
try:
    from delta import configure_spark_with_delta_pip
    builder = (SparkSession.builder
        .appName("local-warmup")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", "spark-warehouse")
        .config("spark.ui.showConsoleProgress", "false"))
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
except ImportError:
    # delta-spark missing — fall back to plain Spark (no MERGE/time-travel in this notebook)
    spark = (SparkSession.builder.appName("local-warmup").master("local[*]").getOrCreate())

spark.sparkContext.setLogLevel("ERROR")
print("Spark", spark.version)

# %% [markdown]
# ## 2. Lazy evaluation — transformations vs actions

# %%
df = (spark.range(0, 1_000_000)
      .withColumn("grp", F.col("id") % 100)
      .withColumn("val", F.rand(seed=42)))
print("Plan built. No job yet.")
df.count()    # action — now Spark actually runs something

# %% [markdown]
# ## 3. Read the physical plan
# Look for `Exchange` (shuffle), join strategy, `PushedFilters`, scan column list.

# %%
df.where(F.col("grp") < 5).select("id", "val").explain("formatted")

# %% [markdown]
# ## 4. Narrow vs wide — count shuffles
# `select`/`filter`/`withColumn` = narrow. `groupBy`/`join`/`distinct`/`orderBy` = wide.

# %%
narrow = df.select("id", "val").where(F.col("val") > 0.5)
narrow.explain("formatted")    # no Exchange

# %%
wide = df.groupBy("grp").agg(F.sum("val").alias("total"))
wide.explain("formatted")      # Exchange present
wide.count()

# %% [markdown]
# ## 5. Partition distribution (serverless-safe, no RDDs)

# %%
(df.groupBy(F.spark_partition_id().alias("partition")).count()
   .orderBy("partition").show(truncate=False))

# %% [markdown]
# ## 6. Broadcast join — small dim into big fact

# %%
from pyspark.sql.functions import broadcast

big = df.select(F.col("grp").alias("key"), "val")
small = spark.range(0, 100).withColumnRenamed("id", "key") \
        .withColumn("label", F.concat(F.lit("k_"), F.col("key")))

# Force a sort-merge join by disabling auto-broadcast (for contrast)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
big.join(small, "key").explain("formatted")     # SortMergeJoin, Exchange on both sides

# %%
# Now force broadcast — the big side stays put
big.join(broadcast(small), "key").explain("formatted")

# Restore default
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)

# %% [markdown]
# ## 7. UDF vs built-in (the latency demo)

# %%
import time

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

cats = spark.range(500_000).withColumn("s", F.concat(F.lit(" Hello_"), F.col("id"))).select("s")

t0 = time.time()
cats.withColumn("clean", udf(lambda x: x.strip().lower() if x else None, StringType())("s")) \
    .write.format("noop").mode("overwrite").save()
udf_t = time.time() - t0

t0 = time.time()
cats.withColumn("clean", F.lower(F.trim("s"))) \
    .write.format("noop").mode("overwrite").save()
builtin_t = time.time() - t0

print(f"UDF       : {udf_t:0.2f}s")
print(f"Built-in  : {builtin_t:0.2f}s   ({udf_t/builtin_t:0.1f}x faster)")

# %% [markdown]
# ## 8. Done
# If every cell above ran, your local setup is good. Move on to `02_local_optimization.py`.

# %%
spark.stop()
print("done")
