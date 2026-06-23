---
title: Spark Optimization Challenge
tags: [spark, pyspark, challenge, hands-on]
---

# 05 — Spark Optimization Challenge (hands-on)

[← Optimization Playbook](04-Spark-Optimization-Playbook.md) · Next: [Python Feature-Dev →](06-Python-Feature-Dev-Challenge.md)

> **How to use this:** paste the data-gen + slow app into a Free Edition notebook and run it. Set a **25-minute timer**. Find as many of the **6 deliberate problems** as you can *and explain each one out loud* before reading the solution. Treat the headings below like an interviewer would reveal them — don't scroll ahead.

This mirrors Phase 2: *a running but poorly performing PySpark app with 4–6 improvable areas.*

---

## Step 0 — Generate reproducible data (serverless-safe)

```python
from pyspark.sql import functions as F

# 20M-row fact table of orders (uses spark.range, no RDDs)
orders = (spark.range(0, 20_000_000)
    .withColumn("product_id", (F.col("id") % 5000).cast("int"))
    .withColumn("customer_id", (F.col("id") % 250000).cast("int"))
    .withColumn("qty",        (F.col("id") % 7 + 1).cast("int"))
    .withColumn("unit_price", (F.round(F.rand(seed=1) * 100 + 1, 2)))
    .withColumn("year",       F.when(F.col("id") % 10 < 3, 2024).otherwise(2025))
    .withColumn("raw_category",
        F.when(F.col("product_id") % 3 == 0, F.lit(" Electronics "))
         .when(F.col("product_id") % 3 == 1, F.lit("home goods"))
         .otherwise(F.lit("TOYS")))
    .drop("id"))

# Small product dimension (5000 rows) — this is a lookup table
products = (spark.range(0, 5000)
    .withColumnRenamed("id", "product_id")
    .withColumn("product_id", F.col("product_id").cast("int"))
    .withColumn("product_name", F.concat(F.lit("Product_"), F.col("product_id")))
    .withColumn("supplier", F.concat(F.lit("Supplier_"), (F.col("product_id") % 50))))

orders.write.mode("overwrite").saveAsTable("workspace.default.orders")
products.write.mode("overwrite").saveAsTable("workspace.default.products")
```

## Step 1 — The slow app (this is what you're handed)

```python
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

orders   = spark.table("workspace.default.orders")
products = spark.table("workspace.default.products")

# A Python UDF to normalize the category text
def normalize_category(s):
    if s is None:
        return None
    return s.strip().lower()

normalize_udf = udf(normalize_category, StringType())

# Join everything first, keep all columns
enriched = orders.join(products, "product_id")          # (A)
enriched = enriched.withColumn("category", normalize_udf("raw_category"))  # (B)
enriched = enriched.withColumn("revenue", F.col("qty") * F.col("unit_price"))

# Aggregate, then filter to 2025 at the very end
by_cat = (enriched
    .groupBy("category", "year")
    .agg(F.sum("revenue").alias("total_revenue"),
         F.countDistinct("customer_id").alias("customers")))    # (C)

by_cat_2025 = by_cat.where(F.col("year") == 2025)               # (D)

# Sort the entire result and pull it all to the driver to "look at it"
final = by_cat_2025.orderBy(F.desc("total_revenue"))            # (E)
rows = final.collect()                                          # (F)
for r in rows:
    print(r["category"], r["total_revenue"])
```

It runs. It's slow. **Find the 4–6 problems.** Diagnose with `final.explain("formatted")` and the query profile before you change anything.

---

## Step 2 — Tasks (do these in order, narrating)

1. **Profile first.** Run `enriched.explain("formatted")` and `by_cat_2025.explain("formatted")`. Identify the join strategy and every `Exchange`.
2. Fix the issues you find, **biggest impact first**.
3. After each fix, state the **business translation** in one sentence.
4. Produce a final, clean version that returns the answer **without** pulling everything to the driver.
5. Be ready to answer: *"How does this scale if `orders` is 20 **billion** rows instead of 20 million?"*

---

## Step 3 — Don't peek. Try first. ⛔

<br><br><br>

---

## Solution — the 6 problems and the fixes

> Labels (A)–(F) refer to the lines in the slow app.

### Problem 1 — (B) Python UDF for `strip().lower()`
A Python UDF serializes every one of 20M rows to a Python process. Pure built-in work.
```python
# FIX
.withColumn("category", F.lower(F.trim("raw_category")))
```
**Translation:** *"That custom function processes rows one at a time in Python; the built-in does it in native vectorized code — same output, far faster."*

### Problem 2 — (A) Sort-merge join of a 20M fact against a 5k dimension
`products` is tiny. Broadcast it so the fact table never shuffles.
```python
# FIX
from pyspark.sql.functions import broadcast
enriched = orders.join(broadcast(products), "product_id")
```
Confirm in the plan: `SortMergeJoin` → `BroadcastHashJoin`, and the big `Exchange` on the orders side disappears.
**Translation:** *"The product list is tiny, so we copy it to every machine instead of reshuffling 20 million order rows."*

### Problem 3 — (D) Filter applied *after* the aggregation
We only want 2025, but we aggregate *all* years first, then drop 2024. Push the filter before the join/aggregation so we never process 2024 rows. (And `year` is cheap to filter early.)
```python
# FIX — filter at the source
orders_2025 = orders.where(F.col("year") == 2025)
```
**Translation:** *"We were crunching a full year of data we immediately threw away — filtering up front means we only touch the rows we actually report on."*

### Problem 4 — projection / column pruning
We carry `product_name`, `supplier`, `customer_id`, etc. through the pipeline but the final report only needs category, year, revenue, and a distinct customer count. Select only what's needed (and we don't even need the join for revenue — `raw_category` is on `orders`! If the report doesn't use product/supplier, **drop the join entirely**). Recognizing the join is *unnecessary for this output* is the senior move.
```python
# FIX — if product attributes aren't in the output, skip the join completely
slim = (orders.where(F.col("year") == 2025)
              .select("raw_category", "customer_id", "qty", "unit_price"))
```
**Translation:** *"The report doesn't actually use any product details, so the join was pure overhead — removing it eliminates a whole stage."* *(If the interviewer says the join IS needed, keep it but broadcast + prune columns.)*

### Problem 5 — (E) `orderBy` on the full result is fine, but (F) `collect()` is the real risk
`collect()` pulls every row to the driver. Here the aggregated result is small, so it's survivable — but it's a bad habit and a landmine if the grouping cardinality is high. Return the DataFrame, or `show`/`limit`, or `write` it.
```python
# FIX — keep work distributed; peek with show, persist with write
result = (slim
    .withColumn("category", F.lower(F.trim("raw_category")))
    .withColumn("revenue", F.col("qty") * F.col("unit_price"))
    .groupBy("category")
    .agg(F.sum("revenue").alias("total_revenue"),
         F.countDistinct("customer_id").alias("customers"))
    .orderBy(F.desc("total_revenue")))

result.show(truncate=False)      # not collect()
# result.write.mode("overwrite").saveAsTable("workspace.default.revenue_by_category")
```
**Translation:** *"Pulling the whole result into one machine is how you crash the driver in production; we keep the work spread out and only ship back the small summary."*

### Problem 6 — recomputation / no materialization (the scaling story)
If `enriched`/`slim` is reused by several downstream reports, each action re-runs the lineage. On classic compute you'd `cache()`; on **serverless** you **materialize to Delta**:
```python
slim.write.mode("overwrite").saveAsTable("workspace.default.orders_2025_slim")
slim = spark.table("workspace.default.orders_2025_slim")
```
**Translation:** *"If five dashboards read this intermediate step, we compute it once and reuse it instead of rebuilding it five times."*

---

## The clean final version

```python
from pyspark.sql import functions as F

orders = spark.table("workspace.default.orders")

revenue_by_category = (orders
    .where(F.col("year") == 2025)                                  # filter early (P3)
    .select("raw_category", "customer_id", "qty", "unit_price")    # prune (P4)
    .withColumn("category", F.lower(F.trim("raw_category")))       # built-in not UDF (P1)
    .withColumn("revenue", F.col("qty") * F.col("unit_price"))
    .groupBy("category")
    .agg(F.sum("revenue").alias("total_revenue"),
         F.countDistinct("customer_id").alias("customers"))
    .orderBy(F.desc("total_revenue")))

revenue_by_category.show(truncate=False)                           # no collect (P5)
```
*(If product/supplier attributes ARE required in the output, add `.join(broadcast(products.select(...needed...)), "product_id")` — broadcast + pruned (P2).)*

---

## Answering "what if it's 20 billion rows?"

- The fixes scale *with* the data: filter-early and column-pruning cut I/O proportionally; the broadcast join keeps the fact table from ever shuffling regardless of size.
- `countDistinct` becomes the expensive part at huge scale (it shuffles); consider `approx_count_distinct` if an exact count isn't required — *ask the customer whether approximate is acceptable.*
- Make sure the source is **Delta**, partitioned/clustered on `year` so the `where(year=2025)` does **partition pruning** and skips files.
- Lean on **AQE** to coalesce shuffle partitions and split any skew; mention you'd watch the query profile for a single long-running task as the skew signal.
- Never `collect()`; write the result to a table the dashboards read.

> **What to say if you only get partway:** "I found and fixed the UDF and the join; with more time I'd push the filter up, prune columns, and replace the `collect`. I'd confirm each in the query profile." Naming the remaining work shows you saw it.
