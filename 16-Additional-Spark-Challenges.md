---
title: Additional Spark Challenges
tags: [spark, challenge, hands-on, optimization]
---

# 16 — Additional Spark Challenges

[← Common Spark Errors](15-Common-Spark-Errors-Debug.md) · Next: [Additional Python Challenges →](17-Additional-Python-Challenges.md)

> [05](05-Spark-Optimization-Challenge.md) gives you **one** slow PySpark app. Real interview luck = different problem shapes. Here are **two more** with different bottleneck profiles. Same rules: **25-minute timer, narrate out loud, diagnose with `explain` + Query Profile before changing anything.**

---

## Challenge B — Skewed join + window aggregation

**Story:** "We have order events and a customer dimension. Marketing wants a daily report of the top-revenue customer per region. The job is taking 40 minutes and one task always seems to be the last one standing."

### Data gen

```python
from pyspark.sql import functions as F

# 30M order events with a deliberately hot customer
events = (spark.range(0, 30_000_000)
    .withColumn("customer_id",
        F.when(F.col("id") % 1000 < 500, F.lit(1))         # customer #1 = 50% of rows
         .otherwise((F.col("id") % 200000).cast("int")))
    .withColumn("region",
        F.when(F.col("id") % 3 == 0, "EU")
         .when(F.col("id") % 3 == 1, "US").otherwise("APAC"))
    .withColumn("order_date", F.date_sub(F.current_date(), (F.col("id") % 30).cast("int")))
    .withColumn("amount", F.round(F.rand(seed=2) * 500, 2))
    .drop("id"))

customers = (spark.range(0, 200_000)
    .withColumnRenamed("id", "customer_id")
    .withColumn("customer_id", F.col("customer_id").cast("int"))
    .withColumn("name", F.concat(F.lit("Cust_"), F.col("customer_id")))
    .withColumn("segment", F.when(F.col("customer_id") % 5 == 0, "VIP").otherwise("STD")))

events.write.mode("overwrite").saveAsTable("workspace.default.events_b")
customers.write.mode("overwrite").saveAsTable("workspace.default.customers_b")
```

### The slow app (try to fix it)

```python
events    = spark.table("workspace.default.events_b")
customers = spark.table("workspace.default.customers_b")

# Join everything, then aggregate, then window, then collect
joined = events.join(customers, "customer_id")              # (A)

per_cust = (joined
    .groupBy("region", "customer_id", "name", "segment")
    .agg(F.sum("amount").alias("revenue")))

from pyspark.sql.window import Window
w = Window.partitionBy("region").orderBy(F.desc("revenue"))
ranked = per_cust.withColumn("rk", F.row_number().over(w))   # (B)
top = ranked.where(F.col("rk") == 1)                         # (C)

rows = top.orderBy("region").collect()                       # (D)
for r in rows:
    print(r["region"], r["name"], r["revenue"])
```

### What you should find

<details><summary>Spoilers — 5 issues</summary>

1. **(A) The join is shuffling 30M events to merge in `name` and `segment`** — but `customers` is 200k rows = a few MB. **`broadcast(customers)`** the small side. (You could also defer the join until after the aggregation: aggregate by `customer_id` first, then broadcast-join `customers` onto the much smaller per-customer result. That's the biggest single win.)
2. **Skew on `customer_id=1`** — half the events live on one key, so the post-`broadcast` aggregation is balanced (no shuffle on the big side), but **without** the broadcast you get a giant single-task hotspot. AQE skew handling helps; broadcasting eliminates it entirely.
3. **(B) `row_number()` over `(region)` with no upper bound** sorts every customer per region — fine here because per-customer aggregation already shrank the rows, but on raw events it'd be a disaster. Aggregating first is the right order.
4. **Projection bloat** — `select("region","customer_id","name","segment","amount")` only, not `events.*` joined with `customers.*`.
5. **(D) `collect()`** — small result here, but the habit is wrong. Use `show` or `write`.

</details>

### The clean version

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import broadcast

events    = spark.table("workspace.default.events_b").select("customer_id", "region", "amount")
customers = spark.table("workspace.default.customers_b").select("customer_id", "name", "segment")

# 1. Aggregate per (region, customer) FIRST — shrinks 30M rows to ~600k
per_cust = (events.groupBy("region", "customer_id")
                  .agg(F.sum("amount").alias("revenue")))

# 2. NOW broadcast-join the (small) per_cust against the (small) customer dim — both fit
enriched = per_cust.join(broadcast(customers), "customer_id")

# 3. Window only over the already-aggregated data
w = Window.partitionBy("region").orderBy(F.desc("revenue"))
top = enriched.withColumn("rk", F.row_number().over(w)).where("rk = 1").drop("rk")

top.orderBy("region").show(truncate=False)
```

### Customer translation

> *"The original plan shuffled 30 million events just to attach a customer name. By aggregating per customer first we reduce that to ~600 thousand rows, and the join becomes free with a broadcast. The hot key that was making one task crawl never gets a chance to bite — there's no big shuffle to skew."*

---

## Challenge C — Streaming-like incremental load with the wrong watermark

**Story:** "Hourly batch reads new orders from a Bronze Delta table, MERGEs into Silver, then computes a 24h rolling spend per customer for fraud scoring. It's currently running for ~50 minutes when it should be 5."

### Data gen (idempotent)

```python
from pyspark.sql import functions as F

# Bronze: 10M raw events over 7 days
spark.sql("DROP TABLE IF EXISTS workspace.default.bronze_orders")
(spark.range(0, 10_000_000)
   .withColumn("order_id", F.col("id").cast("long"))
   .withColumn("customer_id", (F.col("id") % 50000).cast("int"))
   .withColumn("amount", F.round(F.rand(seed=3) * 200, 2))
   .withColumn("event_time",
       F.timestamp_seconds((F.unix_timestamp(F.current_timestamp()) - (F.col("id") % (7*24*3600))).cast("long")))
   .drop("id")
   .write.mode("overwrite")
   .saveAsTable("workspace.default.bronze_orders"))

# Silver: empty, will be MERGEd into
spark.sql("DROP TABLE IF EXISTS workspace.default.silver_orders")
spark.sql("""CREATE TABLE workspace.default.silver_orders
             (order_id BIGINT, customer_id INT, amount DOUBLE, event_time TIMESTAMP)
             USING DELTA""")
```

### The slow app

```python
bronze = spark.table("workspace.default.bronze_orders")
silver = spark.table("workspace.default.silver_orders")

# Step 1: MERGE everything (wrong — doesn't filter to new data)
bronze.createOrReplaceTempView("bronze_v")
spark.sql("""
MERGE INTO workspace.default.silver_orders s
USING bronze_v b
ON s.order_id = b.order_id
WHEN NOT MATCHED THEN INSERT *
""")

# Step 2: 24h rolling spend per customer, computed on the FULL silver every run
from pyspark.sql.window import Window
silver = spark.table("workspace.default.silver_orders")
w = (Window.partitionBy("customer_id")
           .orderBy(F.col("event_time").cast("long"))
           .rangeBetween(-24*3600, 0))
scored = silver.withColumn("spend_24h", F.sum("amount").over(w))

# Step 3: dump everything to driver "to email a CSV"
rows = scored.collect()
```

### What's wrong

<details><summary>Spoilers — 6 issues</summary>

1. **MERGE without a predicate** scans the entire Silver target every run. Add a **partition or watermark predicate**: `ON s.order_id = b.order_id AND s.event_time > current_timestamp() - INTERVAL 2 DAYS` (matches the new-data window only).
2. **No incremental filter on Bronze.** Track a watermark (last `event_time` processed) in a side table; filter `bronze.where(event_time > last_hwm)` so MERGE only sees new rows.
3. **The 24h window runs over ALL of Silver.** For *daily* fraud scoring, we only need to recompute for customers with new events; or run the window only on the last 25h of data (24h window + 1h batch padding).
4. **Window has no upper bound on the orderBy column type** — `cast("long")` is fine, but if `event_time` is already a timestamp, `rangeBetween` understands seconds via cast.
5. **`collect()` at the end** to "email a CSV" — for 10M rows of (customer_id, spend_24h) that's hundreds of MB pulled through the driver. Write to a Delta table and have the email job read from there.
6. **Silver table has no partitioning or clustering**, so MERGE has to scan every file. `CLUSTER BY (event_time)` (Liquid) or partition by `DATE(event_time)` for time-range pruning.

</details>

### Clean(er) version — annotated

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1) Get watermark (last event_time loaded). Tiny query, cheap.
last_hwm = (spark.table("workspace.default.silver_orders")
                  .agg(F.max("event_time")).first()[0])
# fallback for first-ever run
last_hwm = last_hwm or "1970-01-01 00:00:00"

# 2) Filter Bronze to new data only
new_bronze = (spark.table("workspace.default.bronze_orders")
                   .where(F.col("event_time") > F.lit(last_hwm)))
new_bronze.createOrReplaceTempView("new_bronze_v")

# 3) MERGE with a predicate so Silver only scans matching files
spark.sql("""
MERGE INTO workspace.default.silver_orders s
USING new_bronze_v b
ON s.order_id = b.order_id
   AND s.event_time >= b.event_time - INTERVAL 1 DAY
   AND s.event_time <= b.event_time + INTERVAL 1 DAY
WHEN NOT MATCHED THEN INSERT *
""")

# 4) 24h window only on the affected slice (last 25h, AQE-friendly)
recent = (spark.table("workspace.default.silver_orders")
                .where(F.col("event_time") > F.current_timestamp() - F.expr("INTERVAL 25 HOURS")))

w = (Window.partitionBy("customer_id")
           .orderBy(F.col("event_time").cast("long"))
           .rangeBetween(-24*3600, 0))
scored = recent.withColumn("spend_24h", F.sum("amount").over(w))

# 5) Write — never collect
(scored.write.mode("overwrite")
       .saveAsTable("workspace.default.gold_24h_spend"))
```

### Customer translation

> *"Two things were killing it: every run was re-scanning the full Silver table and recomputing the rolling window from scratch. By tracking a watermark, filtering Bronze to new rows, and constraining the window to the last 25 hours, we go from 50 minutes to a handful — and the result lands in a Delta table the fraud team can subscribe to instead of an emailed CSV that doesn't scale."*

---

## How to rotate these for practice

- **First lap:** Challenge A (in [05](05-Spark-Optimization-Challenge.md)). Pure breadth — covers all 6 levers.
- **Second lap:** Challenge B. Tests **skew recognition** and **the ordering of join vs aggregation**.
- **Third lap:** Challenge C. Tests **incremental ingestion thinking** and **MERGE pruning** — the realest production scenario.

After each, **say the business translation aloud**. If you can't, the fix isn't really yours yet.
