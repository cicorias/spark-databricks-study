---
title: Timed Speed Drills
tags: [drills, practice, timed, warm-up]
---

# 18 — Timed Speed Drills (5 / 10 / 15 minute warm-ups)

[← Additional Python Challenges](17-Additional-Python-Challenges.md) · Next: [Self-Assessment Rubric →](19-Self-Assessment-Rubric.md)

> The interview is **two 25-minute sprints back-to-back**. The goal of these drills is to make the first 60 seconds of any prompt automatic — so you spend cognitive budget on the *problem*, not on remembering syntax. Do at least one drill from each section the day before the interview.

## How to drill

1. **Set a timer**, write the answer in a scratch file or out loud, then check.
2. **Narrate** even when alone — that's the muscle being trained.
3. **Score yourself**: ✅ correct + narrated, ⚠️ correct but silent, ❌ wrong/incomplete. Re-drill anything not ✅.

---

## Section A — 5-minute syntax sprints (DataFrame)

Set a 5-minute timer per drill. Write working PySpark.

### A1. Read a Delta table, keep only 2025 rows, sum revenue per region.
<details><summary>Answer</summary>

```python
from pyspark.sql import functions as F
(spark.table("workspace.default.orders")
   .where(F.col("year") == 2025)
   .groupBy("region")
   .agg(F.sum("amount").alias("revenue"))
   .show())
```
</details>

### A2. Broadcast-join a 5k-row `products` dim into a 20M-row `orders` fact, then group by `category`.
<details><summary>Answer</summary>

```python
from pyspark.sql.functions import broadcast
(orders.join(broadcast(products), "product_id")
       .groupBy("category").agg(F.sum("amount").alias("rev"))
       .show())
```
</details>

### A3. Find the latest order per customer (row_number window).
<details><summary>Answer</summary>

```python
from pyspark.sql.window import Window
w = Window.partitionBy("customer_id").orderBy(F.desc("order_date"))
(orders.withColumn("rn", F.row_number().over(w))
       .where("rn = 1").drop("rn"))
```
</details>

### A4. Replace this UDF with built-ins:
```python
udf(lambda s: s.strip().lower() if s else None, StringType())
```
<details><summary>Answer</summary>

```python
F.lower(F.trim("col"))
```
</details>

### A5. Compute partition distribution **without** RDDs.
<details><summary>Answer</summary>

```python
(df.groupBy(F.spark_partition_id().alias("pid")).count()
   .orderBy(F.desc("count")).show())
```
</details>

### A6. Write a DataFrame to a managed Delta table, overwriting any existing one.
<details><summary>Answer</summary>

```python
df.write.mode("overwrite").saveAsTable("workspace.default.my_table")
```
</details>

---

## Section B — 5-minute syntax sprints (SQL)

### B1. Top 3 customers by revenue, region-by-region.
<details><summary>Answer</summary>

```sql
SELECT * FROM (
  SELECT region, customer_id, revenue,
         ROW_NUMBER() OVER (PARTITION BY region ORDER BY revenue DESC) AS rn
  FROM customer_revenue
) WHERE rn <= 3;
```
</details>

### B2. Customers with **no** orders.
<details><summary>Answer</summary>

```sql
SELECT c.* FROM customers c
LEFT ANTI JOIN orders o ON o.customer_id = c.customer_id;
```
</details>

### B3. Idempotent MERGE upsert into `silver.orders` on `order_id`.
<details><summary>Answer</summary>

```sql
MERGE INTO silver.orders s
USING bronze.orders b
ON s.order_id = b.order_id
WHEN MATCHED       THEN UPDATE SET *
WHEN NOT MATCHED   THEN INSERT *;
```
</details>

### B4. Compact small files, then z-order on `customer_id`.
<details><summary>Answer</summary>

```sql
OPTIMIZE silver.orders ZORDER BY (customer_id);
```
</details>

### B5. Query yesterday's version of a Delta table.
<details><summary>Answer</summary>

```sql
SELECT * FROM silver.orders TIMESTAMP AS OF date_sub(current_date(), 1);
```
</details>

---

## Section C — 10-minute mini-challenges

### C1. Given the slow snippet below, name **all 3 issues** and fix them. (10 min)

```python
from pyspark.sql.functions import udf
upper = udf(lambda s: s.upper())                          # ?

df = (spark.table("orders")
        .join(spark.table("products"), "product_id")      # ?
        .where(F.col("year") == 2025))

rows = df.collect()                                       # ?
```

<details><summary>Answers (3)</summary>

1. **UDF for `upper`** → `F.upper(...)` (vectorized).
2. **Filter applied after the join** → push `where(year=2025)` before the join, so the join sees fewer rows.
3. **`collect()`** → use `show`/`limit`/`write`; here especially because the join hasn't been broadcasted (could be huge).

Also worth catching: **the join may be sort-merge**. Wrap `products` in `broadcast(...)` if it's small.
</details>

### C2. Identify which of these are wide vs narrow. (10 min)

`select`, `where`, `withColumn`, `union`, `groupBy().agg()`, `join` (default), `join` (broadcast), `repartition(n)`, `coalesce(n)`, `distinct`, `orderBy`, `dropDuplicates(["k"])`, `cache`.

<details><summary>Answer</summary>

- **Narrow:** `select`, `where`, `withColumn`, `union`, broadcast-`join`, `coalesce(n)`, `cache`
- **Wide:** default `join`, `groupBy().agg()`, `repartition(n)`, `distinct`, `orderBy`, `dropDuplicates(["k"])`

`dropDuplicates` is wide because it has to shuffle by the key columns to find duplicates across partitions.
</details>

### C3. Skew diagnosis. The following groupBy is slow — one task always finishes last. Write the 3-line check that proves it's skew. (5 min)
<details><summary>Answer</summary>

```python
(df.groupBy(F.spark_partition_id().alias("pid")).count()
   .orderBy(F.desc("count")).show(5))
# If the top partition has 10–100x more rows than the rest, it's skew.
```
</details>

---

## Section D — 15-minute scenarios (narrate the plan, don't write code)

You have 15 minutes. **Speak the answer aloud** for 2–3 minutes; don't write code unless needed for clarity.

### D1. "We have a 100M-row clickstream Delta table partitioned by `event_date`. A new dashboard filters on `user_id`. It's slow. What do you change?"
<details><summary>Talking points</summary>

- Partition by date is correct for time-range queries, but a `user_id` filter scans every date partition. Add **Z-ORDER (or Liquid Cluster) on `user_id`** so each partition's files can be skipped.
- Confirm with `DESCRIBE DETAIL` that file count is reasonable; run `OPTIMIZE` if there are many small files.
- Watch out: if cardinality of `user_id` is very high (10M+), the stats overhead grows; verify with the query profile that data skipping is firing.
- Business translation: *"Today every dashboard click reads all 100M rows; after clustering, a typical user query reads <1%."*
</details>

### D2. "Our nightly job runs in 30 minutes. Above 20M rows it falls off a cliff. What's likely?"
<details><summary>Talking points</summary>

- Two prime suspects: a **shuffle that scales worse than linear** (probably an unsalted skewed join), or **`spark.sql.shuffle.partitions=200`** with rows-per-partition growing too large.
- AQE handles a lot of this; check if it's enabled and if the query profile shows AQE actually coalescing/splitting.
- If a single key is huge, salt it; if the dim side fits in memory, broadcast it.
- Confirm there's no `collect()`/`toPandas()` lurking — those scale O(rows × drivers).
</details>

### D3. "Customer ran `MERGE` and the next read shows duplicate rows. How?"
<details><summary>Talking points</summary>

- MERGE on a key that **isn't actually unique** in the source. (`MERGE … ON s.id = b.id` with `b` having two rows per `id` inserts both.)
- The "deduplicate the source first" step (`dropDuplicates`/window+row_number) is missing.
- Or: two concurrent writers (`ConcurrentAppendException` would've fired — check the Delta history).
- Fix: `DESCRIBE HISTORY`, find the merge operation, look at metrics; dedup source as a pre-step.
</details>

### D4. "AI suggested adding `.cache()` to speed up our pipeline. We're on serverless. Your move."
<details><summary>Answer</summary>

> *"Cache APIs are restricted on serverless — they might raise. Instead, if this DataFrame is reused, I'll materialize it to a Delta table once and read it back from there. Same effect, works in the actual runtime, and survives across cells/jobs."*
</details>

---

## Cool-down — the 60-second pocket script

Practice this verbatim 3 times before the interview. Use it to open **every** problem:

> *"Okay, let me read this end to end first and tell you what I think it's doing… [read aloud]. So the intent is X, inputs are Y, output is Z. My hunch for the biggest cost is the shuffle in the join. Before I touch anything I'll confirm with the query profile. Sound good?"*

That opening alone signals computational thinking, code stewardship, and customer collaboration — three of the four graded signals, in one sentence.
