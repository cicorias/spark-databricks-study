---
title: Spark SQL Drills
tags: [spark-sql, drills, hands-on]
---

# 13 — Spark SQL Drills

[← Delta Lake Deep Dive](12-Delta-Lake-Deep-Dive.md) · Next: [Data Engineering Patterns →](14-Data-Engineering-Patterns.md) · Lab: [`notebooks/databricks/07_window_functions.py`](notebooks/databricks/07_window_functions.py)

> Spark SQL is **identical to writing DataFrame code** from the optimizer's point of view — same Catalyst, same AQE. Use whichever reads clearer for the team. Warm up with these 10 problems; each has a SQL-first and a DataFrame answer. **Cover the answer and try first.**

## Setup — a tiny dataset that exercises every pattern

```sql
CREATE OR REPLACE TEMP VIEW orders AS
SELECT * FROM VALUES
  (1, 101, '2026-06-01', 'EU', 100.00),
  (2, 101, '2026-06-03', 'EU', 250.00),
  (3, 102, '2026-06-01', 'US', 175.00),
  (4, 102, '2026-06-02', 'US', 175.00),     -- duplicate of #3 in everything but id
  (5, 103, '2026-06-04', 'US',  50.00),
  (6, 104, '2026-06-05', 'EU',   0.00),
  (7, 104, '2026-06-06', 'EU', 999.00),
  (8, 105, '2026-06-07', 'APAC', NULL)
AS t(order_id, customer_id, order_date, region, amount);

CREATE OR REPLACE TEMP VIEW customers AS
SELECT * FROM VALUES
  (101, 'Alice'), (102, 'Bob'), (103, 'Carol'), (104, 'Dave'), (105, 'Eve'), (999, 'Ghost')
AS t(customer_id, name);
```

---

## Q1 — Total revenue per region, excluding nulls
<details><summary>SQL</summary>

```sql
SELECT region, SUM(amount) AS revenue
FROM orders
WHERE amount IS NOT NULL
GROUP BY region
ORDER BY revenue DESC;
```
</details>
<details><summary>DataFrame</summary>

```python
(spark.table("orders")
   .where(F.col("amount").isNotNull())
   .groupBy("region").agg(F.sum("amount").alias("revenue"))
   .orderBy(F.desc("revenue"))).show()
```
</details>

## Q2 — Top 1 order per customer (latest by date)
<details><summary>SQL</summary>

```sql
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS rn
  FROM orders
) WHERE rn = 1;
```
</details>
<details><summary>DataFrame</summary>

```python
w = Window.partitionBy("customer_id").orderBy(F.desc("order_date"))
spark.table("orders").withColumn("rn", F.row_number().over(w)).where("rn = 1").drop("rn")
```
</details>

## Q3 — Customers with no orders (anti-join)
<details><summary>SQL</summary>

```sql
SELECT c.* FROM customers c
LEFT ANTI JOIN orders o ON o.customer_id = c.customer_id;
```
</details>
<details><summary>DataFrame</summary>

```python
customers.join(orders, "customer_id", "left_anti")
```
</details>
**Watch:** `LEFT ANTI` is faster than `WHERE NOT EXISTS` / `NOT IN` and handles NULLs correctly.

## Q4 — Deduplicate orders that are identical except for `order_id`
<details><summary>SQL</summary>

```sql
SELECT customer_id, order_date, region, amount
FROM orders
GROUP BY customer_id, order_date, region, amount;
-- or, keep the smallest order_id from each duplicate group:
SELECT MIN(order_id) AS order_id, customer_id, order_date, region, amount
FROM orders
GROUP BY customer_id, order_date, region, amount;
```
</details>
<details><summary>DataFrame</summary>

```python
orders.dropDuplicates(["customer_id", "order_date", "region", "amount"])
```
</details>

## Q5 — 7-day rolling revenue per region
<details><summary>SQL</summary>

```sql
SELECT region, order_date,
       SUM(amount) OVER (
         PARTITION BY region
         ORDER BY CAST(order_date AS TIMESTAMP)
         RANGE BETWEEN INTERVAL 6 DAYS PRECEDING AND CURRENT ROW
       ) AS rev_7d
FROM orders
ORDER BY region, order_date;
```
</details>

## Q6 — % of total revenue per customer
<details><summary>SQL</summary>

```sql
WITH per_cust AS (
  SELECT customer_id, SUM(amount) AS cust_total FROM orders GROUP BY customer_id
)
SELECT customer_id, cust_total,
       100.0 * cust_total / SUM(cust_total) OVER () AS pct_of_total
FROM per_cust
ORDER BY pct_of_total DESC;
```
</details>
**Note:** `SUM() OVER ()` (no `PARTITION BY`) computes a single grand-total — convenient, but the un-partitioned window funnels all data through one task on a big table. For huge inputs, compute the total as a scalar subquery / broadcast it instead.

## Q7 — Median amount per region (approx is fine)
<details><summary>SQL</summary>

```sql
SELECT region,
       APPROX_PERCENTILE(amount, 0.5) AS median_approx
FROM orders WHERE amount IS NOT NULL
GROUP BY region;
```
</details>
**Why approx?** Exact percentile on big data requires a global sort — expensive. `APPROX_PERCENTILE` is a streaming algorithm — close enough for dashboards, orders-of-magnitude cheaper.

## Q8 — Pivot: revenue by region as columns
<details><summary>SQL</summary>

```sql
SELECT * FROM (
  SELECT customer_id, region, amount FROM orders
)
PIVOT (
  SUM(amount) FOR region IN ('EU', 'US', 'APAC')
);
```
</details>
<details><summary>DataFrame</summary>

```python
orders.groupBy("customer_id").pivot("region", ["EU", "US", "APAC"]).sum("amount")
```
</details>
**Watch:** always pass the explicit value list to `pivot(...)` — without it, Spark runs a job to find distinct values, which is extra work.

## Q9 — Find duplicate keys that *shouldn't* exist (data-quality check)
<details><summary>SQL</summary>

```sql
SELECT order_id, COUNT(*) AS n
FROM orders
GROUP BY order_id
HAVING COUNT(*) > 1;
```
</details>
This is the SQL version of the `Unique` rule from [`06`](06-Python-Feature-Dev-Challenge.md) — and it scales, unlike the Python version.

## Q10 — Gap-and-island: number consecutive same-region orders into runs per customer
<details><summary>SQL</summary>

```sql
WITH flagged AS (
  SELECT *,
    CASE WHEN region <> LAG(region) OVER (PARTITION BY customer_id ORDER BY order_date)
              OR LAG(region) OVER (PARTITION BY customer_id ORDER BY order_date) IS NULL
         THEN 1 ELSE 0 END AS new_run
  FROM orders
)
SELECT *, SUM(new_run) OVER (PARTITION BY customer_id ORDER BY order_date) AS run_id
FROM flagged;
```
</details>
Same trick as the sessionization lab — the running sum of "is this a boundary?" gives a stable group id.

---

## Common SQL-side traps to verbalize

- **`NOT IN` with NULLs** silently drops rows. Prefer `LEFT ANTI JOIN` or `NOT EXISTS`.
- **Implicit `COUNT(DISTINCT …)`** at huge scale = a shuffle. Consider `APPROX_COUNT_DISTINCT` if exactness isn't required (ask the customer).
- **`ORDER BY` without `LIMIT`** on a full big result is a global sort — usually you only need a top-N.
- **`OR` across columns in a join condition** often forces a SortMergeJoin Spark can't pick a strategy for. Rewrite as `UNION ALL` of two equi-joins.
- **`STRING_TO_*` parsing in a `WHERE`** breaks predicate pushdown — parse first into a typed column, then filter.

## When to use SQL vs DataFrame in the interview

Either is fine — both compile through Catalyst. SQL is often shorter for aggregations and window functions; DataFrame chains read better for long ETL pipelines. **Mention you can switch**, and use whichever the slow-app baseline already uses (don't rewrite for style).
