---
title: Delta Lake Deep Dive
tags: [delta, merge, scd, optimize, time-travel, liquid-clustering]
---

# 12 — Delta Lake Deep Dive

[← Local Dev Setup](11-Local-PySpark-Setup-uv.md) · Next: [Spark SQL Drills →](13-Spark-SQL-Drills.md) · Lab: [`notebooks/databricks/06_delta_merge_scd.py`](notebooks/databricks/06_delta_merge_scd.py)

> The interview won't ask you to recite Delta's spec — it will ask "how would you handle late-arriving updates / restore yesterday's view / make this read faster." Know **MERGE**, **time travel**, **OPTIMIZE/ZORDER**, **Liquid Clustering**, and the **schema evolution** flags. The rest you can look up.

## Why Delta exists, in one sentence

Plain Parquet on object storage gives you cheap columnar reads but no ACID, no updates, no time travel, no concurrent writers. Delta wraps Parquet with a **transaction log** (`_delta_log/`) that delivers all four — without leaving open formats.

## MERGE — upsert pattern

```sql
MERGE INTO target t
USING source s ON t.id = s.id
WHEN MATCHED       THEN UPDATE SET *
WHEN NOT MATCHED   THEN INSERT *;
```

What to mention out loud:

- **Idempotent re-runs.** If today's batch arrives twice, MERGE doesn't double-insert (assuming the key is right).
- **Predicate on the join key matters.** Without it, MERGE has to scan all target files. With it, Delta uses **file pruning** to touch only files containing matching keys.
- **The cost is rewriting matched files**, not appending — Delta is copy-on-write by default. Many small targeted updates = file-rewrite churn; consider Deletion Vectors.
- **Deletion Vectors** (when enabled) record deletes as a side-file instead of rewriting Parquet, making `MERGE` and `DELETE` much faster on selective predicates.

## SCD2 — slowly changing dimensions

Track full history per business key. The standard SQL pattern needs **two MERGE passes** because MERGE can't simultaneously "close old row" and "insert new row" for the same key. See [`06_delta_merge_scd.py`](notebooks/databricks/06_delta_merge_scd.py) for working code.

Schema cheatsheet:

```
business_key   <natural id>
<attrs…>       <slowly-changing columns>
effective_from DATE NOT NULL
effective_to   DATE        -- NULL means "current"
is_current     BOOLEAN     -- denormalized convenience flag
```

## Time travel

```sql
-- by version number
SELECT * FROM events VERSION AS OF 42;
-- by timestamp
SELECT * FROM events TIMESTAMP AS OF '2026-06-01T00:00:00';
-- API
spark.read.format("delta").option("versionAsOf", 42).table("events")
```

**Retention:** controlled by `delta.deletedFileRetentionDuration` (default 7 days) + `delta.logRetentionDuration` (default 30 days). After `VACUUM`, time-travel beyond the retention window is gone. **Don't `VACUUM 0 HOURS` casually** — that breaks all in-flight readers.

## OPTIMIZE — the small-files fix

```sql
OPTIMIZE my_table;
OPTIMIZE my_table WHERE date >= '2026-06-01';            -- scope to a window
OPTIMIZE my_table ZORDER BY (customer_id);
```

What it does: rewrites many small files into fewer ~128 MB ones. Side-effects: data skipping stats get fresher; readers get faster; storage doesn't shrink until `VACUUM`. Run nightly on heavily-written tables.

## ZORDER vs Liquid Clustering

| | ZORDER | Liquid Clustering |
|--|--------|-------------------|
| Setup | After every write/OPTIMIZE | Declared at table creation (`CLUSTER BY`) |
| Maintenance | Re-run periodically | Auto on `OPTIMIZE`, incremental |
| Changing keys | Reorders everything from scratch | `ALTER TABLE … CLUSTER BY` is cheap |
| When to use | Older tables, classic compute | New Delta tables on modern Databricks runtimes |

Both help **data skipping**: a filter on the clustering column lets the engine skip files whose stats don't overlap the predicate.

## Schema evolution

```python
# Add new columns from the source on write
(new_df.write.format("delta").mode("append")
       .option("mergeSchema", "true")
       .saveAsTable("target"))

# Allow MERGE to evolve schema
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
```

Watch out: `mergeSchema=true` accepts **new columns**, not type-incompatible changes. A renamed column looks like "drop old + add new" — usually not what you want.

## Performance levers (in priority order, for Delta-on-serverless)

1. **Partition/cluster on the column you filter by most.** Filter pushdown + file skipping is the cheapest read win.
2. **OPTIMIZE regularly** for heavy-write tables. The small-files problem is real.
3. **Predicate in MERGE** — give the optimizer a way to prune target files.
4. **Deletion Vectors** for tables with frequent point updates/deletes.
5. **Z-ORDER/Liquid Cluster on the secondary filter** (the column you filter on *after* the partition column).
6. **Stats columns**: Delta auto-collects min/max for the first 32 columns; reorder columns or use `delta.dataSkippingNumIndexedCols` if your filter column is deep in the schema.

## The customer-translation lines

- **MERGE / SCD2:** *"We track every change in customer state, not just the latest — so reports can answer 'what did the table look like on March 5?'"*
- **Time travel:** *"If a bad batch corrupts the table, we can roll the read back to the prior version while we investigate — no restore-from-backup window."*
- **OPTIMIZE:** *"Every future query gets faster — not just today's job — because reads hit a handful of right-sized files instead of thousands of tiny ones."*
- **ZORDER / Liquid Cluster:** *"We physically co-locate rows that get filtered together so the engine skips most files for a typical query."*

## Things candidates get wrong

| Mistake | Why it's wrong |
|---------|----------------|
| "MERGE is faster than append" | It does **more** work (read + match + rewrite). It's faster than the equivalent custom upsert, not faster than an append-only write. |
| "Just `VACUUM` to free space" | Default safety is 7 days. Setting `0 HOURS` breaks active readers; learn the retention configs first. |
| "Use ZORDER on every column" | Stats degrade past ~3 ZORDER columns; pick the one or two you actually filter by. |
| "Partition by `customer_id`" | High-cardinality partitioning creates thousands of tiny folders — usually wrong; Z-ORDER or Liquid Cluster instead. |
| "Delta only works on Databricks" | Open-source Delta Lake (`delta-spark`) runs on any Spark — locally with this repo's setup, too. |
