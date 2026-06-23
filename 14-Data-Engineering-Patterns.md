---
title: Data Engineering Patterns
tags: [patterns, ingestion, scd, dedupe, streaming, idempotency]
---

# 14 — Data Engineering Patterns (the customer-shaped problems)

[← Spark SQL Drills](13-Spark-SQL-Drills.md) · Next: [Common Spark Errors →](15-Common-Spark-Errors-Debug.md)

> FDE interviews live in the *shape* of real customer problems: late-arriving data, incremental loads, deduplication, slowly changing dimensions, idempotent re-runs. These are the seven patterns to recognize on sight.

## 1. Medallion (Bronze → Silver → Gold)

The Databricks reference architecture. Useful **vocabulary** even when the customer doesn't use the names.

| Layer | Contents | Operations |
|-------|----------|------------|
| **Bronze** | Raw, ingested as-is (Kafka/files/CDC), schema-on-write | Append-only; one source = one table |
| **Silver** | Cleaned, validated, deduplicated, joined | `MERGE` upserts; data-quality checks |
| **Gold**   | Business-ready aggregates / serving tables | `OPTIMIZE`+`ZORDER`/Liquid; star-schema or denormalized |

What to mention out loud: *"I'd land raw into Bronze so we can replay if Silver logic changes; Silver is where dedup/SCD happens; Gold is what the dashboards read."*

## 2. Idempotent incremental loads

A nightly job that fails halfway must be safely re-runnable. Three reliable ingredients:

1. **Watermark column** in the source (`updated_at` / event time / monotonically increasing id).
2. **MERGE** on the natural key, not append. Re-running today's batch doesn't duplicate.
3. **A high-water-mark table** that records the max watermark processed per source.

```python
last_hwm = (spark.table("ops.hwm")
                 .where("source = 'orders'")
                 .agg(F.max("hwm")).first()[0])

incoming = (spark.table("bronze.orders")
                 .where(F.col("updated_at") > F.lit(last_hwm)))

# MERGE into silver…
# then bump the HWM in a separate transaction (don't lose this step on retry):
new_hwm = incoming.agg(F.max("updated_at")).first()[0]
spark.sql(f"INSERT INTO ops.hwm VALUES ('orders', '{new_hwm}')")
```

**Customer phrasing:** *"The job is safe to re-run from scratch — worst case we redo work, never duplicate or skip rows."*

## 3. Deduplication patterns

Spark gives you three ways; pick the one whose **semantics** matches your case.

| Goal | Tool | Notes |
|------|------|-------|
| "Same row twice, drop one" | `df.dropDuplicates()` (no args) | Considers all columns |
| "Same business key, keep the latest" | `Window.partitionBy(key).orderBy(desc(ts))` + `row_number()==1` | Tie-breaker comes from `orderBy` |
| "Drop near-duplicates" (fuzzy) | `approx_count_distinct` + custom dedupe key | Domain-specific; no built-in |

**Trap:** `dropDuplicates(["id"])` keeps an **arbitrary** row when keys collide. If you need "latest wins," use the window pattern.

## 4. Slowly Changing Dimensions (SCD)

| Type | What you store | When to use |
|------|----------------|-------------|
| **SCD0** | Never update | Reference data that truly doesn't change (country codes) |
| **SCD1** | Overwrite — only the latest value | Don't care about history |
| **SCD2** | Full history with `effective_from/to` + `is_current` | History matters; default for customer-facing analytics |
| **SCD3** | One "previous value" column | Rarely the right answer; SCD2 is cleaner |

Working SCD2 MERGE pattern lives in [`notebooks/databricks/06_delta_merge_scd.py`](notebooks/databricks/06_delta_merge_scd.py).

## 5. CDC ingestion shapes

If the customer says "we have a CDC feed," ask **what shape**:

- **Snapshot per period** — full table dump every N hours. Use `MERGE` with the natural key.
- **Insert/update/delete events** — each row has an operation flag. Use `APPLY CHANGES INTO` (Delta Live Tables) or hand-rolled MERGE that handles all three op types.
- **Log-based CDC (Debezium/Fivetran/etc.)** — has before/after images. Easier than it looks — the after-image plus op flag is sufficient.

> Saying *"is the source a snapshot or an event stream, and is there a soft-delete flag?"* is exactly what an FDE asks a customer on day one.

## 6. Late-arriving data + watermarks (streaming)

```python
events_with_wm = (events
    .withWatermark("event_time", "10 minutes")
    .groupBy(F.window("event_time", "5 minutes"), "user_id")
    .count())
```

**Watermark = "drop state for windows that closed > N minutes ago."** Lower watermark = freshness; higher = forgiveness for late data. Talk about the **tradeoff** explicitly — there's no universally right value.

## 7. Schema evolution gracefully

```python
(new_batch.write.format("delta").mode("append")
   .option("mergeSchema", "true")
   .saveAsTable("bronze.orders"))
```

What this handles: new columns appearing in the source. What it **doesn't** handle:

- Renamed columns (looks like drop + add)
- Type incompatibilities (`INT` → `STRING`)
- Removed columns (downstream code that selects them will break)

**Customer phrasing:** *"`mergeSchema` future-proofs us against the source adding a column, but we still need a contract for renames and type changes — usually a Bronze→Silver mapping step."*

## 8. Data-quality gates — fail loud, not silent

Wrap critical loads in assertions:

```python
silver = transform(bronze)

assert silver.count() > 0,                              "empty silver — upstream bug?"
assert silver.filter("amount < 0").count() == 0,        "negative amounts — fix source"
assert silver.filter("customer_id IS NULL").count() == 0, "customer_id required"

silver.write.mode("overwrite").saveAsTable("silver.orders")
```

Or push the same checks into the [DQ engine from challenge 06](06-Python-Feature-Dev-Challenge.md). The interview-worthy line: *"I'd rather fail the pipeline than load bad data into a dashboard the CFO sees."*

## 9. Reproducibility — pin everything that drifts

- **Random seeds** in any sampling or salt: `F.rand(seed=42)`.
- **Schemas declared**, not inferred (`spark.read.schema(my_schema).json(...)`).
- **Time-window logic with explicit zones** (`F.from_utc_timestamp("ts", "America/New_York")`).
- **Cluster of named libs** (not `latest`) in `pyproject.toml` / cluster libs.

## 10. The seven patterns drilled into one paragraph

> *"For ingestion I'd land Bronze append-only, MERGE into Silver with a watermark for idempotent re-runs, and `OPTIMIZE` Gold. SCD2 if history matters; otherwise SCD1 + a CDC source decides MERGE vs APPLY CHANGES. Deduplicate with a window if 'latest wins,' otherwise `dropDuplicates`. Late data gets a watermark in streaming. New columns are handled with `mergeSchema=true`. And I'd wrap critical loads in data-quality assertions so failures surface loud."*

If you can say that out loud comfortably in 30 seconds, you've covered 80% of the real-world pipeline questions.
