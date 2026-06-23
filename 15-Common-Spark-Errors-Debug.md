---
title: Common Spark Errors & Debugging
tags: [errors, debugging, troubleshooting, spark]
---

# 15 — Common Spark Errors & Debugging

[← Data Engineering Patterns](14-Data-Engineering-Patterns.md) · Next: [Additional Spark Challenges →](16-Additional-Spark-Challenges.md)

> The grading rubric scores **Resilience** explicitly: "when you hit a bug, how do you navigate the unknown." This note is the playbook for the 12 errors you're most likely to see (or cause) in 25 minutes of pair-programming, and the 4-step recovery script that works on all of them.

## The 4-step recovery script (use this every time)

1. **Read the error out loud.** "It says `AnalysisException: cannot resolve 'amounts' given input columns [amount, region, …]`. So it's a column-name typo." Naming the error is half the fix.
2. **State a hypothesis.** "I'm guessing the column is `amount`, not `amounts`."
3. **Test the smallest possible thing** to confirm. `df.columns` or `df.limit(1).show()`.
4. **Narrate the fix and the side-effects.** "I'll change it to `amount` — and check there aren't other typos in the same cell."

**This sequence — read → hypothesize → test → narrate — is what they're grading.** A clean recovery scores higher than never failing.

---

## The 12 errors you're likely to hit (or cause)

### 1. `AnalysisException: cannot resolve 'foo' given input columns [...]`
**Cause:** column typo, wrong table, or `withColumn` shadowed earlier.
**Fix:** `df.columns` and re-check the name. On Spark Connect (serverless), analysis is deferred to execution, so this might surface late.

### 2. `AnalysisException: Reference 'id' is ambiguous`
**Cause:** both sides of a join have a column called `id` and you used the bare name.
**Fix:** disambiguate. `df.join(other, df.id == other.id)` then `.select(df.id, …)`, or rename one side, or use the **string join**: `df.join(other, "id")` which collapses to one `id` column.

### 3. `Py4JJavaError: ... java.lang.OutOfMemoryError: Java heap space` (driver)
**Cause:** `collect()` / `toPandas()` on a big DataFrame; building a huge in-memory object on the driver.
**Fix:** never `collect()` a full DataFrame. Use `show(n)`, `limit(n).toPandas()`, or `write` to a table. On serverless the driver is shared/limited, so this bites earlier.

### 4. `Py4JJavaError: ... ExecutorLostFailure ... Container killed by YARN for exceeding memory limits`
**Cause:** per-task memory blew up — usually broadcasting something too big, a skewed key, or a UDF building giant intermediate objects.
**Fix:** check the **query profile** for one outlier task (skew). If you `broadcast(...)`'d something, confirm it's actually small. Consider AQE skew handling or salt the hot key.

### 5. `AmbiguousReferenceError` after a self-join
**Cause:** `df.join(df, "id")` makes column references ambiguous.
**Fix:** alias each side: `a = df.alias("a"); b = df.alias("b"); a.join(b, F.col("a.id")==F.col("b.id"))`.

### 6. `PicklingError: Could not serialize object` inside a UDF
**Cause:** a UDF closure captured something that doesn't pickle (a SparkSession, a network connection, a logger).
**Fix:** move heavy setup *inside* the UDF, or — better — drop the UDF for a built-in. See [04 Lever 4](04-Spark-Optimization-Playbook.md).

### 7. `Job aborted ... Total size of serialized results ... is bigger than spark.driver.maxResultSize`
**Cause:** an action that returns too much to the driver, even without `collect()`. Common with `toPandas()` or `take(huge_n)`.
**Fix:** push the work back to the cluster (`write` instead of return), or paginate the read.

### 8. `delta.exceptions.ConcurrentAppendException`
**Cause:** two writers tried to commit to the same Delta table at the same time, touching overlapping files.
**Fix:** add a partition-restriction predicate (`.option("replaceWhere", "date = '...'")`) so each writer only touches its slice; or serialize the writes; or use a queue.

### 9. `Cannot use streaming source with checkpoint X` after a code change
**Cause:** a structured-streaming checkpoint encodes the source schema/options; changing certain things invalidates it.
**Fix:** for compatible changes, restart; for breaking changes, point to a new checkpoint dir and re-bootstrap.

### 10. `org.apache.spark.SparkException: Task not serializable`
**Cause:** a method referenced inside a transformation captured `self` of a non-serializable enclosing class.
**Fix:** pull the values you need into local variables before the lambda. Avoid passing class methods directly into `map`/`filter` lambdas.

### 11. `[CONFIG_NOT_AVAILABLE] ... spark.foo.bar is not available` (on serverless)
**Cause:** trying to `spark.conf.set(...)` a configuration that isn't on the serverless allow-list.
**Fix:** stick to the known-good set (`shuffle.partitions`, `autoBroadcastJoinThreshold`, AQE keys). Make sure the config name actually exists — see [07's hallucination list](07-AI-Stewardship-Genie-Code.md).

### 12. Silent wrongness — no error, but the row count is off
**The most dangerous bug class.** Causes: inner join that should be left, missing null handling, wrong watermark, a typo turning a filter into a tautology (`F.col("x") == "x"` is "always equal to the string 'x'" — that's not what you meant).
**Fix:** **counts before and after.** When you join, `df.count()` should be ≤ left × right; when you `where`, the count should *drop*. Print before/after in critical pipelines.

---

## The shorter "is it weird?" diagnostic checklist

When *something* is off and you don't know what:

- **`df.printSchema()`** — are the types what you expect? `string` vs `timestamp` vs `int` accounts for a shocking fraction of bugs.
- **`df.limit(5).show(truncate=False)`** — what does a real row look like?
- **`df.count()` before and after each join/filter** — did the count change in the direction you expected?
- **`df.explain("formatted")`** — Exchange counts, join strategy, PushedFilters. Are filters reaching the scan?
- **`F.spark_partition_id()` group count** — is one partition dominating? (Skew.)
- **Query Profile** (open from the cell output on Databricks) — where did wall-clock actually go?

## "I'm stuck" — the 2-minute bail-out script

The interview email says you can use a bail-out. After 2 minutes on syntax or an import:

> *"I'm spending more on this than it's worth. Can we skip and I'll come back to it? I want to keep the budget on the harder issue."*

That sentence is point-scoring — it shows prioritization. Time spent debugging a comma is time not spent on the broadcast join.

---

## Quick reference: where the symptom probably lives

| Symptom | First place to look |
|---------|--------------------|
| Whole job slow | Query Profile → biggest stage → biggest task |
| One task slow, rest fine | Skew — `spark_partition_id` distribution |
| OOM on driver | `collect`/`toPandas`/`take(huge)` somewhere |
| OOM on executor | Broadcasting something not-small, or per-task memory in a UDF |
| Wrong count after join | Inner vs left vs right; ambiguous key; NULL handling |
| Wrong sum/avg | Filter applied too late, or duplicates feeding the agg |
| `AnalysisException` at execute time, not parse time | Spark Connect deferred analysis — re-read the column name |
| Hangs with no error | Open Query Profile; check for one straggler task |
