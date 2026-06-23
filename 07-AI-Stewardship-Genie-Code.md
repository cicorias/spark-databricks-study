---
title: AI Stewardship — Genie Code
tags: [databricks, genie, ai, prompting, hallucinations]
---

# 07 — AI Stewardship: Using Genie Code Well

[← Python Feature-Dev](06-Python-Feature-Dev-Challenge.md) · Next: [Mock Q&A →](08-Mock-QA-and-Talking-Points.md)

> *"You are the pilot. You must audit the AI's output. If it hallucinates or writes inefficient code, we want to see you identify and correct it."* This is a **graded signal of its own** (AI Stewardship). The win condition isn't "don't use AI" or "use AI for everything" — it's **use it fast, then visibly catch its mistakes.**

## What Genie Code is (2026)

As of March 2026, the in-product **Databricks Assistant became Genie Code** — a context-aware AI assistant/agent that generates and runs code, explains and debugs it, and uses **Unity Catalog metadata** (your tables, columns, lineage) for grounding. It runs in notebooks, the SQL editor, and more, in two modes:

- **Chat mode** — quick Q&A and code generation. *Use this in the interview* — it's predictable and you stay in control.
- **Agent mode** — autonomous multi-step work that can run, edit, and modify things with your permission. Powerful, but harder to audit live; if it's on, watch what it proposes and approve step by step.

Open it from the icon in the top-right of the workspace; type prompts or `/` slash commands.

## How to prompt it well (so the output is auditable)

Good prompts are **specific, scoped, and constraint-bearing**:

- **State the goal + the constraints:** *"Rewrite this join to broadcast the small dimension table. We're on serverless, so don't use `.cache()` or `sparkContext`."*
- **Give it the schema/context:** mention the table and columns, or let it read the UC metadata.
- **Ask for an explanation, not just code:** *"Explain why this is faster"* — then you can verify the reasoning, and you'll have the customer-translation ready.
- **Iterate in small steps:** one transformation at a time beats "optimize this whole notebook," which is hard to audit.
- **Use it to explain unfamiliar code:** *"Explain what this function does and its inputs/outputs"* is a legitimate, fast way to build the data contract in Phase 1.

> Narrate your prompting: *"I'll ask Genie for a first pass, then I'll check the join strategy myself in the plan."* That sentence alone shows the grader you're the pilot.

## The audit checklist (run this on every Genie output)

1. **Does it run?** Run it on a small slice first.
2. **Is it correct?** Does the result match what you expected? Spot-check counts/rows.
3. **Is it efficient?** Did it add a needless shuffle, a Python UDF, a `collect()`, a `distinct()`?
4. **Is it serverless-legal?** No `sparkContext` / RDDs / `.cache()` / unsupported configs. (See [02](02-Databricks-Free-Edition-Serverless-Gotchas.md).)
5. **Are the APIs real?** Function names, parameters, config keys — do they actually exist?
6. **Did it preserve the contract?** In Phase 1, did it keep inputs/outputs and the extension points, or quietly rewrite working code?

## Common hallucinations & mistakes to catch (the catalog)

| Category | What the AI does | How you catch it |
|----------|------------------|------------------|
| **Made-up config keys** | Invents `spark.sql.fastJoin.enabled` or similar | If you don't recognize a conf, doubt it; check docs. Real skew/AQE keys: `spark.sql.adaptive.enabled`, `spark.sql.adaptive.skewJoin.enabled`, `spark.sql.autoBroadcastJoinThreshold`, `spark.sql.shuffle.partitions` |
| **Serverless-illegal code** | Suggests `df.cache()`, `spark.sparkContext`, `df.rdd.getNumPartitions()` | You know these fail on Free Edition — call it out and use the Delta/`spark_partition_id` alternative |
| **Wrong function signature** | `F.broadcast` with extra args, or a param that doesn't exist | Hover/check the API; run it on a tiny input |
| **Deprecated / wrong API** | `registerTempTable` (old), `sqlContext.*`, RDD methods | Use current equivalents (`createOrReplaceTempView`, `spark.sql`) |
| **Plausible-but-wrong logic** | Off-by-one, wrong join key, inner vs left join changes row counts | Check row counts before/after; read the join type |
| **Inefficiency dressed as a fix** | Adds `.distinct()` "to be safe", `orderBy` you don't need, or a UDF | Ask "does this add a shuffle? do we need it?" |
| **Over-eager rewrite** | Replaces a whole working function instead of adding the feature | Diff against the original; keep the contract |
| **Fake citations / table names** | References a column or table that isn't there | Cross-check against the actual schema |

## The 10-second audit script (say it out loud)

> *"Okay, Genie gave me this. Before I trust it: does it run on a small slice? Does the row count look right? Any shuffle or UDF it didn't need? Anything serverless won't allow? … This `.cache()` won't work here — I'll materialize to Delta instead."*

## Practice drill (do this in a notebook)

Paste this *deliberately flawed* "AI-suggested" optimization and find everything wrong before reading the answers:

```python
# "Genie's optimization" — audit it
sc = spark.sparkContext                                   # ?
df = spark.table("workspace.default.orders")
n = df.rdd.getNumPartitions()                             # ?
df = df.cache()                                           # ?
spark.conf.set("spark.sql.turboJoin.enabled", "true")     # ?
from pyspark.sql.functions import udf
clean = udf(lambda s: s.strip())                          # ?
df = df.withColumn("cat", clean("raw_category"))
result = df.join(spark.table("workspace.default.products"), "product_id")  # ?
rows = result.collect()                                   # ?
```

<details><summary>What's wrong (7 issues)</summary>

1. `spark.sparkContext` — **not available** on serverless.
2. `df.rdd.getNumPartitions()` — RDD path, **not available**; use `spark_partition_id()` grouping.
3. `df.cache()` — cache APIs **restricted** on serverless; materialize to Delta instead.
4. `spark.sql.turboJoin.enabled` — **made-up config**; doesn't exist.
5. Python `udf` for `strip()` — use `F.trim(...)`; the UDF is slow and blocks optimization. (Also missing a return type.)
6. The join to `products` (5k rows) should be a **`broadcast`** join, and is only needed if the output uses product columns.
7. `result.collect()` pulls everything to the **driver** — use `show`/`write`.
</details>

## Bottom line

Treat Genie like a fast, confident junior engineer: great for a first draft and for explaining unfamiliar code, but **everything it produces is your responsibility.** The candidates who score highest *use it visibly* and *correct it visibly*.
