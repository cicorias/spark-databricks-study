# Mission: Databricks FDE Coding Interview

## Session Structure

Two timed phases, back-to-back:

1. **Phase 1 — Python feature work (~25 min):** Digest and understand an existing customer codebase. Debug as required. Add a new feature. Be prepared to explain how the code executes and scales. AI tools (Genie Code) are permitted — but you are the pilot; audit every output.

2. **Phase 2 — Spark Optimization (~25 min):** Given a running but poor-performing PySpark application. Find and fix **4–6 areas** that can be optimized. Explain the reasoning behind every improvement. Ask clarifying questions. Be ready to discuss alternatives.

## Why

Pass the Databricks Forward Deployed Engineer (FDE) 60-minute coding interview. The session is pair-programming with real FDEs as "teammates/customers" on Databricks Free Edition. Success means being hired into an FDE role where you solve customer data engineering problems daily with Spark and Python.

## Success looks like

- Fix a slow PySpark application — find and explain 4–6 real performance issues in ~25 minutes while narrating every decision out loud
- Add a feature to an existing Python codebase you've never seen before, in ~25 minutes, reasoning about it in business terms
- Use the Databricks Assistant (Genie Code) effectively: good prompts, catch hallucinations, audit output before accepting it
- Explain *why* any piece of code runs the way it does at scale — jobs, stages, tasks, shuffles, partitions — in plain language a non-engineer can follow
- Recover gracefully from bugs and logic errors: narrate, diagnose, fix, move on

## Constraints

- Interview is imminent (prep window: days, not weeks)
- Environment is Databricks Free Edition (serverless, no Spark UI full access, no RDD, Python 3.12 / Spark 3.5)
- Open-book: Databricks docs + Genie Code allowed; ChatGPT/Claude/Cursor NOT allowed during the interview
- Must commit progress files to git so study can continue across machines

## Out of scope

- Spark Streaming / Structured Streaming deep dives (not on the interview)
- MLflow, Unity Catalog administration, Terraform for Databricks
- Full Stack spike path (user is on Data Engineering track)
- Spark internals below the Catalyst/Tungsten layer
