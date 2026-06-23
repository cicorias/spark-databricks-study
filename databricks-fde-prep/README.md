# Databricks FDE Interview Prep

Study materials for the Databricks **Forward Deployed Engineer** pair-programming interview: a 60-minute, open-book, think-out-loud session in Databricks Free Edition. Two graded phases — (1) read/debug an unfamiliar customer PySpark codebase with the Databricks Assistant, and (2) optimize a running-but-slow Spark job and justify the changes. Graded on Computational Thinking, Code Stewardship, AI Stewardship, and Resilience.

## Contents

| File | What it is |
| --- | --- |
| `PRIORITIES.md` | The tiered prep plan — what to study first and why, mapped to the two interview phases. |
| `STUDY-PACK.md` | Drills (active motions), flash cards (recall), and think-out-loud scenarios. |
| `notebooks/slow_spark_app_BROKEN.py` | The slow app, anti-patterns only, with TODOs. Practice fixing it cold. |
| `notebooks/slow_spark_app_SOLUTION.py` | The fixes + example outputs + out-loud explanations and customer one-liners. |

## How to use

1. Sign up for [Databricks Free Edition](https://www.databricks.com/learn/free-edition).
2. Import `notebooks/slow_spark_app_BROKEN.py` (Workspace → Import → File), attach serverless or a DBR 13.3+ cluster, and practice fixing each section cold while watching the Query Profile / Spark UI. Check yourself against `slow_spark_app_SOLUTION.py`.
3. Work `STUDY-PACK.md` daily — run the ❌ flash cards until solid, rehearse scenarios out loud.
4. Follow the one-week plan in `PRIORITIES.md`.
5. For more hands-on reps: [jrlasak/databricks-code-practice](https://github.com/jrlasak/databricks-code-practice) (104 exercises) — do each yourself *before* asking the Assistant, then diff.

## The two things to recite the morning of

- **Optimization framework:** read less data → move less data → handle skew → reuse work → right-size parallelism → avoid Python UDFs (always verify in the UI).
- **"What breaks at 1 TB?"** collect-to-driver, skew, shuffle blow-up, tiny files, Python UDF cost.

---
*Personal study notes; not affiliated with or endorsed by Databricks, Inc.*
