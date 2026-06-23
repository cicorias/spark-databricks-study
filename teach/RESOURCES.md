# Databricks FDE Interview Resources

## Knowledge

- [Databricks Free Edition](https://www.databricks.com/try-databricks)
  The actual interview environment. Sign up and use before the interview. Use for: cluster familiarity, Genie Code practice, notebook workflow.

- [PySpark 3.5 API Reference](https://spark.apache.org/docs/3.5.0/api/python/)
  Authoritative. Use for: confirming exact method signatures, checking join strategies, aggregation functions.

- [Delta Lake Documentation](https://docs.delta.io/latest/index.html)
  Use for: MERGE syntax, OPTIMIZE, VACUUM, time travel, Liquid Clustering — all Phase-2 likely topics.

- [Databricks Spark SQL Guide](https://docs.databricks.com/spark/latest/spark-sql/index.html)
  Use for: SQL functions, window functions, query hints (BROADCAST).

- [Apache Spark Performance Tuning Guide](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
  Use for: AQE settings, broadcast threshold, shuffle partition tuning.

- [Databricks Best Practices: Spark Performance](https://docs.databricks.com/optimizations/index.html)
  Use for: Photon, caching, Z-ordering — Databricks-specific optimizations.

### In-repo reference notes (high-trust, purpose-built)

- `03-Spark-Mental-Models.md` — Lazy eval, partitions, narrow/wide, jobs/stages/tasks, Catalyst, AQE
- `04-Spark-Optimization-Playbook.md` — The 6 levers: broadcast, pushdown, caching, UDF avoidance, skew, partitioning
- `12-Delta-Lake-Deep-Dive.md` — MERGE / SCD2 / OPTIMIZE / time travel / Liquid Clustering
- `14-Data-Engineering-Patterns.md` — Medallion, idempotent loads, CDC, SCD, watermarks
- `07-AI-Stewardship-Genie-Code.md` — Prompting, hallucination audit checklist
- `15-Common-Spark-Errors-Debug.md` — 12-error playbook + 4-step recovery script

## Wisdom (Communities)

- [Databricks Community Forum](https://community.databricks.com/)
  High-signal, often answered by Databricks employees. Use for: weird Free Edition behavior, MERGE edge cases.

- [r/apachespark](https://reddit.com/r/apachespark)
  Use for: performance war stories, partition sizing heuristics.

- [Stack Overflow — pyspark tag](https://stackoverflow.com/questions/tagged/pyspark)
  Use for: specific API questions during practice.

## Gaps

- No high-quality free resource for "think-out-loud" pair programming practice (wisdom only comes from actually doing it)
- Genie Code hallucination examples: need to build these from own practice sessions
