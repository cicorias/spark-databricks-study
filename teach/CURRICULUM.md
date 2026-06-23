# Interview Prep Curriculum

Progress is tracked via `learning-records/`. A lesson is:
- **not started** — no learning record for that lesson number
- **started** — learning record exists with `Status: in-progress`
- **complete** — learning record exists (no `in-progress` status)

Run `mise run teach:status` to see current state.
Run `mise run teach:next` to open the next lesson.
Run `mise run teach:lesson <N>` to (re)open any lesson.
Run `mise run teach:done <N>` to mark a lesson complete.

---

## Lesson Map

| # | Lesson | Source Note | Grading Signal |
|---|--------|------------|----------------|
| 01 | Lazy Eval & Actions — why Spark doesn't run until it has to | 03-Spark-Mental-Models | Computational Thinking |
| 02 | Partitions & Parallelism — the unit of work | 03-Spark-Mental-Models | Computational Thinking |
| 03 | Narrow vs Wide Transforms & The Shuffle | 03-Spark-Mental-Models | Computational Thinking |
| 04 | Jobs → Stages → Tasks — reading an execution plan | 03-Spark-Mental-Models | Computational Thinking |
| 05 | Catalyst & AQE — the optimizer and its limits | 03-Spark-Mental-Models | Computational Thinking |
| 06 | Broadcast Joins — eliminate the big-side shuffle | 04-Spark-Optimization-Playbook | Code Stewardship |
| 07 | Filter & Project Early — predicate & projection pushdown | 04-Spark-Optimization-Playbook | Code Stewardship |
| 08 | Caching — when to persist, when to skip it | 04-Spark-Optimization-Playbook | Code Stewardship |
| 09 | UDF Avoidance — built-ins, Pandas UDFs, and Photon | 04-Spark-Optimization-Playbook | Code Stewardship |
| 10 | Skew Handling — salting and AQE skew split | 04-Spark-Optimization-Playbook | Code Stewardship |
| 11 | Repartition vs Coalesce — controlling partition count | 04-Spark-Optimization-Playbook | Code Stewardship |
| 12 | Delta Lake Fundamentals — ACID, time travel, schema enforcement | 12-Delta-Lake-Deep-Dive | Code Stewardship |
| 13 | MERGE & SCD2 — upserts and slowly-changing dimensions | 12-Delta-Lake-Deep-Dive | Code Stewardship |
| 14 | OPTIMIZE, VACUUM & Liquid Clustering | 12-Delta-Lake-Deep-Dive | Code Stewardship |
| 15 | Medallion Architecture & Idempotent Loads | 14-Data-Engineering-Patterns | Computational Thinking |
| 16 | Reading Code You Didn't Write — Python feature-dev strategy | 06-Python-Feature-Dev-Challenge | Code Stewardship |
| 17 | Genie Code Prompting & Hallucination Audits | 07-AI-Stewardship-Genie-Code | AI Stewardship |
| 18 | Common Spark Errors & the 4-Step Recovery Script | 15-Common-Spark-Errors-Debug | Resilience |
| 19 | Narrating Out Loud — the meta-skill that beats everything | 01-Interview-Overview-and-Strategy | All four signals |

---

## Priority Order (time-constrained prep)

If you have **≤ 3 days**: do lessons 01–05 (mental models), 06–07 (top optimizations), 12–13 (Delta), 17 (AI stewardship), 19 (narration).

If you have **≤ 5 days**: add 08–11 (remaining levers), 15 (medallion), 16 (reading code), 18 (error recovery).

Full run: all 19 lessons in order.
