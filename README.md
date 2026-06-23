---
title: Databricks FDE Interview Prep
tags: [databricks, spark, pyspark, interview, fde]
created: 2026-06-09
---

# Databricks Forward Deployed Engineer — Coding Interview Prep

A self-contained study vault for the Databricks **Forward Deployed Engineer (FDE)** coding round. Built to be read in [Obsidian](https://obsidian.md) and version-controlled on GitHub. All cross-links are standard relative Markdown links, so they render in both.

> If you prefer Obsidian wikilinks, you can convert `[text](01-file.md)` → `[[01-file]]` with a find-and-replace, but it isn't necessary.

## The interview in one paragraph

A 60-minute **pair-programming** session with one or two FDEs acting as teammates/customers. It's **open-book** (docs, API references, and the in-product AI assistant **Genie Code** are allowed; external AI tools like ChatGPT/Cursor are not). Everything runs on **Databricks Free Edition** (serverless). The two graded segments per your scheduling email:

1. **~25 min — Spark Optimization.** You're given a *running but slow* PySpark app with **4–6** things to improve. Fix them and *explain why*.
2. **~25 min — Python feature work.** Add a feature to an existing Python product; read and reason about code you didn't write.

They grade four signals: **Computational Thinking, Code Stewardship, AI Stewardship, Resilience.** The single most important behavior is **thinking out loud.**

## How to use this vault

Three layers, drilled in order:

1. **Reading notes** (01–04, 07, 12, 14, 15) — mental models you need to *speak* fluently.
2. **Hands-on labs** (05, 06, 10–11, 16–17 + `notebooks/`) — runnable code on Databricks **and** locally.
3. **Drills + self-assessment** (08, 09, 13, 18, 19) — rehearsal under the clock.

| # | Note | Type | Use it for |
|---|------|------|-----------|
| 01 | [Interview Overview & Strategy](01-Interview-Overview-and-Strategy.md) | Read | What's graded, the "vibe", time management, talking out loud |
| 02 | [Free Edition / Serverless Gotchas](02-Databricks-Free-Edition-Serverless-Gotchas.md) | Read + Setup | The constraints that trip people up live (cache, Spark UI, RDD) |
| 03 | [Spark Mental Models](03-Spark-Mental-Models.md) | Read | "Explain how your code executes and scales" |
| 04 | [Spark Optimization Playbook](04-Spark-Optimization-Playbook.md) | Read + Code | The 6 levers, before/after, business translation |
| 05 | [Spark Optimization Challenge](05-Spark-Optimization-Challenge.md) | Hands-on (timed) | A full slow app to fix; the original 25-min drill |
| 06 | [Python Feature-Dev Challenge](06-Python-Feature-Dev-Challenge.md) | Hands-on (timed) | Read existing code, add features, reference solutions |
| 07 | [AI Stewardship — Genie Code](07-AI-Stewardship-Genie-Code.md) | Read + Drill | Prompt well, catch hallucinations, audit checklist |
| 08 | [Mock Q&A & Talking Points](08-Mock-QA-and-Talking-Points.md) | Drill | Rapid-fire questions with answers |
| 09 | [PySpark Cheatsheet](09-Cheatsheet-PySpark.md) | Reference | Snippets you'll actually type |
| 10 | [Hands-On Lab Index](10-Hands-On-Lab-Index.md) | Nav | All the runnable notebooks |
| 11 | [Local Dev Setup (mise + uv + Databricks CLI)](11-Local-PySpark-Setup-uv.md) | Setup | `mise install` → daily local practice |
| 12 | [Delta Lake Deep Dive](12-Delta-Lake-Deep-Dive.md) | Read | MERGE / SCD2 / OPTIMIZE / time travel / Liquid Clustering |
| 13 | [Spark SQL Drills](13-Spark-SQL-Drills.md) | Drill | 10 SQL warm-ups, SQL + DataFrame answers |
| 14 | [Data Engineering Patterns](14-Data-Engineering-Patterns.md) | Read | Medallion, idempotent loads, CDC, SCD, watermarks |
| 15 | [Common Spark Errors & Debugging](15-Common-Spark-Errors-Debug.md) | Read | 12-error playbook + the 4-step recovery script |
| 16 | [Additional Spark Challenges](16-Additional-Spark-Challenges.md) | Hands-on (timed) | 2 more slow apps — skew/window + incremental MERGE |
| 17 | [Additional Python Challenges](17-Additional-Python-Challenges.md) | Hands-on (timed) | Stream pipeline + retry/backoff decorator |
| 18 | [Timed Speed Drills](18-Timed-Speed-Drills.md) | Drill | 5/10/15-min warm-ups for syntax + scenarios |
| 19 | [Self-Assessment Rubric](19-Self-Assessment-Rubric.md) | Score yourself | Grade your dress rehearsal |

## Runnable notebooks

Two parallel tracks (full index in [10](10-Hands-On-Lab-Index.md)):

```
notebooks/
├── databricks/                    # .py with `# COMMAND ----------` markers — import to Free Edition
│   ├── 01_environment_check.py
│   ├── 02_spark_fundamentals.py
│   ├── 03_optimization_challenge_start.py   ← the 25-min timed lab
│   ├── 04_optimization_challenge_solution.py
│   ├── 05_broadcast_and_skew.py
│   ├── 06_delta_merge_scd.py
│   ├── 07_window_functions.py
│   └── 08_dq_engine_python.py               ← the Python 25-min timed lab
└── local/                         # jupytext percent format — open in VS Code or convert with `mise run to-ipynb`
    ├── 01_local_warmup.py
    ├── 02_local_optimization.py
    └── 03_local_dq_engine.py
```

## Getting started in 5 minutes

```bash
# Tools (one-time per machine)
curl https://mise.run | sh                   # install mise
echo 'eval "$(mise activate bash)"' >> ~/.bashrc && exec bash

# Project (one-time per clone)
git clone <this repo> && cd spark-databricks-study
mise trust && mise install                    # installs Python 3.12, JDK 17, uv, databricks-cli
mise run setup                                # uv sync — installs PySpark, Delta, Jupyter
mise run smoke                                # verify local PySpark works

# Databricks side (when you're ready)
mise run init-env                             # interactive .env setup (host, profile, dest)
mise run db:login                             # OAuth into Free Edition
mise run db:import                            # push notebooks/databricks/ into your workspace
```

Versions match **Databricks Serverless Environment v4** (Python 3.12.3, JDK 17, PySpark 3.5.x). See [11](11-Local-PySpark-Setup-uv.md).

## Guided study — interactive lessons

A step-by-step teaching system lives in `teach/`. Progress is tracked via git-committed learning records so you can stop on one machine and resume on another.

```bash
# First time on a machine
git clone <this repo> && cd spark-databricks-study
mise trust && mise install && mise run setup  # same as above

# Start (or resume) studying
mise run teach:status          # see all 19 lessons: ⬜ not started / 🟡 started / ✅ complete
mise run teach:next            # open the next incomplete lesson in your browser
```

```bash
# After finishing a lesson
mise run teach:done 01         # writes a learning record marking lesson 01 complete
git add teach/ && git commit -m "complete lesson 01" && git push

# Picking up on a different machine
git pull
mise run teach:status          # see where you left off
mise run teach:next            # jump back in
```

```bash
# Other commands
mise run teach:lesson 06       # (re)open any specific lesson by number
mise run teach:restart 01      # reset a lesson to not-started so you can replay it
```

The 19 lessons follow the interview's two phases: Spark mental models + optimization (Phase 2) first, then Python feature-dev and AI stewardship (Phase 1). See [`teach/CURRICULUM.md`](teach/CURRICULUM.md) for the full map and a priority list if prep time is short.

## Suggested 5–7 day plan

- **Day 1 — Orient.** Read 01 and 02. Run `mise install && mise run setup && mise run smoke`. Sign up for Free Edition. Open `notebooks/databricks/01_environment_check.py` in the workspace.
- **Day 2 — Internals.** Read 03. Run `notebooks/local/01_local_warmup.py`. Whiteboard *transformation vs action*, *narrow vs wide*, *jobs→stages→tasks*.
- **Day 3 — Levers + Delta.** Read 04 and 12. Run `notebooks/databricks/05_broadcast_and_skew.py` and `06_delta_merge_scd.py`.
- **Day 4 — Spark hands-on.** Do 05 with a 25-minute timer (`notebooks/databricks/03_…_start.py`). Don't peek at the solution until you've found at least 4 issues. Optionally do one of the [16](16-Additional-Spark-Challenges.md) follow-ups.
- **Day 5 — Python hands-on.** Do 06 with a 25-minute timer. Optionally do one [17](17-Additional-Python-Challenges.md) follow-up.
- **Day 6 — AI + drills.** Read 07, run the hallucination drill. Then 08 and 13 and 18 rapid-fire out loud.
- **Day 7 — Dress rehearsal.** Redo 05 and 06 end-to-end while screen-recording. Watch back with [19](19-Self-Assessment-Rubric.md) and score yourself.

## The one rule that beats everything

> **Narrate.** A correct answer delivered in silence scores worse than a slightly-wrong answer where you reason out loud, catch yourself, and recover. They are hiring someone to sit next to a customer and think in the open. Show them that person.


## Interview Overview (my understanding of the format)

A roughly 60-minute pair-programming session intended to mirror the day-to-day work
of a Forward Deployed Engineer (FDE): solving realistic customer problems with code,
on the Databricks platform. The emphasis is on first-principles thinking and an
AI-builder mindset rather than memorized syntax.

### The "vibe"
- Treat it as collaborative pair programming — the interviewer is a teammate/customer.
- Open-book: docs and the in-product Databricks Assistant are fair game.
- Think out loud — narrate your mental model and trade-offs constantly.
- Customer-translate: explain your code in business terms a stakeholder would follow.

### Setup & environment
- Runs on Databricks Free Edition (serverless).
- You join a shared workspace and screen-share while working.
- Sign up for a Free Edition account and get comfortable with notebooks, running
  Python/SQL cells, and the built-in Assistant ahead of time.
- Keep code clean, structured, commented, and reproducible.
- Only the built-in Databricks Assistant is allowed — no external AI tools
  (Claude, Cursor, ChatGPT). API docs are fine.
- Use only public or self-generated data — never sensitive/proprietary data.

### What to expect
Two timed phases, back-to-back:

**Phase 1 — Python feature work**
- Digest and understand an unfamiliar customer codebase, debug as needed, add a feature.
- The Assistant is allowed, but *you are the pilot* — audit its output and catch
  hallucinations or inefficiencies.
- Be ready to discuss how the code executes and scales.

**Phase 2 — Spark optimization**
- You're handed a running but poorly performing Spark application.
- Find and fix performance issues and explain the reasoning behind each.
- Ask clarifying questions; be ready to discuss alternatives.

### How to prepare
- Brush up on Python and your common tooling.
- Sign up for Databricks Free Edition and experiment with the Assistant.
- Practice: spend ~50–60 min on a small coding exercise in a Databricks notebook
  on a dataset of your choice, using the Assistant — the closest analog to the real thing.

### Evaluation signals
- **Computational Thinking** — decompose a messy problem into a clean sequence of transforms.
- **Code Stewardship** — write working code *and* explain what it does under the hood;
  read and reason about code you didn't write.
- **AI Stewardship** — prompt the Assistant well, evaluate its output, catch when it's wrong.
- **Resilience** — when you hit a bug, navigate the unknown calmly and methodically.

### Final tips
- Don't drown in syntax — if you're stuck >2 min on an import or a comma, say so and use
  the offered "bail out" to keep moving. This tests architecture, not typing speed.
- Own the final answer — whether hand-written or Assistant-generated, be ready to defend
  every line.