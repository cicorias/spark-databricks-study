---
title: Interview Overview & Strategy
tags: [databricks, interview, strategy, fde]
---

# 01 — Interview Overview & Strategy

[← Back to README](README.md) · Next: [Free Edition Gotchas →](02-Databricks-Free-Edition-Serverless-Gotchas.md)

## What the role actually is

A Forward Deployed Engineer sits *with the customer*. You parachute into a messy environment, understand a half-working pipeline you didn't build, fix it, and **explain the fix in terms the customer's business cares about**. So the interview is less "LeetCode" and more "can I trust this person in front of my biggest account." Every behavior below maps to that.

## The format

- **~25 min Spark Optimization** — a running-but-slow PySpark app with 4–6 improvable areas.
- **~25 min Python feature work** — add a feature to an existing product; read code you didn't write.
- **Open book.** Docs, API references, and **Genie Code** (the in-product AI) are allowed. External AI tools are not.
- **Screen-share + pair.** Treat the interviewer as a teammate or customer.

## The four graded signals (and how to show each)

| Signal | What they're checking | How you visibly demonstrate it |
|--------|----------------------|--------------------------------|
| **Computational Thinking** | Can you decompose a messy problem into a clean sequence of transforms? | Say the plan *before* coding: "First I'll profile, then attack the biggest shuffle, then the skew." |
| **Code Stewardship** | Can you explain what code does under the hood, including code you didn't write? | Read unfamiliar code aloud, restate its intent, name the data contract (inputs/outputs). |
| **AI Stewardship** | Do you prompt Genie well, evaluate its output, and catch when it's wrong? | Ask for a draft, then say "let me audit this" and find the issue. See [07](07-AI-Stewardship-Genie-Code.md). |
| **Resilience** | When you hit a bug, how do you navigate the unknown? | Stay calm, form a hypothesis, test it, narrate. A clean recovery scores *higher* than never failing. |

## The "vibe" — how to behave

1. **Think out loud, constantly.** Narrate your mental model, the tradeoffs, what you expect a cell to do *before* you run it. Dead air is the enemy.
2. **State the plan before the code.** 20–30 seconds of "here's how I'd approach this" buys trust and lets the interviewer redirect early if you're off.
3. **Ask clarifying questions.** "Is this table fact or dimension? Roughly how big? Is it run once or hourly?" These are exactly the questions an FDE asks a customer, and they change the right answer.
4. **Customer-translate everything.** After a fix, give the business version: "This broadcast join avoids shuffling 40M rows across the cluster — it'll cut the nightly job from ~40 min to a few minutes, so the dashboard is ready before the team logs in."
5. **You're the pilot, AI is the copilot.** Use Genie to go faster, but **audit every line.** Own the final answer; be ready to defend each line whether you wrote it or it did.
6. **Use the Bail Out.** If you're stuck >2 minutes on syntax/an import, say so out loud and ask to move on. They explicitly offer this. Burning time on a comma signals poor prioritization.

## Time management (don't blow the budget)

Each segment is only ~25 minutes. A rough internal clock:

- **Spark (25 min):** ~3 min profile/understand → ~3 min state the plan and rank issues by impact → ~15 min implement fixes biggest-first → ~4 min summarize wins + business translation.
- **Python (25 min):** ~5 min read & restate the existing code and the data contract → ~3 min plan the feature & edge cases → ~13 min implement + a quick test → ~4 min walk through what you did and what you'd add with more time.

Always **bank a partial win**: get *something* working end-to-end before polishing. A running 80% solution beats a perfect-but-unfinished one.

## Things to say that score points

- "Before I optimize, let me look at the **query profile** to see where the time actually goes — I don't want to guess." (See [02](02-Databricks-Free-Edition-Serverless-Gotchas.md) — on serverless the Spark UI isn't available, the query profile is.)
- "This is a wide transformation, so it triggers a shuffle — that's almost always the most expensive thing here, so I'll start there."
- "Let me check the assumptions: is the right side small enough to broadcast? If it's under ~10–30 MB, yes."
- "I'll write to a Delta table here rather than recompute, because this DataFrame is reused three times downstream."
- "Genie suggested `.cache()` — but we're on serverless where that's restricted, so I'll materialize to Delta instead." *(This single sentence demonstrates three signals at once.)*

## Things that lose points

- Silence while typing.
- Reaching for Genie before you understand the problem.
- Pasting Genie's output without reading it.
- Optimizing the cheap thing (a tiny filter) while ignoring the giant shuffle.
- Premature `collect()` / `toPandas()` that pulls everything to the driver.
- Defending code you can't explain.

## A pocket script for the first 60 seconds of each segment

> "Okay — let me read this end to end first and tell you what I think it's doing… [read aloud]. So the intent is X, inputs are Y, output is Z. My hunch for the biggest cost is the shuffle in the join. Before I touch anything I'll confirm with the query profile. Sound good?"

That opening alone signals computational thinking, code stewardship, and customer collaboration.
