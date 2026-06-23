---
title: Self-Assessment Rubric
tags: [rubric, self-assessment, readiness, fde]
---

# 19 — Self-Assessment Rubric

[← Timed Speed Drills](18-Timed-Speed-Drills.md) · [Back to README](README.md)

> Use this to grade your **dress-rehearsal** lap of the optimization + Python challenges. Score honestly. The interview rubric is heavy on **how you think** — not just whether the code runs.

## How to use it

1. Record yourself doing [`05`](05-Spark-Optimization-Challenge.md) and [`06`](06-Python-Feature-Dev-Challenge.md) end-to-end (25 min each). Screen + audio.
2. Watch it back **at 1.25×** with this rubric open.
3. Tally the scores. Anything below 3/5 in a single signal → drill the relevant note before the real interview.

## The four graded signals (from the interview email)

| Signal | What they're looking for |
|--------|--------------------------|
| **Computational Thinking** | Decompose messy problems into a clean sequence of transforms; pick the right tool; reason about scale |
| **Code Stewardship** | Read unfamiliar code aloud; restate its intent; extend it the way it wants to be extended; defend every line |
| **AI Stewardship** | Use Genie fast, audit its output visibly, catch hallucinations, own the result |
| **Resilience** | When a bug hits: stay calm, hypothesize, test, narrate, recover |

---

## Spark Optimization (25 min) — score 0–5 per row

| # | Behavior | 0 | 3 | 5 |
|---|----------|---|---|---|
| 1 | **Stated a plan** in the first 60 seconds before touching code | Dove straight into edits | Mentioned 1–2 hunches | Ranked issues by impact, named the diagnostic order |
| 2 | **Used `explain` / Query Profile** before optimizing | Guessed | Ran one explain | Read the plan aloud, named `Exchange` / join strategy, *then* changed code |
| 3 | **Found the broadcast-join opportunity** | Missed | Broadcast eventually | Spotted small dim immediately; confirmed in plan that the Exchange disappeared |
| 4 | **Pushed filters / pruned columns early** | Left untouched | Did one of the two | Moved filter to source AND pruned columns; explained partition pruning |
| 5 | **Removed the Python UDF** | Kept it | Replaced with built-in | Replaced AND said why (Catalyst/Photon, vectorization) |
| 6 | **Killed `collect()` / driver pulls** | Kept it | Replaced with `show` | Replaced AND named when `collect` IS safe |
| 7 | **Called out the serverless constraint** (no `.cache()`) | Suggested `.cache()` | Caught self mid-suggestion | Volunteered "materialize to Delta" without prompting |
| 8 | **Customer-translated** each fix | None | One translation | One per fix, in business units (minutes/dollars/customers) |
| 9 | **Narrated continuously** — no >10s silences | Frequent dead air | Occasional silences | Continuous thinking aloud, even during typing |
| 10 | **Closed with the scale story** ("what if it's 20B rows?") | Didn't | Partial | Named the levers that scale (broadcast, partition pruning, AQE, approx_count_distinct) |

**Score:** ____ / 50 → readiness: **40+** strong / **30–39** ready with practice / **<30** drill [04](04-Spark-Optimization-Playbook.md) and redo [05](05-Spark-Optimization-Challenge.md)

---

## Python Feature Dev (25 min) — score 0–5 per row

| # | Behavior | 0 | 3 | 5 |
|---|----------|---|---|---|
| 1 | **Found the entry point** before changing anything | Dove in | Identified vaguely | Named the function/class, traced inputs/outputs aloud |
| 2 | **Stated the data contract** (types, shapes, invariants) | Skipped | Mentioned types | Stated invariants and what could go wrong if violated |
| 3 | **Named the extension point** before extending | Bolted on a special case | Subclassed correctly | Said "this is a strategy pattern; the right move is a subclass" |
| 4 | **Wrote a quick test / assertion** | None | One assert | Test that proves both the happy path AND an edge case |
| 5 | **Preserved the data contract** in the change | Broke it | Mostly kept it | Kept it AND said so explicitly |
| 6 | **Handled an edge case** (None, empty, duplicate, exception) | Ignored | Handled one | Enumerated several, chose a defensible policy |
| 7 | **Resilience: a rule/operator that raises doesn't crash the run** | Hadn't thought about it | Fixed when prompted | Brought it up unprompted as "the existing code has a robustness gap" |
| 8 | **Connected the feature to scale / Spark** | No mention | Brief mention | Showed the equivalent distributed query in PySpark |
| 9 | **Audited Genie's output** (if used) | Pasted as-is | Read it | Read line-by-line, flagged at least one issue (real or potential) |
| 10 | **Closed by summarizing the change + what's next** | Just stopped | Summarized | Summarized + named what you'd do with more time |

**Score:** ____ / 50 → readiness: **40+** strong / **30–39** ready with practice / **<30** drill [06](06-Python-Feature-Dev-Challenge.md) and [17](17-Additional-Python-Challenges.md)

---

## Cross-cutting signals

| Behavior | Y / N | Notes |
|----------|-------|-------|
| Asked **at least one clarifying question** like an FDE would ask a customer ("hourly or nightly?") | | |
| **Bailed out** when stuck >2 min on a comma/import instead of burning the clock | | |
| **Acknowledged** when Genie/you were wrong and recovered cleanly | | |
| Avoided **defending code you couldn't explain** | | |
| Closed each segment with a **one-sentence business translation** | | |

---

## The two questions to ask yourself after every dress rehearsal

1. **"If I were the interviewer, would I want this person sitting next to my biggest customer next week?"**
   If the honest answer is *not yet*, what specific behavior made the answer wobble?
2. **"What's the single biggest thing I'd change about my next attempt?"**
   Drill that one thing deliberately tomorrow. Not five things — one.

---

## Readiness check — the 5 things every offer-grade candidate does without thinking

1. Opens with **"let me read this end to end first"** before touching anything.
2. Runs `explain("formatted")` / opens Query Profile *before* changing code.
3. Says **"on serverless I'd materialize to Delta"** instead of `.cache()`, unprompted.
4. **Customer-translates** every fix into time or money in one sentence.
5. **Narrates continuously**, including the moments when they don't yet know the answer.

If all 5 are reflexive, you're ready. If 1–2 are still effortful, drill the relevant notes this week. If 3+ feel forced, give yourself one more pass through [01](01-Interview-Overview-and-Strategy.md) and the dress rehearsal.
