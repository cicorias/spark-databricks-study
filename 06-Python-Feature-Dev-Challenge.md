---
title: Python Feature-Dev Challenge
tags: [python, challenge, hands-on, code-reading]
---

# 06 — Python Feature-Dev Challenge (hands-on)

[← Spark Optimization Challenge](05-Spark-Optimization-Challenge.md) · Next: [AI Stewardship →](07-AI-Stewardship-Genie-Code.md)

> Phase 1 is *"add a feature on an existing product"* and *"digest and understand the customer code base."* The skill being graded is **code stewardship**: read code you didn't write, restate its intent, then extend it cleanly without breaking it. This challenge gives you a small but real codebase and a tiered set of features to add.

## How to read unfamiliar code (do this out loud, every time)

1. **Find the entry point** — what does a caller actually invoke? (`run`, `main`, the public method.)
2. **Trace the data contract** — what goes in, what comes out, what shape? Name the types.
3. **Name the extension points** — where is this *designed* to be extended? (base classes, registries, callbacks.)
4. **Spot the invariants** — what must stay true? (e.g., "every rule returns a result; results never raise.")
5. **Only then change it** — add the feature *the way the code wants to be extended*, not by bolting on a special case.

Saying "let me restate what this does before I touch it" is itself a point-scoring move.

---

## The existing product: a lightweight data-validation engine

A customer uses this to validate batches of records (e.g., rows read from a file) before loading them into their warehouse. It's plain Python so it's portable and testable. Read it, then do the tasks.

```python
# dq.py — minimal data-quality validation engine (existing product)
from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass
class RuleResult:
    rule_name: str
    column: str
    passed: bool
    failed_count: int
    message: str = ""


class Rule:
    """Base class. A rule inspects a column across all records and returns a RuleResult."""
    def __init__(self, column: str):
        self.column = column

    @property
    def name(self) -> str:
        return self.__class__.__name__

    def check(self, records: list[dict]) -> RuleResult:
        raise NotImplementedError


class NotNull(Rule):
    def check(self, records: list[dict]) -> RuleResult:
        failed = [r for r in records if r.get(self.column) is None]
        return RuleResult(self.name, self.column, len(failed) == 0, len(failed),
                          f"{len(failed)} null values in '{self.column}'")


class Unique(Rule):
    def check(self, records: list[dict]) -> RuleResult:
        seen, dupes = set(), 0
        for r in records:
            v = r.get(self.column)
            if v in seen:
                dupes += 1
            seen.add(v)
        return RuleResult(self.name, self.column, dupes == 0, dupes,
                          f"{dupes} duplicate values in '{self.column}'")


class InRange(Rule):
    def __init__(self, column: str, low: float, high: float):
        super().__init__(column)
        self.low, self.high = low, high

    def check(self, records: list[dict]) -> RuleResult:
        failed = [r for r in records
                  if r.get(self.column) is None
                  or not (self.low <= r[self.column] <= self.high)]
        return RuleResult(self.name, self.column, len(failed) == 0, len(failed),
                          f"{len(failed)} values of '{self.column}' outside "
                          f"[{self.low}, {self.high}]")


@dataclass
class Report:
    results: list[RuleResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(r.passed for r in self.results)

    def summary(self) -> str:
        lines = ["PASS" if r.passed else "FAIL" for r in self.results]
        return "\n".join(f"[{s}] {r.rule_name}({r.column}): {r.message}"
                         for s, r in zip(lines, self.results))


class Validator:
    """Runs a list of rules against a batch of records and produces a Report."""
    def __init__(self, rules: list[Rule]):
        self.rules = rules

    def validate(self, records: list[dict]) -> Report:
        return Report([rule.check(records) for rule in self.rules])


# --- usage ---
if __name__ == "__main__":
    data = [
        {"id": 1, "email": "a@x.com", "age": 30},
        {"id": 2, "email": None,      "age": 200},
        {"id": 2, "email": "c@x.com", "age": 25},   # duplicate id
    ]
    v = Validator([NotNull("email"), Unique("id"), InRange("age", 0, 120)])
    report = v.validate(data)
    print(report.summary())
    print("Overall:", "PASS" if report.passed else "FAIL")
```

---

## Reading comprehension (answer aloud before changing anything)

1. What's the entry point a caller uses, and what does it return?
2. What's the data contract — what is `records`, and what shape does a `RuleResult` have?
3. Where is this code *designed* to be extended? (How would you add a new kind of check?)
4. What invariant do all rules share? What happens if a rule raises an exception today?
5. Is `Unique` correct? Trace it on `[1, 2, 2, 2]` — how many dupes does it report?

<details><summary>Answers</summary>

1. `Validator.validate(records)` → returns a `Report` (a list of `RuleResult`s + `passed`/`summary` helpers).
2. `records` is a `list[dict]` (each dict = one row, column name → value). A `RuleResult` carries `rule_name, column, passed, failed_count, message`.
3. Subclass `Rule` and implement `check`. The `Validator` takes any list of `Rule`s — that's the extension point (a classic strategy pattern).
4. Invariant: every rule returns a `RuleResult` (never `None`, ideally never raises). **Today a raising rule would crash the whole `validate` run** — that's a robustness gap and a great thing to flag/fix.
5. `[1,2,2,2]` → `seen` grows {1},{1,2}; the 3rd `2` is a dupe (count 1), the 4th `2` is a dupe (count 2) ⇒ **2 dupes**. Correct (it counts duplicate *occurrences*, not distinct duplicated values — worth stating which definition).
</details>

---

## Feature tasks (tiered — do as many as time allows)

> The interviewer will likely give you **one** of these. Practice all of them. After each, write a one-line test and say the business value.

### Tier 1 — Add a `Regex` rule (new check type)
Add a rule that fails records where a column doesn't match a pattern (e.g., email format). This proves you found the extension point.

### Tier 2 — Add severity + make the run robust
Give each rule a `severity` (`"error"` or `"warning"`). `Report.passed` should be True if only *warnings* fail. Also make `Validator.validate` **resilient**: if a rule raises, capture it as a failed `RuleResult` instead of crashing the batch.

### Tier 3 — Config-driven validation + summary stats
Let a caller define rules from a list of dicts (config), so non-engineers can edit checks without touching code. Add a `Report.stats()` returning total rules, passed, failed, and total failed records.

---

## Reference solutions

<details><summary>Tier 1 — Regex rule</summary>

```python
import re

class Regex(Rule):
    def __init__(self, column: str, pattern: str):
        super().__init__(column)
        self.pattern = re.compile(pattern)

    def check(self, records: list[dict]) -> RuleResult:
        failed = [r for r in records
                  if r.get(self.column) is None
                  or not self.pattern.fullmatch(str(r[self.column]))]
        return RuleResult(self.name, self.column, len(failed) == 0, len(failed),
                          f"{len(failed)} values of '{self.column}' fail pattern "
                          f"{self.pattern.pattern}")

# test
assert Regex("email", r"[^@]+@[^@]+\.[^@]+").check(
    [{"email": "a@x.com"}, {"email": "nope"}]).failed_count == 1
```
**Why it fits:** it subclasses `Rule` and returns a `RuleResult` like everything else — no changes to `Validator` needed. *That's* extending the way the code wants.
**Value:** *"Now they can enforce formats like email or product-SKU patterns at load time, catching bad data before it pollutes the warehouse."*
</details>

<details><summary>Tier 2 — Severity + resilient runner</summary>

```python
# Rule gains a severity (default "error")
class Rule:
    def __init__(self, column: str, severity: str = "error"):
        self.column = column
        self.severity = severity
    # ... name, check as before; subclasses pass severity through super().__init__

# RuleResult carries severity through
@dataclass
class RuleResult:
    rule_name: str
    column: str
    passed: bool
    failed_count: int
    message: str = ""
    severity: str = "error"

# Report: only ERROR-level failures block
@property
def passed(self) -> bool:
    return all(r.passed for r in self.results if r.severity == "error")

# Validator: never let one rule crash the batch
def validate(self, records: list[dict]) -> Report:
    results = []
    for rule in self.rules:
        try:
            results.append(rule.check(records))
        except Exception as e:          # capture instead of crash
            results.append(RuleResult(rule.name, rule.column, False, -1,
                                      f"rule raised: {e}", getattr(rule, "severity", "error")))
    return Report(results)
```
**Value:** *"Warnings flag data smells without blocking the load, while errors stop it. And one broken rule can't take down the whole nightly validation — we still get results for every other rule."* The resilience fix maps directly to the **Resilience** grading signal.
</details>

<details><summary>Tier 3 — Config-driven rules + stats</summary>

```python
RULE_REGISTRY = {"NotNull": NotNull, "Unique": Unique,
                 "InRange": InRange, "Regex": Regex}

def build_rules(config: list[dict]) -> list[Rule]:
    """config: [{"type": "InRange", "column": "age", "low": 0, "high": 120}, ...]"""
    rules = []
    for spec in config:
        spec = dict(spec)                       # don't mutate caller's dict
        cls = RULE_REGISTRY[spec.pop("type")]
        rules.append(cls(**spec))
    return rules

# Report.stats()
def stats(self) -> dict:
    return {
        "total_rules": len(self.results),
        "passed": sum(r.passed for r in self.results),
        "failed": sum(not r.passed for r in self.results),
        "total_failed_records": sum(max(r.failed_count, 0) for r in self.results),
    }

# usage
cfg = [{"type": "NotNull", "column": "email"},
       {"type": "InRange", "column": "age", "low": 0, "high": 120}]
Validator(build_rules(cfg)).validate(data).stats()
```
**Value:** *"Analysts can define and change validation rules in a config file or table — no code deploy — which is exactly what a customer wants for evolving data contracts."*
</details>

---

## Connecting it to Spark (mention this — it shows range)

This engine works on `list[dict]`, which is fine for small batches but **pulls data to the driver** at scale. For big data you'd express the same checks as DataFrame aggregations so they run distributed:

```python
from pyspark.sql import functions as F
# NotNull("email") and InRange("age",0,120) in one pass, no driver pull:
df.select(
    F.sum(F.col("email").isNull().cast("int")).alias("email_nulls"),
    F.sum((~F.col("age").between(0, 120)).cast("int")).alias("age_out_of_range"),
).show()
```
> Saying *"the pure-Python version is great for a sample, but for the full table I'd push these checks into Spark so we don't collect everything to the driver"* connects this phase to the Spark phase and signals you think about scale.

## Watch for the same trap with Genie here
If you ask Genie to add a feature, it may **rewrite working code** or add a special-case that ignores the `Rule` extension point. Audit that it (1) keeps the data contract, (2) extends via subclassing/registry, (3) doesn't silently change `passed` semantics. See [07](07-AI-Stewardship-Genie-Code.md).
