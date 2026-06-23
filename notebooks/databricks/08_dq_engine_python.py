# Databricks notebook source
# MAGIC %md
# MAGIC # 08 — Python Feature-Dev Challenge: Data Quality Engine
# MAGIC
# MAGIC Mirrors Phase 1 of the interview: read code you didn't write, restate its intent, then **add a feature the way the code wants to be extended.**
# MAGIC
# MAGIC See [06 — Python Feature-Dev Challenge](../../06-Python-Feature-Dev-Challenge.md) for the tasks and reference solutions.
# MAGIC
# MAGIC **Set a 25-minute timer.** Read the engine *aloud* (the data contract, the extension point) **before** writing any code.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The existing product

# COMMAND ----------

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


# Sanity demo
data = [
    {"id": 1, "email": "a@x.com", "age": 30},
    {"id": 2, "email": None,      "age": 200},
    {"id": 2, "email": "c@x.com", "age": 25},   # duplicate id
]
report = Validator([NotNull("email"), Unique("id"), InRange("age", 0, 120)]).validate(data)
print(report.summary())
print("Overall:", "PASS" if report.passed else "FAIL")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read it before changing it — answer these out loud
# MAGIC
# MAGIC 1. **Entry point** — what does a caller invoke, and what comes back?
# MAGIC 2. **Data contract** — what's the shape of `records` and `RuleResult`?
# MAGIC 3. **Extension point** — how do I add a new check without modifying `Validator`?
# MAGIC 4. **Invariant** — what must every rule guarantee? What happens today if a rule raises?
# MAGIC 5. **Bug check** — is `Unique` counting distinct duplicated values, or duplicate occurrences? (On `[1,2,2,2]` it reports 2.)
# MAGIC
# MAGIC (Answers in [06](../../06-Python-Feature-Dev-Challenge.md).)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Your task (pick ONE; if time remains, do the next)
# MAGIC
# MAGIC ### Tier 1 — Add a `Regex` rule
# MAGIC New check type that fails records whose column doesn't match a regex pattern (e.g., email format). Subclass `Rule`. Write one assertion to prove it works.
# MAGIC
# MAGIC ### Tier 2 — Severity + resilient runner
# MAGIC - Add `severity` (`"error"` or `"warning"`) to each rule.
# MAGIC - `Report.passed` is True if only warnings fail.
# MAGIC - Make `Validator.validate` resilient: a rule that raises becomes a failed `RuleResult` instead of crashing the batch.
# MAGIC
# MAGIC ### Tier 3 — Config-driven rules + summary stats
# MAGIC - `build_rules(config: list[dict]) -> list[Rule]` so analysts can edit checks in JSON without touching code.
# MAGIC - `Report.stats() -> dict` returning total/passed/failed/total_failed_records.

# COMMAND ----------

# YOUR CODE HERE — Tier 1


# COMMAND ----------

# YOUR CODE HERE — Tier 2 (if time)


# COMMAND ----------

# YOUR CODE HERE — Tier 3 (if time)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Connect it to Spark — the senior move at the end
# MAGIC
# MAGIC The engine works on `list[dict]` — fine for a sample, but **pulls data to the driver** at scale. Show range by expressing the same checks as a distributed Spark aggregation:

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.createDataFrame(data)
(df.select(
    F.sum(F.col("email").isNull().cast("int")).alias("email_nulls"),
    F.sum((~F.col("age").between(0, 120)).cast("int")).alias("age_out_of_range"),
    (F.count("*") - F.countDistinct("id")).alias("duplicate_ids"),
).show())

# COMMAND ----------

# MAGIC %md
# MAGIC > *"The pure-Python version is great for samples, but for the full table I'd push these checks into Spark so we don't collect everything to the driver."* — that one sentence connects the two halves of the interview and shows you think about scale.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Audit Genie if you used it
# MAGIC See [07 — AI Stewardship](../../07-AI-Stewardship-Genie-Code.md). When you ask Genie to add a feature here, check that it:
# MAGIC
# MAGIC 1. **Kept the data contract** (`list[dict]` in, `RuleResult` out)
# MAGIC 2. **Extended via subclassing**, not by special-casing `Validator`
# MAGIC 3. **Didn't silently change `Report.passed` semantics**
# MAGIC 4. **Didn't add a `Rule` that swallows exceptions** (we want them surfaced as failed results, not hidden)
