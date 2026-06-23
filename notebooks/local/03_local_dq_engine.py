# ---
# jupyter:
#   jupytext:
#     formats: py:percent,ipynb
#     text_representation:
#       extension: .py
#       format_name: percent
# ---

# %% [markdown]
# # 03 — Local DQ Engine Challenge
#
# Pure Python — no Spark required for the engine itself, only for the "connect this to Spark" bonus
# at the end.
#
# Same engine and tasks as `notebooks/databricks/08_dq_engine_python.py`. Use this one when you
# want to drill the Python-feature challenge locally without spinning up Databricks.

# %% [markdown]
# ## The existing product

# %%
from __future__ import annotations
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
    def __init__(self, column: str):
        self.column = column

    @property
    def name(self) -> str:
        return self.__class__.__name__

    def check(self, records: list[dict]) -> RuleResult:
        raise NotImplementedError


class NotNull(Rule):
    def check(self, records):
        failed = [r for r in records if r.get(self.column) is None]
        return RuleResult(self.name, self.column, len(failed) == 0, len(failed),
                          f"{len(failed)} null values in '{self.column}'")


class Unique(Rule):
    def check(self, records):
        seen, dupes = set(), 0
        for r in records:
            v = r.get(self.column)
            if v in seen:
                dupes += 1
            seen.add(v)
        return RuleResult(self.name, self.column, dupes == 0, dupes,
                          f"{dupes} duplicate values in '{self.column}'")


class InRange(Rule):
    def __init__(self, column, low, high):
        super().__init__(column)
        self.low, self.high = low, high

    def check(self, records):
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
    def __init__(self, rules: list[Rule]):
        self.rules = rules

    def validate(self, records: list[dict]) -> Report:
        return Report([rule.check(records) for rule in self.rules])


# Sanity check
data = [
    {"id": 1, "email": "a@x.com", "age": 30},
    {"id": 2, "email": None,      "age": 200},
    {"id": 2, "email": "c@x.com", "age": 25},
]
report = Validator([NotNull("email"), Unique("id"), InRange("age", 0, 120)]).validate(data)
print(report.summary())
print("Overall:", "PASS" if report.passed else "FAIL")

# %% [markdown]
# ## Reading comprehension — answer these aloud
#
# 1. Entry point + return type?
# 2. Data contract — shape of `records` and `RuleResult`?
# 3. Extension point — how do you add a new check?
# 4. Invariant — what does every rule guarantee? What if one raises today?
# 5. `Unique` on `[1,2,2,2]` — what failed_count does it report? (Answer: 2)

# %% [markdown]
# ## YOUR CODE — Tier 1: Regex rule

# %%
import re

class Regex(Rule):
    def __init__(self, column, pattern):
        super().__init__(column)
        self.pattern = re.compile(pattern)

    def check(self, records):
        failed = [r for r in records
                  if r.get(self.column) is None
                  or not self.pattern.fullmatch(str(r[self.column]))]
        return RuleResult(self.name, self.column, len(failed) == 0, len(failed),
                          f"{len(failed)} values of '{self.column}' fail pattern "
                          f"{self.pattern.pattern}")

assert Regex("email", r"[^@]+@[^@]+\.[^@]+").check(
    [{"email": "a@x.com"}, {"email": "nope"}]).failed_count == 1
print("Tier 1 — Regex rule passes ✓")

# %% [markdown]
# ## YOUR CODE — Tier 2: Severity + resilient runner
#
# Make `Validator.validate` resilient — a rule that raises is captured as a failed `RuleResult`,
# not allowed to crash the whole batch. Add severity (`"error"` / `"warning"`); only errors block.

# %%
# Patch Rule + Report + Validator in place
def _rule_init(self, column, severity="error"):
    self.column = column
    self.severity = severity
Rule.__init__ = _rule_init   # type: ignore

# RuleResult gains severity
RuleResult.__annotations__ = {**RuleResult.__annotations__, "severity": str}
def _result_init(self, rule_name, column, passed, failed_count, message="", severity="error"):
    self.rule_name = rule_name
    self.column = column
    self.passed = passed
    self.failed_count = failed_count
    self.message = message
    self.severity = severity
RuleResult.__init__ = _result_init   # type: ignore

# Report only ERROR-level failures block
def _passed(self):
    return all(r.passed for r in self.results if getattr(r, "severity", "error") == "error")
Report.passed = property(_passed)   # type: ignore

# Validator wraps each rule in try/except
def _validate(self, records):
    results = []
    for rule in self.rules:
        try:
            results.append(rule.check(records))
        except Exception as e:
            results.append(RuleResult(rule.name, rule.column, False, -1,
                                      f"rule raised: {e}",
                                      getattr(rule, "severity", "error")))
    return Report(results)
Validator.validate = _validate   # type: ignore

# Demo: a broken rule + a warning that fails -> still PASS overall
class Broken(Rule):
    def check(self, records):
        raise RuntimeError("kaboom")

warn_rule = InRange("age", 0, 120)
warn_rule.severity = "warning"

rep = Validator([NotNull("email"), warn_rule, Broken("id")]).validate(data)
print(rep.summary())
print("Overall passed (warnings + broken rule):", rep.passed)   # False — because Broken is severity=error

# %% [markdown]
# ## YOUR CODE — Tier 3: Config-driven rules + stats

# %%
RULE_REGISTRY = {"NotNull": NotNull, "Unique": Unique, "InRange": InRange, "Regex": Regex}

def build_rules(config: list[dict]) -> list[Rule]:
    rules = []
    for spec in config:
        spec = dict(spec)
        cls = RULE_REGISTRY[spec.pop("type")]
        rules.append(cls(**spec))
    return rules

def stats(self) -> dict:
    return {
        "total_rules": len(self.results),
        "passed": sum(r.passed for r in self.results),
        "failed": sum(not r.passed for r in self.results),
        "total_failed_records": sum(max(r.failed_count, 0) for r in self.results),
    }
Report.stats = stats   # type: ignore

cfg = [
    {"type": "NotNull", "column": "email"},
    {"type": "InRange", "column": "age", "low": 0, "high": 120},
    {"type": "Regex",   "column": "email", "pattern": r"[^@]+@[^@]+\.[^@]+"},
]
report = Validator(build_rules(cfg)).validate(data)
print(report.summary())
print(report.stats())

# %% [markdown]
# ## Bonus — connect it to Spark
# At scale, push the same checks down to Spark so we don't pull data to the driver.

# %%
try:
    from pyspark.sql import SparkSession, functions as F
    spark = SparkSession.builder.appName("dq-bonus").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    df = spark.createDataFrame(data)
    (df.select(
        F.sum(F.col("email").isNull().cast("int")).alias("email_nulls"),
        F.sum((~F.col("age").between(0, 120)).cast("int")).alias("age_out_of_range"),
        (F.count("*") - F.countDistinct("id")).alias("duplicate_ids"),
     ).show())
    spark.stop()
except ImportError:
    print("pyspark not installed; skipping bonus.")

# %% [markdown]
# > *"The pure-Python version is great for samples, but for the full table I'd push these checks
# > into Spark so we don't collect everything to the driver."* — say this in the interview to
# > connect Phase 1 to Phase 2.
