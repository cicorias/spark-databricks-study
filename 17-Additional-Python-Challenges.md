---
title: Additional Python Challenges
tags: [python, challenge, hands-on, code-reading]
---

# 17 — Additional Python Feature-Dev Challenges

[← Additional Spark Challenges](16-Additional-Spark-Challenges.md) · Next: [Timed Speed Drills →](18-Timed-Speed-Drills.md)

> [06](06-Python-Feature-Dev-Challenge.md) gives you the DQ validation engine. Here are **two more** Python codebases with the same *shape*: small but real, with a clear extension point. Read each one aloud first, then pick a feature task. 25-minute timer per challenge.

---

## Challenge P2 — Streaming aggregation pipeline (mini)

**Story:** an analytics team has a tiny in-memory pipeline that consumes events from an iterator and runs a chain of operators (filter / map / window / sink). They want you to add a new operator type without changing the existing ones.

### The existing product

```python
# pipeline.py — minimal stream-style pipeline
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Callable, Iterable, Any
from collections import defaultdict, deque


@dataclass
class Event:
    ts: int                  # epoch seconds
    user_id: int
    amount: float


class Operator:
    """An operator consumes events one at a time and may emit zero or more events."""
    def process(self, event: Event) -> Iterable[Event]:
        raise NotImplementedError
    def flush(self) -> Iterable[Event]:
        return ()


class Filter(Operator):
    def __init__(self, pred: Callable[[Event], bool]):
        self.pred = pred
    def process(self, event):
        if self.pred(event):
            yield event


class Map(Operator):
    def __init__(self, fn: Callable[[Event], Event]):
        self.fn = fn
    def process(self, event):
        yield self.fn(event)


class TumblingSumByUser(Operator):
    """Per-user tumbling-window sum of `amount` every `window_s` seconds."""
    def __init__(self, window_s: int):
        self.window_s = window_s
        self.buckets: dict[tuple[int, int], float] = defaultdict(float)

    def process(self, event):
        bucket = event.ts - (event.ts % self.window_s)
        self.buckets[(event.user_id, bucket)] += event.amount
        return ()    # results emit on flush

    def flush(self):
        for (user_id, bucket), total in sorted(self.buckets.items()):
            yield Event(ts=bucket, user_id=user_id, amount=total)
        self.buckets.clear()


@dataclass
class Pipeline:
    ops: list[Operator] = field(default_factory=list)

    def run(self, source: Iterable[Event]) -> list[Event]:
        out: list[Event] = []
        for evt in source:
            self._fan_out([evt], 0, out)
        # final flush
        self._fan_out([], len(self.ops), out, flushing=True)
        return out

    def _fan_out(self, events, start_idx, out, flushing=False):
        if start_idx == len(self.ops):
            out.extend(events)
            return
        op = self.ops[start_idx]
        if flushing:
            for produced in op.flush():
                self._fan_out([produced], start_idx + 1, out, flushing=False)
            # then continue flushing downstream
            self._fan_out([], start_idx + 1, out, flushing=True)
        else:
            for e in events:
                for produced in op.process(e):
                    self._fan_out([produced], start_idx + 1, out)
```

### Reading comprehension (do this out loud)

1. **Entry point.** `Pipeline.run(source)` consumes an iterable of `Event`s and returns a `list[Event]`. Each `Operator` exposes `process(event) -> Iterable[Event]` and an optional `flush()`.
2. **Extension point.** Subclass `Operator`. The runtime calls `process` per event and `flush` once at end-of-stream. Operators are composable in any order via the `ops` list.
3. **Invariant.** An operator should never raise — it should either yield outputs or yield nothing.
4. **Subtle bug.** What happens if an upstream operator (e.g., `TumblingSumByUser`) emits at flush, and a downstream operator (e.g., another `TumblingSumByUser`) **also** has buffered state? Trace `_fan_out` carefully — does the downstream get a chance to flush *its* state after the upstream's flushed events arrive? *(Answer: yes — the recursive `flushing=True` continuation handles it. Worth proving with a test.)*

### Tasks (pick one for 25 min)

**Tier 1 — `Throttle(n_per_sec)`** : drop events when the per-user rate exceeds `n` events per second. Extend via subclassing `Operator`; keep per-user state in `self`.

**Tier 2 — `SessionWindow(gap_s)`** : emit one summary `Event` per *session* per user (a session = consecutive events with gap ≤ `gap_s`). Output `Event.ts` = session start, `amount` = sum within session. Watch the flush semantics.

**Tier 3 — Add backpressure-friendly batching**: extend `Operator` with an optional `process_batch(events: list[Event]) -> Iterable[Event]` so downstream operators can opt into batched processing. The default implementation calls `process` per event for back-compat.

### Reference solutions

<details><summary>Tier 1 — Throttle</summary>

```python
from collections import deque

class Throttle(Operator):
    def __init__(self, n_per_sec: int):
        self.n = n_per_sec
        self.windows: dict[int, deque] = {}     # user_id -> deque of recent ts

    def process(self, event):
        dq = self.windows.setdefault(event.user_id, deque())
        # evict timestamps older than 1 second
        while dq and dq[0] <= event.ts - 1:
            dq.popleft()
        if len(dq) < self.n:
            dq.append(event.ts)
            yield event
        # else drop silently
```

**Why it fits:** subclasses `Operator`, no changes to `Pipeline`. State is per-instance, no globals.

**Test:**

```python
out = Pipeline(ops=[Throttle(2)]).run([
    Event(ts=0, user_id=1, amount=1),
    Event(ts=0, user_id=1, amount=1),
    Event(ts=0, user_id=1, amount=1),     # 3rd within same second → dropped
])
assert len(out) == 2
```
</details>

<details><summary>Tier 2 — SessionWindow</summary>

```python
class SessionWindow(Operator):
    def __init__(self, gap_s: int):
        self.gap = gap_s
        self.active: dict[int, dict] = {}    # user_id -> {start, last_ts, total}

    def _close(self, user_id: int) -> Event:
        s = self.active.pop(user_id)
        return Event(ts=s["start"], user_id=user_id, amount=s["total"])

    def process(self, event):
        s = self.active.get(event.user_id)
        if s and event.ts - s["last_ts"] <= self.gap:
            s["last_ts"] = event.ts
            s["total"]  += event.amount
        else:
            if s:
                yield self._close(event.user_id)
            self.active[event.user_id] = {"start": event.ts,
                                          "last_ts": event.ts,
                                          "total":   event.amount}

    def flush(self):
        for user_id in list(self.active.keys()):
            yield self._close(user_id)
```
</details>

<details><summary>Tier 3 — process_batch</summary>

```python
class Operator:
    def process(self, event):
        raise NotImplementedError
    def process_batch(self, events):
        for e in events:
            yield from self.process(e)
    def flush(self):
        return ()

# Pipeline._fan_out gains a batch path — only when an operator overrides process_batch.
# (Sketch — left as the actual coding task.)
```
</details>

### Customer translation

> *"They needed to drop per-user spam without disturbing the rest of the pipeline. Because the runtime accepts any `Operator` subclass, we add `Throttle` as a one-file change — no edits to existing operators, no risk to the windowed aggregations."*

---

## Challenge P3 — Retry/backoff helper for flaky API calls

**Story:** ingestion calls a third-party API that flakes ~5% of the time. Today they wrap every call in a `try/except`. They want a clean `@retry(...)` decorator + `RetryPolicy` they can apply project-wide.

### The existing product

```python
# api_client.py — what they have today
import time, requests
import random

def fetch_user(user_id: int) -> dict:
    # Pretend this is the upstream — sometimes raises
    if random.random() < 0.05:
        raise requests.HTTPError("503 from upstream")
    time.sleep(0.01)
    return {"id": user_id, "name": f"user_{user_id}"}

# usage today (spread across the codebase)
def hydrate(users):
    out = []
    for uid in users:
        for _ in range(3):
            try:
                out.append(fetch_user(uid))
                break
            except Exception:
                time.sleep(0.5)
        else:
            out.append({"id": uid, "error": "max retries"})
    return out
```

### Tasks

**Tier 1.** Implement a `@retry(max_attempts: int, backoff_s: float)` decorator. Replace `hydrate`'s inline retry. Preserve the function's name + docstring (`functools.wraps`).

**Tier 2.** Add **exponential backoff with jitter** (`backoff = base * (2 ** attempt) + random.uniform(0, jitter_s)`); make it cap at `max_backoff_s`. Add a `retry_on: tuple[type[BaseException], ...] = (Exception,)` parameter so non-retriable errors (e.g., `ValueError`) raise immediately.

**Tier 3.** Refactor into a `RetryPolicy` dataclass that the decorator wraps, so policies can be **composed** (e.g., a global default + a per-call override) and **named** for observability (`policy.name = "api-flaky"`). Emit a structured log line per attempt and per final outcome.

### Reference (Tier 2)

<details><summary>Solution</summary>

```python
import functools
import random
import time
import logging

log = logging.getLogger(__name__)

def retry(max_attempts=3, base_s=0.1, max_backoff_s=2.0, jitter_s=0.1,
          retry_on: tuple = (Exception,)):
    def deco(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            attempt = 0
            while True:
                try:
                    return fn(*args, **kwargs)
                except retry_on as e:
                    attempt += 1
                    if attempt >= max_attempts:
                        log.warning("retry exhausted fn=%s attempts=%d err=%r",
                                    fn.__name__, attempt, e)
                        raise
                    sleep = min(max_backoff_s, base_s * (2 ** (attempt - 1))) + \
                            random.uniform(0, jitter_s)
                    log.info("retry fn=%s attempt=%d sleep=%.2fs err=%r",
                             fn.__name__, attempt, sleep, e)
                    time.sleep(sleep)
        return wrapper
    return deco

# usage
@retry(max_attempts=4, retry_on=(requests.HTTPError, ConnectionError))
def fetch_user(user_id: int) -> dict:
    ...
```

**Test:**

```python
calls = {"n": 0}

@retry(max_attempts=4, base_s=0, jitter_s=0)
def flaky():
    calls["n"] += 1
    if calls["n"] < 3:
        raise RuntimeError("nope")
    return "ok"

assert flaky() == "ok"
assert calls["n"] == 3
```
</details>

### Customer translation

> *"Every team was rolling their own try/except. With one decorator they get exponential backoff with jitter, a tunable retry budget, and structured logs they can ship to their observability stack — without touching call-site code beyond a single annotation."*

### What to watch Genie for

- Genie will often generate `for i in range(max_attempts)` and **return None on exhaustion** instead of re-raising — silently swallows errors. Catch this.
- It may forget `@functools.wraps`, breaking introspection and Sentry traces.
- It often hard-codes the sleep duration instead of exposing it as a parameter — review for configurability.
