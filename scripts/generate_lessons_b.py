"""
Generate lessons 12-19 for the FDE interview prep teaching system.
Run: uv run python scripts/generate_lessons_b.py
"""
from __future__ import annotations
import re, sys
from pathlib import Path

# import shared render function from the first generator
sys.path.insert(0, str(Path(__file__).parent))
from generate_lessons import render, OUT

LESSONS = []

# ── 12 Delta Lake Fundamentals ───────────────────────────────────────────────
SETUP_12 = """# Lesson 12 setup — create Delta tables for Delta Lake hands-on
from pyspark.sql import functions as F
from pyspark.sql.types import *
import datetime, random

spark.sql("CREATE DATABASE IF NOT EXISTS lesson_db")

# events table — used for time travel and schema evolution demos
events_schema = StructType([
    StructField("event_id",   LongType(),   False),
    StructField("customer_id",IntegerType(),False),
    StructField("event_type", StringType(), False),
    StructField("amount",     DoubleType(), False),
    StructField("event_date", DateType(),   False),
])
rows = [(i, random.randint(1,50), random.choice(["purchase","refund","click"]),
         round(random.uniform(1,500),2),
         datetime.date(2025, random.randint(1,6), random.randint(1,28)))
        for i in range(10_000)]
events_df = spark.createDataFrame(rows, events_schema)
(events_df.write.mode("overwrite").format("delta")
    .saveAsTable("lesson_db.events"))

# customers table for MERGE demo
customers_df = spark.createDataFrame(
    [(i, f"Customer {i}", ["active","inactive"][i%2], datetime.date(2025,1,1))
     for i in range(1, 51)],
    ["customer_id","name","status","updated_at"]
)
(customers_df.write.mode("overwrite").format("delta")
    .saveAsTable("lesson_db.customers_dim"))

print("Setup complete: lesson_db.events, lesson_db.customers_dim")
spark.sql("SELECT COUNT(*) as total FROM lesson_db.events").show()
"""

TEARDOWN_12 = """# Teardown lesson 12
spark.sql("DROP TABLE IF EXISTS lesson_db.events")
spark.sql("DROP TABLE IF EXISTS lesson_db.customers_dim")
spark.sql("DROP TABLE IF EXISTS lesson_db.events_updated")
spark.sql("DROP DATABASE IF EXISTS lesson_db")
print("Teardown complete.")
"""

LESSONS.append((12,
"Delta Lake Fundamentals",
"ACID, time travel, schema enforcement — what Delta adds over plain Parquet",
["Code Stewardship"],
"11-repartition-vs-coalesce.html", "11: Repartition vs Coalesce",
"13-merge-and-scd2.html", "13: MERGE &amp; SCD2",
"12-Delta-Lake-Deep-Dive.md",
SETUP_12,
"""
<section>
<h2>Why Delta exists</h2>
<p>Plain Parquet gives cheap columnar reads but no ACID, no updates, no time travel, and no concurrent write safety. Delta wraps Parquet with a <strong>transaction log</strong> (<code>_delta_log/</code>) that delivers all four — without leaving open formats.</p>
<pre># What a Delta table actually is
spark.sql("DESCRIBE DETAIL lesson_db.events").select("format","location","numFiles","sizeInBytes").show(truncate=False)

# The transaction log records every commit
import subprocess
# On DBFS/Unity Catalog you'd inspect _delta_log/ — on serverless use DESCRIBE HISTORY
spark.sql("DESCRIBE HISTORY lesson_db.events").select("version","timestamp","operation","operationParameters").show(truncate=False)</pre>
</section>

<section>
<h2>Time travel</h2>
<pre># Read the current version
spark.table("lesson_db.events").count()

# Modify the table to create a new version
spark.sql("UPDATE lesson_db.events SET amount = amount * 1.1 WHERE event_type = 'purchase'")
spark.sql("DESCRIBE HISTORY lesson_db.events").select("version","operation").show()

# Read a previous version
spark.read.format("delta").option("versionAsOf", 0).table("lesson_db.events").count()

# Or by timestamp
from datetime import datetime
spark.read.format("delta").option("timestampAsOf", "2025-01-01").table("lesson_db.events")  # if it existed then</pre>
<div class="box"><strong>Customer translation</strong>"If a bad batch corrupts the table, we can read back to the prior version while we investigate — no restore-from-backup window."</div>
</section>

<section>
<h2>Schema enforcement and evolution</h2>
<pre>from pyspark.sql.types import *
# Schema enforcement: Delta rejects incompatible writes
bad_df = spark.createDataFrame([(1, "oops")], ["event_id", "wrong_col"])
try:
    bad_df.write.format("delta").mode("append").saveAsTable("lesson_db.events")
except Exception as e:
    print("Schema enforcement blocked bad write:", type(e).__name__)

# Schema evolution: add a new column on write
events_with_new_col = spark.table("lesson_db.events").withColumn("channel", F.lit("web"))
(events_with_new_col.write.format("delta").mode("append")
  .option("mergeSchema", "true")    # allows new columns
  .saveAsTable("lesson_db.events"))
spark.table("lesson_db.events").printSchema()  # now has 'channel' column</pre>
<div class="box w"><strong>Watch out</strong>mergeSchema=true accepts NEW columns — not type changes. A renamed column looks like "drop old + add new" — usually not what you want.</div>
</section>

<section>
<h2>VACUUM — cleaning up old versions</h2>
<pre># VACUUM removes files no longer needed by any version within the retention window
# Default retention: 7 days
spark.sql("VACUUM lesson_db.events RETAIN 168 HOURS DRY RUN").show()
# DO NOT run VACUUM 0 HOURS — breaks all in-flight readers and concurrent queries</pre>
</section>
""",
[
    ("q1",
     "What does the Delta transaction log (_delta_log/) enable that plain Parquet cannot provide?",
     [("Faster columnar reads.", False),
      ("ACID transactions, concurrent writes, time travel, and schema enforcement.", True),
      ("Automatic data compression.", False),
      ("Predicate pushdown.", False)],
     "Correct. The transaction log records every commit as a JSON entry. This enables ACID (each write is atomic), concurrent writer coordination, reading previous versions (time travel), and schema validation on every write.",
     "Plain Parquet has no transaction log. Delta's _delta_log/ provides ACID, time travel, concurrent write coordination, and schema enforcement."
    ),
    ("q2",
     "A batch job corrupts data in lesson_db.events. How do you recover to the last-known-good state?",
     [("Restore from a backup file stored outside Delta.", False),
      ("Read from a previous Delta version: spark.read.format('delta').option('versionAsOf', N).table(...) or RESTORE TABLE TO VERSION AS OF N.", True),
      ("Run VACUUM to remove the bad version.", False),
      ("DROP TABLE and recreate from source.", False)],
     "Correct. Delta time travel lets you read or restore any version within the retention window. RESTORE TABLE events TO VERSION AS OF N rewrites the table to match that version. VACUUM doesn't help — it removes old files, not bad data.",
     "RESTORE TABLE TO VERSION AS OF N or read with versionAsOf option. VACUUM removes old files — it doesn't restore data."
    ),
    ("q3",
     "You append a DataFrame with .option('mergeSchema', 'true') that has a new column 'channel'. An existing column 'amount' is now a StringType instead of DoubleType. What happens?",
     [("Both changes are accepted — mergeSchema handles all evolution.", False),
      ("The new 'channel' column is added. The type change on 'amount' is rejected — mergeSchema only accepts new columns, not type-incompatible changes.", True),
      ("Both changes are rejected.", False),
      ("The append succeeds silently but Delta coerces types automatically.", False)],
     "Correct. mergeSchema=true adds new columns to the schema. It does NOT allow type-incompatible changes. A DoubleType → StringType change on an existing column will raise an AnalysisException.",
     "mergeSchema adds new columns. Type-incompatible changes on existing columns are rejected regardless."
    )
],
"\"How would you recover from a bad data load that corrupted lesson_db.events?\"",
"check DESCRIBE HISTORY to find the last-known-good version → use RESTORE TABLE lesson_db.events TO VERSION AS OF N or read with .option('versionAsOf', N) to verify the good state → once confirmed, RESTORE TABLE rewrites the current pointer to that version → no restore-from-backup needed — Delta's transaction log tracks every commit → also don't VACUUM aggressively, it removes the files you need for time travel",
TEARDOWN_12
))

# ── 13 MERGE & SCD2 ──────────────────────────────────────────────────────────
SETUP_13 = """# Lesson 13 setup — MERGE and SCD2 tables
from pyspark.sql import functions as F
import datetime

spark.sql("CREATE DATABASE IF NOT EXISTS lesson_db")

# Target: customer_dim (SCD2 — full history)
spark.sql('''
  CREATE TABLE IF NOT EXISTS lesson_db.customer_scd2 (
    customer_id  INT,
    name         STRING,
    status       STRING,
    region       STRING,
    effective_from DATE,
    effective_to   DATE,
    is_current   BOOLEAN
  ) USING DELTA
''')

# Seed it with initial rows (version 0 of each customer)
initial = spark.createDataFrame(
    [(i, f"Customer {i}", "active", ["US","EU","APAC"][i%3],
      datetime.date(2025,1,1), None, True)
     for i in range(1, 21)],
    ["customer_id","name","status","region","effective_from","effective_to","is_current"]
)
initial.write.mode("overwrite").format("delta").saveAsTable("lesson_db.customer_scd2")

# Simple target for basic MERGE demo
spark.sql('''
  CREATE TABLE IF NOT EXISTS lesson_db.orders_target (
    order_id   BIGINT, customer_id INT, amount DOUBLE, status STRING
  ) USING DELTA
''')
spark.createDataFrame(
    [(i, i%10+1, float(i*10), "pending") for i in range(1,11)],
    ["order_id","customer_id","amount","status"]
).write.mode("overwrite").format("delta").saveAsTable("lesson_db.orders_target")

print("Setup complete.")
spark.sql("SELECT COUNT(*) FROM lesson_db.customer_scd2").show()
spark.sql("SELECT * FROM lesson_db.orders_target LIMIT 5").show()
"""

TEARDOWN_13 = """# Teardown lesson 13
for t in ["lesson_db.customer_scd2", "lesson_db.orders_target"]:
    spark.sql(f"DROP TABLE IF EXISTS {t}")
spark.sql("DROP DATABASE IF EXISTS lesson_db")
print("Done.")
"""

LESSONS.append((13,
"MERGE &amp; SCD2",
"Upserts and slowly-changing dimensions — the most common real customer pattern",
["Code Stewardship"],
"12-delta-lake-fundamentals.html", "12: Delta Fundamentals",
"14-optimize-vacuum-clustering.html", "14: OPTIMIZE &amp; Clustering",
"12-Delta-Lake-Deep-Dive.md",
SETUP_13,
"""
<section>
<h2>MERGE — the upsert pattern</h2>
<pre># Incoming updates from today's CDC feed
updates = spark.createDataFrame(
    [(1, "Customer 1 Updated", "vip",   "US"),   # existing — update
     (2, "Customer 2",         "active","EU"),   # existing — no real change
     (99,"New Customer",       "active","US")],  # new row — insert
    ["customer_id","name","status","region"]
)

# MERGE: update matches, insert new rows
spark.sql('''
  MERGE INTO lesson_db.orders_target t
  USING (SELECT 1 AS order_id, 999 AS customer_id, 50.0 AS amount, 'shipped' AS status) s
  ON t.order_id = s.order_id
  WHEN MATCHED THEN UPDATE SET t.status = s.status, t.amount = s.amount
  WHEN NOT MATCHED THEN INSERT *
''')
spark.sql("SELECT * FROM lesson_db.orders_target ORDER BY order_id").show()</pre>
<div class="box"><strong>Key points to say aloud</strong>
MERGE is idempotent — re-running today's batch doesn't duplicate rows. Always include a predicate on the join key so Delta's file pruning only touches relevant files. Cost = rewriting matched files (copy-on-write).</div>
</section>

<section>
<h2>SCD2 — tracking history with effective dates</h2>
<pre># SCD2 schema: each row has effective_from, effective_to (null = current), is_current flag
spark.sql("SELECT * FROM lesson_db.customer_scd2 WHERE customer_id <= 3").show()

# Step 1: close old rows for customers that changed
changed = spark.createDataFrame(
    [(1,"Customer 1","vip","US"), (5,"Customer 5","inactive","EU")],
    ["customer_id","name","status","region"]
)
import datetime
today = datetime.date.today()

spark.sql(f'''
  MERGE INTO lesson_db.customer_scd2 t
  USING (SELECT * FROM lesson_db.customer_scd2 AS curr
         JOIN (VALUES (1),(5)) AS chg(customer_id) USING (customer_id)
         WHERE curr.is_current = true) s
  ON t.customer_id = s.customer_id AND t.is_current = true
  WHEN MATCHED THEN UPDATE SET t.effective_to = \'{today}\', t.is_current = false
''')

# Step 2: insert new rows for changed customers
new_rows_df = changed.withColumn("effective_from", F.lit(today)).withColumn("effective_to", F.lit(None).cast("date")).withColumn("is_current", F.lit(True))
new_rows_df.write.mode("append").format("delta").saveAsTable("lesson_db.customer_scd2")

# Verify: customer 1 now has 2 rows (old closed, new active)
spark.sql("SELECT * FROM lesson_db.customer_scd2 WHERE customer_id = 1 ORDER BY effective_from").show()</pre>
<div class="box"><strong>SCD2 requires 2 MERGE passes</strong>: one to close old rows, one to insert new rows — MERGE can't do both to the same key in a single pass. Working pattern in notebooks/databricks/06_delta_merge_scd.py.</div>
</section>

<section>
<h2>Performance: add a predicate to MERGE</h2>
<pre># Without predicate: MERGE scans entire target table
# With predicate: Delta uses file pruning to only touch recent files
spark.sql('''
  MERGE INTO lesson_db.orders_target t
  USING (SELECT 5 AS order_id, 200.0 AS amount, 'delivered' AS status) s
  ON t.order_id = s.order_id AND t.order_id BETWEEN 1 AND 10  -- file pruning hint
  WHEN MATCHED THEN UPDATE SET t.amount = s.amount, t.status = s.status
''')</pre>
</section>
""",
[
    ("q1",
     "Why is MERGE preferred over overwrite for incremental loads?",
     [("MERGE is faster than overwrite on all datasets.", False),
      ("MERGE is idempotent on the natural key — re-running the same batch doesn't create duplicates. Overwrite replaces everything.", True),
      ("MERGE uses less storage than overwrite.", False),
      ("MERGE is required by Delta Lake — overwrite is not supported.", False)],
     "Correct. MERGE checks the join condition before inserting or updating. If today's batch is processed twice, matched rows are updated (idempotent) and unmatched rows are inserted once. A full overwrite would lose all data not in the batch.",
     "MERGE = idempotent upsert. Re-running doesn't duplicate. Overwrite = replace entire table."
    ),
    ("q2",
     "SCD2 needs to 'close' an existing row AND 'insert' a new row for the same customer. Why do you need two MERGE passes?",
     [("MERGE only supports one WHEN MATCHED clause.", False),
      ("A MERGE can update (close) an existing row OR insert a new row in one pass, but cannot do both to the same key in the same statement.", True),
      ("Delta doesn't support MERGE on tables with a date column.", False),
      ("Two passes are not needed — one MERGE handles both operations.", False)],
     "Correct. MERGE processes each matched key once. You can't close the current row AND insert a new row for the same key in a single MERGE — the second operation would try to match against the just-updated row. Pass 1: close old row. Pass 2: insert new row.",
     "MERGE processes each key once. Close (update) and insert for the same key requires two separate passes."
    ),
    ("q3",
     "You run MERGE without a predicate on the join key. What performance problem does this cause?",
     [("MERGE fails without a predicate.", False),
      ("Delta must scan ALL target files to find matches instead of pruning to relevant files. On large tables this is very slow.", True),
      ("The MERGE rewrites the entire table on every run.", False),
      ("No problem — Delta uses Z-order to find matches automatically.", False)],
     "Correct. A predicate like AND t.date >= '2025-06-01' lets Delta's file pruning skip files that can't contain matching rows. Without it, every file is read. On a large table this can turn a 30-second MERGE into a 30-minute one.",
     "Without a key predicate, Delta reads all files. With a predicate, file pruning skips non-matching files."
    )
],
"\"How would you handle a daily feed of customer updates that need to be tracked historically?\"",
"SCD2 pattern: table has effective_from, effective_to (null=current), is_current flag → daily: run MERGE pass 1 to close changed rows (set effective_to=today, is_current=false) → run MERGE pass 2 to insert new current rows for changed customers → result: full history per customer, current state queryable with WHERE is_current=true → this is idempotent: re-running the batch closes already-closed rows (no-op) and inserts only genuinely new rows",
TEARDOWN_13
))

# ── 14 OPTIMIZE, VACUUM, Liquid Clustering ──────────────────────────────────
SETUP_14 = """# Lesson 14 setup
from pyspark.sql import functions as F
import random, datetime

spark.sql("CREATE DATABASE IF NOT EXISTS lesson_db")

# Create a fragmented table (many small files simulated by many small writes)
spark.sql("DROP TABLE IF EXISTS lesson_db.fragmented")
spark.sql('''
  CREATE TABLE lesson_db.fragmented (
    id INT, category STRING, amount DOUBLE, event_date DATE
  ) USING DELTA
''')

# Write in 20 small batches to create many small files
for day in range(1, 21):
    batch = spark.createDataFrame(
        [(i, ["A","B","C","D"][i%4], round(random.uniform(1,1000),2),
          datetime.date(2025,6,day))
         for i in range(500)],
        ["id","category","amount","event_date"]
    )
    batch.write.mode("append").format("delta").saveAsTable("lesson_db.fragmented")

# Show the small-files problem
spark.sql("DESCRIBE DETAIL lesson_db.fragmented").select("numFiles","sizeInBytes").show()
print("Setup complete — fragmented table has many small files.")
"""

TEARDOWN_14 = """# Teardown lesson 14
spark.sql("DROP TABLE IF EXISTS lesson_db.fragmented")
spark.sql("DROP TABLE IF EXISTS lesson_db.clustered_events")
spark.sql("DROP DATABASE IF EXISTS lesson_db")
print("Done.")
"""

LESSONS.append((14,
"OPTIMIZE, VACUUM &amp; Liquid Clustering",
"Make reads fast: compact small files, co-locate related data",
["Code Stewardship"],
"13-merge-and-scd2.html", "13: MERGE &amp; SCD2",
"15-medallion-architecture.html", "15: Medallion Architecture",
"12-Delta-Lake-Deep-Dive.md",
SETUP_14,
"""
<section>
<h2>The small-files problem</h2>
<pre># See how fragmented the table is
spark.sql("DESCRIBE DETAIL lesson_db.fragmented").select("numFiles","sizeInBytes").show()
# 20 batches × small writes = many tiny files

# Read performance suffers: Spark opens each file separately
import time
t0 = time.time()
spark.table("lesson_db.fragmented").count()
print(f"Read time (fragmented): {time.time()-t0:.2f}s")</pre>
</section>

<section>
<h2>OPTIMIZE — compact small files</h2>
<pre># OPTIMIZE rewrites small files into ~128MB chunks
spark.sql("OPTIMIZE lesson_db.fragmented")
spark.sql("DESCRIBE DETAIL lesson_db.fragmented").select("numFiles","sizeInBytes").show()
# numFiles should drop significantly

# Measure read performance after OPTIMIZE
t0 = time.time()
spark.table("lesson_db.fragmented").count()
print(f"Read time (after OPTIMIZE): {time.time()-t0:.2f}s")

# OPTIMIZE can target a partition range to avoid full-table rewrite
spark.sql("OPTIMIZE lesson_db.fragmented WHERE event_date >= '2025-06-15'")</pre>
<div class="box"><strong>Customer translation</strong>"Every future query gets faster — not just today's — because reads hit a handful of right-sized files instead of dozens of tiny ones."</div>
</section>

<section>
<h2>ZORDER vs Liquid Clustering</h2>
<pre># ZORDER: co-locate rows by column value for data skipping
spark.sql("OPTIMIZE lesson_db.fragmented ZORDER BY (category)")
# Now queries filtering by category skip more files

# Liquid Clustering: declare at table creation — auto-maintained by OPTIMIZE
spark.sql("DROP TABLE IF EXISTS lesson_db.clustered_events")
spark.sql('''
  CREATE TABLE lesson_db.clustered_events (
    id INT, category STRING, amount DOUBLE, event_date DATE
  ) USING DELTA CLUSTER BY (category, event_date)
''')
# Insert data and run OPTIMIZE — clustering is maintained automatically
spark.table("lesson_db.fragmented").write.mode("append").format("delta").saveAsTable("lesson_db.clustered_events")
spark.sql("OPTIMIZE lesson_db.clustered_events")</pre>
</section>

<section>
<h2>VACUUM — clean up old files</h2>
<pre># VACUUM removes files no longer needed by any live version (within retention window)
spark.sql("DESCRIBE HISTORY lesson_db.fragmented").select("version","timestamp","operation").show(5)

# Safe: respect default 7-day retention
spark.sql("VACUUM lesson_db.fragmented RETAIN 168 HOURS DRY RUN").show()

# DANGEROUS — do not run in production without understanding the consequences:
# spark.sql("VACUUM lesson_db.fragmented RETAIN 0 HOURS")
# ↑ Breaks time travel and any in-flight queries that reference old file versions</pre>
<div class="box w"><strong>Never VACUUM 0 HOURS casually</strong>It removes all files not in the current snapshot — breaking time travel and any reader that opened a file reference before the VACUUM ran.</div>
</section>
""",
[
    ("q1",
     "lesson_db.fragmented was written in 20 small batches. What problem does this cause for readers?",
     [("No problem — Delta reads files in parallel so many small files are fine.", False),
      ("Each file requires a separate open + metadata read. Many tiny files means excessive overhead per query, even if total data is small.", True),
      ("Delta automatically merges files on read so there's no overhead.", False),
      ("Small files only matter for write performance, not reads.", False)],
     "Correct. Even if data is small in total, each file has per-file overhead: open, read footer/statistics, close. Hundreds of tiny files multiply this overhead. OPTIMIZE compacts them into a handful of right-sized files.",
     "Many small files = many per-file open/close overhead on reads. OPTIMIZE compacts them."
    ),
    ("q2",
     "What's the difference between ZORDER and Liquid Clustering?",
     [("They're equivalent — just different syntax.", False),
      ("ZORDER must be re-run after every write with OPTIMIZE; Liquid Clustering is declared at table creation and maintained automatically by OPTIMIZE.", True),
      ("ZORDER is faster than Liquid Clustering.", False),
      ("Liquid Clustering is only available on Delta 3.0+.", False)],
     "Correct. ZORDER is applied by running OPTIMIZE ... ZORDER BY (...) — you must re-run it periodically. Liquid Clustering (CLUSTER BY at table creation) is maintained incrementally by every OPTIMIZE run, and you can change cluster keys cheaply with ALTER TABLE.",
     "ZORDER = run periodically. Liquid Clustering = declared at CREATE TABLE, maintained automatically."
    ),
    ("q3",
     "Why should you never run VACUUM RETAIN 0 HOURS in production?",
     [("VACUUM with 0 hours syntax is not valid.", False),
      ("It removes all files not in the current snapshot — breaking time travel, concurrent readers, and any in-flight queries that reference old file versions.", True),
      ("It's too slow — it scans all files in the table.", False),
      ("It breaks the schema enforcement mechanism.", False)],
     "Correct. VACUUM removes physical files. With 0 hours retention, it removes everything except the current snapshot. Any reader that opened a file reference before the VACUUM ran will fail. Time travel back before the VACUUM is also gone.",
     "VACUUM 0 HOURS removes ALL old files — breaks time travel, concurrent reads, in-flight queries."
    )
],
"\"A nightly job appends 1,000 small files to a Delta table every day. After 30 days the table is slow to query. What do you do?\"",
"classic small-files problem → OPTIMIZE compacts them: run OPTIMIZE lesson_db.table or scope it to a date range to avoid full rewrite → files rewired to ~128 MB chunks → every future query hits fewer files → schedule OPTIMIZE nightly after the append job → optionally add ZORDER by the most-filtered column (e.g. event_date) so filters skip more files → customer: every future query gets faster — not just today's — because reads hit a handful of right-sized files",
TEARDOWN_14
))

# ── 15 Medallion Architecture ────────────────────────────────────────────────
SETUP_15 = """# Lesson 15 setup — build a 3-layer Medallion pipeline from scratch
from pyspark.sql import functions as F
from pyspark.sql.types import *
import datetime, random, json

spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
spark.sql("CREATE DATABASE IF NOT EXISTS silver")
spark.sql("CREATE DATABASE IF NOT EXISTS gold")

# Simulate raw source data landing in Bronze (messy, as-is)
raw_rows = []
for i in range(2000):
    row = {
        "raw_id": str(i),
        "cust_id": str(random.randint(1,50)),
        "amount": str(round(random.uniform(-50, 2000), 2)),  # some negatives (bad data)
        "status": random.choice(["completed","COMPLETED","pending","","refunded"]),
        "ts": f"2025-0{random.randint(1,6)}-{random.randint(1,28):02d}T{random.randint(0,23):02d}:00:00"
    }
    raw_rows.append(row)

# Also inject a duplicate
raw_rows.append(raw_rows[0])

raw_df = spark.createDataFrame(
    [(r["raw_id"], r["cust_id"], r["amount"], r["status"], r["ts"]) for r in raw_rows],
    ["raw_id","cust_id","amount","status","ts"]
)
raw_df.write.mode("overwrite").format("delta").saveAsTable("bronze.orders_raw")
print(f"Bronze: {raw_df.count()} rows including duplicates and bad data")
"""

TEARDOWN_15 = """# Teardown lesson 15
for db in ["bronze","silver","gold"]:
    spark.sql(f"DROP DATABASE IF EXISTS {db} CASCADE")
print("Done.")
"""

LESSONS.append((15,
"Medallion Architecture &amp; Idempotent Loads",
"Bronze → Silver → Gold: the reference architecture every FDE knows",
["Computational Thinking", "Code Stewardship"],
"14-optimize-vacuum-clustering.html", "14: OPTIMIZE",
"16-reading-code-you-didnt-write.html", "16: Reading Code",
"14-Data-Engineering-Patterns.md",
SETUP_15,
"""
<section>
<h2>The three layers</h2>
<table style="width:100%;border-collapse:collapse;margin-bottom:1rem;font-size:.9rem">
<tr style="background:#1a1d27"><th style="padding:.5rem .8rem;border:1px solid #2e3147;text-align:left">Layer</th><th style="padding:.5rem .8rem;border:1px solid #2e3147;text-align:left">Contents</th><th style="padding:.5rem .8rem;border:1px solid #2e3147;text-align:left">Operations</th></tr>
<tr><td style="padding:.4rem .8rem;border:1px solid #2e3147"><strong>Bronze</strong></td><td style="padding:.4rem .8rem;border:1px solid #2e3147">Raw, as-ingested — exactly what arrived from the source</td><td style="padding:.4rem .8rem;border:1px solid #2e3147">Append-only; one source = one table; never transform here</td></tr>
<tr><td style="padding:.4rem .8rem;border:1px solid #2e3147"><strong>Silver</strong></td><td style="padding:.4rem .8rem;border:1px solid #2e3147">Cleaned, validated, deduped, type-cast, joined</td><td style="padding:.4rem .8rem;border:1px solid #2e3147">MERGE upserts; DQ checks; schema enforcement</td></tr>
<tr><td style="padding:.4rem .8rem;border:1px solid #2e3147"><strong>Gold</strong></td><td style="padding:.4rem .8rem;border:1px solid #2e3147">Business-ready aggregates / serving tables</td><td style="padding:.4rem .8rem;border:1px solid #2e3147">OPTIMIZE + Liquid Clustering; denormalized for dashboards</td></tr>
</table>
<div class="box"><strong>Interview one-liner</strong>"I'd land raw data into Bronze so we can always replay if Silver logic changes. Silver is where we clean and dedupe. Gold is what the dashboards read — pre-aggregated, optimized for fast queries."</div>
</section>

<section>
<h2>Bronze → Silver: clean and deduplicate</h2>
<pre>from pyspark.sql import functions as F

# Read raw bronze data (messy types, duplicates, bad values)
bronze = spark.table("bronze.orders_raw")
bronze.show(5)
print(f"Bronze count (with duplicates): {bronze.count()}")

# Silver: cast types, normalize, filter invalid, deduplicate
silver = (bronze
  .withColumn("order_id",    F.col("raw_id").cast("long"))
  .withColumn("customer_id", F.col("cust_id").cast("int"))
  .withColumn("amount",      F.col("amount").cast("double"))
  .withColumn("status",      F.lower(F.trim(F.col("status"))))
  .withColumn("event_ts",    F.to_timestamp(F.col("ts")))
  .where(F.col("amount") > 0)              # drop negatives
  .where(F.col("status").isin("completed","pending","refunded"))
  .dropDuplicates(["order_id"])            # remove exact duplicates
  .select("order_id","customer_id","amount","status","event_ts")
)
silver.write.mode("overwrite").format("delta").saveAsTable("silver.orders")
print(f"Silver count (cleaned): {silver.count()}")</pre>
</section>

<section>
<h2>Silver → Gold: aggregate for dashboards</h2>
<pre># Gold: daily revenue by status (what the dashboard reads)
gold = (spark.table("silver.orders")
  .withColumn("date", F.to_date("event_ts"))
  .groupBy("date","status")
  .agg(
    F.sum("amount").alias("revenue"),
    F.count("*").alias("order_count"),
    F.avg("amount").alias("avg_order_value")
  )
  .orderBy("date","status")
)
gold.write.mode("overwrite").format("delta").saveAsTable("gold.daily_revenue")
gold.show(10)
print("Gold table ready for dashboards.")</pre>
</section>

<section>
<h2>Idempotent incremental loads</h2>
<pre"># Key ingredients for a re-runnable nightly job:
# 1. Watermark column (updated_at / event_ts)
# 2. MERGE on natural key (not append)
# 3. High-water-mark table

# Simulate: read only new rows since last run
last_hwm = spark.sql(
    "SELECT COALESCE(MAX(event_ts), '2025-01-01') AS hwm FROM silver.orders"
).first()["hwm"]
print(f"Processing rows after: {last_hwm}")

new_rows = spark.table("bronze.orders_raw").where(F.col("ts") > str(last_hwm))
print(f"New rows to process: {new_rows.count()}")</pre>
<div class="box"><strong>Customer phrase</strong>"The job is safe to re-run — worst case we redo work, we never duplicate or skip rows."</div>
</section>
""",
[
    ("q1",
     "Why is Bronze kept as raw, unmodified data rather than cleaning it on ingestion?",
     [("Bronze is required to be raw by Delta Lake spec.", False),
      ("If Silver business logic changes or has a bug, you can re-derive Silver from Bronze without re-ingesting from the source system.", True),
      ("Cleaning data in Bronze would be too slow.", False),
      ("Bronze tables cannot be queried, so transformation doesn't help.", False)],
     "Correct. Bronze is your source of truth for what actually arrived. If you discover a cleaning bug 30 days later, you can fix the Silver logic and replay from Bronze — no re-extraction from the often-expensive source system.",
     "Bronze = replay source. If Silver logic changes, reprocess from Bronze without hitting the source system again."
    ),
    ("q2",
     "dropDuplicates(['order_id']) keeps an arbitrary row when multiple rows share the same order_id. When is this a problem?",
     [("Never — any row with the same order_id is acceptable.", False),
      ("When you need to keep the latest version (highest timestamp). Use Window.partitionBy('order_id').orderBy(desc('event_ts')) + row_number()==1 instead.", True),
      ("dropDuplicates(['order_id']) always keeps the first row by insertion order.", False),
      ("It's only a problem with more than 2 duplicates.", False)],
     "Correct. dropDuplicates picks an arbitrary row from each duplicate group. If you need 'latest wins', use a window function: Window.partitionBy(key).orderBy(desc(timestamp)) and keep row_number()==1.",
     "dropDuplicates = arbitrary row. For 'latest wins', use window: partitionBy(key).orderBy(desc(ts)), keep row_number()==1."
    ),
    ("q3",
     "What are the three ingredients for a safe, re-runnable incremental load?",
     [("Full overwrite + timestamp in filename + retry logic.", False),
      ("A watermark column, MERGE on natural key (not append), and a high-water-mark table tracking the last processed watermark.", True),
      ("Append-only writes + OPTIMIZE after each run + VACUUM weekly.", False),
      ("Schema enforcement + DQ checks + email alerts on failure.", False)],
     "Correct. Watermark = know what's new. MERGE = idempotent upsert (re-run safe). HWM table = durable progress tracking so retries pick up exactly where they left off.",
     "Watermark + MERGE on natural key + HWM table = idempotent, re-runnable incremental load."
    )
],
"\"A customer wants to track all order updates and have dashboards read clean aggregated data. How would you design this?\"",
"Medallion: Bronze = raw order events append-only (replay source) → Silver = clean, deduplicate on order_id, cast types, filter invalid, MERGE on order_id for idempotent loads → Gold = aggregate (daily revenue by status) with OPTIMIZE + Liquid Clustering on the filter columns → nightly job: read only new rows since last watermark, MERGE into Silver, rebuild Gold from Silver → safe to re-run: worst case redo work, never duplicate",
TEARDOWN_15
))

# ── 16 Reading Code You Didn't Write ─────────────────────────────────────────
SETUP_16 = """# Lesson 16 setup — load the data quality engine (Phase 1 codebase)
# This is the customer codebase you'll read and extend in Phase 1

dq_code = '''
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

@dataclass
class RuleResult:
    rule_name: str
    column: str
    passed: bool
    failed_count: int
    message: str = ""

class Rule:
    # Base class. Inspects a column across all records and returns a RuleResult.
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
        return RuleResult(self.name, self.column, len(failed)==0, len(failed),
                          f"{len(failed)} null values in {self.column!r}")

class InRange(Rule):
    def __init__(self, column, lo, hi):
        super().__init__(column)
        self.lo, self.hi = lo, hi
    def check(self, records):
        failed = [r for r in records if not (self.lo <= (r.get(self.column) or 0) <= self.hi)]
        return RuleResult(self.name, self.column, len(failed)==0, len(failed),
                          f"{len(failed)} values outside [{self.lo},{self.hi}]")

class Engine:
    # Runs a list of Rules against a batch of records. Returns all RuleResults.
    def __init__(self):
        self._rules: list[Rule] = []
    def add_rule(self, rule: Rule) -> "Engine":
        self._rules.append(rule)
        return self
    def run(self, records: list[dict]) -> list[RuleResult]:
        return [r.check(records) for r in self._rules]
    def summary(self, results: list[RuleResult]) -> dict:
        return {
            "total": len(results),
            "passed": sum(1 for r in results if r.passed),
            "failed": sum(1 for r in results if not r.passed),
        }
'''

# Write to a temp file so we can import it in the next cell
import tempfile, os, sys
tmp = tempfile.mkdtemp()
with open(os.path.join(tmp, "dq.py"), "w") as f:
    f.write(dq_code)
sys.path.insert(0, tmp)

# Verify the engine works
import dq
engine = dq.Engine()
engine.add_rule(dq.NotNull("amount"))
engine.add_rule(dq.InRange("amount", 0, 10000))

records = [
    {"amount": 50.0},
    {"amount": None},
    {"amount": -5.0},
    {"amount": 200.0},
]
results = engine.run(records)
print(engine.summary(results))
for r in results:
    print(r)

print("\\nSetup complete. The dq module is imported and ready.")
print("Your task: add a new Rule called 'OneOf' that checks a column value is in a fixed set.")
"""

TEARDOWN_16 = """# Teardown lesson 16 — nothing to drop (no Delta tables created)
print("No Delta tables to drop for lesson 16.")
"""

LESSONS.append((16,
"Reading Code You Didn't Write",
"Phase 1 strategy: trace the contract before you touch anything",
["Code Stewardship"],
"15-medallion-architecture.html", "15: Medallion",
"17-genie-code-prompting.html", "17: Genie Code",
"06-Python-Feature-Dev-Challenge.md",
SETUP_16,
"""
<section>
<h2>The 5-step protocol — say every step out loud</h2>
<ol>
<li><strong>Find the entry point.</strong> What does a caller actually invoke? Trace from the outside in.</li>
<li><strong>Trace the data contract.</strong> What goes in, what comes out, what type/shape?</li>
<li><strong>Name the extension points.</strong> Where is this designed to be extended? (base classes, registries)</li>
<li><strong>Spot the invariants.</strong> What must stay true? (e.g., every Rule.check() returns a RuleResult)</li>
<li><strong>Only then change it.</strong> Add the feature the way the code wants to be extended.</li>
</ol>
<div class="box i"><strong>Scoring move</strong>"Let me restate what this code does before I touch it." This sentence is itself a graded behavior — it shows code stewardship.</div>
</section>

<section>
<h2>Step 1: trace the existing engine</h2>
<pre"># Apply the 5 steps to dq.py RIGHT NOW — say each one aloud
import dq

# Step 1: entry point
print(dir(dq.Engine))           # methods available
help(dq.Engine.run)             # what does run() do?

# Step 2: data contract
# INPUT: Engine.run(records: list[dict])
# OUTPUT: list[RuleResult]
# RuleResult has: rule_name, column, passed (bool), failed_count, message

# Step 3: extension point
# Rule is the base class — subclass it and implement check()
print(dq.Rule.__subclasses__())  # what Rules already exist?

# Step 4: invariant
# Every Rule.check() MUST return a RuleResult — Engine.summary() assumes this
# Rules never raise — they return a failing RuleResult instead</pre>
</section>

<section>
<h2>Step 5: add a new Rule the right way</h2>
<pre># TASK: implement OneOf — checks that column value is in an allowed set
# Bad approach: bolting special-case logic into Engine.run()
# Good approach: subclass Rule, implement check()

class OneOf(dq.Rule):
    # Passes if every record's column value is in the allowed set.
    def __init__(self, column: str, allowed: set):
        super().__init__(column)
        self.allowed = set(allowed)

    def check(self, records: list[dict]) -> dq.RuleResult:
        failed = [r for r in records if r.get(self.column) not in self.allowed]
        return dq.RuleResult(
            self.name, self.column,
            passed=len(failed) == 0,
            failed_count=len(failed),
            message=f"{len(failed)} values not in {self.allowed}"
        )

# Test it (always test before claiming it works)
records = [
    {"status": "completed"},
    {"status": "COMPLETED"},   # fails — case matters
    {"status": "pending"},
    {"status": "invalid"},     # fails
]
rule = OneOf("status", {"completed", "pending", "refunded"})
result = rule.check(records)
print(result)
assert result.failed_count == 2, f"Expected 2 failures, got {result.failed_count}"
print("OneOf rule works correctly.")

# Use it in the engine
engine = dq.Engine()
engine.add_rule(OneOf("status", {"completed","pending","refunded"}))
print(engine.summary(engine.run(records)))</pre>
</section>

<section>
<h2>Explaining it to the customer</h2>
<div class="box"><strong>Practice saying this</strong>"The engine is extensible by design — Rule is the base class and each rule encapsulates one check. I added OneOf by subclassing Rule and implementing check() to return a RuleResult. I didn't need to touch Engine at all — that's the extension point. The invariant is that check() always returns a RuleResult and never raises, so the engine can always call summary()."</div>
</section>
""",
[
    ("q1",
     "A caller does: engine = Engine(); engine.add_rule(NotNull('amount')); results = engine.run(records). What is results?",
     [("A boolean — True if all rules passed.", False),
      ("A list[RuleResult], one RuleResult per rule added.", True),
      ("A dict with 'passed' and 'failed' counts.", False),
      ("None — results are printed by run(), not returned.", False)],
     "Correct. Engine.run() returns list[RuleResult] — one result per rule in self._rules. The summary() method then reduces this list to counts. Tracing this contract is step 2 of the 5-step protocol.",
     "Engine.run() returns list[RuleResult]. Engine.summary() reduces to counts."
    ),
    ("q2",
     "You want to add a rule that checks string length. What is the correct approach?",
     [("Add an if/elif branch inside Engine.run() for the new check.", False),
      ("Subclass Rule and implement check() to return a RuleResult.", True),
      ("Monkey-patch Rule.check() at runtime.", False),
      ("Add a new method directly to the Engine class.", False)],
     "Correct. The extension point is the Rule base class. Subclassing preserves the invariant (check() returns RuleResult) and works with the existing Engine without modification.",
     "Rule is the extension point. Subclass it and implement check(). Never modify Engine — that breaks the open/closed principle."
    ),
    ("q3",
     "What invariant must every Rule.check() implementation preserve?",
     [("It must print a log message.", False),
      ("It must return a RuleResult and must not raise an exception.", True),
      ("It must accept both list[dict] and DataFrame inputs.", False),
      ("It must run in under 1 second.", False)],
     "Correct. Engine.run() and Engine.summary() assume every check() returns a RuleResult. If a Rule raises instead of returning, the engine breaks. This is the invariant you must preserve when adding new rules.",
     "Every Rule.check() must return a RuleResult and never raise. Engine.summary() depends on this contract."
    )
],
"\"You're given this codebase in Phase 1. Walk me through how you'd approach adding a new rule type.\"",
"step 1: find the entry point — Engine.run() calls rule.check() for each rule → step 2: trace the contract — check() takes list[dict], returns RuleResult → step 3: extension point — Rule base class, subclass and implement check() → step 4: invariant — check() always returns RuleResult, never raises → step 5: implement by subclassing Rule — don't touch Engine → then test immediately on a small fixture: 2 passing rows, 2 failing → say: 'I'm reusing the existing extension point rather than modifying Engine, which keeps the contract intact'",
TEARDOWN_16
))

# ── 17 Genie Code Prompting ──────────────────────────────────────────────────
SETUP_17 = """# Lesson 17 setup — prepare the "bad AI output" audit exercise
# No Delta tables needed; this lesson is about prompting and auditing AI output

print("Lesson 17: AI Stewardship")
print("This lesson is about Genie Code prompting and hallucination audits.")
print("The 'setup' here is reading the deliberately flawed code below.")
print()

bad_code = '''
# "Genie Code suggestion" — audit this before running

sc = spark.sparkContext                                    # line 1
df = spark.table("workspace.default.orders")
n = df.rdd.getNumPartitions()                              # line 3
df = df.cache()                                            # line 4
spark.conf.set("spark.sql.turboJoin.enabled", "true")      # line 5
from pyspark.sql.functions import udf
strip_udf = udf(lambda s: s.strip())                       # line 7
df = df.withColumn("category", strip_udf("raw_category"))
result = df.join(
    spark.table("workspace.default.products"), "product_id"  # line 10
)
rows = result.collect()                                    # line 11
'''
print("Genie's suggestion:")
print(bad_code)
print("Your task: find ALL the problems before running a single cell.")
"""

TEARDOWN_17 = """# Teardown lesson 17 — nothing to clean up
print("No cleanup needed for lesson 17.")
"""

LESSONS.append((17,
"Genie Code Prompting &amp; Hallucination Audits",
"You are the pilot — use AI to go faster, then visibly catch its mistakes",
["AI Stewardship"],
"16-reading-code-you-didnt-write.html", "16: Reading Code",
"18-common-spark-errors.html", "18: Common Errors",
"07-AI-Stewardship-Genie-Code.md",
SETUP_17,
"""
<section>
<h2>The win condition</h2>
<p>The interview scores <strong>AI Stewardship</strong> explicitly. The win condition is not "don't use AI" or "use AI for everything" — it's <strong>use it fast, then visibly catch its mistakes.</strong></p>
<div class="box i"><strong>Say this while prompting</strong>"I'll ask Genie for a first pass, then I'll check the join strategy myself in the plan." That sentence shows the grader you're the pilot.</div>
</section>

<section>
<h2>Good prompt anatomy</h2>
<pre># WEAK prompt — vague, no constraints, hard to audit
# "Make this faster"

# STRONG prompt — specific goal + constraints + ask for explanation
prompt = (
    "Rewrite this join to use a broadcast hint on the products table. "
    "We're on Databricks serverless so don't use .cache() or sparkContext. "
    "After the code, explain in one sentence why this is faster."
)
# Constraints in prompts = auditable output
# "explain why" = you get the customer translation for free</pre>
</section>

<section>
<h2>The 6-point audit checklist</h2>
<pre># For EVERY Genie output, run through this before executing:

# 1. Does it RUN? Test on a small slice first
#    result.limit(100).show()  -- not result.count() on the full table

# 2. Is it CORRECT? Spot-check row counts and values
#    print(result.count())   # does it match expectations?

# 3. Is it EFFICIENT? Look for:
#    - Needless shuffle (unnecessary groupBy, orderBy, distinct)
#    - Python UDF where a built-in exists
#    - .collect() on a large DataFrame

# 4. Is it SERVERLESS-LEGAL? Forbidden on Free Edition:
#    - spark.sparkContext
#    - df.rdd.*
#    - df.cache() / df.persist()
#    - spark.conf.set with non-whitelisted configs

# 5. Are the APIS REAL? Invented config keys are common
#    Real AQE keys: spark.sql.adaptive.enabled
#                   spark.sql.adaptive.skewJoin.enabled
#                   spark.sql.autoBroadcastJoinThreshold
#    Real shuffle key: spark.sql.shuffle.partitions

# 6. Did it PRESERVE THE CONTRACT? (Phase 1)
#    Same function signatures? Same return types? Extension points kept?
print("Checklist ready.")</pre>
</section>

<section>
<h2>Audit the bad code from setup</h2>
<pre># Re-read the bad_code from setup. Find all 7 problems before looking below.
# Hint: each numbered comment is a clue.

issues = {
    "line 1":  "spark.sparkContext — NOT available on serverless/Free Edition",
    "line 3":  "df.rdd.getNumPartitions() — RDD API unavailable on serverless. Use spark_partition_id() groupBy",
    "line 4":  "df.cache() — restricted on serverless. Materialize to Delta instead",
    "line 5":  "spark.sql.turboJoin.enabled — MADE UP config key, does not exist",
    "line 7":  "udf(lambda s: s.strip()) — use F.trim() built-in; UDF is slow + missing return type",
    "line 10": "join to products with no broadcast hint — products is likely small, should be broadcast(products)",
    "line 11": "result.collect() — pulls ALL rows to driver. Use show(n) or write() instead",
}
for line, issue in issues.items():
    print(f"{line}: {issue}")</pre>
<div class="box"><strong>Say this out loud during the interview</strong>"Genie suggested .cache() — but we're on serverless where that's restricted, so I'll materialize to Delta instead." That single sentence demonstrates three signals at once: AI Stewardship, Code Stewardship, serverless awareness.</div>
</section>
""",
[
    ("q1",
     "Genie outputs a config: spark.conf.set('spark.sql.fastMerge.enabled', 'true'). What do you do?",
     [("Accept it — Genie knows the Spark config namespace.", False),
      ("Flag it as a likely hallucination. Real AQE/optimization configs have known names. If you don't recognize it, look it up before running.", True),
      ("Run it and check if performance improves.", False),
      ("Ask Genie to explain the config — if the explanation sounds right, accept it.", False)],
     "Correct. Invented config keys are one of the most common AI hallucinations. fastMerge.enabled does not exist. Real config keys: spark.sql.adaptive.enabled, spark.sql.autoBroadcastJoinThreshold, spark.sql.shuffle.partitions. When in doubt, check the docs.",
     "Made-up config keys are a top hallucination pattern. If you don't recognize it, doubt it and check docs."
    ),
    ("q2",
     "Genie writes a prompt reply with df.cache() in the solution. You're on Databricks Free Edition. What do you say?",
     [("Accept it — cache() might work on newer Free Edition clusters.", False),
      ("'Genie suggested .cache() but that's restricted on serverless — I'll materialize to Delta instead: write to a table, read back.' Then fix it.", True),
      ("Skip the cache() line and move on without mentioning it.", False),
      ("Wrap the cache() in a try/except to handle if it fails.", False)],
     "Correct. Calling this out explicitly scores points for AI Stewardship AND serverless awareness. The fix is Delta materialization. Silently skipping it misses the scoring opportunity.",
     "Say it out loud: 'cache() is serverless-restricted — materializing to Delta instead.' This scores AI Stewardship."
    ),
    ("q3",
     "What's the single most important behavior during Genie prompting that the interview grades?",
     [("Using as many Genie prompts as possible to show AI fluency.", False),
      ("Visibly auditing Genie's output — running the checklist out loud, catching mistakes, explaining why they're wrong.", True),
      ("Writing longer, more detailed prompts.", False),
      ("Avoiding Genie to show you can code without AI.", False)],
     "Correct. The graders explicitly want to see you as the pilot. Audit visibly: 'Let me check the join strategy in the plan... this should be broadcast... and this collect() should be a show(). Let me fix those.' Correcting Genie out loud is the win condition.",
     "Visibly auditing — running the checklist out loud and catching mistakes — is the graded behavior."
    )
],
"\"Walk me through how you'd use Genie Code in Phase 2 to fix a slow join.\"",
"prompt with constraints: 'rewrite this join to broadcast the products table — we're on serverless, no cache()' → run Genie → before executing: audit the output — does it run? is the join strategy right in explain()? any serverless-illegal code? any invented config keys? any collect() pulling to driver? → if Genie adds cache(): say out loud 'cache() is serverless-restricted, I'll materialize to Delta instead' → run on a small slice first, verify row counts, then apply to full dataset → own the final answer: 'I can explain every line whether I wrote it or Genie did'",
TEARDOWN_17
))

# ── 18 Common Spark Errors ───────────────────────────────────────────────────
SETUP_18 = """# Lesson 18 setup — create tables that will trigger real Spark errors
from pyspark.sql import functions as F
from pyspark.sql.types import *
import datetime, random

spark.sql("CREATE DATABASE IF NOT EXISTS lesson_db")

# table with potential ambiguous column names
orders = spark.createDataFrame(
    [(i, random.randint(1,10), float(i*5)) for i in range(1000)],
    ["id","customer_id","amount"]
)
orders.write.mode("overwrite").format("delta").saveAsTable("lesson_db.err_orders")

customers = spark.createDataFrame(
    [(i, f"Customer {i}") for i in range(1,11)],
    ["id","name"]   # note: 'id' collides with orders.id
)
customers.write.mode("overwrite").format("delta").saveAsTable("lesson_db.err_customers")

print("Setup complete: lesson_db.err_orders, lesson_db.err_customers")
print("Both tables have a column named 'id' — ready to trigger ambiguous column errors.")
"""

TEARDOWN_18 = """# Teardown lesson 18
spark.sql("DROP TABLE IF EXISTS lesson_db.err_orders")
spark.sql("DROP TABLE IF EXISTS lesson_db.err_customers")
spark.sql("DROP DATABASE IF EXISTS lesson_db")
print("Done.")
"""

LESSONS.append((18,
"Common Spark Errors &amp; Debugging",
"The 4-step recovery script — what they grade under Resilience",
["Resilience"],
"17-genie-code-prompting.html", "17: Genie Code",
"19-narrating-out-loud.html", "19: Narrating Out Loud",
"15-Common-Spark-Errors-Debug.md",
SETUP_18,
"""
<section>
<h2>The 4-step recovery script (say it every time)</h2>
<ol>
<li><strong>Read the error out loud.</strong> "It says AnalysisException: cannot resolve 'amounts'..." Naming the error is half the fix.</li>
<li><strong>State a hypothesis.</strong> "I'm guessing the column is 'amount', not 'amounts'."</li>
<li><strong>Test the smallest possible thing.</strong> <code>df.columns</code> or <code>df.limit(1).show()</code></li>
<li><strong>Narrate the fix and side-effects.</strong> "I'll change it to 'amount' — and check there are no other typos."</li>
</ol>
<div class="box i"><strong>Resilience is graded</strong>A clean recovery scores HIGHER than never failing. The sequence read → hypothesize → test → narrate is what they're watching for.</div>
</section>

<section>
<h2>Error 1: AnalysisException — ambiguous column</h2>
<pre>from pyspark.sql import functions as F
orders    = spark.table("lesson_db.err_orders")    # has col 'id'
customers = spark.table("lesson_db.err_customers") # also has col 'id'

# This FAILS: Reference 'id' is ambiguous (both tables have 'id')
try:
    bad = orders.join(customers, orders.id == customers.id).select("id","name","amount")
    bad.show()
except Exception as e:
    print("Error:", type(e).__name__, str(e)[:120])

# FIX 1: use string join key (collapses to one 'id' column)
# Only works when the key has the same name on both sides
# BUT here the semantic meaning differs (orders.id = order ID, customers.id = customer ID)
# So the right join is on customer_id, not id:
fixed = orders.join(customers, orders.customer_id == customers.id).select(
    orders.id.alias("order_id"), customers.name, orders.amount
)
fixed.show(5)</pre>
</section>

<section>
<h2>Error 2: column typo (AnalysisException: cannot resolve)</h2>
<pre">try:
    orders.select(F.col("amounts")).show()  # typo: 'amounts' not 'amount'
except Exception as e:
    print("Error:", str(e)[:100])

# 4-step recovery:
# 1. Read: "cannot resolve 'amounts' given input columns [id, customer_id, amount]"
# 2. Hypothesis: column name typo — it's 'amount' not 'amounts'
# 3. Test: print the actual columns
print("Actual columns:", orders.columns)
# 4. Fix:
orders.select(F.col("amount")).show(3)</pre>
</section>

<section>
<h2>Error 3: collect() OOM — the silent danger</h2>
<pre># Demonstrate why collect() is dangerous
# (We'll simulate with a small table, but in prod this OOMs the driver)
print("Safe: limit + show")
orders.limit(5).show()

print("Safe: write to Delta (distributed)")
orders.write.format("noop").mode("overwrite").save()

print("Dangerous (don't run on large data):")
print("  rows = orders.collect()  ← pulls ALL rows to driver")
print("  Use show(n) or write() instead")</pre>
</section>

<section>
<h2>The diagnostic checklist</h2>
<pre># When something is wrong and you don't know what:
orders.printSchema()                     # are types what you expect?
orders.limit(5).show(truncate=False)    # what does a real row look like?

# Count before/after each transformation
print("Before filter:", orders.count())
filtered = orders.where(F.col("amount") > 100)
print("After filter:", filtered.count())    # should drop

# Check partition distribution (skew?)
orders.groupBy(F.spark_partition_id().alias("pid")).count().orderBy(F.desc("count")).show(5)

# Read the plan
filtered.explain("formatted")</pre>
</section>
""",
[
    ("q1",
     "You get: AnalysisException: Reference 'id' is ambiguous, could be: id#1, id#2. What's the cause and fix?",
     [("A column named 'id' has duplicate values.", False),
      ("Both sides of the join have a column named 'id'. Fix: alias them (orders.id.alias('order_id')) or use a string join key that collapses to one column.", True),
      ("'id' is a reserved keyword in Spark SQL.", False),
      ("The DataFrame was created with duplicate column definitions.", False)],
     "Correct. Both tables have 'id' so Spark can't determine which one you mean. Fix options: use df1.id vs df2.id explicitly and alias, or join on a string key which collapses duplicates, or rename a column before joining.",
     "Ambiguous reference = same column name on both sides of a join. Fix: use df1.col vs df2.col explicitly, then alias."
    ),
    ("q2",
     "You hit an error. Describe the 4-step recovery sequence in order.",
     [("Fix → test → document → move on.", False),
      ("Read the error out loud → state a hypothesis → test the smallest thing → narrate the fix and side-effects.", True),
      ("Google the error → copy a Stack Overflow answer → run it → hope it works.", False),
      ("Ask the interviewer what's wrong → let them suggest a fix → implement it.", False)],
     "Correct. Read → hypothesize → test small → narrate fix. This sequence is exactly what the Resilience signal grades. A clean recovery using this sequence scores higher than never failing.",
     "Read → hypothesize → test small → narrate fix. This is the graded recovery sequence."
    ),
    ("q3",
     "A job runs without an error but the count is 10% lower than expected after a join. What do you check first?",
     [("Re-run the job — it was probably a transient error.", False),
      ("Check the join type (inner vs left vs right). An inner join silently drops rows that don't match on both sides.", True),
      ("Check if the cluster ran out of memory.", False),
      ("Check the Spark UI for failed tasks.", False)],
     "Correct. Silent wrong counts after joins are almost always a join type issue. An inner join drops non-matching rows silently. If you expected to keep all rows from the left table, you need a left join. Always print counts before and after joins during development.",
     "Silent count drop after join = inner vs left/right mismatch. Check join type first."
    )
],
"\"You run a join and the result has 15% fewer rows than expected. No error was thrown. Walk me through your diagnosis.\"",
"1) read: no error but row count is off → 2) hypothesis: join type is wrong — inner join is dropping non-matching rows → 3) test smallest thing: print left.count(), right.count(), joined.count() and compare → also check: left.select('customer_id').distinct().count() vs right.select('customer_id').distinct().count() — any keys in left but not right? → 4) fix: switch inner join to left join if you want to keep all left rows → also check for null keys and duplicate rows on either side of the join",
TEARDOWN_18
))

# ── 19 Narrating Out Loud ────────────────────────────────────────────────────
SETUP_19 = """# Lesson 19 — the meta-skill: narrating out loud
# No Delta tables needed. This lesson is a full dress rehearsal.

print("Lesson 19: Narrating Out Loud")
print("This is the meta-skill that the interview explicitly grades above all others.")
print()
print("Session structure:")
print("  Part 1: understand WHY narration matters and what it covers")
print("  Part 2: timed drill — narrate 5 Phase-2 scenarios out loud")
print("  Part 3: timed drill — narrate 3 Phase-1 scenarios out loud")
print()
print("Target: each narration fluent in under 45 seconds.")
"""

TEARDOWN_19 = """print("Lesson 19 complete. You now have all the tools for the interview.")
print()
print("Final checklist:")
print("  [ ] Signed up for Databricks Free Edition")
print("  [ ] Practiced opening a notebook and running PySpark")
print("  [ ] Can explain all 6 optimization levers without notes")
print("  [ ] Can do the 4-step error recovery without prompting")
print("  [ ] Have used Genie Code at least once")
print()
print("The one rule: NARRATE. Think out loud. They're hiring someone to")
print("sit next to a customer — show them that person.")
"""

LESSONS.append((19,
"Narrating Out Loud",
"The meta-skill that beats everything — practice until it's reflex",
["All four signals"],
"18-common-spark-errors.html", "18: Common Errors",
"", "",
"01-Interview-Overview-and-Strategy.md",
SETUP_19,
"""
<section>
<h2>Why narration is the highest-leverage skill</h2>
<p>The recruiter email states it directly: <em>"Think Out Loud: This is the most important requirement."</em></p>
<p>The reason: FDEs sit next to customers. A solution delivered in silence provides no insight into reasoning, no ability to course-correct, no demonstration of the collaborative thinking the role requires. A slightly-wrong answer with clear audible reasoning — catching yourself and recovering — scores higher than a silent correct answer.</p>
<div class="box i"><strong>Dead air is the enemy.</strong>Every second of silence during a Spark optimization is a missed scoring opportunity.</div>
</section>

<section>
<h2>What narration actually covers</h2>
<ul>
<li><strong>Before running anything:</strong> "Here's what I think this code is doing... my hypothesis about the bottleneck is..."</li>
<li><strong>While reading the plan:</strong> "I see two Exchange nodes which means two shuffles — the biggest cost is probably here..."</li>
<li><strong>While writing code:</strong> "I'm adding broadcast() on the products table because it's tiny — I want to verify that assumption by checking the count..."</li>
<li><strong>When stuck:</strong> "I'm spending more than 2 minutes on this import — can we bail out and come back?"</li>
<li><strong>Business translation:</strong> "The customer impact of this change is... instead of reshuffling 100M rows..."</li>
<li><strong>After using Genie:</strong> "Genie gave me this — before I trust it, let me check the join strategy and make sure there's no serverless-illegal code..."</li>
</ul>
</section>

<section>
<h2>Phase 2 time budget (25 min)</h2>
<pre># Internalize this clock — say it before Phase 2 starts
time_budget = {
    "profile and understand": "~3 min",
    "state plan, rank by impact": "~3 min",
    "implement fixes, biggest first": "~15 min",
    "summarize wins + business translation": "~4 min",
}
for phase, budget in time_budget.items():
    print(f"  {budget}: {phase}")

# The 30-second opening statement (say this at the start of Phase 2)
opening = (
    "Let me rank by impact. Shuffles cost the most, so first I'll check the joins --\n"
    "is anything a sort-merge that should be a broadcast? Then I'll make sure we filter\n"
    "and select columns before the heavy work. Then partitioning and any Python UDFs.\n"
    "Skew and recomputation last, if the profile points there. I'll confirm each guess\n"
    "in the query profile rather than assume."
)
print(opening)</pre>
</section>

<section>
<h2>Phase 1 time budget (25 min)</h2>
<pre>time_budget_p1 = {
    "read and restate existing code + contract": "~5 min",
    "plan the feature, name edge cases": "~3 min",
    "implement + quick test": "~13 min",
    "walk through what you did, what you'd add with more time": "~4 min",
}
for phase, budget in time_budget_p1.items():
    print(f"  {budget}: {phase}")

# The opening statement for Phase 1
opening_p1 = (
    "Before I change anything I want to find the entry point and restate what this\n"
    "code does -- what goes in, what comes out. Then I'll find where it's designed\n"
    "to be extended. Let me think out loud while I read it."
)
print(opening_p1)</pre>
</section>

<section>
<h2>Things that score points (say these)</h2>
<pre"># Commit these phrases to muscle memory
scoring_lines = [
    "Before I optimize, let me look at the query profile to see where time actually goes.",
    "This is a wide transformation, so it triggers a shuffle — that's almost always the most expensive thing here.",
    "Let me check assumptions: is the right side small enough to broadcast? If it's under ~100 MB, yes.",
    "I'll write to a Delta table here rather than recompute — this DataFrame is used multiple times downstream.",
    "Genie suggested .cache() — but we're on serverless where that's restricted, so I'll materialize to Delta instead.",
    "Can we bail out on this syntax issue? I want to keep the budget on the harder problem.",
]
for line in scoring_lines:
    print(f"  • {line}")
    print()</pre>
</section>
""",
[
    ("q1",
     "The recruiter email says 'Think Out Loud: This is the MOST IMPORTANT requirement.' Why does narration matter more than getting the right answer?",
     [("It doesn't — a correct answer always beats a narrated wrong one.", False),
      ("FDEs work with customers who need to understand the reasoning, not just the output. An FDE who thinks silently is untrustworthy in front of a customer — the interview tests the collaborative behavior, not just the answer.", True),
      ("Narration helps the interviewer understand what the candidate is typing.", False),
      ("It's just a convention of pair-programming interviews.", False)],
     "Correct. The FDE role requires explaining reasoning to customers in real time. Demonstrating that behavior — even on a wrong path that self-corrects — shows the graders they can trust you next to a customer account.",
     "FDEs sit with customers. The interview tests the behavior, not just the answer. Visible reasoning = trustworthy."
    ),
    ("q2",
     "You're 3 minutes into Phase 2 and stuck on a Python import error. What do you do?",
     [("Keep debugging until it's resolved — don't move on with a broken import.", False),
      ("Use the bail-out: 'I'm spending more than 2 minutes on this import — can we skip and come back? I want to keep the budget on the harder problem.'", True),
      ("Ask Genie to fix the import.", False),
      ("Apologize and skip it without saying anything.", False)],
     "Correct. The bail-out is explicitly offered in the interview email. Using it shows prioritization — a senior engineering behavior. Burning 5 minutes on a comma when there's a major optimization to find is poor time management.",
     "The bail-out is offered. Using it shows good prioritization. 'I want to keep the budget on the harder problem' is a scoring phrase."
    ),
    ("q3",
     "After fixing a broadcast join, what should you say to the interviewer?",
     [("Nothing — the code speaks for itself.", False),
      ("The business translation: 'Instead of reshuffling 100M rows across the cluster, we send the tiny lookup table to every machine. That's the difference between a 40-minute nightly job and a few minutes.'", True),
      ("A technical explanation of how BroadcastHashJoin works internally.", False),
      ("Ask if the fix is correct before explaining it.", False)],
     "Correct. Every fix should end with a customer-translation. The business version of the optimization is what an FDE would tell a customer — it demonstrates both Code Stewardship and Computational Thinking.",
     "Always customer-translate your fixes. The business impact sentence is a direct scoring opportunity."
    )
],
"\"The interview is starting. Walk me through your first 60 seconds of Phase 2.\"",
"'Let me read this end to end first and tell you what I think it's doing...' [read aloud] '...so the intent is X, inputs are Y, the output goes to Z. My hunch for the biggest cost is the join here — I can see both sides are large so this is probably a sort-merge join shuffling both sides. Before I touch anything I'll run explain() to confirm and look at the query profile if available. Does that sound like the right starting point?' — this opening demonstrates computational thinking, code stewardship, and customer collaboration in one sentence",
TEARDOWN_19
))

print(f"Defined {len(LESSONS)} lessons (12-19). Writing HTML files...")
for entry in LESSONS:
    (num, title, subtitle, signals,
     prev_file, prev_label, next_file, next_label, source_note,
     setup_code, body_html, quizzes, narrate_q, narrate_a, teardown_code) = entry

    html = render(num, title, subtitle, signals,
                  prev_file, prev_label, next_file, next_label, source_note,
                  setup_code, body_html, quizzes, narrate_q, narrate_a, teardown_code)

    padded = f"{num:02d}"
    slug = re.sub(r'[^a-z0-9]+', '-',
                  title.lower()
                       .replace('&amp;', 'and')
                       .replace('&', 'and')
                       .replace('→', '')
                  ).strip('-')
    filename = f"{padded}-{slug}.html"
    (OUT / filename).write_text(html)
    print(f"  wrote {filename}")

print("\nDone.")
