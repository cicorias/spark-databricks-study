"""
Generate all lesson HTML files for the FDE interview prep teaching system.
Each lesson includes: setup code (creates real tables), content, quizzes, teardown.
Run: uv run python scripts/generate_lessons.py
"""
from __future__ import annotations
import re
from pathlib import Path

ROOT = Path(__file__).parent.parent
OUT = ROOT / "teach" / "lessons"
OUT.mkdir(parents=True, exist_ok=True)

# ── shared CSS / JS ──────────────────────────────────────────────────────────

HEAD_CSS = """
:root{--bg:#0f1117;--sur:#1a1d27;--bdr:#2e3147;--acc:#ff3621;--grn:#00a972;--txt:#e8eaf0;--mut:#8b8fa8;--cb:#12151e;--amb:#e8a020;--blu:#4a90e2}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--txt);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;font-size:15px;line-height:1.7;padding:2rem 1rem}
.page{max-width:800px;margin:0 auto}
.lh{margin-bottom:2rem}
.ln{color:var(--acc);font-size:.78rem;font-weight:700;letter-spacing:.12em;text-transform:uppercase}
h1{font-size:1.85rem;font-weight:800;margin:.2rem 0 .4rem}
.sub{color:var(--mut);font-size:1rem}
.badges{display:flex;gap:.5rem;flex-wrap:wrap;margin-top:.6rem}
.badge{font-size:.68rem;font-weight:600;letter-spacing:.08em;text-transform:uppercase;padding:.2rem .55rem;border-radius:999px;border:1px solid var(--bdr);color:var(--mut)}
.badge.p{border-color:var(--acc);color:var(--acc)}
.nav{display:flex;justify-content:space-between;margin:1.5rem 0;font-size:.84rem;color:var(--mut)}
.nav a{color:var(--grn);text-decoration:none}
section{margin-bottom:2rem}
h2{font-size:1rem;font-weight:700;color:var(--grn);margin-bottom:.6rem;border-bottom:1px solid var(--bdr);padding-bottom:.3rem}
h3{font-size:.95rem;font-weight:600;margin:1rem 0 .3rem;color:var(--txt)}
p{margin-bottom:.8rem}
ul,ol{padding-left:1.4rem;margin-bottom:.8rem}
li{margin-bottom:.3rem}
pre{background:var(--cb);border:1px solid var(--bdr);border-radius:8px;padding:1rem 1.2rem;overflow-x:auto;font-size:.82rem;line-height:1.55;margin-bottom:.9rem;font-family:"JetBrains Mono","Fira Code","Cascadia Code",monospace;white-space:pre}
p code,li code{background:var(--cb);border:1px solid var(--bdr);border-radius:3px;padding:.1em .35em;font-size:.82em;font-family:"JetBrains Mono","Fira Code",monospace}
.box{border-left:3px solid var(--grn);background:var(--sur);border-radius:0 8px 8px 0;padding:.8rem 1.1rem;margin-bottom:.9rem}
.box.w{border-left-color:var(--amb)}
.box.i{border-left-color:var(--acc)}
.box strong{display:block;margin-bottom:.25rem;font-size:.72rem;text-transform:uppercase;letter-spacing:.09em;color:var(--mut)}
/* setup / teardown banners */
.setup-banner,.teardown-banner{border-radius:8px;padding:1rem 1.2rem;margin-bottom:1rem}
.setup-banner{background:#0a1f14;border:1px solid #1a5c38}
.setup-banner h2{color:#00d488;border-color:#1a5c38}
.teardown-banner{background:#1a0e0a;border:1px solid #5c2a1a}
.teardown-banner h2{color:#e87040;border-color:#5c2a1a}
.copy-btn{float:right;background:transparent;border:1px solid var(--bdr);color:var(--mut);font-size:.72rem;padding:.2rem .55rem;border-radius:4px;cursor:pointer;font-family:monospace;margin-bottom:.3rem}
.copy-btn:hover{border-color:var(--grn);color:var(--grn)}
/* quiz */
.quiz{background:var(--sur);border:1px solid var(--bdr);border-radius:10px;padding:1.2rem;margin-bottom:1.2rem}
.quiz h3{margin-top:0;font-size:.93rem}
.opts{list-style:none;padding:0;margin:.8rem 0}
.opts li{padding:.5rem .9rem;border:1px solid var(--bdr);border-radius:7px;margin-bottom:.4rem;cursor:pointer;font-size:.87rem;transition:border-color .15s,background .15s}
.opts li:hover{border-color:var(--grn);background:rgba(0,169,114,.05)}
.opts li.ok{border-color:var(--grn);background:rgba(0,169,114,.12)}
.opts li.no{border-color:var(--acc);background:rgba(255,54,33,.1)}
.fb{display:none;font-size:.84rem;margin-top:.6rem;padding:.6rem .9rem;border-radius:7px}
.fb.show{display:block}
.fb.ok{background:rgba(0,169,114,.1);color:var(--grn)}
.fb.no{background:rgba(255,54,33,.1);color:#ff7a6b}
.pbw{background:var(--sur);border:1px solid var(--bdr);border-radius:999px;height:7px;overflow:hidden;margin-bottom:.3rem}
.pb{height:100%;background:var(--grn);transition:width .4s;border-radius:999px;width:0%}
.pl{font-size:.72rem;color:var(--mut);text-align:right}
.done-box{text-align:center;padding:1.8rem;border:1px solid var(--bdr);border-radius:10px;background:var(--sur);margin-top:1.5rem}
.done-box h2{border:none;color:var(--grn);font-size:1.3rem}
.cmd{display:inline-block;background:var(--cb);border:1px solid var(--grn);border-radius:7px;padding:.5rem 1rem;font-family:monospace;font-size:.87rem;color:var(--grn);margin:.8rem 0}
.nxt-btn{display:inline-block;margin-top:.4rem;padding:.6rem 1.4rem;background:var(--acc);color:white;font-weight:700;font-size:.87rem;border-radius:7px;text-decoration:none}
footer{margin-top:2.5rem;padding-top:.9rem;border-top:1px solid var(--bdr);color:var(--mut);font-size:.78rem;display:flex;justify-content:space-between;flex-wrap:wrap;gap:.4rem}
footer a{color:var(--mut)}
"""

QUIZ_JS = """
const TOTAL=3; let done=0; const answered={};
function ans(el,qid,ok){
  if(answered[qid])return; answered[qid]=true;
  el.classList.add(ok?'ok':'no');
  const fb=document.getElementById(qid+'-fb');
  fb.innerHTML=(ok?'✓ ':'✗ ')+FB[qid][ok?'ok':'no'];
  fb.className='fb show '+(ok?'ok':'no');
  if(ok){done++; document.getElementById('pb').style.width=Math.round(done/TOTAL*100)+'%'; document.getElementById('pl').textContent=done+' / '+TOTAL+' checks';}
  if(done===TOTAL){document.getElementById('done-title').textContent='Lesson complete ✓ — mark it done:';const b=document.getElementById('nxt-btn');if(b){b.style.opacity='1';b.style.pointerEvents='auto';}}
}
function copyCode(id){const el=document.getElementById(id);if(el){navigator.clipboard.writeText(el.textContent).then(()=>{}).catch(()=>{});}}
"""

def esc(s: str) -> str:
    return s.replace('\\', '\\\\').replace('"', '\\"').replace('\n', ' ')

def make_quiz(quizzes: list) -> tuple[str, str]:
    """Returns (quiz_html, fb_js)"""
    html = ""
    fb_parts = []
    for qid, question, options, ok_fb, fail_fb in quizzes:
        items = "\n".join(
            f'        <li onclick="ans(this,\'{qid}\',{str(c).lower()})">{t}</li>'
            for t, c in options
        )
        html += f"""
<div class="quiz" id="{qid}">
  <h3>{question}</h3>
  <ul class="opts">
{items}
  </ul>
  <div class="fb" id="{qid}-fb"></div>
</div>"""
        fb_parts.append(f'  {qid}:{{ok:"{esc(ok_fb)}",no:"{esc(fail_fb)}"}}')
    fb_js = "const FB={\n" + ",\n".join(fb_parts) + "\n};"
    return html, fb_js


def render(num: int, title: str, subtitle: str, signals: list[str],
           prev_file: str, prev_label: str,
           next_file: str, next_label: str,
           source_note: str,
           setup_code: str,
           body_html: str,
           quizzes: list,
           narrate_q: str, narrate_a: str,
           teardown_code: str) -> str:

    padded = f"{num:02d}"
    badge_html = "".join(
        f'<span class="badge{" p" if i == 0 else ""}">{s}</span>'
        for i, s in enumerate(signals)
    )
    prev_nav = f'<a href="{prev_file}">← {prev_label}</a>' if prev_file else '<span>← Curriculum</span>'
    next_nav = f'<a href="{next_file}">{next_label} →</a>' if next_file else '<span>All lessons done 🎉</span>'
    quiz_html, fb_js = make_quiz(quizzes)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Lesson {padded} — {title}</title>
<style>{HEAD_CSS}</style>
</head>
<body><div class="page">

<div class="lh">
  <div class="ln">Lesson {padded} of 19</div>
  <h1>{title}</h1>
  <p class="sub">{subtitle}</p>
  <div class="badges">{badge_html}</div>
</div>

<div class="pbw"><div class="pb" id="pb"></div></div>
<p class="pl" id="pl">0 / 3 checks</p>

<div class="nav">{prev_nav}{next_nav}</div>

<!-- ── SETUP ────────────────────────────────────────────────────── -->
<section class="setup-banner">
<h2>🛠 Setup — run this cell first</h2>
<p>Paste into a Databricks notebook cell and run before working through the examples below.</p>
<button class="copy-btn" onclick="copyCode('setup-code')">copy</button>
<pre id="setup-code">{setup_code}</pre>
</section>

<!-- ── LESSON BODY ──────────────────────────────────────────────── -->
{body_html}

<!-- ── QUIZZES ──────────────────────────────────────────────────── -->
<section>
<h2>Knowledge checks</h2>
{quiz_html}
</section>

<!-- ── NARRATE IT BACK ─────────────────────────────────────────── -->
<section>
<h2>Narrate it back</h2>
<div class="box i">
  <strong>Interviewer prompt — say this out loud in under 45 seconds</strong>
  {narrate_q}
</div>
<p><em>Cover: {narrate_a}</em></p>
</section>

<!-- ── TEARDOWN ─────────────────────────────────────────────────── -->
<section class="teardown-banner">
<h2>🧹 Teardown — clean up after the lesson</h2>
<button class="copy-btn" onclick="copyCode('teardown-code')">copy</button>
<pre id="teardown-code">{teardown_code}</pre>
</section>

<!-- ── DONE ────────────────────────────────────────────────────── -->
<div class="done-box" id="done-box">
  <h2 id="done-title">Lesson {padded} — when you're ready</h2>
  <p>Pass all three knowledge checks above to unlock the next lesson, or run this now to mark it complete:</p>
  <div class="cmd">mise run teach:done {padded}</div>
  <p style="color:var(--mut);font-size:.82rem;margin-top:.4rem">
    <code>git add teach/ &amp;&amp; git commit -m "lesson {padded} done" &amp;&amp; git push</code>
  </p>
  <br><a href="{next_file}" class="nxt-btn" id="nxt-btn" style="opacity:.45;pointer-events:none">{next_label} →</a>
</div>

<div class="nav" style="margin-top:1.5rem">{prev_nav}{next_nav}</div>
<footer>
  <span>Source: <a href="../../{source_note}">{source_note}</a></span>
  <span>
    <a href="../reference/interview-cheatsheet.html">Cheat Sheet</a> ·
    <a href="https://spark.apache.org/docs/3.5.0/api/python/" target="_blank">PySpark 3.5 API</a>
  </span>
</footer>

</div>
<script>
{fb_js}
{QUIZ_JS}
</script>
</body></html>"""


# ── LESSON DATA ──────────────────────────────────────────────────────────────
# Each entry: (num, title, subtitle, signals, prev_file, prev_label,
#              next_file, next_label, source_note,
#              setup_code, body_html, quizzes, narrate_q, narrate_a, teardown_code)

SETUP_SHARED = """# Shared setup — creates Delta tables used across lessons 02-11
# Run once per cluster session; safe to re-run (uses overwrite mode)

from pyspark.sql import functions as F
from pyspark.sql.types import *
import random, datetime

spark.sql("CREATE DATABASE IF NOT EXISTS lesson_db")

# ── fact table: 500k order rows ──────────────────────────────────────────────
orders_schema = StructType([
    StructField("order_id",    LongType(),   False),
    StructField("customer_id", IntegerType(),False),
    StructField("product_id",  IntegerType(),False),
    StructField("country",     StringType(), False),
    StructField("amount",      DoubleType(), False),
    StructField("year",        IntegerType(),False),
    StructField("order_date",  DateType(),   False),
])
rows = []
countries = ["US","US","US","US","GB","DE","FR","BR","IN","JP"]
for i in range(500_000):
    cid = random.randint(1, 200)    # 200 customers, skewed toward low ids
    if cid <= 5: cid = 1            # customer 1 = hot key (skew demo)
    rows.append((
        i, cid, random.randint(1,100),
        countries[random.randint(0,9)],
        round(random.uniform(5.0, 2000.0), 2),
        random.choice([2023,2024,2025]),
        datetime.date(random.choice([2023,2024,2025]),
                      random.randint(1,12), random.randint(1,28))
    ))
orders_df = spark.createDataFrame(rows, orders_schema)
(orders_df.write.mode("overwrite").format("delta")
    .partitionBy("year")
    .saveAsTable("lesson_db.orders"))

# ── dimension table: 100 products (small — broadcast candidate) ──────────────
products_df = spark.createDataFrame(
    [(i, f"Product {i}", ["Electronics","Clothing","Food","Sports"][i%4],
      round(10 + i * 2.5, 2))
     for i in range(1, 101)],
    ["product_id","name","category","list_price"]
)
(products_df.write.mode("overwrite").format("delta")
    .saveAsTable("lesson_db.products"))

# ── customers dimension (200 rows, skewed: customer 1 has most orders) ────────
customers_df = spark.createDataFrame(
    [(i, f"Customer {i}", ["US","GB","DE","FR","BR"][i%5])
     for i in range(1, 201)],
    ["customer_id","name","region"]
)
(customers_df.write.mode("overwrite").format("delta")
    .saveAsTable("lesson_db.customers"))

print("Setup complete. Tables: lesson_db.orders, lesson_db.products, lesson_db.customers")
spark.sql("SELECT year, COUNT(*) as cnt FROM lesson_db.orders GROUP BY year ORDER BY year").show()
"""

TEARDOWN_SHARED = """# Teardown — drop lesson tables when you're done
spark.sql("DROP TABLE IF EXISTS lesson_db.orders")
spark.sql("DROP TABLE IF EXISTS lesson_db.products")
spark.sql("DROP TABLE IF EXISTS lesson_db.customers")
spark.sql("DROP DATABASE IF EXISTS lesson_db")
print("Teardown complete.")
"""

LESSONS = []

# ── 02 ────────────────────────────────────────────────────────────────────────
LESSONS.append((2,
"Partitions &amp; Parallelism",
"The unit of work — partition count determines how fast your job runs",
["Computational Thinking"],
"01-lazy-eval-and-actions.html", "01: Lazy Eval",
"03-narrow-vs-wide-and-the-shuffle.html", "03: Narrow vs Wide",
"03-Spark-Mental-Models.md",
SETUP_SHARED,
"""
<section>
<h2>The model: 1 partition = 1 task = 1 core</h2>
<p>A DataFrame is split into <strong>partitions</strong>. Each partition is processed by one <strong>task</strong> on one <strong>core</strong>. Parallelism ≈ number of partitions running simultaneously on your cluster.</p>
<pre># Check partition distribution (serverless-safe — no RDD API)
from pyspark.sql import functions as F

orders = spark.table("lesson_db.orders")
(orders
  .groupBy(F.spark_partition_id().alias("partition_id"))
  .count()
  .orderBy("partition_id")
  .show())</pre>
<div class="box w"><strong>Serverless gotcha</strong><code>df.rdd.getNumPartitions()</code> fails on Databricks serverless. Always use <code>spark_partition_id()</code> groupBy instead.</div>
</section>

<section>
<h2>Too few vs too many</h2>
<pre># After a groupBy, shuffle creates spark.sql.shuffle.partitions partitions (default 200)
result = orders.groupBy("country").agg(F.sum("amount").alias("total"))
print("Default shuffle partitions:", spark.conf.get("spark.sql.shuffle.partitions"))
result.groupBy(F.spark_partition_id()).count().orderBy("spark_partition_id()").show(10)

# Right-size for small/medium datasets
spark.conf.set("spark.sql.shuffle.partitions", "20")
result2 = orders.groupBy("country").agg(F.sum("amount").alias("total"))
result2.groupBy(F.spark_partition_id()).count().show()</pre>
<p>Compare the output. With default 200 most partitions will be tiny (or empty) for our 500k-row table. With 20 they're better balanced.</p>
</section>

<section>
<h2>coalesce vs repartition</h2>
<pre># coalesce — reduce WITHOUT a shuffle (cheap, narrow)
# Use before writes to avoid many tiny output files
orders.coalesce(4).write.mode("overwrite").format("delta").saveAsTable("lesson_db.orders_compact")
spark.sql("DESCRIBE DETAIL lesson_db.orders_compact").select("numFiles").show()

# repartition — full shuffle (can increase OR decrease)
# Use when you need to co-locate by key for a subsequent join
orders_by_customer = orders.repartition(50, "customer_id")
(orders_by_customer
  .groupBy(F.spark_partition_id())
  .count()
  .orderBy(F.desc("count"))
  .show(5))</pre>
<div class="box"><strong>Interview one-liner</strong>"coalesce is cheap — no shuffle, just merges partitions. repartition does a full shuffle and can grow or shrink. I use coalesce before writes to avoid small-files problems."</div>
</section>
""",
[
    ("q1",
     "Your 500k-row orders table has spark.sql.shuffle.partitions=200. After a groupBy, you have 200 tiny partitions. What's the fastest safe fix on serverless?",
     [("Set spark.sql.shuffle.partitions to a lower number that matches the data size.", True),
      ("Call df.repartition(200) to redistribute.", False),
      ("Use df.cache() to avoid the re-shuffle.", False),
      ("Add more executors to the serverless cluster.", False)],
     "Correct. Lowering shuffle.partitions means the groupBy shuffle creates fewer, larger partitions. AQE also coalesces automatically, but explicit sizing is reliable. repartition would add another shuffle, and cache() is restricted on serverless.",
     "AQE helps but setting shuffle.partitions explicitly is the direct fix. repartition adds a shuffle; cache() is serverless-restricted."
    ),
    ("q2",
     "You're writing a DataFrame with 400 partitions to a Delta table. You want fewer, larger output files without paying for a shuffle. Which call?",
     [("df.repartition(8).write...", False),
      ("df.coalesce(8).write...", True),
      ("spark.conf.set('spark.sql.shuffle.partitions', 8) before write", False),
      ("OPTIMIZE the table after writing.", False)],
     "Correct. coalesce(8) is a narrow (no-shuffle) operation — it merges existing partitions. repartition would do a full shuffle. OPTIMIZE compacts after the fact but you'd still write 400 files first. shuffle.partitions doesn't affect non-shuffle writes.",
     "coalesce = no shuffle, just merges existing partitions. repartition = full shuffle. For pre-write compaction, coalesce is the right call."
    ),
    ("q3",
     "In the lesson_db.orders table, customer_id=1 has far more rows than other customers. You're about to join orders to customers on customer_id. What partition problem might this cause?",
     [("No problem — the join distributes evenly by definition.", False),
      ("Data skew — the partition holding customer_id=1 will be much larger than others, causing one task to run far longer than the rest.", True),
      ("The join will fail due to a null key error.", False),
      ("Too many partitions will be created for customer_id=1.", False)],
     "Correct. We intentionally made customer_id=1 a hot key. In a shuffle join, all rows with the same key end up on the same partition. One giant task = the whole stage waits. This is data skew — covered in Lesson 10.",
     "A hot key means one partition gets far more data than others — data skew. One slow task holds up the whole stage."
    )
],
"\"Your job creates 200 partitions but the dataset is only 50 MB. What's happening and how do you fix it?\"",
"default spark.sql.shuffle.partitions=200 creates 200 post-shuffle partitions regardless of data size → most are near-empty → scheduling overhead for 200 tasks dominates actual compute → fix: lower shuffle.partitions to match data (e.g. 8-20 for small data) → AQE also auto-coalesces these down if adaptive.enabled is true (it is by default)",
TEARDOWN_SHARED + "\nspark.sql('DROP TABLE IF EXISTS lesson_db.orders_compact')"
))

# ── 03 ────────────────────────────────────────────────────────────────────────
LESSONS.append((3,
"Narrow vs Wide &amp; The Shuffle",
"The most important distinction in all of Spark performance",
["Computational Thinking"],
"02-partitions-parallelism.html", "02: Partitions",
"04-jobs-stages-tasks.html", "04: Jobs → Stages → Tasks",
"03-Spark-Mental-Models.md",
SETUP_SHARED,
"""
<section>
<h2>The distinction</h2>
<p><strong>Narrow transform</strong>: each output partition depends on exactly one input partition. Data stays on the same machine. Examples: <code>select</code>, <code>filter</code>, <code>withColumn</code>, <code>union</code>.</p>
<p><strong>Wide transform</strong>: output partitions depend on many input partitions. Data must redistribute across the network by key — this is the <strong>shuffle</strong>. Examples: <code>groupBy</code>, <code>join</code> (non-broadcast), <code>distinct</code>, <code>orderBy</code>.</p>
<div class="box"><strong>One-liner</strong>"Narrow stays on the same machine. Wide shuffles across the network. The shuffle is where time and money go — almost every optimization is 'fewer or smaller shuffles.'"</div>
</section>

<section>
<h2>See it in the plan</h2>
<pre>from pyspark.sql import functions as F
orders = spark.table("lesson_db.orders")

# Narrow — no Exchange in plan
filtered = orders.filter(F.col("year") == 2024).select("order_id", "amount", "country")
print("=== NARROW PLAN ===")
filtered.explain("formatted")

# Wide — Exchange node appears
aggregated = orders.groupBy("country").agg(F.sum("amount").alias("total"))
print("=== WIDE PLAN ===")
aggregated.explain("formatted")
# Look for: Exchange hashpartitioning(country, ...)  ← shuffle boundary</pre>
</section>

<section>
<h2>What a shuffle actually costs</h2>
<pre># Time a narrow vs wide operation on the same data
import time

# Narrow: filter + select (no shuffle)
t0 = time.time()
orders.filter(F.col("year") == 2025).select("order_id","amount").write.format("noop").mode("overwrite").save()
print(f"Narrow (filter+select): {time.time()-t0:.2f}s")

# Wide: groupBy country (shuffle required)
t0 = time.time()
orders.groupBy("country").agg(F.sum("amount")).write.format("noop").mode("overwrite").save()
print(f"Wide (groupBy): {time.time()-t0:.2f}s")</pre>
</section>

<section>
<h2>The hidden shuffle trap: unnecessary orderBy</h2>
<pre># BAD: orderBy on a large table = full shuffle + global sort
# Only do this when the downstream system requires sorted output
expensive = (orders
  .groupBy("country").agg(F.sum("amount").alias("total"))
  .orderBy(F.desc("total")))   # ← adds a second Exchange node
expensive.explain("formatted")
# You'll see TWO Exchange nodes: one for groupBy, one for orderBy

# GOOD: just get the result, sort in Python/display if needed
result = (orders
  .groupBy("country").agg(F.sum("amount").alias("total")))
result.show()  # display is sorted by show() without a cluster-wide sort</pre>
<div class="box w"><strong>Interview trap</strong>Adding <code>orderBy</code> "to make output readable" on a 100M-row table adds the most expensive operation in the job. Only sort when the downstream system requires it.</div>
</section>
""",
[
    ("q1",
     "Which pair are both wide transformations?",
     [("filter + withColumn", False),
      ("groupBy + join (non-broadcast)", True),
      ("select + union", False),
      ("filter + select", False)],
     "Correct. groupBy and join (non-broadcast) are both wide — they must redistribute data by key across the cluster. filter, withColumn, select, and union are narrow — each partition processes independently.",
     "Wide = requires a shuffle. groupBy and non-broadcast join both need to redistribute rows by key. filter, select, withColumn, and union are narrow."
    ),
    ("q2",
     "You see two Exchange nodes in df.explain(). What does this mean for stage count?",
     [("1 stage", False),
      ("2 stages", False),
      ("3 stages", True),
      ("Depends on partition count", False)],
     "Correct. Each Exchange (shuffle boundary) separates one stage from the next. N Exchange nodes = N+1 stages. 2 Exchange nodes = 3 stages: before first shuffle, between shuffles, after second shuffle.",
     "N Exchange nodes = N+1 stages. Stages are cut at every shuffle boundary."
    ),
    ("q3",
     "orders.groupBy('country').agg(sum('amount')).orderBy(desc('total')) — how many shuffles does this produce?",
     [("0 — Catalyst removes unnecessary shuffles.", False),
      ("1 — only the groupBy shuffles.", False),
      ("2 — groupBy produces one shuffle, orderBy produces a second.", True),
      ("3 — one per transformation.", False)],
     "Correct. groupBy needs a shuffle to co-locate rows by country. orderBy needs a separate shuffle to globally sort the aggregated result. Running explain() will show two Exchange nodes.",
     "groupBy = 1 shuffle (Exchange). orderBy = another shuffle (Exchange). Total = 2 shuffles, 3 stages."
    )
],
"\"Why is filter cheap but groupBy expensive? Explain it as if I'm a non-technical customer.\"",
"filter is narrow — each machine processes its own partition independently, no coordination needed → groupBy is wide — to sum amounts by country, ALL rows for each country must land on the same machine, so Spark redistributes every row across the network first (the shuffle) → the shuffle writes to disk, crosses the network, re-reads — that's where time and money go → every Spark optimization is about doing fewer or smaller shuffles",
TEARDOWN_SHARED
))

# ── 04 ────────────────────────────────────────────────────────────────────────
LESSONS.append((4,
"Jobs → Stages → Tasks",
"Reading the execution hierarchy so you can find where time actually goes",
["Computational Thinking"],
"03-narrow-vs-wide-and-the-shuffle.html", "03: Narrow vs Wide",
"05-catalyst-and-aqe.html", "05: Catalyst &amp; AQE",
"03-Spark-Mental-Models.md",
SETUP_SHARED,
"""
<section>
<h2>The hierarchy</h2>
<ul>
<li><strong>Job</strong> — one action call (<code>count()</code>, <code>write()</code>, <code>show()</code>). One action = one job.</li>
<li><strong>Stage</strong> — cut at every shuffle boundary (<code>Exchange</code> in the plan). Stages run sequentially within a job.</li>
<li><strong>Task</strong> — one partition processed on one core. All tasks in a stage run in parallel.</li>
</ul>
<pre>from pyspark.sql import functions as F
orders = spark.table("lesson_db.orders")

# This action triggers 1 job. count Exchange nodes = number of stages - 1.
result = (orders
  .filter(F.col("year") == 2024)         # narrow — no Exchange
  .groupBy("country")                    # wide — Exchange 1
  .agg(F.sum("amount").alias("total"))
  .orderBy(F.desc("total")))             # wide — Exchange 2

result.explain("formatted")
# Count the Exchange nodes → add 1 → that's your stage count
result.show()  # action → triggers the job</pre>
</section>

<section>
<h2>Diagnosing where time goes</h2>
<pre># Use explain() to find the expensive stage (most Exchange nodes, biggest data)
# On Databricks: open Query Profile from the cell output after running an action
# It shows wall-clock time per stage and per task — find the bottleneck

# Simulate skew: see what one dominant task looks like
from pyspark.sql import functions as F
skew_check = (orders
  .groupBy("customer_id")
  .agg(F.count("*").alias("order_count"))
  .orderBy(F.desc("order_count")))
skew_check.show(5)
# customer_id=1 has far more orders — if this feeds a join, one task dominates</pre>
<div class="box i"><strong>Scoring move</strong>"Before I optimize, let me look at the Query Profile to see where the time actually goes — I don't want to guess." Say this at the start of Phase 2.</div>
</section>

<section>
<h2>The stage-waits-for-last-task problem</h2>
<pre># A stage finishes when ALL tasks finish. One slow task = whole stage waits.
# Demonstrate: show partition distribution to predict skew
(orders
  .groupBy(F.spark_partition_id().alias("pid"))
  .count()
  .orderBy(F.desc("count"))
  .show(5))
# If one partition has 10x more rows, that task will be 10x slower than others.</pre>
</section>
""",
[
    ("q1",
     "orders.filter(...).groupBy(...).agg(...).write.save(path) — how many jobs, and roughly how many stages?",
     [("1 job, 1 stage", False),
      ("1 job, 2 stages (groupBy = 1 Exchange)", True),
      ("2 jobs (filter + groupBy)", False),
      ("3 jobs (one per transformation)", False)],
     "Correct. write() is one action = one job. The filter is narrow (no Exchange). The groupBy creates one Exchange = 2 stages. Stage 1: scan + filter + partial agg. Stage 2: shuffle + final agg + write.",
     "One action = one job. Stages = shuffle boundaries + 1. filter is narrow (no Exchange), groupBy creates one Exchange = 2 stages."
    ),
    ("q2",
     "A stage has 50 tasks. 49 finish in 3 seconds, one takes 8 minutes. What does this mean for the job?",
     [("The job completes in ~3 seconds since most tasks are done.", False),
      ("The stage (and everything downstream) blocks on that one slow task — total stage time ≈ 8 minutes.", True),
      ("Spark automatically restarts the slow task on a different node.", False),
      ("The 49 fast tasks will help the slow one by sharing partitions.", False)],
     "Correct. A stage completes when its LAST task completes. One skewed partition = one slow task = the entire stage waits. This is why skew is so damaging — it turns a 3-second stage into an 8-minute one.",
     "Stages complete when ALL tasks finish. One outlier task blocks the entire stage and everything downstream."
    ),
    ("q3",
     "You run df.count() in a loop 5 times to log row counts at each step. What's the actual cost?",
     [("One job total — Spark batches repeated actions.", False),
      ("5 jobs — each count() is an action that re-executes the full upstream lineage from scratch.", True),
      ("0 additional cost — Spark caches after the first count().", False),
      ("The cost depends on whether df is persisted.", False)],
     "Correct. Every count() is an action. Spark has no implicit caching — each call re-runs the full lineage. In a loop over 5 stages, you've run the expensive pipeline 5 times for free. Materialize to Delta first.",
     "Each count() = one action = one job = full lineage re-execution. There's no implicit caching between actions."
    )
],
"\"Walk me through what happens when I call orders.groupBy('country').agg(sum('amount')).count()\"",
"groupBy and agg are transformations → build the logical plan → count() is the action → Catalyst optimizes the whole chain → physical plan produced with one Exchange node (groupBy shuffle) → one job submitted → 2 stages cut at the Exchange → Stage 1: scan + partial agg, tasks = one per input partition → Exchange ships shuffle data → Stage 2: final agg, tasks = one per shuffle partition → count result returned to driver",
TEARDOWN_SHARED
))

# ── 05 ────────────────────────────────────────────────────────────────────────
LESSONS.append((5,
"Catalyst &amp; AQE",
"The optimizer that runs before you do — and exactly what it can't fix",
["Computational Thinking"],
"04-jobs-stages-tasks.html", "04: Jobs → Stages → Tasks",
"06-broadcast-joins.html", "06: Broadcast Joins",
"03-Spark-Mental-Models.md",
SETUP_SHARED,
"""
<section>
<h2>Catalyst: optimize before running</h2>
<p>Catalyst rewrites your logical plan before executing anything. Key rewrites:</p>
<ul>
<li><strong>Predicate pushdown</strong> — moves <code>filter</code> to the scan, so Spark reads only matching rows/files</li>
<li><strong>Projection pruning</strong> — removes columns you never use</li>
<li><strong>Constant folding</strong> — evaluates <code>1 + 1</code> at plan time, not per row</li>
<li><strong>Join reordering / strategy selection</strong> — picks broadcast vs sort-merge</li>
</ul>
<pre>from pyspark.sql import functions as F
orders = spark.table("lesson_db.orders")

# You write filter AFTER join — Catalyst moves it BEFORE
plan_demo = (orders
  .join(spark.table("lesson_db.products"), "product_id")
  .where(F.col("year") == 2024))   # written after join
plan_demo.explain("formatted")
# Look for: PushedFilters near Scan node — Catalyst moved the filter down</pre>
</section>

<section>
<h2>AQE: re-optimize at runtime</h2>
<p><strong>Adaptive Query Execution</strong> is on by default. It re-plans using real statistics from completed stages:</p>
<pre># Check AQE is on
print(spark.conf.get("spark.sql.adaptive.enabled"))       # should be true
print(spark.conf.get("spark.sql.adaptive.coalescePartitions.enabled"))  # true

# AQE in action: set high shuffle.partitions, then watch AQE coalesce
spark.conf.set("spark.sql.shuffle.partitions", "200")
result = orders.groupBy("country").agg(F.sum("amount"))
result.explain("formatted")   # look for AdaptiveSparkPlan at top
result.cache()   # force execution so we can check
result.count()
# In Query Profile: actual partitions after AQE coalescing will be < 200

# AQE auto-broadcast: if products side turns out small, AQE promotes to broadcast
orders.join(spark.table("lesson_db.products"), "product_id").explain("formatted")</pre>
</section>

<section>
<h2>What AQE cannot fix</h2>
<pre>from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Python UDF: AQE cannot help here — data serializes row-by-row to Python
upper_udf = udf(lambda s: s.upper() if s else None, StringType())
slow_df = orders.withColumn("country_upper", upper_udf("country"))
slow_df.explain("formatted")   # no optimization visible for the UDF step

# Built-in: stays inside Tungsten/Photon, Catalyst can optimize
fast_df = orders.withColumn("country_upper", F.upper("country"))
fast_df.explain("formatted")   # identical result, far faster execution</pre>
<div class="box w"><strong>AQE cannot</strong>: avoid a shuffle you introduced unnecessarily · fix a Python UDF · push a filter you placed after an expensive join · optimize inside arbitrary Python code</div>
</section>
""",
[
    ("q1",
     "You write orders.join(products,'product_id').where(col('year')==2024). What does Catalyst actually do with the filter?",
     [("Runs the join first, then applies the filter on the result.", False),
      ("Pushes the filter down to the scan so only 2024 rows flow into the join.", True),
      ("Removes the filter since it's redundant after the join.", False),
      ("AQE decides at runtime whether to push the filter.", False)],
     "Correct. Catalyst's predicate pushdown moves the filter to execute at the scan layer before the join. Check explain() and you'll see PushedFilters: [EqualTo(year,2024)] near the Scan node, not after the join.",
     "Catalyst predicate pushdown moves filters toward the scan regardless of where you wrote them."
    ),
    ("q2",
     "AQE is on. spark.sql.shuffle.partitions=200. After a groupBy on a 500k-row table, how many actual output partitions will there likely be?",
     [("Always 200, regardless of AQE.", False),
      ("Fewer than 200 — AQE coalesces tiny post-shuffle partitions together.", True),
      ("1 — AQE always reduces to a single partition.", False),
      ("AQE doesn't affect post-shuffle partition count.", False)],
     "Correct. With AQE coalescePartitions enabled (default), AQE reads real statistics after the shuffle and merges tiny partitions together. For a 500k-row table with 200 partitions most are near-empty — AQE coalesces them to something reasonable.",
     "AQE coalesces tiny shuffle partitions using real statistics from the completed shuffle stage."
    ),
    ("q3",
     "A Python UDF does the same thing as F.upper(). AQE is on. Will AQE fix the performance gap?",
     [("Yes — AQE optimizes all expressions including UDFs.", False),
      ("No — AQE works on execution statistics (partition sizes, row counts). It cannot replace Python code with a built-in.", True),
      ("Partially — AQE will optimize the UDF on small partitions.", False),
      ("Yes — Photon accelerates Python UDFs in Databricks.", False)],
     "Correct. AQE optimizes the plan structure — partition coalescing, join strategy, skew splits. It cannot inspect or replace your Python code. The UDF still serializes every row to Python and back, regardless of AQE.",
     "AQE works on plan structure and partition statistics. It cannot replace Python UDF logic with a built-in."
    )
],
"\"What does Catalyst do, and why can you write a filter after a join and still have it apply at the scan?\"",
"Catalyst is Spark's query optimizer — it rewrites the logical plan before execution → predicate pushdown moves filters toward the scan layer regardless of where you wrote them → Catalyst sees the whole chain so it knows the filter is independent of the join → in explain() you see PushedFilters near the Scan node → this is why declarative DataFrame/SQL beats imperative Python loops: Catalyst can only help you if you express intent as a plan",
TEARDOWN_SHARED
))

# ── 06 ────────────────────────────────────────────────────────────────────────
LESSONS.append((6,
"Broadcast Joins",
"Lever 1: eliminate the big-side shuffle entirely",
["Code Stewardship", "Computational Thinking"],
"05-catalyst-and-aqe.html", "05: Catalyst &amp; AQE",
"07-filter-and-project-early.html", "07: Filter Early",
"04-Spark-Optimization-Playbook.md",
SETUP_SHARED,
"""
<section>
<h2>The problem: sort-merge join shuffles both sides</h2>
<pre>from pyspark.sql import functions as F
orders   = spark.table("lesson_db.orders")    # 500k rows — fact table
products = spark.table("lesson_db.products")  # 100 rows  — dimension table

# Default join: Spark shuffles BOTH sides by product_id
bad = orders.join(products, "product_id")
bad.explain("formatted")
# Look for: SortMergeJoin or BroadcastHashJoin
# If Spark auto-broadcasts products (it may, since 100 rows is tiny),
# check spark.sql.autoBroadcastJoinThreshold and try with a bigger dim table</pre>
</section>

<section>
<h2>The fix: broadcast the small side</h2>
<pre>from pyspark.sql.functions import broadcast

# Explicit broadcast hint: products (100 rows) is sent to every executor
# orders (500k rows) NEVER moves — no shuffle on the big side
good = orders.join(broadcast(products), "product_id")
good.explain("formatted")
# Look for: BroadcastHashJoin (no Exchange on the orders side)

# Compare plans — the Exchange on the left side disappears
print("=== WITHOUT broadcast ===")
orders.join(products.hint("no_broadcast"), "product_id").explain()
print("=== WITH broadcast ===")
orders.join(broadcast(products), "product_id").explain()</pre>
<div class="box"><strong>Customer translation</strong>"The product lookup is 100 rows. Instead of reshuffling 500,000 order rows across the cluster, we send the tiny product table to every machine. The big table stays put — that's the difference between minutes and seconds."</div>
</section>

<section>
<h2>When to use / when not to</h2>
<pre># Check the actual size before deciding to broadcast
products.count()   # 100 rows — safe
# Rule of thumb: broadcast if ≤ ~100 MB

# AQE can auto-broadcast at runtime
print("Auto-broadcast threshold:", spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))
# Default: 10MB. AQE raises this adaptively.

# Dangerous: broadcasting something unexpectedly large
# large_table = spark.table("lesson_db.orders")
# orders.join(broadcast(large_table), "customer_id")
# ↑ This would send 500k rows to EVERY executor → OOM risk</pre>
<div class="box w"><strong>Always verify size</strong>Broadcasting a large table sends a full copy to every executor. If it OOMs one executor, the whole stage fails. Verify: <code>spark.table("dim").count()</code> or check DESCRIBE DETAIL.</div>
</section>
""",
[
    ("q1",
     "orders.join(products, 'product_id') shows SortMergeJoin in explain(). products has 100 rows. What's the fix?",
     [("orders.join(broadcast(products), 'product_id')", True),
      ("products.join(orders, 'product_id')", False),
      ("orders.repartition(100, 'product_id').join(products, 'product_id')", False),
      ("spark.conf.set('spark.sql.sortMerge.enabled', 'false')", False)],
     "Correct. Wrapping the small side in broadcast() sends products to every executor so orders never needs to shuffle. You'll see BroadcastHashJoin instead of SortMergeJoin in the plan.",
     "broadcast() on the small side = big side never shuffles. The fix is orders.join(broadcast(products), 'product_id')."
    ),
    ("q2",
     "After adding broadcast(products), what do you expect to see in explain()?",
     [("SortMergeJoin — the hint doesn't change the physical strategy.", False),
      ("BroadcastHashJoin with no Exchange on the orders (large) side.", True),
      ("BroadcastNestedLoopJoin.", False),
      ("No change until you call an action.", False)],
     "Correct. BroadcastHashJoin replaces SortMergeJoin, and the Exchange node on the large side disappears. That missing Exchange is proof the 500k-row table never shuffled.",
     "After broadcast(): BroadcastHashJoin in plan, no Exchange on the large side."
    ),
    ("q3",
     "When would broadcast() cause executors to OOM?",
     [("When the fact table has more than 1 billion rows.", False),
      ("When the broadcast table is actually large — every executor receives a full copy.", True),
      ("When AQE is enabled.", False),
      ("When the join key is a string.", False)],
     "Correct. Every executor gets a full copy of the broadcast table. If products was actually 500 MB rather than tiny, broadcasting it to 10 executors means 5 GB of memory just for the broadcast, on top of everything else. Always check.",
     "broadcast sends a FULL COPY to every executor. Large broadcast table × many executors = OOM."
    )
],
"\"I have a 500M-row transactions table joining a 10,000-row store lookup. Walk me through the optimization.\"",
"default sort-merge join shuffles both 500M rows and 10k rows by key — huge network cost → the store table is tiny → broadcast it: fact.join(broadcast(stores), 'store_id') → every executor gets the 10k store rows, transactions never move → explain() shows BroadcastHashJoin with no Exchange on the fact side → customer translation: instead of reshuffling half a billion rows we send a tiny copy to every machine",
TEARDOWN_SHARED
))

# ── 07 ────────────────────────────────────────────────────────────────────────
LESSONS.append((7,
"Filter &amp; Project Early",
"Lever 2: read only what you need",
["Code Stewardship"],
"06-broadcast-joins.html", "06: Broadcast Joins",
"08-caching-and-materialization.html", "08: Caching",
"04-Spark-Optimization-Playbook.md",
SETUP_SHARED,
"""
<section>
<h2>The problem: reading everything, filtering last</h2>
<pre>from pyspark.sql import functions as F
orders = spark.table("lesson_db.orders")

# BAD: read all years, all columns, then filter at the end
bad = (orders                           # all years, all 7 columns
  .join(spark.table("lesson_db.products"), "product_id")
  .groupBy("country")
  .agg(F.sum("amount").alias("total"))
  .where(F.col("year") == 2024))        # filter applied WAY too late

bad.explain("formatted")
# PushedFilters may still appear (Catalyst pushes it down)
# But the select("*") still reads all columns from disk</pre>
</section>

<section>
<h2>The fix: filter and select before the expensive work</h2>
<pre># GOOD: push filter and column selection before the join
orders_2024 = (orders
  .where(F.col("year") == 2024)                   # partition prune: skip 2023,2025 folders
  .select("order_id", "product_id", "country", "amount"))  # only 4 of 7 cols

products_slim = (spark.table("lesson_db.products")
  .select("product_id", "category"))               # only needed columns

good = (orders_2024
  .join(products_slim, "product_id")
  .groupBy("country")
  .agg(F.sum("amount").alias("total")))

good.explain("formatted")
# Check: PushedFilters: [EqualTo(year,2024)]
# Check: ReadSchema only shows the 4 selected columns</pre>
<div class="box"><strong>Customer translation</strong>"We were loading all 3 years and all 7 columns just to use 1 year and 4 columns. Filtering and selecting first means we read a fraction of the data — less I/O, less network, lower cost."</div>
</section>

<section>
<h2>Verify pushdown is working</h2>
<pre># Check what the scan actually reads
(orders
  .where(F.col("year") == 2024)
  .select("order_id","amount")
  .explain("formatted"))
# Good: Scan ... PushedFilters: [EqualTo(year,2024)], ReadSchema: struct&lt;order_id,amount&gt;
# Bad:  Scan ... PushedFilters: [],                   ReadSchema: struct&lt;all columns&gt;

# Check partition pruning worked (Delta partitioned by year)
spark.sql(
  "EXPLAIN FORMATTED "
  "SELECT order_id, amount FROM lesson_db.orders WHERE year = 2024"
).show(50, truncate=False)</pre>
</section>
""",
[
    ("q1",
     "explain() shows PushedFilters: [] on a Delta scan. What does this mean?",
     [("All filters were applied — empty means nothing was filtered out.", False),
      ("No filter reached the scan layer — Spark is reading all data and filtering afterward, wasting I/O.", True),
      ("PushedFilters only appears for Parquet, not Delta.", False),
      ("AQE will push the filter at runtime.", False)],
     "Correct. PushedFilters shows predicates Catalyst moved to the scan. Empty = nothing was pushed. Spark reads all rows from disk and filters them after the fact — every extra byte costs I/O.",
     "PushedFilters empty = no filter reached the scan. Spark reads everything from disk before filtering."
    ),
    ("q2",
     "lesson_db.orders is partitioned by year. You write orders.where(col('year')==2024). What physical optimization happens?",
     [("Row-level filtering — all files are opened but non-2024 rows are skipped.", False),
      ("Partition pruning — Spark skips the entire year=2023 and year=2025 folders. Zero bytes read from other years.", True),
      ("File-level filtering — each file is sampled to check for 2024 rows.", False),
      ("No optimization — partitioning only helps for writes.", False)],
     "Correct. The table is physically partitioned by year (separate folders). A filter on year causes partition pruning — Spark skips all non-2024 folders entirely. This is often the single biggest I/O win on time-series data.",
     "Partition pruning skips entire folders for non-matching partition values. Partitioned by year + filter on year = zero I/O for other years."
    ),
    ("q3",
     "You join orders (500k rows, 7 cols) to products (100 rows, 5 cols) but only use product_id and category from products. What should you do?",
     [("Nothing — Catalyst automatically prunes unused join columns.", False),
      ("Select only the needed columns from products before joining: products.select('product_id','category')", True),
      ("Use .drop() to remove unused columns after the join.", False),
      ("Column pruning only applies to the left side of a join.", False)],
     "Correct. Explicitly .select() before the join reduces the data volume in the shuffle and makes the optimization explicit. While Catalyst does some projection pruning, being explicit is more reliable, especially across join and aggregation boundaries.",
     "Explicitly select only needed columns before joins. Reduces shuffle data volume and makes the plan explicit."
    )
],
"\"I see the pipeline reads select('*') then applies a WHERE at the end. What are the two things you'd change and why?\"",
"1) move the WHERE before the join/agg — on a Delta table partitioned by year this becomes partition pruning, skipping entire folders. 2) replace select('*') with .select() for only needed columns — Parquet is columnar so you only pay I/O for the columns you read. Both changes reduce data volume before the expensive join and groupBy, which shrinks shuffle size. Customer: we were loading all years and all columns just to use one year and three columns.",
TEARDOWN_SHARED
))

# ── 08 ────────────────────────────────────────────────────────────────────────
LESSONS.append((8,
"Caching &amp; Materialization",
"Lever 6: stop recomputing — and the serverless gotcha that always trips people up",
["Code Stewardship"],
"07-filter-and-project-early.html", "07: Filter Early",
"09-udf-avoidance.html", "09: UDF Avoidance",
"04-Spark-Optimization-Playbook.md",
SETUP_SHARED,
"""
<section>
<h2>The hidden cost: lineage re-execution</h2>
<pre>from pyspark.sql import functions as F
orders = spark.table("lesson_db.orders")

# Create an "expensive" base pipeline
base = (orders
  .join(spark.table("lesson_db.products"), "product_id")
  .join(spark.table("lesson_db.customers"), "customer_id")
  .where(F.col("year") == 2024))

# BAD: base is re-executed from scratch for each action
import time
t0 = time.time()
eu_count = base.where(F.col("region") == "DE").count()
us_count = base.where(F.col("region") == "US").count()
print(f"Two counts (recomputed twice): {time.time()-t0:.2f}s")</pre>
</section>

<section>
<h2>The serverless-safe fix: materialize to Delta</h2>
<div class="box w"><strong>Critical serverless gotcha</strong><code>df.cache()</code> is restricted on Databricks serverless/Free Edition. It will silently do nothing or fail. The correct fix is to write to a Delta table and read it back. Calling this out during the interview scores points.</div>
<pre># GOOD: write once, read multiple times
(base.write
  .mode("overwrite")
  .saveAsTable("lesson_db.base_2024"))

# Read back — now it's just a Delta scan, not a full recompute
base_cached = spark.table("lesson_db.base_2024")

t0 = time.time()
eu_count = base_cached.where(F.col("region") == "DE").count()
us_count = base_cached.where(F.col("region") == "US").count()
print(f"Two counts (from Delta): {time.time()-t0:.2f}s")</pre>
<div class="box"><strong>Customer translation</strong>"We were rebuilding the same intermediate result twice. Computing it once means the follow-up steps are near-instant — and we avoid pulling millions of rows to one machine."</div>
</section>

<section>
<h2>Never collect() large DataFrames</h2>
<pre># DANGEROUS: pulls ALL rows to the driver process
# rows = base_cached.collect()   # DO NOT RUN on large data — OOM risk

# SAFE alternatives
base_cached.limit(10).show()                   # peek at 10 rows
base_cached.limit(10).toPandas()               # small sample to Pandas
base_cached.write.format("noop").mode("overwrite").save()  # trigger write without storing</pre>
</section>
""",
[
    ("q1",
     "df.cache() on Databricks Free Edition (serverless) — what happens?",
     [("Works as expected — cache() is supported everywhere.", False),
      ("cache() is restricted on serverless. The correct serverless alternative is to write to a Delta table and read it back.", True),
      ("cache() only works if the DataFrame is under 100 MB.", False),
      ("cache() triggers an immediate action and stores in memory.", False)],
     "Correct. cache() is not available on Databricks serverless/Free Edition. Write to a Delta table (saveAsTable or save) and read back with spark.table(). Identifying this in the interview is a scoring point — it shows serverless awareness.",
     "cache() is serverless-restricted. Materialize to Delta instead: write → saveAsTable, read → spark.table()."
    ),
    ("q2",
     "base is used 3 times with count() calls. It's not cached or materialized. How many times does the pipeline execute?",
     [("Once — Spark batches repeated actions on the same DataFrame.", False),
      ("Three times — each count() re-executes the full lineage.", True),
      ("Twice — the first call is cached automatically.", False),
      ("Depends on whether AQE is enabled.", False)],
     "Correct. Spark has no implicit caching between actions. Each count() is an independent action that re-runs the entire lineage (join + filter) from scratch. 3 actions = 3 full pipeline runs.",
     "No implicit caching. Each action re-runs the full lineage. 3 actions = 3 runs."
    ),
    ("q3",
     "rows = base.collect() on a 50M-row DataFrame. What's the risk?",
     [("collect() is a transformation, so it's lazy and safe.", False),
      ("collect() pulls ALL 50M rows from all executors into the driver's memory — likely OOM or very slow.", True),
      ("collect() only returns the first 1000 rows by default.", False),
      ("collect() is fine since Databricks serverless has large driver memory.", False)],
     "Correct. collect() sends every row from every executor to the driver process. The driver's memory is limited. On 50M rows this is likely an OOM or at minimum a very slow operation. Use limit(n).show() or write() instead.",
     "collect() = all rows to driver memory. On large DataFrames: OOM. Use show(n), limit(n), or write() instead."
    )
],
"\"I see base = expensive_pipeline() used with three count() calls. What's wrong and how do you fix it on Databricks Free Edition?\"",
"each count() is an action that re-executes the full expensive_pipeline lineage from scratch — 3 counts = 3 full runs → fix: materialize to Delta (write once, read back) → on Free Edition serverless, .cache() is restricted so Delta materialization is the correct approach → also replace any collect() with limit().show() → customer: we're paying for 3 runs when 1 would do",
TEARDOWN_SHARED + "\nspark.sql('DROP TABLE IF EXISTS lesson_db.base_2024')"
))

# ── 09 ────────────────────────────────────────────────────────────────────────
LESSONS.append((9,
"UDF Avoidance",
"Lever 4: stay in the fast path — built-ins, pandas UDFs, and Photon",
["Code Stewardship"],
"08-caching-and-materialization.html", "08: Caching",
"10-skew-handling.html", "10: Skew Handling",
"04-Spark-Optimization-Playbook.md",
SETUP_SHARED,
"""
<section>
<h2>Why Python UDFs are slow</h2>
<p>A Python UDF is a black box to Catalyst. For every row:</p>
<ol>
<li>Serialize the row from JVM binary format to Python</li>
<li>Execute your Python function</li>
<li>Serialize the result back to the JVM</li>
</ol>
<p>This row-by-row serialization breaks Tungsten (JVM codegen), Photon (native C++), and all Catalyst optimizations. A UDF doing the same thing as a built-in is routinely 10–100× slower.</p>
</section>

<section>
<h2>Side-by-side: UDF vs built-in</h2>
<pre>from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import time

orders = spark.table("lesson_db.orders")

# BAD: Python UDF — row-by-row serialization, opaque to Catalyst
upper_udf = udf(lambda s: s.upper() if s else None, StringType())
t0 = time.time()
orders.withColumn("country_up", upper_udf("country")).write.format("noop").mode("overwrite").save()
print(f"Python UDF: {time.time()-t0:.2f}s")

# GOOD: built-in — vectorized, Photon-friendly, Catalyst-visible
t0 = time.time()
orders.withColumn("country_up", F.upper("country")).write.format("noop").mode("overwrite").save()
print(f"Built-in F.upper: {time.time()-t0:.2f}s")</pre>
</section>

<section>
<h2>When you can't avoid custom logic: vectorized UDFs</h2>
<pre>import pandas as pd
from pyspark.sql.functions import pandas_udf

# Vectorized (pandas) UDF — works on Arrow batches, not row-by-row
# Still slower than built-ins but MUCH faster than plain Python UDFs
@pandas_udf(StringType())
def custom_clean(s: pd.Series) -> pd.Series:
    return s.str.strip().str.upper()

orders.withColumn("country_clean", custom_clean("country")).limit(5).show()

# Compare plans
print("=== Python UDF plan ===")
orders.withColumn("x", upper_udf("country")).explain()
print("=== Pandas UDF plan ===")
orders.withColumn("x", custom_clean("country")).explain()
print("=== Built-in plan ===")
orders.withColumn("x", F.upper("country")).explain()</pre>
<div class="box"><strong>Customer translation</strong>"That custom function forces Spark to hand every row to Python one at a time. Swapping in the built-in lets the engine process whole batches in native code — same result, a fraction of the time."</div>
</section>
""",
[
    ("q1",
     "A colleague wrote udf(lambda s: s.strip(), StringType()) to trim whitespace. What's the fix?",
     [("Change StringType() to TrimType().", False),
      ("Replace with F.trim('column') — it's the built-in equivalent.", True),
      ("Wrap the UDF in a pandas_udf for better performance.", False),
      ("Add .cache() before the withColumn call.", False)],
     "Correct. F.trim() is the built-in that does exactly what the UDF does. It stays inside the Catalyst/Tungsten/Photon fast path — no row-by-row Python serialization.",
     "F.trim() is the built-in equivalent. Always check built-ins before writing a UDF."
    ),
    ("q2",
     "You need truly custom logic that doesn't exist as a built-in. You must write a UDF. What's the best choice?",
     [("A plain Python UDF with udf(lambda...)", False),
      ("A pandas_udf (vectorized UDF) — operates on Arrow batches, not row-by-row.", True),
      ("Write the logic in SQL using spark.sql()", False),
      ("A Scala UDF registered via spark.udf.register()", False)],
     "Correct. If custom logic is unavoidable, a vectorized (pandas) UDF is the best Python option. It uses Apache Arrow to transfer column batches between JVM and Python — much faster than row-by-row. Scala UDFs are faster still but require Scala.",
     "When a UDF is unavoidable: pandas_udf > plain Python UDF. pandas_udf works on Arrow batches; Python UDF works row-by-row."
    ),
    ("q3",
     "Why can't Photon (Databricks' native C++ engine) accelerate a Python UDF?",
     [("Photon doesn't support UDFs at all.", False),
      ("Python UDFs force data to leave the JVM/native path and serialize to a Python process. Photon can only execute operators it owns natively — a Python function is opaque to it.", True),
      ("Photon only runs on GPU nodes.", False),
      ("Photon accelerates UDFs if they're decorated with @photon_udf.", False)],
     "Correct. Photon (and Tungsten) execute compiled native code on JVM/C++ objects. A Python UDF breaks that chain — data must serialize out to Python, execute there, and serialize back. Photon can't see inside a Python function.",
     "Photon executes natively. Python UDFs require data to leave the native path → Photon can't optimize them."
    )
],
"\"I see a udf(lambda s: s.upper()) in this pipeline. What's wrong with it and how would you fix it?\"",
"Python UDF = row-by-row serialization between JVM and Python → breaks Catalyst optimization, Tungsten codegen, and Photon → 10-100x slower than equivalent built-in → fix: F.upper('column') → in explain() the built-in is a visible plan node (Catalyst can fuse it); the UDF is an opaque EvalPython node → customer: that function hands every row to Python one at a time; the built-in processes whole batches in native code",
TEARDOWN_SHARED
))

# ── 10 ────────────────────────────────────────────────────────────────────────
LESSONS.append((10,
"Skew Handling",
"Lever 5: when one partition ruins the whole stage",
["Code Stewardship", "Computational Thinking"],
"09-udf-avoidance.html", "09: UDF Avoidance",
"11-repartition-vs-coalesce.html", "11: Repartition vs Coalesce",
"04-Spark-Optimization-Playbook.md",
SETUP_SHARED,
"""
<section>
<h2>What skew looks like and why it's expensive</h2>
<p>In the setup, we made <code>customer_id=1</code> a hot key — it has far more orders than any other customer. When this column is used in a join or groupBy key, one partition ends up with most of the data, one task takes much longer, and the entire stage waits.</p>
<pre>from pyspark.sql import functions as F
orders = spark.table("lesson_db.orders")

# Diagnose skew: check partition distribution after a groupBy
spark.conf.set("spark.sql.shuffle.partitions", "20")
dist = (orders
  .groupBy(F.spark_partition_id().alias("partition_id"))
  .count()
  .orderBy(F.desc("count")))
dist.show()

# Check customer distribution — customer_id=1 should dominate
(orders
  .groupBy("customer_id")
  .count()
  .orderBy(F.desc("count"))
  .show(5))</pre>
</section>

<section>
<h2>Fix 1: AQE skew join (free, on by default)</h2>
<pre># Check AQE skew settings
print(spark.conf.get("spark.sql.adaptive.skewJoin.enabled"))        # default: true
print(spark.conf.get("spark.sql.adaptive.skewJoin.skewedPartitionFactor"))  # default: 5

# AQE automatically detects and splits skewed partitions in joins
# It replicates the matching side to handle the split
orders.join(spark.table("lesson_db.customers"), "customer_id").explain("formatted")
# Look for: CustomShuffleReaderExec (AQE split skewed partitions)</pre>
</section>

<section>
<h2>Fix 2: broadcast the small side (best when possible)</h2>
<pre">from pyspark.sql.functions import broadcast

customers = spark.table("lesson_db.customers")  # 200 rows — tiny!

# If one side is small, broadcast it → no shuffle on the big side → no skew possible
no_skew = orders.join(broadcast(customers), "customer_id")
no_skew.explain("formatted")
# BroadcastHashJoin — skew is impossible because orders never shuffles</pre>
</section>

<section>
<h2>Fix 3: salt the hot key</h2>
<pre># When you can't broadcast (both sides are large):
# Add random salt to spread the hot key across multiple partitions

SALT = 8  # number of buckets

orders_salted = orders.withColumn("salt", (F.rand() * SALT).cast("int"))

# Expand customers so each customer appears SALT times (once per salt value)
customers_salted = (spark.table("lesson_db.customers")
  .withColumn("salt",
    F.explode(F.array([F.lit(i) for i in range(SALT)]))))

# Join on (customer_id, salt) — hot key is now split across SALT tasks
result = orders_salted.join(customers_salted, ["customer_id", "salt"])
result.groupBy(F.spark_partition_id()).count().orderBy(F.desc("count")).show(5)</pre>
<div class="box"><strong>Customer translation</strong>"One customer has half the rows, so one machine was doing half the work while the rest sat idle. Spreading that key across multiple tasks balances the load — all machines finish together."</div>
</section>
""",
[
    ("q1",
     "One task in a stage runs 10x longer than the other 49. What's the most likely cause?",
     [("The cluster ran out of memory for that executor.", False),
      ("Data skew — one partition has far more rows than the others.", True),
      ("That executor has slower hardware.", False),
      ("Spark scheduled too many tasks for that executor.", False)],
     "Correct. When one task runs much longer than the rest, data skew is the most common cause. One partition has a disproportionate share of rows (e.g., a hot join/group key). The stage can't complete until that task finishes.",
     "One outlier task = almost always data skew. One key has far more rows → one huge partition → one slow task."
    ),
    ("q2",
     "AQE skew join is on by default. Does that mean you never need to worry about skew?",
     [("Yes — AQE handles all skew automatically.", False),
      ("No — AQE splits skewed partitions in joins, but it can't fix skew in groupBy aggregations, and broadcasting the small side (if possible) is still faster.", True),
      ("AQE only handles skew when spark.sql.adaptive.skewJoin.enabled is explicitly set to true.", False),
      ("AQE handles skew but only on Databricks Premium.", False)],
     "Correct. AQE skew join helps with join skew. For groupBy skew it's less effective. If one side of a join is small enough to broadcast, that eliminates the shuffle entirely — no skew possible. AQE is a safety net, not a complete solution.",
     "AQE helps with join skew but broadcasting the small side is better (eliminates the shuffle). AQE can't fix groupBy skew."
    ),
    ("q3",
     "You can't broadcast (both sides are large, the hot key is a customer with 50% of rows). What technique spreads the load?",
     [("Increase spark.sql.shuffle.partitions to force more tasks.", False),
      ("Salt the hot key — add a random integer to the key and expand the small side to match, so the hot key is spread across multiple partitions.", True),
      ("Use coalesce() before the join.", False),
      ("Filter out the hot key entirely.", False)],
     "Correct. Salting appends a random integer (0 to N-1) to the join key, splitting the hot key across N partitions. The smaller side must be expanded (explode the salt values) to match. This distributes the skewed load evenly.",
     "Salting = add random int to the key + explode the other side. Splits the hot key across N partitions."
    )
],
"\"One task in a stage takes 8 minutes, the other 49 take 3 seconds. Walk me through diagnosing and fixing it.\"",
"symptom = data skew → diagnose: groupBy(spark_partition_id()).count() to see which partition is huge → identify the hot key: groupBy('customer_id').count().orderBy(desc) → fixes in order: (1) AQE skew join is on by default — check if it's helping; (2) if the other side of the join is small, broadcast it — eliminates the shuffle entirely, no skew possible; (3) if both sides are large, salt the hot key: add random int, explode the other side, join on (key, salt)",
TEARDOWN_SHARED
))

# ── 11 ────────────────────────────────────────────────────────────────────────
LESSONS.append((11,
"Repartition vs Coalesce",
"Lever 3: controlling partition count — when to shuffle and when not to",
["Code Stewardship"],
"10-skew-handling.html", "10: Skew Handling",
"12-delta-lake-fundamentals.html", "12: Delta Fundamentals",
"04-Spark-Optimization-Playbook.md",
SETUP_SHARED,
"""
<section>
<h2>The two tools</h2>
<p><code>coalesce(n)</code> — <strong>narrow</strong>, no shuffle. Merges existing partitions together. Can only <em>reduce</em> partition count. Cheap.</p>
<p><code>repartition(n)</code> or <code>repartition(n, col)</code> — <strong>wide</strong>, full shuffle. Can increase or decrease. Can hash-partition by a column. Expensive.</p>
<pre>from pyspark.sql import functions as F
orders = spark.table("lesson_db.orders")

print("Original partition count:")
orders.groupBy(F.spark_partition_id()).count().count()

# coalesce: reduce without shuffle (before writes)
coalesced = orders.coalesce(4)
print("After coalesce(4):")
coalesced.explain()   # no Exchange node

# repartition: full shuffle, can increase or change distribution
repartitioned = orders.repartition(50, "customer_id")
print("After repartition(50, 'customer_id'):")
repartitioned.explain()   # Exchange node present</pre>
</section>

<section>
<h2>Use coalesce before writes</h2>
<pre># Writing with too many partitions creates many small files
# Small files = slow reads for all future queries

# BAD: 200+ tiny output files
orders.write.mode("overwrite").format("delta").saveAsTable("lesson_db.orders_many_files")
spark.sql("DESCRIBE DETAIL lesson_db.orders_many_files").select("numFiles","sizeInBytes").show()

# GOOD: coalesce first — fewer, larger files
orders.coalesce(8).write.mode("overwrite").format("delta").saveAsTable("lesson_db.orders_few_files")
spark.sql("DESCRIBE DETAIL lesson_db.orders_few_files").select("numFiles","sizeInBytes").show()</pre>
</section>

<section>
<h2>Use repartition when co-locating by key</h2>
<pre># Before a join on customer_id, co-locate both sides on the same key
# so the join shuffle is cheaper (data already on the right partition)
orders_by_cust = orders.repartition(50, "customer_id")
customers = spark.table("lesson_db.customers").repartition(50, "customer_id")

joined = orders_by_cust.join(customers, "customer_id")
joined.explain("formatted")
# The join may not need a re-shuffle if both sides are already partitioned by the same key</pre>
<div class="box"><strong>Customer translation</strong>"We were writing 200 tiny files, which makes every downstream read slow. Coalescing writes a handful of right-sized files — speeds up everything that reads this table."</div>
</section>
""",
[
    ("q1",
     "You have a DataFrame with 300 partitions and want to write it to Delta as ~8 files. Which is correct?",
     [("df.repartition(8).write... — full shuffle to redistribute into 8 partitions.", False),
      ("df.coalesce(8).write... — narrow operation, no shuffle, merges to 8 partitions.", True),
      ("spark.conf.set('spark.sql.shuffle.partitions', '8') then write.", False),
      ("OPTIMIZE the table after writing.", False)],
     "Correct. coalesce(8) is narrow — no shuffle, just merges existing 300 partitions into 8. repartition(8) also works but adds an unnecessary shuffle. OPTIMIZE runs after the fact. shuffle.partitions only affects post-shuffle stages.",
     "coalesce = no shuffle, just merges. repartition = full shuffle. For pre-write compaction, coalesce is cheaper."
    ),
    ("q2",
     "You need to INCREASE partition count from 10 to 200 to allow more parallelism. Which do you use?",
     [("coalesce(200) — it increases partition count efficiently.", False),
      ("repartition(200) — coalesce cannot increase partition count.", True),
      ("spark.conf.set('spark.sql.shuffle.partitions', '200')", False),
      ("Either coalesce or repartition — they're equivalent for this.", False)],
     "Correct. coalesce can only REDUCE partition count (it merges). To increase partitions you must use repartition, which does a full shuffle to redistribute data into more partitions.",
     "coalesce can only reduce (it merges partitions). repartition can increase or decrease via a full shuffle."
    ),
    ("q3",
     "repartition(50, 'customer_id') vs repartition(50). What's different?",
     [("They're identical — the column argument is ignored.", False),
      ("repartition(50, 'customer_id') hash-partitions by customer_id so all rows for the same customer land on the same partition. repartition(50) distributes round-robin.", True),
      ("repartition(50, 'customer_id') sorts within partitions by customer_id.", False),
      ("repartition(col) doesn't trigger a shuffle.", False)],
     "Correct. repartition(n, col) hash-partitions by that column — all rows with the same customer_id go to the same partition. This is useful before a join on customer_id (co-location). Plain repartition(n) is round-robin — uniform distribution but no key co-location.",
     "repartition(n, col) = hash-partition by col (same key → same partition). repartition(n) = round-robin."
    )
],
"\"Your pipeline writes a DataFrame with 400 partitions to Delta. Each file is ~50 KB. What's wrong and how do you fix it?\"",
"400 tiny files = small-files problem → every future reader must open 400 files → slow metadata + scan → fix before write: df.coalesce(8).write.format('delta')... → coalesce is narrow (no shuffle), just merges partitions → target ~128 MB per file → after the fact: OPTIMIZE on the Delta table compacts small files → customer: every future query hits 8 right-sized files instead of 400 tiny ones",
TEARDOWN_SHARED + "\nspark.sql('DROP TABLE IF EXISTS lesson_db.orders_many_files')\nspark.sql('DROP TABLE IF EXISTS lesson_db.orders_few_files')"
))

if __name__ == "__main__":
    print(f"Defined {len(LESSONS)} lessons. Writing HTML files...")
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
