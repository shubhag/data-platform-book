# Chapter 8 — The Warehouse and the Lake

Everything built so far answers one kind of question: *given a query, return the right documents, fast.* This chapter is about the other kind — "how has search quality changed over two years, by team?" — which needs a different engine, a different way of shaping data, and a different place to keep it. It assumes you have never touched OLAP.

**By the end you'll be able to:**

- Explain why analytical queries need a columnar engine, and name the tricks that make one fast.
- Model analytical data with facts, dimensions, and a declared grain.
- Describe how Delta Lake's transaction log turns a folder of files into a real table.
- Say what Databricks actually is, and which settings decide its bill.
- Build a bronze → silver → gold pipeline where every layer is safe to rebuild.

## 8.1 The question nobody in this book has asked yet

Seven chapters in, Lantern works: documents flow from PostgreSQL through Kafka and Flink into OpenSearch, and employees find things. Then the VP of Engineering asks:

> "Search volume is up 40% this quarter but the support team says people can't find anything. Which teams' documents are getting searched and not clicked? Has that changed since we shipped semantic search in March? And how does it compare to the same period last year?"

Nothing we have built can answer it:

- **OpenSearch** holds the current state of documents, not two years of every search and click.
- **Kafka** holds ninety days at best, readable only in sequence.
- **PostgreSQL** *technically* can — by scanning two years of click log, joining to documents and teams, and grouping by week. On the database serving your product, at 10 a.m. on a Tuesday.

This work is called **analytics**, or **OLAP**, and it has its own engines, modelling discipline, and vendor ecosystem. We cover the fundamentals (§8.2–8.4), the lakehouse and **Delta Lake** (§8.5–8.6), then **Databricks** and the **medallion architecture** (§8.7–8.11).

## 8.2 OLTP and OLAP: two databases wearing the same SQL

**OLTP (Online Transaction Processing)** is the database behind an application: PostgreSQL, MySQL, DynamoDB. It runs thousands of small operations per second — fetch one document, record one edit — each touching a few rows, returning in milliseconds, and needing to be correct *right now*.

**OLAP (Online Analytical Processing)** is the database behind decisions: Snowflake, BigQuery, Redshift, ClickHouse, Databricks SQL. It runs a few enormous queries, each touching hundreds of millions of rows and returning a handful:

```sql
-- Which teams' documents get searched but not clicked, by week?
SELECT t.team_name, date_trunc('week', s.search_ts) AS week,
       count(c.click_id) / count(*)::float        AS ctr
FROM searches s
LEFT JOIN clicks c ON c.search_id = s.search_id
JOIN documents d   ON d.id = s.top_result_doc_id
JOIN teams t       ON t.id = d.owner_team_id
WHERE s.search_ts >= '2024-01-01'
GROUP BY 1, 2;
```

It reads 800 million searches and 400 million clicks to return maybe 400 rows. Twenty seconds is fine; taking the application down is not.

| | OLTP | OLAP |
|---|---|---|
| Unit of work | one row | one column, across everything |
| Query count | thousands/sec | tens/hour |
| Rows touched per query | 1–100 | 10⁶–10¹⁰ |
| Latency target | 1–10 ms | 1 s – 5 min |
| Writes | constant, small, random | bulk, append or replace |
| Storage layout | **row**-oriented | **column**-oriented |
| Data shape | normalized (no duplication) | denormalized (duplication is fine) |
| Data age | current state | full history |
| Typical size | 100 GB – 10 TB | 10 TB – 10 PB |
| Failure mode | a user sees an error | a dashboard is wrong |

Every row of that table follows from the first. Read it as one idea with ten consequences.

### Why you cannot just run it on PostgreSQL

**One: the row store reads everything.** PostgreSQL stores each row contiguously on an 8 KB page (§4.4). Our `searches` table is 800 million rows of 40 columns, 200 bytes each: 160 GB, of which the query needs three columns, about 30 bytes per row. Reading whole pages means **160 GB to use 24 GB** — 160 seconds of pure I/O at a very good 1 GB/s.

A **column store** keeps each column in its own run of bytes and reads only the three it needs. Those compress very well (§8.3), so 24 GB becomes maybe 3 GB. **3 GB versus 160 GB** is not tuning; it is structural.

**Two: one machine.** A PostgreSQL query gets a few workers on one machine. An OLAP engine splits the scan across dozens of machines — 60 machines reading 50 MB each. This is §1.2's partitioning, applied to a query.

**Three: interference — the one that gets people fired.** The analytical query evicts the application's cache, holds long transactions that block vacuum, and eats the product's I/O. The dashboard is late *and* checkout is slow.

> **The rule:** operational data is for serving the product; a reshaped copy of it is for answering questions about the product. The copy is not an optimisation, it is an isolation boundary.

**HTAP (Hybrid Transactional/Analytical Processing)** systems such as SingleStore and TiDB serve both from one engine, usually a row store for recent writes plus a column store for the bulk. They work for mid-size workloads, but at this book's scale separation is still the default, because isolation matters as much as speed.

## 8.3 How a columnar engine actually answers a query

Columnar layout (§4.4) lets an engine skip almost all the work. Each trick below is also something you can accidentally defeat.

A table is a directory of Parquet files, such as `s3://lantern-lake/gold/searches/event_date=2026-09-16/part-00000.parquet` (128 MB). Each file ends in a footer:

```
┌─ part-00000.parquet ────────────────────────────────┐
│ Row group 1 (≈128 MB of rows)                       │
│   ├ search_ts   [min=09:00,  max=09:14]             │
│   ├ user_id     [min=u_0001, max=u_9998]            │
│   └ query_text  [ ... ]                             │
│ Row group 2 ...                                     │
├─────────────────────────────────────────────────────┤
│ FOOTER: schema + per-column min/max/nulls/count     │
└─────────────────────────────────────────────────────┘
```

The footer is a small index of the file, and most tricks depend on it.

### Trick 1: partition pruning — don't open the file

Files sit in directories named `column=value`, so filtering on that column means the other directories are never listed.

```sql
-- reads 1 day of files
SELECT count(*) FROM searches WHERE event_date = '2026-09-16';
-- reads 730 days of files: same answer, ruined
SELECT count(*) FROM searches WHERE date(search_ts) = '2026-09-16';
```

The second is the most common performance bug in analytics: `search_ts` is not the partition column, and the function hides it from the optimizer anyway. Filter on the partition column, unwrapped.

### Trick 2: file skipping — read the footer, skip the data

The engine compares your filter with each row group's min/max. Looking for `user_id = 'u_44120'`, a row group with range `u_70000`–`u_79999` cannot contain it, so 128 MB is skipped by reading 2 KB of footer.

This only works if values are **clustered**. If every file holds a random spread of user IDs, every range covers everything and nothing is skipped. `ZORDER` and liquid clustering (§8.6) fix this by putting similar values in the same file.

### Trick 3: column pruning

A query touching two columns reads two columns; the other thirty-eight never leave disk. That is the 160 GB → 24 GB effect from §8.2, and why `SELECT *` in analytics is a cost decision, not a style choice.

### Trick 4: compression that only works on columns

Adjacent values in a column are the same *kind of thing*, so they compress far better than rows.

| Column | Raw | Encoding | On disk |
|---|---|---|---|
| `event_date` (1 value per file) | 8 MB | run-length: `2026-09-16 × 1,000,000` | ~30 bytes |
| `country` (200 distinct) | 6 MB | dictionary: 200 strings + 1M small ints | ~300 KB |
| `search_ts` (sorted, ms apart) | 8 MB | delta: base + tiny offsets | ~900 KB |
| `query_text` (high cardinality) | 60 MB | snappy | ~28 MB |

Whole tables typically compress 5–15×, which cuts both storage and read cost. Sorting before writing helps further, since run-length and delta encodings reward adjacency.

### Trick 5: vectorized execution

A row-at-a-time engine runs an interpreter loop per row, full of virtual calls. A **vectorized** engine processes batches of ~1,000 column values in a tight loop over an array, which the CPU can pipeline, prefetch, and run with SIMD: 10–100× faster. Spark's Tungsten (§7.2) does this, Databricks' **Photon** is a C++ rewrite of it, and ClickHouse and DuckDB are built around it.

### Trick 6: parallelism and the shuffle

Scans parallelize trivially because files are independent. Aggregation runs in two phases: each worker computes partial results, then a **shuffle** (§7.2) sends each key's partials to one worker to combine. So §7.2's skew problems apply unchanged — `GROUP BY country` where one country is 60% of traffic gives one overloaded task.

### Putting the tricks together

The VP's question, over a 2-year, 800-million-row table:

```
Naive row-store scan                  160 GB    ~160 s, no parallelism
+ partition pruning (1 quarter)        20 GB
+ column pruning (3 of 40 columns)      3 GB
+ compression                         400 MB
+ file skipping (clustered on team)   120 MB
+ 64 cores in parallel                  2 MB per core
                                    ──────────────────────
                                      ~1.5 seconds
```

The rest of the chapter is about getting your data into a state where those lines apply.

## 8.4 Modelling for analytics: facts, dimensions, and grain

Analytical data is also *shaped* differently. The discipline is **dimensional modelling**, from Ralph Kimball, and it still applies on Databricks.

### Denormalize on purpose

OLTP **normalizes**: store every fact once and reference it by key, so an update touches one place. In OLAP, data is written once and read ten thousand times, and every join is a shuffle. So you **denormalize** deliberately: copy the team name onto ten million rows so no query has to join for it. The storage costs a fraction of a cent; the join costs 30 seconds, every time.

### Facts and dimensions

- A **fact table** records *events*, one row per thing that happened. It is long, narrow, grows forever, and holds mostly foreign keys plus numbers you can add up (the **measures**).
- A **dimension table** records *things*, one row per user, document, team, or date. It is short, wide, changes slowly, and holds what you filter and group by (the **attributes**).

```
                    ┌──────────────┐
                    │  dim_date    │
                    │  week, month │
                    └──────┬───────┘
  ┌──────────────┐    ┌────▼─────────┐    ┌──────────────┐
  │  dim_user    │◄───┤ fact_search  ├───►│ dim_document │
  │  department  │    │  date_key    │    │  title       │
  │  country     │    │  user_key    │    │  owner_team  │
  └──────────────┘    │  document_key│    └──────────────┘
                      │  latency_ms  │
                      │  clicked     │  ← measures
                      └──────────────┘
```

One fact table surrounded by dimensions, one join deep, is a **star schema**. If the dimensions are themselves normalized (`dim_document` → `dim_team` → `dim_org`), it is a **snowflake schema** — usually a mistake, trading storage you have for joins you pay for on every query.

### Grain: decide it first, write it down

The **grain** is what one row means: "one search event", "one document-day". Get it wrong and every number is subtly wrong.

The classic disaster: a table at "one row per search *result shown*" (ten per search), and an analyst runs `count(*)` expecting searches. Volume is reported at 10× reality, and because it looks plausible, it survives for a year.

> **Write the grain in the table comment before you write the pipeline.** `-- Grain: one row per search event, identified by search_id.`

### Slowly changing dimensions

A user moves from Support to Engineering. Do last year's searches belong to Support or Engineering? Both answers are defensible, and each has a name:

| | How it works | Good for |
|---|---|---|
| **SCD Type 1 — overwrite** | Dimension holds only the current value; old searches now belong to Engineering | Correcting mistakes ("country was misspelled") |
| **SCD Type 2 — new row per change** | Each version has `valid_from` / `valid_to` / `is_current`; facts join to the version live at event time | Anything where history matters: org structure, pricing, entitlements |

```
user_key  user_id  department   valid_from   valid_to     is_current
u_7#1     u_7      Support      2023-01-05   2026-03-31   false
u_7#2     u_7      Engineering  2026-04-01   9999-12-31   true
```

Type 2 is what "point-in-time correctness" means. It fixes the dimension hazard in §4.7's backfill checklist: re-run last year's job against a Type 1 dimension and you get different answers than the original run. Type 2 costs more to build and makes reprocessing deterministic.

### One big table

Columnar storage makes a third option viable: **one very wide table** with dimension attributes already flattened in (`user_department`, `document_title`, `owner_team` right next to `latency_ms` and `clicked`). Two hundred columns is fine, since a query reading four pays for four.

For **BI** (business intelligence: the reporting and charting tools analysts use), this is often the fastest and least error-prone design, since there are no joins to get wrong. The cost: a changed document title must be rewritten across the fact table, not in one dimension row.

Rule of thumb: model **silver** as a proper star (the reusable layer) and build **gold** as wide tables for specific consumers. Those are medallion layers from §4.2, covered fully in §8.8.

## 8.5 Warehouse, lake, lakehouse

Three architectures, in the order the industry tried them. The third, which you will work in, only makes sense as an answer to the first two.

### The data warehouse (1990s–2010s)

A **data warehouse** is a specialised database — Teradata, later Snowflake, BigQuery, Redshift — storing data in its own proprietary format, tightly coupled to its own engine.

| Gets right | Gets wrong, at scale |
|---|---|
| ACID transactions, schema enforcement | Only structured data; images, raw JSON, ML features live elsewhere |
| Indexes, statistics, fine-grained permissions | Loading needs conversion; Python/ML use needs an export |
| SQL that just works | Storage and compute often sold together; one vendor's format |

### The data lake (2010s)

A **data lake** keeps everything as files in cheap object storage (S3, ADLS, GCS), in open formats, and points any engine at them. Storage costs almost nothing, compute is fully decoupled (200 cores for ten minutes, paid for ten minutes), and any data type is welcome, which ML needs.

What it gets wrong is that **a folder is not a table**:

- **No transactions.** A job writing 500 files crashes after 300; readers see a half-written table and no error.
- **No isolation.** A reader listing mid-write sees some new data and some old.
- **No schema enforcement.** Upstream writes `user_id` as an integer; nothing complains until a query fails months later.
- **No updates or deletes.** Deleting one user's rows means rewriting, by hand, every file that might hold them.
- **No history.** Overwrite yesterday's table with a bad job and yesterday is gone.
- **Slow metadata.** Listing two million S3 objects takes minutes, just to plan a query.

A decade of this produced the **data swamp**: petabytes of files nobody trusted.

### The lakehouse (2020s)

The **lakehouse** idea is smaller than its name:

> **Keep the files in open formats in cheap object storage. Add a transaction log next to them that says which files make up the table right now.**

That log adds ACID transactions, snapshot isolation, time travel, updates, deletes, and fast metadata, while keeping open formats, cheap storage, and any engine.

| Format | Origin | Notes |
|---|---|---|
| **Delta Lake** | Databricks | Used in this chapter |
| **Apache Iceberg** | Netflix | Broad-industry choice; supported by Snowflake, AWS, and Databricks |
| **Apache Hudi** | Uber | Strongest on streaming upserts |

The concepts transfer almost perfectly: learn Delta and you can read Iceberg's docs without difficulty. And "a durable log is the source of truth, and current state is derived from it" should sound familiar — it is Kafka (§5.1), PostgreSQL's write-ahead log, Flink's checkpoints, and now your table format.

## 8.6 Delta Lake: the log that turns a folder into a table

The mechanism fits in your head, and once it does, every Delta behaviour becomes predictable.

### What is on disk

```
s3://lantern-lake/silver/documents/
  _delta_log/
    000...000.json               ← commit 0: create table, add 4 files
    000...001.json               ← commit 1: add 2 files
    000...002.json               ← commit 2: remove 1, add 1 (an UPDATE)
    000...010.checkpoint.parquet ← snapshot every 10 commits
  part-00000-6f3a.snappy.parquet
  part-00001-9b1c.snappy.parquet
```

**The Parquet files are not the table; the table is whatever the log says it is.** A file the log never added is invisible, and that is how atomicity works.

### What is in a commit

Each commit is a small JSON file, one action per line (trimmed):

```json
{"commitInfo":{"operation":"MERGE"}}
{"remove":{"path":"part-00003-1a2b.snappy.parquet"}}
{"add":{"path":"part-00017-77ef.snappy.parquet","stats":"{\"numRecords\":982341,
  \"minValues\":{\"doc_id\":\"doc_000001\"},\"maxValues\":{\"doc_id\":\"doc_049999\"}}"}}
```

Three things to notice:

1. **The unit of change is a file, not a row.** An `UPDATE` touching one row rewrites its whole Parquet file, then commits "remove old, add new". So Delta is excellent at bulk changes and merely adequate at single-row ones — the opposite of PostgreSQL. **Deletion vectors** soften this by marking deleted rows in a tiny bitmap and rewriting the file later.
2. **The statistics are in the log.** Per-file min/max sits in the commit, so file skipping (§8.3) needs no S3 listing and no footer reads. Planning over a million-file table takes a second, not ten minutes.
3. **Reading is a replay of the log.** Take the last checkpoint, replay later commits (adding and removing paths), and you get a set of files: your **snapshot**. Every feature below follows from it.

### The features, and why each one is free

**Atomicity.** The writer writes its Parquet files first — invisible, since nothing added them — then makes one atomic write of the next commit file. A job that dies halfway leaves orphan files and no commit; readers see nothing changed.

**Snapshot isolation.** A reader resolves the log once, say at version 7, and reads those files throughout. A writer committing version 8 removes files from the *table* but not from storage, so the reader is unaffected. This is **MVCC** (multi-version concurrency control: readers see a consistent version while writers create new ones), built from a list of filenames.

**Optimistic concurrency.** Two jobs both try to commit version 8. The commit is an atomic put-if-absent, so one wins; the loser checks the winner's changes for overlap and, if none, retries as version 9. Appends to different partitions never conflict; updates to the same rows fail with `ConcurrentAppendException` — a correct failure, not a bug.

**Time travel.** Old versions are older prefixes of the log, and their files remain until vacuumed:

```sql
SELECT * FROM silver.documents VERSION AS OF 7;
SELECT * FROM silver.documents TIMESTAMP AS OF '2026-09-16 09:00:00';
DESCRIBE HISTORY silver.documents;                    -- what changed, and who
RESTORE TABLE silver.documents TO VERSION AS OF 7;    -- undo a bad job
```

`RESTORE` is the headline if you have ever cleaned up after a bad batch job: one statement, seconds to run, because it only rewrites the log. §4.7's "shadow table, diff, swap" becomes "write, diff against `VERSION AS OF`, restore if wrong".

**Schema enforcement and evolution.** The schema lives in the log, so writing a `LongType` into a `StringType` column fails at commit instead of silently corrupting the table. When you *want* new columns, opt in with `.option("mergeSchema", "true")`. This is §4.5's schema registry idea at the sink rather than the producer; the two complement each other.

**MERGE — the upsert.** The most-used Delta statement:

```sql
MERGE INTO silver.documents AS t
USING staged_changes AS s
  ON t.doc_id = s.doc_id
WHEN MATCHED AND s.op = 'DELETE' THEN DELETE
WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET *
WHEN NOT MATCHED AND s.op <> 'DELETE' THEN INSERT *;
```

This *is* §4.7's "upsert by primary key", transactional, over object storage. Run it twice and the table is identical, because the `updated_at` guard makes the second run a no-op. That is why medallion (§8.8) can promise every layer is safe to rebuild.

**Change data feed (CDF).** Enable `delta.enableChangeDataFeed` and Delta records which rows changed in each commit. Downstream jobs read `table_changes('silver.documents', 12, 20)` — rows tagged insert, update, or delete — instead of the whole table. This is CDC (§5.7) on your own tables, and it is how gold aggregates update incrementally.

### The two maintenance operations you must know

**`OPTIMIZE` — because streaming makes small files.** A job appending every 30 seconds writes thousands of small files a day, and §4.4's small-files problem bites.

```sql
OPTIMIZE silver.documents;                                   -- compact into ~1 GB files
OPTIMIZE silver.documents ZORDER BY (owner_team_id, updated_at);
```

`ZORDER` provides the clustering from §8.3 trick 2. It sorts rows along a space-filling curve, which keeps nearby points in a multi-column space close together, so it clusters on two or three columns at once. Newer Databricks offers **liquid clustering** (`CLUSTER BY`), which does the same incrementally and lets you change the keys later without rewriting the table; prefer it.

**`VACUUM` — because old versions cost money.** Removed files stay on storage so time travel works, until you delete them:

```sql
VACUUM silver.documents RETAIN 168 HOURS;   -- keep 7 days of history
```

The trap: **vacuum destroys time travel beyond the retention window.** With the 7-day default, `VERSION AS OF` from last month fails with a missing-file error. For critical tables, raise it deliberately and budget for the storage.

## 8.7 What Databricks actually is

> **Databricks is managed Spark, plus Delta Lake, plus a catalog, plus a workspace, sold as one thing, running in your cloud account.**

Everything in Chapter 7 still holds: the same driver, executors, shuffles, and skew. The company was founded by Spark's creators at Berkeley, so everything is Spark-shaped underneath, including the SQL product. What it adds is no installation, transactional storage, and managed metadata and permissions.

### The pieces, in the order you meet them

**The workspace** is a web UI with notebooks (mixing Python, SQL, Scala, and R cells), jobs, a data browser, and a SQL editor. Explore in notebooks; anything scheduled belongs in Git, via Databricks Repos.

**Compute comes in three types, and choosing wrong is a top-three cost mistake.**

| Type | What it is | Use for | Cost trap |
|---|---|---|---|
| **All-purpose cluster** | Interactive, shared, long-lived | Notebooks, exploration, debugging | Highest DBU rate; idles all afternoon. **Always set auto-termination.** |
| **Job cluster** | Created for one job run, destroyed after | Every scheduled pipeline | Cheaper rate; slower start (mitigated by pools/serverless) |
| **SQL warehouse** | A Photon-backed SQL endpoint | BI tools, dashboards, ad-hoc SQL | Leaving a large one on 24/7 |

Two modifiers sit on top. **Photon** (§8.3 trick 5) costs more DBUs per hour but typically finishes SQL and Delta work 2–3× faster, so it is usually cheaper overall — except for heavy Python UDF workloads, which it cannot accelerate. **Serverless** runs compute in Databricks' account and starts it in seconds.

**Billing is two bills.** You pay Databricks in **DBUs** (Databricks Units, processing per hour, priced by compute type) *and* your cloud provider for VMs and storage — say $4/hour in DBUs plus $6/hour in EC2. Both stop when the cluster does, so auto-termination is the highest-leverage setting in the product.

**Unity Catalog is the governance layer — learn it early.** It gives everything a three-level name, `catalog.schema.table` (e.g. `prod.silver.documents`), and provides in one place:

- `GRANT`-based permissions down to the column level
- automatic column-level **lineage** — §4.6's lineage, for free
- a searchable catalog and audit logs
- **Delta Sharing**, live read access for another organisation without copying the table

The usual convention is a catalog per environment (`dev`, `prod`) and schemas named for medallion layers. Then `GRANT SELECT ON SCHEMA prod.gold TO analysts` lets analysts read gold while bronze PII stays out of reach.

**Auto Loader** ingests new files from cloud storage as they arrive, tracking what it has seen in RocksDB state instead of listing the directory. It is Structured Streaming (§7.3) with a smarter source, shown in §8.8. `trigger(availableNow=True)` means "process everything new, then exit" — a batch job with exactly-once bookkeeping, usually what ingestion wants.

**Declarative pipelines (Delta Live Tables, now Lakeflow)** let you declare tables instead of wiring jobs. Databricks infers the dependency graph, manages checkpoints, and runs data-quality **expectations** — rules attached to a table, such as `@dlt.expect_or_drop("has_id", "doc_id IS NOT NULL")`:

| Expectation | On a failing row |
|---|---|
| `expect` | records a metric, lets the row through |
| `expect_or_drop` | drops the row |
| `expect_or_fail` | stops the pipeline |

Metrics are kept per run, so you see quality *trend* over time (§4.6).

**Everything else** — Workflows (the built-in scheduler, §4.8's Airflow role), MLflow, Feature Store, Model Serving, dashboards, AI assistants — matters in practice but does not change the architecture.

### Where Databricks fits against its competition

| | Databricks | Snowflake | BigQuery | Open lakehouse (Spark/Trino + Iceberg) |
|---|---|---|---|---|
| Strongest at | ML + engineering + SQL in one place | SQL analytics, ease of use | Serverless SQL, zero ops | Cost and control |
| Storage | Delta/Iceberg in *your* bucket | its own (Iceberg now too) | its own (external tables too) | open files, your bucket |
| Ops burden | medium | low | lowest | high |
| Lock-in | low-ish (open format) | higher | higher | lowest |

For SQL dashboards alone, a warehouse is simpler. With ML, streaming, unstructured data, *and* SQL, the lakehouse saves you running two platforms and copying between them — the real argument for Databricks, more than raw speed.

## 8.8 The medallion architecture

**Medallion** (introduced in §4.2) means data flows **bronze → silver → gold**, each arrow one well-defined kind of transformation. The value is that every table has an obvious home, and "where did this number come from?" has a short answer.

```
 SOURCES ───►  BRONZE  ───────►  SILVER  ─────────►  GOLD
              as received       typed, validated    aggregates, wide
              append-only       deduplicated        and feature tables,
              no logic          conformed, SCD2     one per consumer
            (replayable truth) (the reusable layer) (dashboards, ML, APIs)
```

### Bronze: keep what you were given

**Rule: no business logic. None.** Bronze is §4.2's raw-data rule made into a table; the only additions are metadata about arrival.

```python
(spark.readStream.format("cloudFiles")                     # Auto Loader
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/chk/bronze_docs/schema")
    .option("cloudFiles.schemaEvolutionMode", "rescue")    # unknown fields survive
    .load("s3://lantern-raw/documents/")
    .select("*", F.current_timestamp().alias("_ingested_at"))
 .writeStream
    .option("checkpointLocation", "/chk/bronze_docs")
    .trigger(availableNow=True)
    .toTable("prod.bronze.documents"))
```

- **`rescue` mode** puts unknown fields in a `_rescued_data` column instead of dropping them or failing. When upstream adds a field without telling you (§4.5), the data is already there.
- **Strings are fine here.** If `updated_at` is malformed in 0.1% of records, bronze keeps the string; casting in bronze would lose the bad record forever.
- **Append-only, partitioned by ingestion date,** kept as long as you can afford. Everything else is replayed from it.
- **The checkpoint makes it idempotent.** A re-run does not re-ingest files (§4.7).

### Silver: make it true

Silver applies four kinds of change — **typing, validation, deduplication, and conforming** — and fixes the grain. The table is declared with `CLUSTER BY (owner_team_id, updated_at)` and the comment `'Grain: one row per doc_id, current state.'`, and is filled by one `MERGE`:

```sql
MERGE INTO prod.silver.documents AS t
USING (
  SELECT * FROM (
    SELECT doc_id, title, body, owner_team_id,
           lower(trim(language))              AS language,   -- conform
           try_cast(updated_at AS TIMESTAMP)  AS updated_at,
           op = 'd'                           AS is_deleted,
           _ingested_at,
           row_number() OVER (PARTITION BY doc_id ORDER BY updated_at DESC) AS rn
    FROM prod.bronze.documents
    WHERE _ingested_at > (SELECT coalesce(max(_ingested_at), '1900-01-01')
                          FROM prod.silver.documents)
      AND doc_id IS NOT NULL
      AND try_cast(updated_at AS TIMESTAMP) IS NOT NULL
  ) WHERE rn = 1
) AS s
ON t.doc_id = s.doc_id
WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

Each piece applies an earlier rule:

1. **`try_cast`, not `cast`.** A bad timestamp becomes `NULL` and is filtered out instead of killing the job. Send those rows to a quarantine table rather than dropping them silently; a job that quietly loses 3% of input is §4.6's most dangerous failure.
2. **`row_number()` deduplication.** Kafka and CDC deliver at-least-once (§5.6), so an edit can appear twice; keeping the newest per `doc_id` makes the merge well-defined.
3. **The `updated_at >` guard.** Replaying old data can no longer overwrite newer state, so duplicates and out-of-order input are harmless.
4. **`MERGE` is §4.7's idempotent write.** Run it ten times on the same bronze data; the table is identical after the first.
5. **`CLUSTER BY` on the filter columns,** so file skipping (§8.3) works from day one.

For SCD Type 2 history instead of current state, declarative pipelines do it for you: `dlt.apply_changes(...)` with `keys=["doc_id"]`, `sequence_by` the `updated_at` column (to resolve out-of-order arrival), and `stored_as_scd_type=2`. That solves the whole §8.4 Type 2 problem — validity ranges, late data, deletes — and is the strongest argument for the declarative product.

### Gold: shape it for whoever is asking

Gold tables are **per consumer, not per source**: each exists because a specific dashboard, model, or service needs it, and duplication between them is fine.

```sql
-- The weekly search-quality dashboard. Grain: one row per (team, week).
CREATE OR REPLACE TABLE prod.gold.search_quality_weekly AS
SELECT t.team_name,
       date_trunc('week', s.search_ts)        AS week,
       count(*)                               AS searches,
       count_if(s.clicked) / count(*)         AS ctr
FROM prod.silver.search_events s
JOIN prod.silver.documents d ON d.doc_id  = s.top_result_doc_id
JOIN prod.silver.teams     t ON t.team_id = d.owner_team_id
WHERE s.search_ts >= current_date() - INTERVAL 730 DAYS
GROUP BY 1, 2;
```

A second gold table, `document_features` (one row per `doc_id`), computes each live document's recency and 30-day click-through rate for the Chapter 3 reranker. Two tables, two consumers, overlapping data — entirely correct. One "universal" gold table would serve both badly.

Gold is usually **recomputed rather than merged**: `CREATE OR REPLACE TABLE` over a bounded window is idempotent by construction (§4.7 rule 3: prefer overwriting a partition to appending). When that gets too expensive, update incrementally from silver's change data feed.

### The rules that make the layers worth having

1. **Data only flows forward.** Gold never writes back to silver; a cycle means you cannot rebuild a layer without already having it.
2. **Every layer is rebuildable from the one before.** If silver is corrupted, drop it and re-run.
3. **Bronze has no business logic; gold has nothing but.**
4. **Write the grain in every table comment.**
5. **Skipping a layer is allowed; adding a fourth is a smell.** A tiny reference file can go straight to silver; wanting "silver-plus" usually means gold is doing silver's job.
6. **The layers are about trust.** "Can I use this without checking?" Bronze: no. Silver: yes, if you know the grain. Gold: yes, and the numbers match the dashboard.

### What goes wrong with medallion

| Failure | What happens | Fix |
|---|---|---|
| **Bronze becomes a swamp** | No retention, partitioning, or owner; grows to 400 TB and nobody dares delete | Set retention per source; partition it |
| **Silver becomes gold** | Business rules ("active users", a revenue definition) creep in; every consumer inherits one team's view | Silver is *conformed*, not *interpreted* |
| **Gold multiplies** | Six tables compute "search volume", four disagree — §4.3's Lambda drift again | One definition, one table, everyone reads it |

## 8.9 Making it fast, making it cheap

You pay for compute by the second, so fast and cheap are mostly the same goal. In rough order of leverage:

1. **Partition less than you think.** Partition on the column you *filter* on (usually a date), and only if each partition holds **at least ~1 GB**. A 10 GB table partitioned by `(date, country, team)` becomes 40,000 partitions of 250 KB, slower than none. Under ~1 TB, cluster instead.
2. **Cluster on what you filter on.** `CLUSTER BY` or `ZORDER BY` on the 1–3 most selective filter columns turns min/max statistics into skipped files.
3. **Compact on a schedule.** `OPTIMIZE` nightly on streaming-written tables, or set `delta.autoOptimize.optimizeWrite` and `delta.autoOptimize.autoCompact`.
4. **Kill idle compute.** Auto-terminate interactive clusters at 15–30 minutes, use job clusters for everything scheduled, auto-stop SQL warehouses. Idle interactive clusters are often the largest line on the bill.
5. **Turn on Photon** for SQL and Delta work, then measure. Not for Python-UDF-heavy jobs.
6. **Right-size.** Prefer fewer, bigger nodes for shuffles. Autoscaling hurts short jobs (nodes arrive after the work is done). Use spot instances for batch, on-demand for the driver.
7. **No `SELECT *`, and no `display()` of a billion rows.**
8. **Cache deliberately.** `df.cache()` pays off only for a table read repeatedly in one session; the SSD disk cache is usually better.
9. **Keep statistics current with `ANALYZE`,** so the optimizer broadcasts small tables (§7.2).
10. **Tag every cluster and set budget alerts.** You cannot control what you cannot attribute.

### When *not* to use a lakehouse

This list will save more money than the ten above.

| Situation | Use instead |
|---|---|
| Data under ~100 GB | PostgreSQL, DuckDB, or a small warehouse; a four-minute cluster start for a two-second query is a bad trade |
| Single-row lookups by key | The operational database or a key-value store; Delta rewrites files, Postgres updates a row |
| Sub-second point queries for an app | Serve gold from Redis, DynamoDB, or (for text) OpenSearch |
| Highly concurrent small writes | Buffer through Kafka (§5) and write micro-batches, avoiding commit conflicts and small files |

## 8.10 Operating it

| Symptom | Likely cause | First move |
|---|---|---|
| 10× slower, no code change | small files from streaming writes | `DESCRIBE DETAIL` for `numFiles`; `OPTIMIZE` |
| Full scan despite a `WHERE` | function on the partition column, or no clustering | Check the physical plan for `PartitionFilters` / files pruned |
| One task runs an hour while 199 finish | skew on a join or group-by key | §7.2: check AQE, salt the hot key, or broadcast |
| `ConcurrentAppendException` | two writers touching overlapping files | Partition or serialize writes; retry only if idempotent |
| `VERSION AS OF` fails, missing files | `VACUUM` removed them | Raise `delta.deletedFileRetentionDuration` *beforehand* |
| Job succeeds, row count unchanged | empty source, or checkpoint already consumed the files | Check the checkpoint and `numOutputRows`, not just status |
| Costs doubled, no new workloads | idle all-purpose clusters, or a forgotten backfill | Cost by tag; check auto-termination |
| Schema mismatch on append | upstream added or retyped a column | Intended? `mergeSchema`. Unintended? The check works (§4.5) |
| Dashboard disagrees with analyst | different grain, or duplicate gold tables | Compare table comments; one definition, one table |
| Streaming lag climbing | trigger shorter than batch duration, or unbounded state | §7.7: batch vs trigger; state size and watermarks |

Your tools: `DESCRIBE HISTORY`, `DESCRIBE DETAIL`, the Spark UI, Unity Catalog lineage, and expectation metrics.

## 8.11 Lantern gets an analytics layer

Lantern's operational path is unchanged: PostgreSQL → Debezium → Kafka → Flink → OpenSearch, seconds end to end (§9.2 traces it). The analytics path branches off the same Kafka topics.

```
 PostgreSQL ─CDC─► Kafka ─┬─► Flink ─► OpenSearch       (seconds; serving)
                          └─► Auto Loader
                                 ▼
                     BRONZE  documents, search_events, clicks (append-only, 90d)
                                 ▼
                     SILVER  documents (MERGE), search_events (deduped), teams (SCD2)
                                 ▼
                     GOLD    search_quality_weekly → dashboard
                             document_features     → reranker (§3.7), OpenSearch
                             team_activity_daily   → exec dashboard
```

- **Analytics is a consumer, not a fork.** It reads the same topics as serving. A second CDC connector with its own logic would drift, and you would be running Lambda (§4.3) without deciding to.
- **Gold flows back into the product.** `document_features` — popularity, recency, click-through — is pushed into OpenSearch as document fields and into the reranker (§3.7). Analytics improving the product, not just reporting on it, is the most valuable loop in the chapter.
- **Latency needs differ by orders of magnitude, and that is fine.** A search must be indexed in seconds; an hour-stale dashboard costs nothing. Matching machinery to real needs, rather than streaming everything (§4.3), keeps the platform affordable.

Now the VP's question from §8.1:

```sql
SELECT team_name, week, searches, ctr,
       ctr - lag(ctr) OVER (PARTITION BY team_name ORDER BY week) AS ctr_change
FROM prod.gold.search_quality_weekly
WHERE week >= '2026-01-01'
ORDER BY ctr_change ASC
LIMIT 20;
```

Two seconds against a pre-aggregated gold table on a small SQL warehouse, with the product undisturbed. For "since March", compare the weeks either side of the semantic-search release — §3.9's offline evaluation, with two years of real data instead of a 200-query judgment set.

---

## Key takeaways

- OLTP serves the product row by row; OLAP scans columns across everything. Keep them on separate systems.
- Columnar engines are fast because they skip work: pruning partitions, files, and columns, then compressing, vectorizing, and parallelizing.
- Wrap a partition column in a function, or leave rows unclustered, and you scan the whole table.
- Denormalize for analytics, model facts and dimensions, and write every table's grain in its comment.
- Use SCD Type 2 when history matters, so reprocessing gives the same answer twice.
- A lakehouse is open files plus a transaction log; the log provides ACID, isolation, time travel, `MERGE`, and fast metadata.
- Run `OPTIMIZE` for small files and `VACUUM` for old versions — and remember vacuum ends time travel past its window.
- Databricks is managed Spark plus Delta plus Unity Catalog; its bill punishes idle compute above all.
- Medallion: bronze keeps what you received, silver makes it true, gold shapes it per consumer, and each layer rebuilds from the one before.

## Where we are

The book's promise holds here too: keep the input, make every write idempotent, and rebuilding becomes routine rather than heroic. Lantern now has all its pieces, including an analytics platform that feeds back into the product. The next chapter puts them in one diagram, traces a single edit through every hop, and names the eight ideas that have recurred since Chapter 1.
