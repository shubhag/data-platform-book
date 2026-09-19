# Chapter 8 — The Warehouse and the Lake

## 8.1 The question nobody in this book has asked yet

Seven chapters in, Lantern works. Documents flow out of PostgreSQL through Kafka, get chunked and embedded by Flink, land in OpenSearch, and employees find things. Every chapter so far has been in service of one question: *given a query, return the right documents, fast.*

Now the VP of Engineering asks a different kind of question:

> "Search volume is up 40% this quarter but the support team says people can't find anything. Which teams' documents are getting searched and not clicked? Has that changed since we shipped semantic search in March? And how does it compare to the same period last year?"

Sit with that for a moment, because nothing we have built can answer it.

OpenSearch can't: it holds the current state of documents, not a two-year history of every search and click. Kafka can't: it holds ninety days at best, in a form you can only read sequentially. PostgreSQL *technically* can — the data could be there — but the query needs to scan two years of a click log, join it to documents, join that to teams, group by week, and compare two periods. On the database that is also serving your product, at 10 a.m. on a Tuesday. Try it and you will find out what happens.

This is a category of work with its own name, its own storage engines, its own modelling discipline, and its own multi-billion-dollar vendor ecosystem. It is called **analytics**, or **OLAP**, and it is the last major piece of the platform.

This chapter covers it in three movements. First, the fundamentals: what OLAP means, why analytical queries need a fundamentally different engine, and how to model data for them. Second, the modern architecture — the lakehouse, and specifically **Delta Lake**, the thing that makes a folder full of files behave like a database table. Third, **Databricks**: what the product actually is underneath the marketing, and the **medallion architecture** that nearly every Databricks platform is organised around.

I am assuming you know none of this. If a term appears without explanation, that is a bug; everything gets defined.

## 8.2 OLTP and OLAP: two databases wearing the same SQL

The two acronyms are unhelpfully similar and the distinction they mark is enormous.

**OLTP — Online Transaction Processing.** The database behind an application. PostgreSQL, MySQL, DynamoDB. Its workload is thousands of small operations per second, each touching a handful of rows, each needing to be correct *right now*:

```sql
-- Fetch one document to render a page
SELECT * FROM documents WHERE id = 'doc_8814092';

-- Record an edit
UPDATE documents SET body = $1, updated_at = now() WHERE id = $2;
```

Both queries touch one row. Both must return in single-digit milliseconds. Both must be transactional, because a half-applied edit is a bug the user sees.

**OLAP — Online Analytical Processing.** The database behind decisions. Snowflake, BigQuery, Redshift, ClickHouse, Databricks SQL. Its workload is a small number of enormous queries, each touching hundreds of millions of rows and returning a handful:

```sql
-- Which teams' documents get searched but not clicked, by week?
SELECT
  t.team_name,
  date_trunc('week', s.search_ts)          AS week,
  count(*)                                  AS searches,
  count(c.click_id)                         AS clicks,
  count(c.click_id) / count(*)::float       AS ctr
FROM searches s
LEFT JOIN clicks c   ON c.search_id = s.search_id
JOIN documents d     ON d.id        = s.top_result_doc_id
JOIN teams t         ON t.id        = d.owner_team_id
WHERE s.search_ts >= '2024-01-01'
GROUP BY 1, 2
ORDER BY ctr ASC;
```

This query reads 800 million search rows and 400 million click rows, joins three ways, and returns maybe 400 rows. Nobody expects it in 5 milliseconds — 20 seconds is fine, 2 minutes is tolerable. But it must not fall over, and it must not take down the application while it runs.

### The comparison that actually matters

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

Every row of that table follows from the first one. Read it as a single idea with ten consequences rather than ten facts.

### Why you cannot just run the analytical query on PostgreSQL

Three reasons, and it is worth being precise about them because "use the right tool" is not an argument.

**Reason one: the row store reads everything.** PostgreSQL stores a row contiguously on an 8 KB page (§4.4). Suppose `searches` has 40 columns averaging 200 bytes per row, and 800 million rows — 160 GB. Our query needs three columns: `search_ts`, `search_id`, `top_result_doc_id`, about 30 bytes of the 200.

To read those 30 bytes per row, PostgreSQL must read the pages, and the pages contain all 200. So it reads **160 GB to use 24 GB**. At a very good 1 GB/s of effective disk throughput, that is 160 seconds of pure I/O before a single comparison happens.

A column store keeps each column in its own run of bytes. It reads **only** the three columns — 24 GB — and because a single column holds similar values that compress spectacularly well (more on this in §8.3), that 24 GB is maybe 3 GB on disk. Same query, same data: **3 GB versus 160 GB.** That is not a tuning difference. That is a fifty-fold difference in the amount of work, and it is structural.

**Reason two: one core.** A single PostgreSQL query gets limited parallelism — a few workers, on one machine. An OLAP engine splits the scan across hundreds of cores on dozens of machines, each reading a different slice of the files. The 3 GB scan becomes 60 machines reading 50 MB each. This is the partitioning idea of §1.2 applied to a query instead of to storage.

**Reason three: interference, which is the one that gets people fired.** Your analytical query does not merely run slowly; it evicts the application's working set from the buffer cache, holds long transactions that block vacuum, and consumes I/O the product needs. The dashboard is late *and* checkout is slow. This is the real reason the analytical workload is moved off the operational database, and it holds even when the query would technically have worked.

> **The rule:** operational data is for serving the product; a copy of it, reshaped, is for answering questions about the product. The copy is not an optimisation, it is an isolation boundary.

### A third category, since you will meet it

**HTAP** (Hybrid Transactional/Analytical Processing) systems — SingleStore, TiDB, and to a degree Postgres with columnar extensions — try to serve both from one engine, usually by keeping a row-store for recent writes and a column-store for the bulk. They are real and they work for mid-size workloads. At the scale this book is about, the separation in the rule above is still the default, because the isolation matters as much as the speed.

## 8.3 How a columnar engine actually answers a query

You have seen the columnar layout in §4.4. Now watch what it lets an engine *do*, because the mechanisms are the whole reason OLAP is fast, and each one is something you can accidentally defeat.

Here is the physical picture. A table is a directory of Parquet files:

```
s3://lantern-lake/gold/searches/
  event_date=2026-09-15/part-00000-a1f3.snappy.parquet   (128 MB)
  event_date=2026-09-15/part-00001-b7c2.snappy.parquet   (131 MB)
  event_date=2026-09-16/part-00000-c9d1.snappy.parquet   (127 MB)
  ...
```

Inside one Parquet file:

```
┌─ part-00000.parquet ─────────────────────────────────────────┐
│ Row group 1  (≈128 MB of rows)                               │
│   ├ column chunk: search_ts        [min=09:00, max=09:14]    │
│   ├ column chunk: user_id          [min=u_0001, max=u_9998]  │
│   ├ column chunk: query_text       [ ... ]                   │
│   └ column chunk: top_result_doc_id[min=doc_001, max=doc_99] │
│ Row group 2 ...                                              │
├──────────────────────────────────────────────────────────────┤
│ FOOTER: schema + per-row-group, per-column min/max/nulls/count│
└──────────────────────────────────────────────────────────────┘
```

The footer is the important part. It is a small index describing the file's contents, and every trick below depends on it.

### Trick 1: partition pruning — don't open the file

Files are laid out in directories named `column=value`. A query with `WHERE event_date = '2026-09-16'` never lists, let alone opens, the other directories.

```sql
-- reads 1 day of files
SELECT count(*) FROM searches WHERE event_date = '2026-09-16';

-- reads 730 days of files: the same query, ruined
SELECT count(*) FROM searches WHERE date(search_ts) = '2026-09-16';
```

The second one is the single most common performance bug in analytics. `search_ts` is a *timestamp column*, not the *partition column*, and wrapping it in a function hides even that from the optimizer. Same answer, 730× the work. Filter on the partition column, unwrapped.

### Trick 2: file skipping — open the footer, skip the data

Within the surviving partition, the engine reads each file's footer and compares your predicate to the min/max statistics.

```sql
SELECT * FROM searches
WHERE event_date = '2026-09-16' AND user_id = 'u_44120';
```

Row group 1 has `user_id` between `u_0001` and `u_9998`, so `u_44120` *might* be in there — read it. A row group with min `u_70000` and max `u_79999` *cannot* contain it — skip 128 MB by reading 2 KB of footer.

Notice the condition for this to work: the values must be **clustered**. If every file contains a near-random spread of user IDs, every file's min/max spans the whole range, nothing is skippable, and you scan the table. This is what `OPTIMIZE ... ZORDER BY` and liquid clustering (§8.9) exist to fix — they physically reorganise rows so that similar values land in the same file.

### Trick 3: column pruning and projection pushdown

`SELECT count(*) ... WHERE user_id = ...` touches exactly two columns. The other thirty-eight are never read off disk. This is the 160 GB → 24 GB effect from §8.2, and it is why `SELECT *` in an analytical query is not a stylistic preference but a cost decision.

### Trick 4: compression that only works on columns

A column of values is far more compressible than a row of them, because adjacent values are the same *kind of thing*.

| Column | Raw | Encoding | On disk |
|---|---|---|---|
| `event_date` (1 value per file) | 8 MB | run-length: `2026-09-16 × 1,000,000` | ~30 bytes |
| `country` (200 distinct) | 6 MB | dictionary: 200 strings + 1M small ints | ~300 KB |
| `search_ts` (sorted, ms apart) | 8 MB | delta: base + tiny offsets | ~900 KB |
| `query_text` (high cardinality) | 60 MB | snappy | ~28 MB |

Typical whole-table ratios are 5–15×, and the compressed bytes are what you pay to read *and* what you pay to store. Sorting the data before writing it improves this further, because run-length and delta encodings both reward adjacency.

### Trick 5: vectorized execution

A row-at-a-time engine runs an interpreter loop per row: fetch row, check predicate, branch. For a billion rows that is a billion iterations of a loop full of virtual calls.

A vectorized engine processes a **batch of ~1,000 column values at a time** in a tight loop over a contiguous array — which the CPU pipelines, prefetches and vectorizes with SIMD instructions. Ten to a hundred times faster on the same hardware. Spark's Tungsten (§7.2) does this, Databricks' **Photon** is a rewrite of it in C++, and ClickHouse and DuckDB are built around it.

### Trick 6: parallelism and the shuffle, again

The scan parallelizes trivially: files are independent. Aggregation parallelizes in two phases — each worker computes partial counts for the keys it saw, then a **shuffle** (§7.2) sends all partials for a given key to one worker that adds them up.

Which means the entire skew discussion of §7.2 applies unchanged. `GROUP BY country` where 60% of traffic is one country produces one overloaded task. `JOIN` on a key with one dominant value does the same. If you understood the shuffle in Chapter 7, you understand the failure modes of every OLAP engine ever built.

### Putting the tricks together

Our original question, over a 2-year, 800-million-row table:

```
Naive row-store scan                  160 GB    ~160 s + no parallelism
+ partition pruning (1 quarter)        20 GB
+ column pruning (3 of 40 columns)      3 GB
+ compression                         400 MB
+ file skipping (clustered on team)   120 MB
+ 64 cores in parallel                  2 MB per core
                                    ────────────────────────────────
                                      ~1.5 seconds
```

That is the whole chapter's technical content in one table. Everything else is how to get your data into a state where those six lines apply.

## 8.4 Modelling for analytics: facts, dimensions, and grain

Storage engines are half the story. The other half is that analytical data is *shaped* differently, and the shape has a fifty-year-old discipline behind it — dimensional modelling, from Ralph Kimball. It is still correct, and Databricks platforms use it as heavily as Oracle warehouses did.

### Normalized is wrong here, and that is uncomfortable

In OLTP you normalize: every fact stored once, referenced by key, so an update touches one place. That is exactly right when data changes constantly and you read a few rows at a time.

In OLAP, data is written once and read ten thousand times, and every normalization is a join, and every join is a shuffle. So you **denormalize** deliberately: duplicate the team name onto ten million rows so the query does not have to join to get it. Storage costs a fraction of a cent; the join costs 30 seconds, every time, forever.

### Facts and dimensions

**A fact table** records *events*. One row per thing that happened. It is long and narrow, it grows forever, and its columns are mostly foreign keys plus a few numbers you can add up (the **measures**).

**A dimension table** records *things*. One row per user, document, team, date. It is short and wide, it changes slowly, and its columns are the things you filter and group by (the **attributes**).

For Lantern:

```
                    ┌──────────────┐
                    │  dim_date    │
                    │  date_key    │
                    │  week, month │
                    │  is_weekday  │
                    └──────┬───────┘
  ┌──────────────┐         │          ┌──────────────┐
  │  dim_user    │    ┌────▼─────────┐│ dim_document │
  │  user_key    │◄───┤ fact_search  ├►│ document_key │
  │  email_hash  │    │  date_key    │ │  title       │
  │  department  │    │  user_key    │ │  owner_team  │
  │  country     │    │  document_key│ │  doc_type    │
  └──────────────┘    │  query_text  │ │  language    │
                      │  n_results   │ └──────────────┘
                      │  latency_ms  │
                      │  clicked     │  ← measures
                      │  click_rank  │
                      └──────────────┘
```

That shape — one fact table surrounded by dimensions, one join deep — is a **star schema**. If the dimensions themselves are normalized into sub-tables (`dim_document` → `dim_team` → `dim_org`), it is a **snowflake schema**, and it is usually a mistake: you traded storage you have for joins you pay for on every query.

### Grain: decide it first, write it down

**The grain is what one row means.** "One search event." "One document-day." "One user-week."

Get this wrong and every number in the platform is subtly wrong. The classic disaster: a fact table at the grain of "one row per search *result shown*" (ten rows per search), and an analyst writes `count(*)` expecting searches. Search volume is now reported at 10× reality, and because the number is plausible-looking, it can survive for a year.

> **Write the grain in the table comment, in one sentence, before you write the first line of the pipeline.** `-- Grain: one row per search event, identified by search_id.`

### Slowly changing dimensions

A user moves from Support to Engineering. Do last year's searches belong to Support or Engineering?

Both answers are defensible, and dimensional modelling names them:

- **SCD Type 1 — overwrite.** The dimension holds only the current value. History is rewritten: those old searches are now Engineering's. Simple, and right for correcting mistakes ("the country was misspelled").
- **SCD Type 2 — new row per change.** Each version gets a row with `valid_from` / `valid_to` / `is_current`, and the fact joins to the version that was live at the event's timestamp.

```
user_key  user_id  department   valid_from   valid_to     is_current
u_7#1     u_7      Support      2023-01-05   2026-03-31   false
u_7#2     u_7      Engineering  2026-04-01   9999-12-31   true
```

Type 2 is what "point-in-time correctness" means, and it is precisely the dimension-table hazard flagged in §4.7's backfill checklist: if you re-run last year's job against a Type 1 dimension, you get different answers than the original run, for reasons unrelated to your bug. Type 2 costs more to build and it makes reprocessing deterministic. Use it for anything where history matters — org structure, pricing, entitlements.

### Denormalized wide tables, the modern shortcut

With columnar storage and column pruning, a third option becomes viable: skip the star and write **one very wide table** with the dimension attributes already flattened in.

```sql
CREATE TABLE gold.search_events (
  search_id        STRING,
  search_ts        TIMESTAMP,
  event_date       DATE,
  user_id          STRING,
  user_department  STRING,     -- from dim_user, as of search_ts
  user_country     STRING,
  document_id      STRING,
  document_title   STRING,     -- from dim_document
  owner_team       STRING,
  latency_ms       INT,
  clicked          BOOLEAN,
  click_rank       INT
);
```

Two hundred columns is fine — a query reading four of them pays for four. This is the "one big table" pattern, and for BI dashboards — business intelligence, meaning the reporting and charting tools analysts point at your tables — it is often the fastest and least error-prone thing you can build, because there are no joins to get wrong. The cost is that when a document's title changes, you rewrite it in the fact table rather than in one dimension row, which is a job you now own.

Rule of thumb: model **silver** as a proper star (it is the reusable layer), and materialize **gold** as wide tables shaped for specific consumers. Those two names are the middle and last of the three medallion layers introduced in §4.2 and covered fully in §8.8; for now, read silver as "cleaned and trustworthy" and gold as "shaped for one consumer". Which brings us, finally, to where those layers live.

## 8.5 Warehouse, lake, lakehouse

Three architectures, in the order the industry tried them. The third one is what you will work in, and it only makes sense as an answer to the first two.

### The data warehouse (1990s–2010s)

A specialised database — Teradata, later Snowflake, BigQuery, Redshift — that stores data in its own proprietary format, tightly coupled to its own query engine.

What it gets right is everything about being a database: ACID transactions, schema enforcement, indexes and statistics, fine-grained permissions, and SQL that just works. A well-run warehouse is a genuinely pleasant thing.

What it gets wrong, at scale: it only holds structured, modelled data, so images, audio, raw JSON and ML features live somewhere else; loading data in requires a conversion step; getting data *out* for a Python or ML workload means exporting it; storage and compute are often sold together, so you buy compute to hold data; and everything is in one vendor's format, which is a commercial position as much as a technical one.

### The data lake (2010s)

The reaction: keep everything as files in cheap object storage — S3, ADLS, GCS — in open formats (Parquet, JSON, images, anything), and point whatever engine you like at them.

What it gets right: storage costs almost nothing and scales without limit; storage and compute are fully decoupled, so you spin up 200 cores for ten minutes and pay for ten minutes; any engine can read the files; and any data type is welcome, which is what ML needs.

What it gets wrong is that **a folder is not a table**, and this is worth spelling out because it is the exact gap the lakehouse fills:

- **No transactions.** A job writing 500 files crashes after 300. Readers see a half-written table and no error. There is no rollback.
- **No isolation.** A reader listing the directory while a writer adds files gets some of the new data and some of the old.
- **No schema enforcement.** A malformed upstream change writes `user_id` as an integer into a table where it has always been a string. Nothing complains until a query fails months later.
- **No updates or deletes.** Files are immutable. "Delete this user's rows" means rewriting every file that might contain them — by hand.
- **No history.** You overwrote yesterday's table with a bad job. Yesterday is gone.
- **Terrible metadata performance.** `LIST` on a prefix with two million objects on S3 takes minutes, and the engine must do it just to plan the query.

The industry ran lakes for a decade and coined the phrase **data swamp** for the result: petabytes of files nobody trusted, because there was no way to know whether any given directory was complete, current, or correct.

### The lakehouse (2020s)

The synthesis, and the idea is smaller than the name suggests:

> **Keep the files in open formats in cheap object storage. Add a transaction log next to them that says which files constitute the table right now.**

That single addition — a log — gives you ACID transactions, snapshot isolation, time travel, updates, deletes, and fast metadata, without giving up open formats, cheap storage, decoupled compute, or arbitrary engines. Everything else in the lakehouse is a consequence of the log.

The three implementations are **Delta Lake** (Databricks, and the one this chapter uses), **Apache Iceberg** (Netflix originally; now the broad-industry choice, supported by Snowflake, AWS, and Databricks too) and **Apache Hudi** (Uber; strongest on streaming upserts). The concepts transfer almost perfectly between them — if you learn Delta you can read Iceberg documentation without difficulty.

And if the phrase "a durable log that is the source of truth, from which the current state is derived" sounds familiar, it should. It is Kafka's idea (§5.1), it is the write-ahead log in PostgreSQL, it is Flink's checkpointing, and it is now your table format. This is the fourth appearance of the log in this book and the reason Chapter 5 said it was the most important data structure in the field.

## 8.6 Delta Lake: the log that turns a folder into a table

Let me show you the mechanism, because it is simple enough to hold entirely in your head, and once you do, every Delta behaviour — including the surprising ones — becomes predictable.

### What is on disk

```
s3://lantern-lake/silver/documents/
  _delta_log/
    00000000000000000000.json      ← commit 0: create table, add 4 files
    00000000000000000001.json      ← commit 1: add 2 files
    00000000000000000002.json      ← commit 2: remove 1, add 1 (an UPDATE)
    ...
    00000000000000000010.checkpoint.parquet   ← every 10 commits, a snapshot
    _last_checkpoint
  part-00000-6f3a....snappy.parquet
  part-00001-9b1c....snappy.parquet
  ...
```

Plain Parquet files, plus a `_delta_log` directory. **The Parquet files in the directory are not the table.** The table is whatever the log says it is. A Parquet file sitting there that the log never added is invisible — which is how atomicity works.

### What is in a commit

Each commit is a small JSON file, one action per line:

```json
{"commitInfo":{"timestamp":1789459200000,"operation":"MERGE",
  "operationParameters":{"predicate":"(target.doc_id = source.doc_id)"},
  "operationMetrics":{"numTargetRowsUpdated":"1240","numTargetRowsInserted":"87"}}}
{"remove":{"path":"part-00003-1a2b.snappy.parquet","deletionTimestamp":1789459200000,
  "dataChange":true,"size":134217728}}
{"add":{"path":"part-00017-77ef.snappy.parquet","size":135290000,"dataChange":true,
  "stats":"{\"numRecords\":982341,
             \"minValues\":{\"doc_id\":\"doc_000001\",\"updated_at\":\"2026-09-16T00:00:11Z\"},
             \"maxValues\":{\"doc_id\":\"doc_049999\",\"updated_at\":\"2026-09-16T23:59:58Z\"},
             \"nullCount\":{\"owner_team_id\":0}}"}}
```

Three things to notice, because each one explains a family of features.

**One: the unit of change is a file, not a row.** An `UPDATE` touching one row rewrites the whole Parquet file containing it, then commits `remove old, add new`. This is why Delta is excellent at bulk changes and merely adequate at single-row ones — the opposite of PostgreSQL. (Modern Delta softens this with **deletion vectors**: instead of rewriting a 128 MB file to delete 50 rows, it writes a tiny bitmap marking those row positions as dead, and rewrites lazily later. Same semantics, far less write amplification.)

**Two: the statistics are in the log.** `minValues` / `maxValues` per file are right there in the commit, so the engine does *file skipping* (§8.3, trick 2) by reading the log — no S3 listing, no footer reads. This is the "fast metadata" advantage, and it is a big one: planning a query over a million-file table takes a second rather than ten minutes.

**Three: reading is a fold over the log.** To read the table, you take the last checkpoint and replay the JSON commits after it, adding and removing paths, and end up with a set of files. That set is your **snapshot**. Everything below falls out of this.

### The features, and why each one is free

**Atomicity.** The writer writes its Parquet files first — invisible, because no one has added them — and then makes a single atomic write of `00000000000000000003.json`. Either that file exists or it doesn't. A job that dies after writing 300 of 500 data files leaves 300 orphans and no commit; readers see nothing changed. Compare that to the lake's half-written table.

**Snapshot isolation.** A reader resolves the log once, at version 7, and reads those files for the duration of its query. A writer committing version 8 removes files from the *table*, but does not delete them from storage — so the reader's files are still there. Readers never block writers, writers never block readers, and nobody sees a partial result. Exactly the MVCC idea — multi-version concurrency control, where a reader sees a consistent version of the data while writers create new ones — implemented here with nothing more than a list of filenames.

**Optimistic concurrency for writers.** Two jobs both read version 7 and both try to commit version 8. The commit is an atomic put-if-absent, so one wins. The loser re-reads the log, checks whether the winner's changes conflict with its own (did they touch overlapping files? overlapping partitions?), and if not, retries as version 9. Two jobs appending to different partitions never conflict. Two jobs updating the same rows do, and the second one fails with `ConcurrentAppendException` — which is a correct failure, not a bug.

**Time travel.** Old versions are just older prefixes of the log, and their files still exist until vacuumed:

```sql
SELECT * FROM silver.documents VERSION AS OF 7;
SELECT * FROM silver.documents TIMESTAMP AS OF '2026-09-16 09:00:00';

-- what changed, and who did it
DESCRIBE HISTORY silver.documents;

-- undo a bad job, in one statement
RESTORE TABLE silver.documents TO VERSION AS OF 7;
```

I want to dwell on `RESTORE` for a second, because if you have ever been on the receiving end of a bad batch job, the significance is obvious and if you haven't, it isn't. The old recovery procedure was: find yesterday's backup, hope it exists, restore it to a side location, diff, swap, explain to people. The new one is one statement and a version number, and it runs in seconds because it only rewrites the log. It also turns §4.7's "write to a shadow table, diff, swap" into "write, diff against `VERSION AS OF`, restore if wrong".

**Schema enforcement and evolution.** The schema lives in the log too. Write a `LongType` into a `StringType` column and the write fails at commit time with a clear error, rather than corrupting the table silently. When you *want* the change:

```python
(df.write.format("delta").mode("append")
   .option("mergeSchema", "true")     # add new columns, don't reorder or retype
   .saveAsTable("bronze.documents"))
```

This is §4.5's schema registry idea enforced at the storage layer rather than the transport layer, and the two are complementary: the registry catches it at the producer, the table catches it at the sink.

**MERGE — the upsert that makes everything else possible.** The single most-used Delta statement:

```sql
MERGE INTO silver.documents AS t
USING staged_changes AS s
  ON t.doc_id = s.doc_id
WHEN MATCHED AND s.op = 'DELETE' THEN DELETE
WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET *
WHEN NOT MATCHED AND s.op <> 'DELETE' THEN INSERT *;
```

Read that against §4.7's four ways to make a write idempotent. This *is* "upsert by primary key", in SQL, transactionally, over object storage. Run it twice with the same input and the table is identical, because the `updated_at` guard makes the second run a no-op. That property is why the medallion architecture in §8.8 can promise that any layer is safe to rebuild.

**CDF — change data feed.** Turn it on and Delta records which rows changed per commit, so downstream jobs can consume *changes* rather than re-reading the table:

```sql
ALTER TABLE silver.documents SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

SELECT * FROM table_changes('silver.documents', 12, 20);
-- rows carry _change_type: insert | update_preimage | update_postimage | delete
```

This is CDC (§5.7) applied to your own tables, and it is how gold aggregates get updated incrementally instead of by full recomputation.

### The two maintenance operations you must know

Delta's design creates exactly two housekeeping jobs, and neglecting either one is the most common way a Databricks platform gets slow.

**`OPTIMIZE` — because streaming makes small files.** A job appending every 30 seconds writes small files, thousands per day, and §4.4's small-files problem bites: per-file overhead dominates, and the log itself gets long.

```sql
OPTIMIZE silver.documents;                          -- compact into ~1 GB files
OPTIMIZE silver.documents ZORDER BY (owner_team_id, updated_at);
```

`ZORDER` is the clustering from §8.3 trick 2: it sorts rows by a space-filling curve over the named columns — a curve that visits every point in a multi-column space while keeping nearby points close together, which is how it clusters on two or three columns at once rather than just the first — so rows with similar `owner_team_id` land in the same file, so min/max skipping actually skips. Use it on the 1–3 columns your queries filter on most. Newer Databricks offers **liquid clustering** (`CLUSTER BY`), which does the same job incrementally and lets you change the clustering keys later without rewriting the table — prefer it when available, because "we chose the wrong Z-order columns and now cannot change them" is a real and annoying situation.

**`VACUUM` — because old versions cost money.** Removed files stay on storage so time travel works. Eventually you must delete them.

```sql
VACUUM silver.documents RETAIN 168 HOURS;   -- keep 7 days of history
```

The trap is right there in the statement: **vacuum destroys time travel beyond the retention window.** Set it to 7 days and `VERSION AS OF` from last month fails with a missing-file error. The default is 7 days; for critical tables, raise it deliberately and account for the storage.

## 8.7 What Databricks actually is

Now the product. Stripped of marketing, Databricks is:

> **Managed Spark, plus Delta Lake, plus a catalog, plus a workspace, sold as one thing, running in your cloud account.**

Everything you learned in Chapter 7 is still exactly true — Databricks *is* Spark, the same driver, executors, shuffles, and skew. What Databricks adds is that you never install it, the storage format is transactional, and the metadata, permissions, and notebooks are managed for you.

Its origin explains its shape: the company was founded by the people who created Spark at Berkeley. Everything is Spark-shaped underneath, including the SQL product.

### The pieces, in the order you meet them

**The workspace.** A web UI with notebooks, jobs, data browser, and SQL editor. Notebooks are cells of Python, SQL, Scala or R against a running cluster, with `%sql` / `%python` magics to mix languages in one notebook. Excellent for exploration; use real files in Git for anything that runs on a schedule (Databricks Repos gives you Git integration for exactly this reason).

**Compute, which comes in three flavours and choosing wrong is a top-three cost mistake.**

| Type | What it is | Use for | Cost trap |
|---|---|---|---|
| **All-purpose cluster** | Interactive, shared, long-lived | Notebooks, exploration, debugging | Most expensive DBU rate; idles all afternoon. **Always set auto-termination.** |
| **Job cluster** | Created for one job run, destroyed after | Every scheduled pipeline | Cheaper rate; slower start (mitigated by pools/serverless) |
| **SQL warehouse** | A Photon-backed SQL endpoint | BI tools, dashboards, ad-hoc SQL | Leaving a large one on 24/7 |

Plus two modifiers: **Photon**, the C++ vectorized engine (§8.3 trick 5) — costs more DBUs per hour, typically finishes 2–3× faster on SQL and Delta workloads, so usually cheaper overall, but not on heavy Python UDF workloads which it cannot accelerate; and **serverless**, where Databricks runs the compute in its own account and starts it in seconds rather than minutes.

**Billing is two bills, which surprises people.** You pay Databricks in **DBUs** (a unit of processing per hour, varying by compute type) *and* your cloud provider for the underlying VMs and storage. A cluster that costs $4/hour in DBUs might cost another $6/hour in EC2. Both stop when the cluster does, which is why auto-termination is the highest-leverage setting in the entire product.

**Unity Catalog — the governance layer, and the thing to understand early.** It provides a three-level namespace over everything:

```
catalog . schema . table
prod    . silver . documents
```

Plus, in one place: `GRANT`-based permissions down to the column level, automatic column-level **lineage** (which table, notebook and dashboard consumed this column — §4.6's lineage, for free), a searchable data catalog, audit logs, and **Delta Sharing** for giving another organisation live read access to a table without copying it.

The practical convention is a catalog per environment — `dev`, `staging`, `prod` — and schemas named for the medallion layers. Then:

```sql
GRANT SELECT ON SCHEMA prod.gold TO `analysts`;
GRANT USAGE  ON CATALOG prod     TO `analysts`;
-- analysts can read gold, and cannot read bronze PII at all
```

**Auto Loader — incremental file ingestion that scales.** Reads new files from cloud storage as they arrive, tracking what it has already seen in RocksDB state rather than by listing the directory:

```python
(spark.readStream.format("cloudFiles")
   .option("cloudFiles.format", "json")
   .option("cloudFiles.schemaLocation", "/chk/bronze_docs/schema")
   .option("cloudFiles.inferColumnTypes", "true")
   .load("s3://lantern-raw/documents/")
 .writeStream
   .option("checkpointLocation", "/chk/bronze_docs")
   .trigger(availableNow=True)          # §7.3: incremental batch
   .toTable("bronze.documents"))
```

That is Structured Streaming (§7.3) with a smarter source. `availableNow=True` means "process everything new, then exit" — a batch job with exactly-once bookkeeping, which is usually what you want for ingestion. For genuinely low latency, use a continuous trigger instead; nothing else changes.

**Declarative pipelines (Delta Live Tables / Lakeflow).** Instead of writing jobs and wiring dependencies, you declare tables and let Databricks infer the DAG, manage checkpoints, and run data-quality **expectations**:

```python
import dlt
from pyspark.sql import functions as F

@dlt.table(comment="Raw document edits, exactly as received. Grain: one edit event.")
def bronze_documents():
    return (spark.readStream.format("cloudFiles")
              .option("cloudFiles.format", "json")
              .load("s3://lantern-raw/documents/"))

@dlt.table(comment="Valid, typed, deduplicated documents. Grain: one row per doc_id.")
@dlt.expect_or_drop("has_id",     "doc_id IS NOT NULL")
@dlt.expect_or_fail("sane_length","length(body) < 5000000")
@dlt.expect("has_team",           "owner_team_id IS NOT NULL")   # warn only
def silver_documents():
    return (dlt.read_stream("bronze_documents")
              .withColumn("updated_at", F.col("updated_at").cast("timestamp"))
              .dropDuplicates(["doc_id", "updated_at"]))
```

Three expectation strengths, and the distinction matters: `expect` records a metric and lets the row through, `expect_or_drop` quarantines the row, `expect_or_fail` stops the pipeline. This is §4.6's quality dimensions with somewhere to live, and the metrics are recorded per run so you can see quality *trend* rather than only its current state.

**Everything else, briefly.** Workflows (the scheduler/orchestrator — §4.8's Airflow role, built in), MLflow (experiment tracking and model registry), Feature Store, Model Serving, Databricks SQL dashboards, and a set of AI assistants including natural-language querying over your tables. These matter in practice and none of them change the architecture, which is why they get a paragraph.

### Where Databricks fits against its competition

| | Databricks | Snowflake | BigQuery | Open lakehouse (Spark/Trino + Iceberg) |
|---|---|---|---|---|
| Strongest at | ML + engineering + SQL in one place | SQL analytics, ease of use | Serverless SQL, zero ops | Cost and control |
| Storage | Delta/Iceberg in *your* bucket | its own (Iceberg now too) | its own (external tables too) | open files, your bucket |
| Ops burden | medium | low | lowest | high |
| Lock-in | low-ish (open format) | higher | higher | lowest |

The honest summary: if your workload is SQL dashboards and nothing else, a warehouse is simpler. If you have ML, streaming, unstructured data, and SQL, the lakehouse stops you from running two platforms and copying data between them — and that, rather than raw query speed, is the actual argument for Databricks.

## 8.8 The medallion architecture

You met the three layers in §4.2 as a naming convention. Here is the full version, with the rules that make it work and a complete worked example.

**Medallion** is an organising principle: data flows through **bronze → silver → gold**, and each arrow is a single, well-defined kind of transformation. The value is not the names; it is that every table in your platform has an obvious home, and that "where did this number come from" has a short answer.

```
  SOURCES              BRONZE                SILVER                  GOLD
 ┌─────────┐      ┌──────────────┐    ┌────────────────┐    ┌──────────────────┐
 │Postgres │─CDC─►│ as-received  │───►│ typed          │───►│ business aggregates│
 │Kafka    │─────►│ append-only  │    │ validated      │    │ wide tables       │
 │APIs     │─────►│ + metadata   │    │ deduplicated   │    │ feature tables    │
 │Files    │─────►│ no business  │    │ conformed      │    │ shaped per consumer│
 └─────────┘      │   logic      │    │ SCD2 history   │    └──────────────────┘
                  └──────────────┘    └────────────────┘              │
                   replayable truth     the reusable layer      dashboards, ML,
                                                                OpenSearch, APIs
```

### Bronze: keep what you were given

**Rule: no business logic. None.** Bronze is the raw-data rule of §4.2 made into a table. The only things you add are metadata about the arrival.

```python
from pyspark.sql import functions as F

bronze = (spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/chk/bronze_docs/schema")
    .option("cloudFiles.schemaEvolutionMode", "rescue")   # unexpected fields survive
    .load("s3://lantern-raw/documents/")
    .select(
        "*",
        F.col("_metadata.file_path").alias("_source_file"),
        F.current_timestamp().alias("_ingested_at"),
        F.lit("postgres-cdc-v3").alias("_source_system"),
    ))

(bronze.writeStream
    .option("checkpointLocation", "/chk/bronze_docs")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable("prod.bronze.documents"))
```

Things to notice:

- **`rescue` mode.** Fields the schema doesn't know about are captured into a `_rescued_data` column instead of being dropped or failing the job. When the upstream team adds a field without telling you (§4.5, and it *will* happen), the data is already there when you find out.
- **String types are acceptable here.** If `updated_at` arrives as a malformed string in 0.1% of records, bronze keeps the string. Casting is silver's job, and casting in bronze means the malformed record is lost forever.
- **Append-only, partitioned by ingestion date**, retained for as long as you can afford. This is the table you replay everything else from.
- **The checkpoint is what makes it idempotent.** Re-running the job does not re-ingest files; Auto Loader's state knows what it has seen. That is exactly §4.7's "make every write safe to run twice", provided by the framework.

### Silver: make it true

Silver applies exactly four kinds of change — typing, validation, deduplication, and conforming — and it is where the grain is finally fixed.

```sql
CREATE TABLE IF NOT EXISTS prod.silver.documents (
  doc_id         STRING  NOT NULL COMMENT 'Primary key from PostgreSQL',
  title          STRING,
  body           STRING,
  owner_team_id  STRING,
  language       STRING  COMMENT 'ISO 639-1, lowercased',
  created_at     TIMESTAMP,
  updated_at     TIMESTAMP,
  is_deleted     BOOLEAN,
  _ingested_at   TIMESTAMP
)
USING DELTA
CLUSTER BY (owner_team_id, updated_at)
COMMENT 'Grain: one row per doc_id, current state. Source: bronze.documents.';
```

And the transformation, which is one `MERGE` and deserves a careful read:

```sql
MERGE INTO prod.silver.documents AS t
USING (
  -- exactly one row per doc_id: the newest edit in this batch
  SELECT * FROM (
    SELECT
      doc_id,
      title,
      body,
      owner_team_id,
      lower(trim(language))                 AS language,
      try_cast(created_at AS TIMESTAMP)      AS created_at,
      try_cast(updated_at AS TIMESTAMP)      AS updated_at,
      op = 'd'                               AS is_deleted,
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

Five things are happening, and each corresponds to a rule from earlier chapters:

1. **`try_cast` rather than `cast`.** A bad timestamp becomes `NULL` and is filtered out, instead of killing the job. Route those rows to a quarantine table rather than discarding them silently — a job that quietly drops 3% of input is §4.6's most dangerous failure, the one that succeeds while being wrong.
2. **`row_number()` deduplication.** Kafka and CDC deliver at-least-once (§5.6), so the same edit can appear twice. Keeping only the newest row per `doc_id` per batch makes the merge well-defined.
3. **The `updated_at >` guard.** Without it, a replay of old data would overwrite newer state with older state. With it, out-of-order and duplicate input are both harmless.
4. **`MERGE` is the idempotent write** of §4.7. Run this statement ten times on the same bronze data; the table is byte-identical after the first.
5. **`CLUSTER BY` on the filter columns** so that §8.3's file skipping works from day one.

If you want SCD2 history instead of current state, Databricks gives it declaratively:

```python
dlt.create_streaming_table("silver_documents_history")
dlt.apply_changes(
    target="silver_documents_history",
    source="bronze_documents",
    keys=["doc_id"],
    sequence_by=F.col("updated_at"),     # resolves out-of-order arrival
    apply_as_deletes=F.expr("op = 'd'"),
    stored_as_scd_type=2,                # 1 = current only, 2 = full history
)
```

That handles the entire §8.4 SCD Type 2 problem — validity ranges, late data, deletes — in eight lines. It is the single strongest argument for the declarative pipeline product.

### Gold: shape it for whoever is asking

Gold tables are **per consumer**, not per source. Each one exists because a specific dashboard, model, or service needs it, and it is allowed to duplicate data from other gold tables freely.

```sql
-- Gold table 1: the weekly search-quality dashboard.
-- Grain: one row per (team, week).
CREATE OR REPLACE TABLE prod.gold.search_quality_weekly AS
SELECT
  d.owner_team_id,
  t.team_name,
  date_trunc('week', s.search_ts)                            AS week,
  count(*)                                                   AS searches,
  count_if(s.clicked)                                        AS clicks,
  count_if(s.clicked) / count(*)                             AS ctr,
  avg(s.latency_ms)                                          AS avg_latency_ms,
  percentile_approx(s.latency_ms, 0.95)                      AS p95_latency_ms,
  count_if(s.clicked AND s.click_rank <= 3) / count(*)       AS top3_click_rate
FROM prod.silver.search_events s
JOIN prod.silver.documents d ON d.doc_id = s.top_result_doc_id
JOIN prod.silver.teams     t ON t.team_id = d.owner_team_id
WHERE s.search_ts >= current_date() - INTERVAL 730 DAYS
GROUP BY 1, 2, 3;
```

```sql
-- Gold table 2: features for the reranking model (Chapter 3).
-- Grain: one row per doc_id. Consumed by the training job and by serving.
CREATE OR REPLACE TABLE prod.gold.document_features AS
SELECT
  d.doc_id,
  d.owner_team_id,
  length(d.body)                                             AS body_length,
  datediff(current_date(), d.updated_at)                     AS days_since_update,
  coalesce(s.impressions_30d, 0)                             AS impressions_30d,
  coalesce(s.clicks_30d, 0)                                  AS clicks_30d,
  coalesce(s.clicks_30d / nullif(s.impressions_30d, 0), 0)   AS ctr_30d
FROM prod.silver.documents d
LEFT JOIN (
  SELECT top_result_doc_id AS doc_id,
         count(*)          AS impressions_30d,
         count_if(clicked) AS clicks_30d
  FROM prod.silver.search_events
  WHERE search_ts >= current_date() - INTERVAL 30 DAYS
  GROUP BY 1
) s ON s.doc_id = d.doc_id
WHERE d.is_deleted = false;
```

Two gold tables, two consumers, overlapping data, and that is entirely correct. The mistake would be building one "universal" gold table that serves both badly.

Gold tables are also usually **recomputed rather than merged** — `CREATE OR REPLACE TABLE` over a bounded window, which is idempotent by construction (§4.7 rule 3: prefer overwriting a partition to appending to one). When full recomputation gets too expensive, switch to incremental updates driven by the silver table's change data feed.

### The rules that make the layers worth having

> 1. **Data only flows forward.** Gold never writes back to silver. A cycle means you cannot rebuild a layer without already having it.
> 2. **Every layer is rebuildable from the one before it.** Which means that if silver is corrupted, you drop it and re-run. That is the property you are buying.
> 3. **Bronze has no business logic; gold has nothing but.**
> 4. **Write the grain in the table comment.** Every table. One sentence.
> 5. **Skipping a layer is allowed, and adding a fourth is a smell.** A tiny reference file can go straight to silver. But if you find yourself wanting "silver-plus", the real problem is usually that a gold table is doing silver's job.
> 6. **The layers are about trust, not about count.** The question each layer answers is "can I use this without checking?" — bronze: no; silver: yes, if you know the grain; gold: yes, and the numbers match the dashboard.

### What goes wrong with medallion, honestly

Three failure modes, all common:

- **Bronze becomes a swamp.** No retention policy, no partitioning, no ownership; it grows to 400 TB and nobody dares delete anything. Set retention deliberately per source, and partition it.
- **Silver becomes gold.** Business rules creep into silver — a revenue definition here, a filter for "active users" there — and now every consumer inherits one team's definition. Silver is *conformed*, not *interpreted*.
- **Gold multiplies without ownership.** Six tables computing "search volume", four of which disagree. This is the Lambda drift problem (§4.3) in a new costume. The fix is the same: one definition, one table, everyone reads it.

## 8.9 Making it fast, making it cheap

The two goals are mostly the same goal, because you pay for compute by the second. In rough order of leverage:

**1. Partition correctly, which usually means less than you think.** The rule: partition on the column you *filter* on, almost always a date, and only if each partition holds **at least ~1 GB**. Partitioning a 10 GB table by `(date, country, team)` produces 40,000 partitions of 250 KB each, and the resulting small-files problem makes every query slower than no partitioning at all. Under ~1 TB, do not partition; cluster instead.

**2. Cluster on what you filter on.** `CLUSTER BY` (liquid) or `ZORDER BY` on the 1–3 highest-selectivity filter columns. This is what turns min/max statistics into actual skipped files (§8.3).

**3. Compact, on a schedule.** `OPTIMIZE` nightly on every streaming-written table, or turn on auto-compaction and optimized writes:

```sql
ALTER TABLE prod.silver.documents SET TBLPROPERTIES (
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact   = true
);
```

**4. Kill idle compute.** Auto-termination at 15–30 minutes on every interactive cluster; job clusters for everything scheduled; auto-stop on SQL warehouses. In most organisations' bills, idle interactive clusters are the largest single line item, and it is pure waste.

**5. Turn on Photon** for SQL and Delta-heavy work, then measure. Higher rate, shorter runtime, usually net cheaper. Not for Python-UDF-dominated jobs.

**6. Right-size, and prefer fewer bigger nodes for shuffles.** Autoscaling helps variable workloads and hurts short ones (the nodes arrive after the work is done). Spot/preemptible instances for fault-tolerant batch, on-demand for the driver.

**7. Do not `SELECT *`, and do not `display()` a billion rows.** Column pruning only helps if you let it.

**8. Cache deliberately, not reflexively.** `CACHE TABLE` / `df.cache()` is worth it for a table read repeatedly in one session; it is memory pressure otherwise. Databricks' disk cache on SSD-backed instances is usually the better default.

**9. Use `ANALYZE` / keep statistics current** so the optimizer picks broadcast joins for small tables — the biggest single join win (§7.2).

**10. Set a budget alert and tag every cluster** with a team and pipeline name. You cannot control what you cannot attribute. This is organisational, and it is on this list because it works.

### When *not* to use a lakehouse

A short section that will save you more money than the ten items above.

- **Data under ~100 GB.** PostgreSQL, DuckDB, or a small warehouse will be faster, cheaper and simpler. Spark's overhead is real, and a cluster that takes four minutes to start to run a two-second query is a bad trade.
- **Single-row lookups by key.** That is OLTP. Delta rewrites files; Postgres updates a row. Serve from the operational database or a key-value store.
- **Sub-second point queries for an application.** Serve gold into Redis, DynamoDB, or — for text — OpenSearch. The lakehouse computes the answer; something else serves it.
- **Highly concurrent small writes.** Thousands of tiny commits per minute will cause optimistic-concurrency conflicts and a million small files. Buffer through Kafka (§5) and write in micro-batches.

## 8.10 Operating it

The symptom-to-cause table, in the style of the other chapters.

| Symptom | Likely cause | First move |
|---|---|---|
| Query suddenly 10× slower, no code change | small files from streaming writes | `DESCRIBE DETAIL tbl` → check `numFiles` and average size; `OPTIMIZE` |
| Query scans the whole table despite a `WHERE` | function applied to the partition column, or wrong clustering | Read the physical plan for `PartitionFilters` / `files pruned` |
| One task runs for an hour while 199 finish | skew on a join or group-by key | §7.2: check AQE is on, salt the hot key, or broadcast the small side |
| `ConcurrentAppendException` | two writers touching overlapping files | Partition the writes, or serialize them; retry is only correct if the write is idempotent |
| `VERSION AS OF` fails with missing files | `VACUUM` removed them | Raise `delta.deletedFileRetentionDuration` *before* you need history |
| Job succeeds, table row count unchanged | source path empty, or checkpoint already consumed the files | Check the Auto Loader checkpoint and the run's `numOutputRows`, not just its status |
| Costs doubled with no new workloads | idle all-purpose clusters, or a backfill left running | Cluster-level cost attribution by tag; check auto-termination settings |
| Schema-mismatch failure on a nightly append | upstream added or retyped a column | Intended? `mergeSchema`. Unintended? This is the failure working correctly (§4.5) |
| Dashboard numbers disagree with the analyst's query | different grain, or duplicate gold definitions | Compare the table comments; there should be one definition and one table |
| Streaming query lag climbing steadily | trigger interval shorter than batch duration, or state growing unbounded | §7.7: batch duration vs trigger; check state store size and watermarks |

And the observability that makes those first moves possible: `DESCRIBE HISTORY` (who changed what, when, with metrics), `DESCRIBE DETAIL` (file count and size), the Spark UI (stages, skew, spill — unchanged from Chapter 7), Unity Catalog lineage (what breaks if I change this column), and DLT expectation metrics over time (is quality drifting).

## 8.11 Lantern gets an analytics layer

Assembling it for the running example. Lantern's operational path is unchanged: PostgreSQL → Debezium → Kafka → Flink → OpenSearch, seconds end to end (§9.2 traces it in full). The analytics path branches off the same Kafka topics, which is the point — one source of truth, two consumers with different latency requirements.

```
 PostgreSQL ──CDC──► Kafka ──┬──► Flink ──► OpenSearch        (seconds; serving)
                             │
                             └──► Auto Loader
                                     │
                                  BRONZE  documents, search_events, clicks
                                     │     (as received, append-only, 90d)
                                     ▼
                                  SILVER  documents (MERGE, current state)
                                          search_events (deduped, typed)
                                          teams (SCD2)
                                     ▼
                                   GOLD   search_quality_weekly  → dashboard
                                          document_features      → reranker (§3.7)
                                          team_activity_daily    → exec dashboard
                                                                 → back to OpenSearch
```

Three things about that diagram are worth stating explicitly.

**The analytics path is a consumer, not a fork.** It reads the same Kafka topics as the serving path. If it were fed by a second CDC connector with its own logic, the two would drift, and you would be running Lambda (§4.3) without having decided to.

**Gold flows back into the serving path.** `document_features` — popularity, recency, click-through — gets pushed into OpenSearch as document fields and into the reranker (§3.7). The analytical platform improves the product rather than only reporting on it, and this loop is the most valuable thing in the chapter.

**Latency requirements differ by an order of magnitude and that is fine.** A search must be indexed within seconds. The search-quality dashboard being an hour stale costs nothing. Matching each path's machinery to its actual requirement — rather than making everything streaming because streaming is modern (§4.3) — is the difference between a platform that is expensive and one that is not.

Now the VP's question from §8.1:

```sql
SELECT
  team_name,
  week,
  searches,
  ctr,
  ctr - lag(ctr) OVER (PARTITION BY team_name ORDER BY week) AS ctr_change,
  avg(ctr) OVER (PARTITION BY team_name ORDER BY week
                 ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)   AS ctr_4wk_avg
FROM prod.gold.search_quality_weekly
WHERE week >= '2026-01-01'
ORDER BY ctr_change ASC
LIMIT 20;
```

Two seconds, against a pre-aggregated gold table of a few hundred thousand rows, on a small SQL warehouse, while the product serves traffic entirely undisturbed.

And to answer the "since we shipped semantic search in March" part properly, you would compare the weeks either side of the release — which is exactly the offline evaluation of §3.9, now with two years of real data behind it instead of a judgment set of 200 queries.

---

## Where we are

Analytical queries are a different workload from operational ones — few queries, enormous scans, aggregate answers — and that difference forces a different engine. Columnar storage, partition pruning, file skipping via min/max statistics, dictionary and run-length compression, vectorized execution, and parallel scan-and-shuffle combine to turn a 160 GB scan into 2 MB per core. Every one of those tricks can be defeated by a query that wraps the partition column in a function, or by a table whose rows are not clustered on what you filter by.

Analytical data is shaped differently too: facts and dimensions, a declared grain, deliberate denormalization, and Type 2 history when reprocessing must be deterministic.

The lakehouse is the current answer to where that data lives: open Parquet files in cheap object storage, plus a transaction log that says which files are the table. That log — the same idea as Kafka's, PostgreSQL's WAL, and Flink's checkpoints — buys ACID commits, snapshot isolation, time travel, `MERGE`, schema enforcement, and fast metadata, in exchange for two maintenance jobs you must actually run: `OPTIMIZE` and `VACUUM`.

Databricks is that stack sold as a product: managed Spark, Delta, Unity Catalog, Auto Loader, declarative pipelines and SQL warehouses. Its cost model rewards exactly one discipline — do not leave compute running — and its architecture is organised, nearly universally, as medallion: bronze holds what you received, silver makes it true, gold shapes it for a named consumer, data flows only forward, and every layer can be rebuilt from the one before it.

Which is the same promise the whole book has been making, in its final costume: keep the input, make every write idempotent, and rebuilding becomes routine rather than heroic.

Lantern now has all of its pieces — a search index, embeddings, a log, a streaming pipeline, a batch path, and an analytics platform that feeds back into the product. The next chapter puts them in one diagram, traces a single edit through every hop, and names the eight ideas that have been recurring since Chapter 1.
