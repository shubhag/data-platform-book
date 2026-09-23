# Chapter 4 — Moving Data

Getting data into a search index is not an implementation detail; it is most of the work. This chapter covers the principles behind every pipeline: how data moves, what shape it takes on disk, how you know it is any good, and how you make a pipeline safe to run twice.

**By the end you'll be able to:**

- Sketch the five-stage shape of a data platform and explain why the raw layer is sacred.
- Choose between batch, stream, and micro-batch, and spot the Lambda architecture trap.
- Pick the right file format (JSON, Avro, Parquet) and explain why columnar formats are fast.
- Evolve a schema without breaking consumers, and check data quality in four dimensions.
- Make writes idempotent and run a backfill safely.

## 4.1 The problem we have been ignoring

Twice now I have written "documents are written through the bulk API" and moved on. That sentence hides most of the work.

Lantern's documents live in PostgreSQL, edited at a few hundred edits per second. Each edit becomes one or more chunks, and the *old* chunks must be removed so a shrunken document leaves no orphans. Each chunk is embedded (a model inference), then written to OpenSearch in order, without duplicates, within a couple of seconds.

It must do this forever: through deployments, a PostgreSQL failover, a dead OpenSearch node, a flaky embedding service. And someone will ship a bug that produces malformed chunks for six hours. We must reprocess those hours correctly, without duplicating the twelve million documents that were fine. That last requirement separates data engineering from scripting, and it gets most of this chapter's attention.

## 4.2 The shape of a data platform

Nearly every data platform has the same five stages.

```
  SOURCES          INGESTION        STORAGE          PROCESSING       SERVING
 ┌─────────┐      ┌─────────┐     ┌──────────┐     ┌──────────┐    ┌───────────┐
 │ App DBs │─CDC─►│         │     │          │     │  Spark   │    │OpenSearch │
 │ Events  │─────►│  KAFKA  │────►│ S3 lake  │────►│  Flink   │───►│  OLAP DB  │
 │ APIs    │─poll►│         │     │ Parquet  │     │  SQL     │    │  Cache    │
 │ Files   │─────►└─────────┘     └──────────┘     └──────────┘    │  APIs     │
 └─────────┘                                                       └───────────┘
                         catalog · schema registry · lineage · quality checks
```

Data from systems not built for analytics is **ingested** into a durable buffer that decouples producers from consumers, **stored** for bulk reading, **processed** into refined shapes, and **served** to dashboards, search indexes, models, or APIs.

### The layers of refinement

Storage keeps the same data at three levels of refinement, under various names (bronze/silver/gold, raw/cleaned/curated, staging/core/marts).

| Layer | What it holds |
|---|---|
| **Raw (bronze)** | Exactly what you received: unmodified, append-only, unvalidated. Malformed records stay malformed. |
| **Cleaned (silver)** | Typed, validated, deduplicated, joined to reference data. One row per real-world event or entity. The quality checks of §4.6 live here. |
| **Curated (gold)** | Shaped for consumption: business aggregates, feature tables, and, for Lantern, chunked and embedded documents ready to index. |

The raw layer looks like wasted storage and is the most important thing you have: every transformation bug is recoverable if and only if you still have the input.

> **Never throw away raw data you cannot re-fetch.**

Kafka with long retention can be replayed; a webhook that fired once, an API with a thirty-day window, or an event from a phone that no longer exists cannot. For those, the raw layer is the only copy of the truth, and storage is cheap compared to the alternative.

### ETL and ELT

| | Order | When it made sense |
|---|---|---|
| **ETL** (extract, transform, load) | Transform *before* loading; store only the result | Storage expensive, compute scarce |
| **ELT** (extract, load, transform) | Load raw first, transform inside the destination | Storage cheap, compute elastic: today |

ELT now dominates, and it is *strictly more flexible*: you still have the input, so you can re-derive the output when you change your mind about it. Which you will.

## 4.3 Batch and stream

### Batch: bounded data

A **batch** job processes a finite dataset ("reindex everything", "recompute yesterday's chunks"), then exits. Its great advantage is that **you can see all the data at once**. Sorts, joins, and global aggregates are easy because there is a moment when the input is fully read and the answer is final; if the job fails, you run it again.

Batch is efficient and simple to reason about, which is worth more than most engineers admit. Its limits: latency (an hourly job's output is up to an hour stale, plus runtime) and bursty resource use, idle, then a thousand cores for twenty minutes, then idle.

### Stream: unbounded data

A **stream** processing job handles data that never ends, processing each record or small group as it arrives, forever. You get millisecond-to-second latency, smooth resource use, and things batch cannot do: fraud detection, live dashboards, alerting, and keeping Lantern's index seconds behind the database.

The costs are larger than the benefits suggest:

- **You never have all the data.** Every aggregate is provisional. "Edits in the 10:00–10:05 window" has only a current best guess.
- **State must survive crashes.** A three-month-old running counter must survive a machine failure, which is why checkpoints and state backends fill most of Chapter 6.
- **Events arrive late and out of order.** A phone buffers events for forty minutes, then uploads them all. No answer is both correct and prompt, only a tunable compromise: the watermarks of §6.5.
- **Reprocessing history is a special operation**, not the same command with a different date.
- **It never stops.** A three-month-old job has leaked memory, accumulated skew, and drifted from your repository. Batch gets a fresh process every run.

### Micro-batch: the middle

**Micro-batching** collects data for N seconds, runs a small batch job over it, and repeats forever. It is how Spark Structured Streaming works: streaming semantics from batch machinery and tooling. The cost is a latency floor at the batch interval, never below a few hundred milliseconds and in practice seconds. Flink, by contrast, is true record-at-a-time; Chapter 7 compares them.

### Choosing

| | Batch | Stream |
|---|---|---|
| Is minutes-to-hours-old data fine? | ✓ | |
| Do you need sub-second reaction? | | ✓ |
| Global sorts, arbitrary historical joins? | ✓ | painful |
| Is the logic still changing weekly? | ✓ (just re-run) | costly |
| Small team? | ✓ | streaming has real cost |

> **Start with batch. Move a pipeline to streaming when a specific business requirement demands the latency — not because streaming is more modern.**

The same logic in streaming costs roughly two to three times the engineering effort, plus much more operational attention. For Lantern it is justified: users notice whether an edit is searchable within seconds. But Lantern *also* needs batch for the nightly reindex, backfills, and re-embedding when the model changes. Nearly every platform runs both, and the two must agree.

### Two architectures, one of which is a trap

**Lambda architecture** runs both paths on purpose: streaming for fresh-but-approximate results, batch for complete-but-late ones, and a serving layer that merges them. It works, but **you maintain two implementations of the same business logic** in two systems with different semantics. They drift: someone fixes an edge case in batch, forgets streaming, and the dashboard disagrees with the weekly report.

**Kappa architecture** has one implementation, the streaming one. To reprocess history, you replay the log from the start into a second instance of the job writing to a new output, then swap. It needs a durable, long-retention log (Chapter 5) and is the modern preference. Note the pattern (build alongside, verify, atomically swap): it is OpenSearch's alias swap from §2.10, and it will recur.

## 4.4 How data sits on disk

Format choices look like plumbing and routinely make order-of-magnitude differences in cost and speed.

### Rows and columns

```
Row-oriented:      [id=1,name=A,age=30][id=2,name=B,age=25][id=3,name=C,age=41]
Column-oriented:   [1,2,3][A,B,C][30,25,41]
```

In a **row format**, each record is contiguous: ideal for writes and "fetch this one entity". In a **column format**, each field's values are contiguous, so an aggregate over one field skips the other forty.

The second benefit is often bigger: **columns compress far better than rows.** Timestamps delta-encode to a few bits each, country codes dictionary-encode to tiny integers, and a sorted status column collapses to a few run-lengths. A row mixes types, leaving a compressor nothing to exploit.

> **Row format on the write path, columnar on the read path.**

### JSON

JSON is readable, universal, and schema-free, which makes it right for APIs, configuration, and debugging. It is **wrong for anything large**:

- It repeats every field name in every record. A million records store `document_created_timestamp` a million times.
- It has no schema, so `"1"` and `1` differ and a field can change type mid-file.
- It has no type system, so timestamps are strings every reader parses differently.
- It has no built-in compression, and parsing is often a naive pipeline's dominant CPU cost.

Converting JSON to Parquet routinely gives five to ten times less storage and over ten times faster queries, often the highest-return change available.

One variant matters: **NDJSON** (JSON Lines), one object per line with no enclosing array. It is **splittable**: an engine can cut the file at line boundaries and have twenty machines read twenty chunks. A JSON array is not, so one file goes to exactly one core however big your cluster. NDJSON is also what OpenSearch's `_bulk` API speaks (§2.10).

### Avro

**Avro** is a row-oriented binary format built around **schema**, carried in the file header or, in streaming, referenced by ID in a registry. Field names aren't repeated, so it is typically several times smaller than JSON.

Its strength is **schema evolution**, where it is best in class: explicit rules resolve the *writer's* schema against the *reader's*, with defaults, aliases for renamed fields, and union types. Within those rules, newer readers read old data and older readers read new data (§4.5).

Avro is the standard for Kafka messages and the raw event layer. Its weakness is analytics: being row-oriented, a query on one field still reads them all.

### Parquet

**Parquet** is column-oriented, binary, and the de facto standard for data lakes. Its performance comes from its structure:

```
Parquet file
├── Row group 1   (e.g. 128 MB of rows)
│   ├── Column chunk: document_id    ├── pages, encoded + compressed
│   ├── Column chunk: title          │   each with min/max/null statistics
│   └── Column chunk: updated_at     ┘
├── Row group 2 ...
└── Footer: schema + statistics for every column chunk
```

Three mechanisms follow:

- **Column pruning.** `SELECT avg(word_count)` reads only the `word_count` chunks. With forty columns, you read 1/40th of the bytes.
- **Predicate pushdown.** The footer records each column chunk's min and max. A query filtered to `updated_at >= '2026-09-01'` can **skip a whole row group without decompressing a byte** if its maximum is in August. On well-organised data this removes most I/O.
- **Encoding and compression.** Dictionary, run-length, delta, and bit-packing per column, then a general codec on top. Five to twenty times smaller than JSON is routine.

The cost: Parquet is write-once. You append new files and let something else (table formats, below) decide which version wins.

| Codec | Trade-off |
|---|---|
| `snappy` | Fast, moderate compression; the long-time default |
| `zstd` | Noticeably better compression at similar speed; increasingly the right default |
| `gzip` | Compresses well, slow |
| `lz4` | Fastest, largest |

### At a glance

| | JSON | CSV | Avro | Parquet |
|---|---|---|---|---|
| Layout | row | row | row | **column** |
| Binary | no | no | yes | yes |
| Schema | none | none | **embedded** | embedded |
| Evolution | n/a | painful | **excellent** | good |
| Column pruning | ✗ | ✗ | ✗ | ✓ |
| Predicate pushdown | ✗ | ✗ | ✗ | ✓ |
| Splittable | NDJSON only | ~ | ✓ | ✓ |
| Best for | APIs, debugging | interchange | **Kafka, raw events** | **analytics** |

### The small files problem

Object stores and query engines cope badly with many small files. Each costs its own open request and its own processing task, and listing a million objects on S3 is agonisingly slow. This will matter in Chapter 7.

A streaming job writing every ten seconds produces **8,640 files per day, per partition**. After a month you have a quarter of a million files where you wanted thirty, and queries that took seconds take minutes. The fixes: use a longer trigger interval, reduce output parallelism before writing, and run periodic **compaction** to rewrite many small files as few large ones.

> **Target 128 MB to 1 GB per file.**

### Table formats, or: what Parquet is missing

A directory of Parquet files is not a table: no transactions (readers can see half-finished writes), no row updates, no cross-file schema enforcement, no view of last Tuesday.

**Table formats** (Apache Iceberg, Delta Lake, Apache Hudi) add a metadata layer over the Parquet files that provides exactly those:

- **ACID transactions** on object storage. A write stages new files, then atomically swaps a metadata pointer; readers see the old version or the new, never a mix. The alias swap and Kappa's output switch again.
- **Time travel.** Query the table as of a snapshot ID or timestamp, for debugging ("what did this look like before the bad job?") and reproducibility.
- **Row-level updates and deletes** via `MERGE INTO`, either by rewriting affected files (**copy-on-write**) or by writing separate delete markers (**merge-on-read**).
- **Schema and partition evolution** without rewriting existing data.

This combination is the **lakehouse**, the default for a new data lake. Use one; otherwise you solve these problems by hand, badly.

## 4.5 Schemas that change

A producer team adds a field and deploys on Tuesday; three consumer teams deploy on their own schedules, one next month. What happens depends on decisions made before Tuesday, in terms that are easy to get backwards.

| Mode | Meaning | How you achieve it | What it lets you do |
|---|---|---|---|
| **Backward compatible** | **New reader can read old data** | Add fields with defaults; remove optional fields | **Upgrade consumers first**. The most common need, since consumers outnumber producers |
| **Forward compatible** | **Old reader can read new data** | Add optional fields; remove fields that have defaults | **Upgrade the producer first**, consumers catch up later. Needed whenever you can't coordinate every consumer |
| **Full** | Both | Both sets of rules | Upgrade in either order |
| **Breaking** | Anything else | Renaming a field, changing a type, adding a required field with no default | Needs a new topic or table version and a coordinated migration. Keep these rare |

### The schema registry

You could enforce compatibility through code review and good intentions. Everyone who has tried reports the same outcome.

A **schema registry** (Confluent's, AWS Glue's, Apicurio) stores schemas centrally, gives each version an ID, and **rejects an incompatible schema at registration time** according to a configured compatibility mode. Messages carry the small ID instead of the whole schema, keeping the wire format compact.

What changes is *when* you find out: not at 3 a.m. from a consumer crash-looping in production, but in a CI build, by the engineer who made the change, with a message naming the field.

A **data contract** generalises this: a versioned agreement between producer and consumers covering schema, field meanings, freshness SLA, and ownership. The registry enforces the mechanical part. The effect is to turn quality from downstream *detection* into upstream *prevention*, worth more than any monitoring you can build.

## 4.6 Is the data any good?

Bad data is worse than no data, because people act on it. A missing dashboard prompts an investigation; one quietly wrong by fifteen percent prompts a decision. Quality has four dimensions, the ones that actually break.

### Freshness: is the data recent enough?

Measure the maximum event timestamp against now, the time since the last successful run, or consumer lag for a stream. Then apply the insight people miss: **alert on staleness, not only on failure.**

The most dangerous pipeline succeeds while producing nothing. Every job exits zero, and the table hasn't had a row since Thursday because an upstream API started returning an empty array. **A job that succeeds with zero rows must page somebody.**

Write down a freshness SLA per dataset ("the chunks table is at most five minutes behind the source") and monitor it. For Lantern, the number comes from §2.6: an edit is searchable within a few seconds, and we should know when it isn't.

### Completeness: is all of it there?

- Row counts against expectations, or against a trailing average with a tolerance band.
- Null rates per column. A column suddenly ninety percent null almost always means an upstream rename.
- Referential integrity: every chunk's `parent_document_id` exists in the documents table.
- Reconciliation: does `count(source)` equal `count(destination)` for yesterday's partition?
- Gap detection in sequences, offsets, or dates.

### Correctness: is it right?

- Type and range checks: a word count isn't negative; a timestamp isn't in 1970 or 2087.
- Format checks: emails look like emails; currency codes are in the ISO list.
- Business invariants: a document's chunk count matches the chunks that exist; a total equals the sum of its parts.
- Cross-system reconciliation: if finance says yesterday's revenue was X, your pipeline had better agree.

### Consistency: does it agree with itself?

The same metric computed two ways gives the same answer, which is §4.3's Lambda failure mode as a testable assertion. Units and timezones are uniform (store UTC, always, and store currency alongside amounts). Slowly-changing dimension records have no overlapping validity ranges.

And primary keys are unique. **Duplicate keys are the single most common silent data bug there is.** A join fans out, a count doubles, both numbers look plausible, and nobody notices for a quarter. Test key uniqueness on every table, routinely.

### Making it real

Checks that live in a document are not checks:

- **Write them as code, next to the pipeline**, using Great Expectations, dbt tests, Soda, or plain SQL assertions in your orchestrator. Version them, review them, and run them as a *pipeline step*, not a dashboard nobody looks at.
- **Circuit-break on failure.** A failed critical check should **stop publication**, not merely warn. Consumers can't tell known-bad data from good data and will use it. Better a stale table with a loud alarm than a fresh, wrong one.
- **Tier your severities.** `error` blocks, `warn` publishes and alerts, `info` records a trend. If everything stops the world, people ignore the alerts.
- **Add anomaly detection** for what fixed rules can't express: volume shifts, distribution changes, cardinality jumps. Rules catch the failures you predicted; anomaly detection catches the ones you didn't.
- **Maintain lineage**: what depends on this column. You need it before a change, during an incident, and for §4.7's backfills, which usually mean re-running everything downstream.
- **Emit per-run metrics**: rows in, out, and rejected, duration, lag.

## 4.7 Running it twice

### Why duplicates are certain

As §1.7 established: you send a request, get no response, and can't tell whether it succeeded. So you retry, and if it *had* succeeded you've done it twice. No configuration avoids this; it is a property of the situation.

Producers resend after timeouts, consumers crash before recording their position, and batch jobs are re-run after partial failures. Each delivers some record more than once.

> **The goal is not to prevent duplicates. It is to make them harmless.**

### Four ways to make a write idempotent

**1. Upsert on a natural key.** Write each record *at* a key derived from the data, so a second write overwrites the first with identical content. Lantern indexes each chunk with `_id = "{document_id}:{chunk_index}"`, so replaying an edit produces exactly the same chunk documents. Look for this option first.

If the source has no natural key, derive one deterministically: `hash(source_system, entity_id, event_timestamp)`. *Deterministically* is the key word. A UUID generated at write time is not a key; it is a new record every time.

**2. Overwrite the whole partition.** The most useful idempotency pattern in batch. Instead of appending today's output, delete and rewrite today's entire partition:

```sql
-- not this
INSERT INTO chunks SELECT ... WHERE dt = '2026-09-17';

-- this
INSERT OVERWRITE TABLE chunks PARTITION (dt = '2026-09-17') SELECT ...;
```

Re-running produces byte-identical output, with no dedup logic anywhere. In Spark, set `spark.sql.sources.partitionOverwriteMode = dynamic`.

**3. Commit output and position atomically.** Write the output and record how far you got in one transaction, so you can never do one without the other. Examples: Kafka transactions (§5.4), Flink's two-phase commit sinks (§6.4), and Iceberg and Delta's atomic commits. Powerful, but the sink must cooperate.

**4. Keep a dedup window.** Remember IDs you've seen and drop repeats. The set needs a TTL or it grows forever, so late duplicates slip through: a probabilistic guarantee, not a structural one. Use it when the other three aren't available.

### Deduplicating data you already have

| Kind | What it is | How to remove it |
|---|---|---|
| **Exact duplicates** | Byte-identical records | Content hash, or `SELECT DISTINCT` |
| **Logical duplicates** | Several versions of one entity; you want the latest | Ranked window (below) |
| **Streaming duplicates** | Repeats in a live stream | Keyed state of seen IDs with a TTL: Flink's `ValueState` with a timer, or Spark's `dropDuplicatesWithinWatermark`. The TTL trades memory against window size |
| **Entity duplicates** | "Robert Smith" and "Bob Smith" are one person | A different, much harder problem: blocking, similarity scoring, often Chapter 3's embeddings. Shares a word with pipeline dedup and nothing else |

For logical duplicates:

```sql
SELECT * FROM (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY document_id
           ORDER BY event_time DESC, kafka_offset DESC
         ) AS rn
  FROM raw_events
) WHERE rn = 1
```

The secondary sort on `kafka_offset` is not decoration. Timestamps tie, and on a tie `ORDER BY event_time DESC` alone leaves the winner **undefined**; the database may pick differently next run, and your idempotent pipeline flaps. **Always break ties on something totally ordered**: an offset, a sequence number, an ingestion timestamp.

### Reprocessing safely

Back to §4.1's six hours of malformed chunks. Seven questions, in order, each a place I've watched a backfill go wrong.

**1. Can you get the input back?** Is the raw data still in S3, or within Kafka's retention? If not, you have a permanent data loss problem, and the lesson is §4.2's rule about raw data.

**2. Is the computation deterministic?** The obvious hazards are `now()`, `random()`, and auto-increment IDs. The subtle one is **joining against a dimension table that has since changed.** If the original run joined each document to a `teams` table, and three teams have since been renamed, today's re-run produces different output for reasons unrelated to your fix. If historical fidelity matters, you need point-in-time correctness: slowly-changing-dimension tables with validity ranges, joined "as of the event's timestamp". Chapter 6 provides this as the temporal join (§6.6).

**3. Is the write idempotent?** Overwrite the partition or upsert by key. Never blind-append into a backfill; that turns one bad day into two.

**4. Limit the blast radius.** Write to a shadow table, diff it against production, inspect, *then* swap. By now you should see that "build alongside, verify, atomically swap" (§2.10, §4.3, §4.4) is the general shape of every safe change in a data platform.

**5. Throttle it.** A full-parallelism backfill saturates the cluster, starves the real-time pipeline, and pages the on-call. Rate-limit it and run it off-peak.

**6. Tell the downstream.** Consumers may need to re-run too; §4.6's lineage tells you who.

**7. Handle the bookmark explicitly.** Your job stores a position: a Kafka offset, a watermark, a last-processed timestamp. Reset it deliberately and completely. A half-reset leaves a gap that is very hard to detect later, precisely because nothing failed.

### Five rules

> 1. **Keep raw data.** Everything else can be re-derived. Nothing can re-derive the input.
> 2. **Make every write idempotent**, keyed on a deterministic ID.
> 3. **Prefer overwriting a partition to appending to one.**
> 4. **Assume at-least-once delivery everywhere**, and get exactly-once *effects* from idempotency.
> 5. **Make backfill routine**: a parameterised operation you run casually, not a heroic effort with a runbook and a war room.

Follow these and your platform is boring, which is the goal: when a bug ships, the response is "reprocess Tuesday", not an incident review.

## 4.8 The rest of the operational picture

Briefly, since these are more organisation than principle:

- **Orchestration** (Airflow, Dagster, Prefect, Argo) expresses pipelines as dependency graphs with schedules, retries, and backfill support. Three ideas matter: tasks must be **idempotent** so retries are safe; scheduling should be **data-aware**, triggering when the input partition exists rather than when the clock strikes; and **sensors** let a job wait for external readiness instead of guessing a delay.
- **Cost** is a design constraint. The levers: file format and compression, partition pruning, avoiding full scans, right-sizing clusters, spot instances, and lifecycle policies for cold data. A terabyte scan from a forgotten partition filter is a line item.
- **Governance and personal data.** Know where personal data lives, restrict access, and be able to delete it on request. That is hard on an immutable lake. The options are tombstone records, per-user encryption keys you destroy to make data unreadable (**crypto-shredding**), and table formats' row-level deletes. Decide which before someone asks.
- **What actually goes wrong.** The top three incidents are consistently (1) an unannounced upstream schema change, (2) a late or missing partition, and (3) a skewed job blowing its SLA. Instrument for those and you'll catch most of what happens.

---

## Key takeaways

- Keep raw data you cannot re-fetch; every bug is recoverable only if the input survives.
- ELT beats ETL because you can always re-derive the output from the stored input.
- Start with batch; move to streaming only when a real requirement demands the latency.
- Lambda means two implementations that will drift; Kappa replays one log into a new output and swaps.
- Row formats for writing, columnar for reading; Parquet wins through column pruning, predicate pushdown, and compression.
- Target 128 MB to 1 GB per file, and use a table format (Iceberg, Delta, Hudi) for transactions and time travel.
- A schema registry turns a 3 a.m. outage into a failed CI build.
- Alert on staleness, not only failure; a job that succeeds with zero rows must page someone.
- Duplicates are certain: upsert on a deterministic key or overwrite the partition to make them harmless.
- "Build alongside, verify, atomically swap" is the shape of every safe change.

## Where we are

We have the principles but no machinery: Lantern's documents still reach OpenSearch by magic. The first thing it needs is a durable, ordered, replayable place for changes to go, so the indexer can be slow, restarted, or rewound three hours without the database caring. The next chapter builds that from the most unglamorous data structure in computing.
