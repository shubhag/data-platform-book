# Chapter 4 — Moving Data

## 4.1 The problem we have been ignoring

Two chapters ago we built a search index. One chapter ago we added vectors to it. Both times, I wrote sentences like "documents are written through the bulk API" and moved on, as though getting the data there were an implementation detail.

It is not a detail. It is most of the work.

Look at what Lantern actually needs. Documents live in PostgreSQL, where other applications create and modify them at a few hundred edits per second. Each edit must become a chunk — or several chunks, if the document is long, and the *old* chunks must be removed, because a document that shrank should not leave orphaned chunks behind. Each chunk must be embedded, which costs a model inference. Then the whole set must be written to OpenSearch, in order, without duplicates, within a couple of seconds.

And it must do this forever. Through deployments. Through a PostgreSQL failover. Through an OpenSearch node dying. Through the embedding service being briefly unavailable. Through somebody shipping a bug that produces malformed chunks for six hours before anyone notices — after which we must be able to go back and reprocess those six hours, correctly, without duplicating the twelve million documents that were fine.

That last requirement is the one that separates data engineering from scripting, and it is where this chapter spends most of its attention.

This chapter is about the principles: how data moves, what shape it takes on disk, how you know whether it is any good, and — the central practical skill in the field — how you make a pipeline safe to run twice. The next three chapters are about the specific machines that do the moving.

## 4.2 The shape of a data platform

Before the principles, the map. Nearly every data platform, across wildly different companies and technology choices, has the same five-stage shape.

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

Data is produced by systems that were not built for analytics. It is *ingested* into a durable buffer that decouples producers from consumers. It is *stored* in a form suited to bulk reading. It is *processed* into progressively more refined shapes. And it is *served* to whoever consumes it — a dashboard, a search index, a machine-learning model, an API.

### The layers of refinement

Within storage, the convention is to keep the same data at three levels of refinement. The names vary — bronze/silver/gold, raw/cleaned/curated, staging/core/marts — but the idea is constant and worth internalizing.

**Raw (bronze).** Exactly what you received, unmodified, append-only. Not validated, not deduplicated, not prettied up. If the source sent you a malformed record, the malformed record is what you keep.

This layer looks like a waste of storage and it is the most important thing in your platform. It is your insurance policy. Every bug you write in the transformation logic — and you will write them — is recoverable if and only if you still have the input. The rule that follows is absolute:

> **Never throw away raw data you cannot re-fetch.**

Some sources are replayable: Kafka with long retention, a database you can re-query. Many are not: a webhook that fired once, an API with a thirty-day window, an event from a mobile device that no longer exists. For anything in the second category, the raw layer is the only copy of the truth that will ever exist, and storage is cheap compared to the alternative.

**Cleaned (silver).** Typed, validated, deduplicated, joined to reference data. One row per real-world event or entity. This is where data becomes trustworthy, and where the quality checks of §4.6 live.

**Curated (gold).** Shaped for consumption. Business aggregates, feature tables, and — for Lantern — the chunked, embedded documents ready for indexing.

### ETL and ELT

You will hear both, and the difference is a single letter that reflects a decade of changing economics.

**ETL** — extract, transform, load — transforms data *before* loading it into the destination. This was the right architecture when storage was expensive and compute was scarce: you transformed the data down to the shape you needed and stored only that.

**ELT** — extract, load, transform — loads the raw data first and transforms it afterwards, inside the destination. This is now dominant, because storage became extremely cheap and compute became elastic. And it is *strictly more flexible*, for the reason above: you still have the input, so you can re-derive the output when you change your mind about what the output should be. Which you will.

## 4.3 Batch and stream

There are two ways to process data, and the distinction organizes the rest of this book.

### Batch: bounded data

A **batch** job processes a finite dataset. "Reindex everything." "Recompute yesterday's chunks." The job starts, reads a bounded input, produces output, and exits.

What makes batch pleasant is that **you can see all the data at once.** This sounds trivial and is profound. Sorting is easy. Joining is easy. Computing a global aggregate — the average, the total, the top ten — is easy, because there is a well-defined moment at which the input has been fully read and the answer is final. The job either completed or it did not, and if it did not you run it again.

Batch is also *efficient*: enormous sequential reads, excellent compression, high hardware utilisation. And it is *simple to reason about*, which is worth more than most engineers admit.

Its defining limitation is latency. If the job runs hourly, the output is up to an hour stale before you add the runtime. And its resource profile is bursty — idle, then a thousand cores for twenty minutes, then idle.

### Stream: unbounded data

A **stream** processing job handles data that never ends. It starts, and then it runs forever, processing each record or small group of records as it arrives.

The latency is the point: milliseconds to seconds rather than hours. The resource usage is smooth rather than spiky. And it enables a whole class of things batch cannot do — fraud detection, live dashboards, alerting, and keeping Lantern's search index within seconds of the database.

Now the costs, and they are larger than the benefits suggest.

**You never have all the data.** There is no moment at which the input is complete, so *every aggregate is provisional*. "How many documents were edited in the 10:00–10:05 window" has no final answer, only a current best guess that may be revised.

**State must survive crashes.** A running counter in a batch job lives in memory and dies with the job, which is fine because the job is short. A running counter in a streaming job has been accumulating for three months, and it must survive a machine failure. That requirement produces checkpoints, state backends, and recovery protocols — most of Chapter 6.

**Events arrive out of order and late.** A mobile device buffers events for forty minutes with no signal, then uploads them all. Which window do they belong to? When do you stop waiting? There is no answer that is both correct and prompt, only a tunable compromise — the watermarks of §6.4.

**Reprocessing history is a special operation.** In batch, re-running last Tuesday is the same command with a different date. In streaming, it is a distinct procedure requiring thought.

**And it never stops, which is an operational fact with consequences.** A job that has run for three months has leaked memory, accumulated skew, and drifted from the code in your repository. Batch jobs get a fresh process every run; streaming jobs get one process and all the entropy that accumulates in it.

### Micro-batch: the middle

There is a third option that looks like a compromise and is more interesting than that: collect arriving data for N seconds, run a small batch job over it, repeat forever.

This is **micro-batching**, it is how Spark Structured Streaming works, and it is a genuinely clever trick — you get streaming semantics out of batch machinery. All the tooling, optimisation, and operational familiarity of batch, applied every thirty seconds. The cost is that latency is floored at the batch interval; you will not get below a few hundred milliseconds, and in practice you will run at seconds.

Flink, by contrast, is true record-at-a-time. Chapter 7 compares them properly.

### Choosing

| | Batch | Stream |
|---|---|---|
| Is minutes-to-hours-old data fine? | ✓ | |
| Do you need sub-second reaction? | | ✓ |
| Global sorts, arbitrary historical joins? | ✓ | painful |
| Is the logic still changing weekly? | ✓ (just re-run) | costly |
| Small team? | ✓ | streaming has real cost |

And a recommendation I will defend:

> **Start with batch. Move a pipeline to streaming when a specific business requirement demands the latency — not because streaming is more modern.**

The same logic in streaming costs roughly two to three times the engineering effort and considerably more operational attention. That is a good investment when the latency matters and pure waste when it doesn't. A great deal of unnecessary complexity in our industry comes from streaming systems built for requirements that were never stated.

For Lantern, it happens to be justified: "an edit is searchable within seconds" is a real product requirement that users notice. But notice that Lantern *also* needs batch — for the nightly full reindex, for backfills, for re-embedding the corpus when the model changes. Nearly every real platform runs both, and the two paths must produce consistent answers, which brings us to a question of architecture.

### Two architectures, one of which is a trap

**Lambda architecture** runs both paths deliberately: a streaming path for fresh-but-approximate results, a batch path for complete-but-late results, and a serving layer that merges them. It works, it is well-proven, and it has one flaw that tends to dominate: **you now maintain two implementations of the same business logic**, in two different systems, with two different sets of semantics.

They will drift. Someone fixes an edge case in the batch job and forgets the streaming one. The definitions of "active user" diverge by one boundary condition. And then the dashboard disagrees with the weekly report, and somebody spends two weeks finding out why, and this happens at nearly every company that runs Lambda. If you find yourself explaining why two numbers that should be identical are not, you are living in the failure mode.

**Kappa architecture** has one implementation: the streaming one. To reprocess history, you replay the log from the beginning into a second instance of the job writing to a new output, then swap. This requires a durable, long-retention log — which is precisely what Chapter 5 provides — and it is conceptually much cleaner. It is the modern preference, and note the pattern it relies on: build the new version alongside, verify, then atomically swap. The same pattern as OpenSearch's alias swap in §2.10. You will see it a third time before the book is over.

## 4.4 How data sits on disk

Format choices look like plumbing and routinely produce order-of-magnitude differences in cost and speed. There is one central idea, and then a small number of formats.

### Rows and columns

```
Row-oriented:      [id=1,name=A,age=30][id=2,name=B,age=25][id=3,name=C,age=41]
Column-oriented:   [1,2,3][A,B,C][30,25,41]
```

Same data, transposed. And the transposition changes everything about which operations are cheap.

In a **row format**, one complete record is contiguous. Reading or writing a whole record is a single sequential operation. Ideal for writes, and for "fetch me this one entity".

In a **column format**, all the values of one field are contiguous. Reading one column across a million records is a single sequential operation — and reading *only* that column is possible, which means an aggregate over one field does not touch the other forty.

There is a second, less obvious benefit and it is often the larger one: **columns compress dramatically better than rows.** A column of timestamps contains a million similar values, so delta encoding reduces each to a few bits. A column of country codes has two hundred distinct values, so dictionary encoding replaces each with a tiny integer. A sorted column of statuses becomes a handful of run-lengths. Compare that with a row format, where each record is a jumble of a string, a timestamp, and three integers, with no local similarity for a compressor to exploit.

The resulting rule of thumb is simple: **row format on the write path, columnar on the read path.**

### JSON

Human-readable, universal, schema-free, and the right answer for APIs, configuration, and debugging.

It is also **the wrong answer for anything large**, and it is worth being concrete about why. JSON repeats every field name in every record — a million records with a field called `document_created_timestamp` store that string a million times. It has no schema, so `"1"` and `1` are different and nothing stops a field changing type halfway through a file. It has no type system, so timestamps are strings that every reader parses differently. It has no built-in compression. And parsing it is genuinely slow — JSON parsing is frequently the dominant CPU cost in a naive pipeline.

Converting JSON to Parquet routinely yields five to ten times less storage and more than ten times faster queries. If you are storing large volumes of JSON in a data lake, that conversion is the highest-return change available to you.

One variant matters: **NDJSON**, or JSON Lines — one JSON object per line, no enclosing array. This is *splittable*, meaning a processing engine can divide the file at line boundaries and have twenty machines read twenty chunks. A conventional JSON array is not splittable, because you cannot know where you are without parsing from the beginning, which means one file is processed by exactly one core no matter how large your cluster. NDJSON is also what OpenSearch's `_bulk` API speaks (§2.10).

### Avro

A row-oriented binary format whose defining feature is its treatment of **schema**.

An Avro file carries its schema in its header — or, in a streaming context, references a schema by ID in a registry. Data is written as compact binary with no field names repeated, so it is typically several times smaller than the equivalent JSON.

Its real strength is **schema evolution**, and it is best in class here. Avro defines explicit resolution rules between the schema the data was *written* with and the schema the reader *expects*, with support for defaults, for aliases on renamed fields, and for union types. A reader with a newer schema can read older data; a reader with an older schema can read newer data, within rules. This is not a minor convenience — §4.5 explains why it is the difference between an evolving platform and a series of coordinated outages.

Avro is the standard choice for Kafka messages and for the raw event layer. Where it is weak is analytical reading: being row-oriented, a query touching one field still reads all of them.

### Parquet

Column-oriented, binary, and the de facto standard for data lakes. Worth understanding structurally, because the structure is where the performance comes from.

```
Parquet file
├── Row group 1   (e.g. 128 MB of rows)
│   ├── Column chunk: document_id    ├── pages, encoded + compressed
│   ├── Column chunk: title          │   each with min/max/null statistics
│   └── Column chunk: updated_at     ┘
├── Row group 2
│   └── ...
└── Footer: schema + statistics for every column chunk
```

Three mechanisms follow from that layout.

**Column pruning.** `SELECT avg(word_count)` reads the `word_count` column chunks and nothing else. If the table has forty columns, you have read 1/40th of the bytes.

**Predicate pushdown.** Every column chunk's footer records the minimum and maximum value it contains. So a query filtered to `updated_at >= '2026-09-01'` can check a row group's statistics and, if its maximum `updated_at` is in August, **skip the entire row group without decompressing a single byte.** On well-organised data this eliminates the great majority of I/O.

**Encoding and compression.** Dictionary, run-length, delta, and bit-packing encodings applied per column, then a general-purpose codec on top. Five to twenty times smaller than JSON is routine.

The cost is that Parquet is write-once. You cannot efficiently update a row in place; you append new files and let something else sort out which version wins. That "something else" is the subject of the next-but-one section.

On codecs: **`snappy`** is fast with moderate compression and has been the default for years. **`zstd`** achieves noticeably better compression at similar speed and is increasingly the right default. `gzip` compresses well and is slow. `lz4` is fastest and largest.

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

A warning that is going to become relevant in Chapter 7, so let me plant it now.

Object stores and query engines cope badly with large numbers of small files. Each file costs a separate open request with its own network latency. Each file becomes its own processing task, so a thousand tiny files means a thousand tasks with a thousand scheduling overheads for very little work each. And listing a directory containing a million objects on S3 is agonisingly slow.

A streaming job writing output every ten seconds produces **8 640 files per day, per partition.** After a month you have a quarter of a million files where you wanted thirty, and queries that used to take seconds take minutes.

The mitigations: write bigger files by using a longer trigger interval, reduce output parallelism before writing, and run periodic **compaction** to rewrite many small files as few large ones. Target **128 MB to 1 GB per file**. If you remember one number from this section, that is the one.

### Table formats, or: what Parquet is missing

A directory of Parquet files is not a table. It has no transactions, so a reader can see a half-finished write. It has no way to update a row. It has no schema enforcement across files, so nothing stops two files disagreeing. And there is no way to ask what the data looked like last Tuesday.

**Table formats** — Apache Iceberg, Delta Lake, Apache Hudi — add a metadata layer over the Parquet files that provides exactly those things:

**ACID transactions** on object storage. A write stages new files and then atomically swaps a metadata pointer; readers see the old version or the new one, never a mixture. This is the same atomic-swap pattern as an OpenSearch alias, and as Kappa's output switch. Three times now.

**Time travel.** Query the table as of a snapshot ID or a timestamp. Invaluable for debugging ("what did this look like before the bad job ran?") and for reproducibility.

**Row-level updates and deletes**, via `MERGE INTO`, implemented either by rewriting affected files (copy-on-write) or by writing separate delete markers (merge-on-read).

**Schema and partition evolution** without rewriting existing data.

This combination is what people mean by the **lakehouse**, and it is the default choice for a new data lake. Given a choice, use one; the transactional guarantees eliminate a category of problem you would otherwise solve by hand, badly.

## 4.5 Schemas that change

Here is a situation that will occur, repeatedly, forever.

A producer team adds a field to the documents they publish. They deploy on Tuesday. Three consumer teams are reading those documents, and they deploy on their own schedules, one of them next month. What happens?

The answer depends entirely on decisions made before Tuesday, and the vocabulary for those decisions is worth learning precisely, because the terms are easy to confuse and getting them backwards is expensive.

**Backward compatible** means a **new reader can read old data**. You achieve it by adding fields with defaults, or by removing optional fields. What it buys you is the ability to **upgrade consumers first**: deploy the new reader, it handles both old and new data, then upgrade the producer whenever. This is the most commonly required mode, because consumers usually outnumber producers.

**Forward compatible** means an **old reader can read new data**. You achieve it by adding optional fields, or by removing fields that have defaults. What it buys you is the ability to **upgrade the producer first**, with consumers catching up at leisure. This is what you need when a producer must move and you cannot coordinate every consumer — which, in a large organisation, is most of the time.

**Full compatibility** is both. **Breaking** is anything else: renaming a field, changing a type, adding a required field with no default. These require a new topic or table version and a coordinated migration, and the point of the discipline is to make them rare.

### The schema registry

You could enforce compatibility through code review and good intentions. Everyone who has tried reports the same outcome.

A **schema registry** — Confluent's, AWS Glue's, Apicurio — stores schemas centrally, assigns each version an ID, and, crucially, **rejects an incompatible schema at registration time** according to a configured compatibility mode. Messages then carry the small schema ID rather than the whole schema, so the wire format stays compact.

What this changes is *when* you find out. Without a registry, an incompatible change is discovered at three in the morning by a consumer crash-looping in production, and diagnosed by whoever is on call. With a registry, it is discovered in a CI build, by the engineer who made the change, with a message naming the field. Same bug, vastly different cost.

This generalises into something worth stating on its own. A **data contract** is an explicit, versioned agreement between a producer and its consumers, covering the schema, the semantics of each field, the freshness SLA, and who owns it. The schema registry enforces the mechanical part. The rest is organisational, and the effect of having it at all is to move data quality from a downstream *detection* problem to an upstream *prevention* problem. That shift is worth more than any monitoring you can build.

## 4.6 Is the data any good?

Bad data is worse than no data, because people act on it. A missing dashboard prompts an investigation. A dashboard that is quietly wrong by fifteen percent prompts a decision.

There are four dimensions to data quality. They are conventional, and they are conventional because they are the ones that actually break.

### Freshness

*Is the data recent enough?*

Measure: the maximum event timestamp in the dataset compared to now; the time since the last successful run; consumer lag for a streaming pipeline.

And then the essential insight, which is the one people miss: **alert on staleness, not only on failure.** The most dangerous pipeline in the world is one that succeeds while producing nothing. Every dashboard is green. Every job exits zero. And the table has not received a row since Thursday, because an upstream API silently started returning an empty array. A job that succeeds with zero rows must page somebody.

For each dataset, define a freshness SLA in writing — "the chunks table is at most five minutes behind the source" — and monitor against it. For Lantern, that number is the one from §2.6: an edit is searchable within a few seconds, and we should know when it isn't.

### Completeness

*Is all of the data there?*

Row counts compared against expectations, or against a trailing average with a tolerance band. Null rates per column — a column that is suddenly ninety percent null almost always means an upstream rename. Referential integrity: every chunk's `parent_document_id` exists in the documents table. Reconciliation against the source: does `count(source)` equal `count(destination)` for yesterday's partition? And gap detection in sequences, offsets, or dates.

### Correctness

*Is the data right?*

Type and range checks: a word count is not negative, a timestamp is not in 1970 or 2087. Format checks: emails look like emails, currency codes are in the ISO list. Business invariants: a document's chunk count matches the number of chunks that actually exist; a total equals the sum of its parts. And cross-system reconciliation against a trusted figure — if finance says yesterday's revenue was X, your pipeline had better agree.

### Consistency

*Does the data agree with itself and with other systems?*

The same metric computed two ways gives the same answer — which is the Lambda failure mode of §4.3, now expressed as a testable assertion. Primary keys are unique, and I want to single this one out: **duplicate keys are the single most common silent data bug there is.** A join fans out, a count doubles, and because both numbers look plausible nobody notices for a quarter. Test key uniqueness on every table, always, as a matter of routine.

Also: units and timezones are uniform (store UTC, always, and store currency alongside amounts); and slowly-changing dimension records do not have overlapping validity ranges.

### Making it real

Checks that live in a document are not checks. A few principles for making them operational.

**Write them as code, next to the pipeline.** Great Expectations, dbt tests, Soda, or plain SQL assertions in your orchestrator. Version-controlled, reviewed, and run as a *step of the pipeline* — not as a separate dashboard that nobody looks at.

**Circuit-break on failure.** A failed critical check should **stop publication**, not merely warn. This is worth being firm about: publishing known-bad data and sending an alert is worse than publishing nothing, because downstream consumers cannot tell the difference and will use it. Better a stale table with a loud alarm than a fresh, wrong one.

**Tier your severities.** `error` blocks the pipeline. `warn` publishes and alerts. `info` records a trend. Not everything deserves to stop the world, and if everything does, people will start ignoring the alerts.

**Add anomaly detection** for the things you cannot express as a fixed rule — volume shifts, distribution changes, cardinality jumps. Rules catch the failures you predicted; anomaly detection catches the ones you didn't.

**Maintain lineage.** Know which downstream tables, dashboards, and models depend on this column. You need it for impact analysis before a change, and you need it during an incident to know who to tell. It is also what makes the backfills in §4.7 tractable, because a backfill usually requires re-running everything downstream.

**Emit per-run metrics** as first-class telemetry: rows in, rows out, rows rejected, duration, lag. A pipeline that does not report on itself cannot be operated.

## 4.7 Running it twice

We arrive at the heart of the chapter. If you take one thing from it, take this section.

### Why duplicates are certain

Chapter 1 established this (§1.7) and it is worth restating in this context, because it is the foundation of everything below. You send a request. You get no response. You cannot tell whether it succeeded, so you retry, and if it *had* succeeded you have now done it twice.

There is no configuration that avoids this. It is a property of the situation. A producer that times out and resends. A consumer that crashes after processing but before recording its position. A batch job re-run after a partial failure. A deployment that restarts a job mid-batch. Every one of these delivers the same record more than once.

So the goal is not to prevent duplicates. **The goal is to make them harmless.**

### Four ways to make a write idempotent

**Upsert on a natural key.** Write the record *at* a key derived from the data, so a second write overwrites the first with identical content. Lantern indexes each chunk with `_id = "{document_id}:{chunk_index}"`, which means replaying an edit produces exactly the same set of chunk documents. The pipeline can be run as many times as you like.

This is the best option and I would go looking for it before any other. If your source has no natural key, derive one deterministically: `hash(source_system, entity_id, event_timestamp)`. Note the word *deterministically* — a UUID generated at write time is not a key, it is a new record every time.

**Overwrite the whole partition.** The single most useful idempotency pattern in batch processing. Instead of appending today's output, **delete and rewrite the entire partition** for today.

```sql
-- not this
INSERT INTO chunks SELECT ... WHERE dt = '2026-09-17';

-- this
INSERT OVERWRITE TABLE chunks PARTITION (dt = '2026-09-17') SELECT ...;
```

Re-running produces byte-identical output, so running twice is indistinguishable from running once, and there is no dedup logic anywhere. In Spark this is `spark.sql.sources.partitionOverwriteMode = dynamic`. Whenever someone asks how to make a batch job idempotent, this should be your first suggestion.

**Commit output and position atomically.** Write the output and record how far you got in a single transaction, so you can never have done one without the other. Kafka transactions (§5.4), Flink's two-phase commit sinks (§6.3), Iceberg and Delta's atomic commits. Powerful, and it requires the sink to cooperate.

**Keep a dedup window.** Remember the identifiers you have seen and drop repeats. Note *window*: the set must be bounded by a TTL or it grows without limit, and a duplicate arriving after the window expires gets through. This is the fallback when the first three are unavailable, and it is a real technique — but it converts a structural guarantee into a probabilistic one, so prefer the others.

### Deduplicating data you already have

Sometimes the duplicates exist and you must remove them.

**Exact duplicates** — byte-identical records — are easy: deduplicate on a content hash, or `SELECT DISTINCT`.

**Logical duplicates** — several versions of the same entity, where you want the latest — need a ranked window:

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

The secondary sort on `kafka_offset` is not decoration, and it is the kind of detail that separates a pipeline that works from one that mostly works. Event timestamps tie — two edits in the same millisecond, or a system that stamps to the second — and when they tie, `ORDER BY event_time DESC` alone leaves the winner **undefined**. The database picks arbitrarily, and it may pick differently on the next run. Which means your carefully idempotent pipeline produces different output each time it runs, and you have a flapping dataset that nobody can reconcile. Always break ties on something totally ordered: an offset, a sequence number, an ingestion timestamp.

**Streaming deduplication** keeps a keyed state of seen IDs with a TTL — Flink's `ValueState` with a timer, or Spark's `dropDuplicatesWithinWatermark`. The TTL is an explicit trade-off between memory and the size of the window in which you catch duplicates.

**Entity deduplication** — recognising that "Robert Smith" and "Bob Smith" are the same person — is a completely different and much harder problem, involving blocking, similarity scoring, and often the embeddings of Chapter 3. Do not conflate it with pipeline deduplication; they share a word and nothing else.

### Reprocessing safely

Now the scenario from §4.1. A bug produced malformed chunks for six hours. You have fixed it. You need to reprocess those six hours without disturbing anything else.

Seven questions, in order. Each one is a place I have watched a backfill go wrong.

**One: can you get the input back?** Is the raw data still in S3? Is it still within Kafka's retention? If the answer is no, you do not have a backfill problem, you have a permanent data loss problem, and the lesson is §4.2's rule about never discarding raw data.

**Two: is the computation deterministic?** This is the question people skip, and it has more wrong answers than you would think. The obvious hazards are `now()`, `random()`, and auto-increment IDs. The subtle one — and it is genuinely subtle — is **joining against a dimension table that has since changed.**

Suppose the original run enriched each document with its team's name, joining to a `teams` table. Since then, three teams have been renamed and one was deleted. Re-running the job today joins against *today's* teams and produces different output than the original run did, for reasons that have nothing to do with the bug you fixed. If historical fidelity matters, you need point-in-time correctness: slowly-changing-dimension tables with validity ranges, and a join on "the version of the team as of the event's timestamp". Chapter 6 calls this a temporal join and provides it as a primitive (§6.6).

**Three: is the write idempotent?** Overwrite the partition or upsert by key. Never blind-append into a backfill; that is how you turn one bad day into two.

**Four: limit the blast radius.** Write to a shadow table, diff it against production, inspect the differences, *then* swap. This is the alias-swap pattern again (§2.10, §4.3, §4.4) — and by now you should be noticing that "build alongside, verify, atomically swap" is the general shape of every safe change in a data platform.

**Five: throttle it.** A backfill of two years of data at full parallelism will saturate the cluster, starve the real-time pipeline, and page the on-call. Rate-limit it and run it off-peak. A backfill that takes eight hours and bothers nobody is better than one that takes forty minutes and causes an incident.

**Six: tell the downstream.** Consumers of your output may need to re-run too. This is where the lineage from §4.6 earns its cost — you can answer "what else has to be rebuilt?" in a minute rather than by asking around.

**Seven: handle the bookmark explicitly.** Your job stores a position — a Kafka offset, a watermark, a last-processed timestamp. Reset it deliberately and completely. A half-reset state, where the job resumes from somewhere between the old and new positions, produces a gap that is very hard to detect later precisely because nothing failed.

### Five rules

To compress the whole section:

> 1. **Keep raw data.** Everything else can be re-derived. Nothing can re-derive the input.
> 2. **Make every write idempotent**, keyed on a deterministic ID.
> 3. **Prefer overwriting a partition to appending to one.**
> 4. **Assume at-least-once delivery everywhere**, and get exactly-once *effects* from idempotency.
> 5. **Make backfill routine** — a parameterized operation you run casually, not a heroic manual effort with a runbook and a war room.

A team that follows those five rules has a boring data platform. Boring is the goal. It means that when a bug ships, the response is "reprocess Tuesday" rather than an incident review.

## 4.8 The rest of the operational picture

A few things that are essential in practice and which I will treat briefly, because they are more about organisation than about principle.

**Orchestration.** Airflow, Dagster, Prefect, Argo. These express pipelines as dependency graphs with schedules, retries, and backfill support. Three ideas matter: tasks should be **idempotent** (so retries are safe — the same principle again); scheduling should be **data-aware**, triggering when the input partition actually exists rather than when the clock strikes and hoping; and **sensors** let a job wait for external readiness rather than guessing at a delay.

**Cost.** In a cloud data platform, cost is a design constraint rather than an afterthought, and the levers are all things we have already covered: file format and compression, partition pruning, avoiding full scans, right-sizing clusters, spot instances, and lifecycle policies that move cold data to cheaper storage. A query that scans a terabyte because someone forgot a partition filter is a line item.

**Governance and personal data.** Know where personal data lives, restrict access to it, and be able to delete it on request — which is genuinely hard on an append-only immutable lake, since "immutable" and "please delete my data" are in direct tension. The available answers are tombstone records, per-user encryption keys that can be destroyed to render the data unreadable ("crypto-shredding"), and the row-level delete support in table formats. Decide which one you are using before someone asks.

**What actually goes wrong.** If you want to know where to invest monitoring, the top three data platform incidents are, consistently: (1) an upstream schema change nobody announced, (2) a late or missing partition, and (3) a skewed job blowing its SLA. Instrument for those three specifically and you will catch most of what happens.

---

## Where we are

We have the principles. Data is refined through layers, with the raw layer kept forever as the thing everything can be re-derived from. Batch is simple and complete; streaming is fresh and hard; micro-batching splits the difference. Row formats suit writing and columnar formats suit analytics, and Parquet's column pruning plus predicate pushdown is why. Schemas change constantly, and a registry converts a 3 a.m. outage into a failed build. Quality has four dimensions and the most dangerous failure is a job that succeeds while producing nothing. And the central skill is idempotency — write to a deterministic key, overwrite rather than append, and reprocessing becomes routine.

What we do not yet have is any actual machinery. Lantern still has no pipeline; documents are getting into OpenSearch by magic.

The first thing it needs is somewhere for changes to *go* — a durable, ordered, replayable place that decouples the database from the indexer, so that the indexer can be slow, or restarted, or rewound three hours, without the database caring. That component is the subject of the next chapter, and it is built from the most unglamorous data structure in computing.
