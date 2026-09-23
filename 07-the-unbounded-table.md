# Chapter 7 — The Unbounded Table

Some of Lantern's work has no latency requirement at all: reindex two hundred million vectors, reprocess six bad hours, scan two years of view events. That needs a batch engine built for throughput — ideally one whose streaming code is the *same* code, so the two cannot drift apart. Spark is that engine.

**By the end you'll be able to:**

- Explain how Spark turns your code into stages and tasks, and why the shuffle dominates cost.
- Say why DataFrames and SQL beat RDDs and Python UDFs, and tune the five settings that matter.
- Describe Structured Streaming's unbounded-table model, its micro-batch loop, and where its exactly-once guarantee comes from.
- Use watermarks, `foreachBatch`, checkpoints, and `availableNow` correctly, and diagnose a struggling streaming query.
- Choose between Spark, Flink, and Kafka Streams for a given workload.

## 7.1 Two jobs, one engine

Lantern has three jobs of a different shape. Chapter 3's new embedding model means re-embedding two hundred million chunks and building a new index. Chapter 4's six-hour bug means reprocessing that window. And analytics wants to know which documents nobody has opened in two years, across every view event in S3.

None of these is a streaming problem. Nobody cares whether the reindex finishes at two in the morning or four; what matters is **throughput** over an enormous, *bounded* dataset, and each job needs all the data at once — a global aggregate, a full sort, a join against the complete history.

This is batch, the easier case from §4.3, and its dominant engine is **Apache Spark**. Spark earns a chapter because of **Structured Streaming**, which lets you write *the same code* for the batch reindex and the incremental update — a direct answer to §4.3's Lambda problem of two implementations drifting apart. It is built on the batch engine, so we start there.

## 7.2 How Spark runs a job

```
  Driver  (your program; holds the SparkSession)
     │     logical plan → Catalyst → physical plan → DAG of stages
     ▼
  Cluster manager (YARN / Kubernetes / standalone)
     ├──► Executor 1 : [task][task][task]  + memory + cache
     ├──► Executor 2 : [task][task][task]
     └──► Executor 3 : [task][task][task]
```

| Piece | What it is |
|---|---|
| **Driver** | Runs your code, builds the plan, schedules work, collects results. A single process and a favourite bottleneck: `collect()` pulls the whole dataset into its memory and, on large data, dies. |
| **Executor** | A JVM on a worker machine. Runs tasks and holds cached data. |
| **Partition** | A slice of the data. **One task processes one partition.** |
| **Job / stage** | An **action** (`count()`, `write()`, `collect()`) triggers a **job**, split into **stages** at **shuffle boundaries**. |
| **DAG** | The stages form a directed acyclic graph — arrows one way, no loops. The Spark UI draws it; the driver walks it. |

So parallelism equals your partition count, capped by total executor cores: too few partitions and the cluster idles, too many and scheduling overhead swamps tiny tasks. Within a stage, tasks run independently; between stages, data crosses the network.

### What you actually write

Everything starts from a **SparkSession**, your program's handle on the cluster (`spark` in a notebook): `spark.read...` gives you data, `spark.sql("...")` runs a query. Spark has had three APIs, and the choice between them is the largest performance factor in this chapter.

The **RDD** (Resilient Distributed Dataset) is the 2012 original: a distributed collection of arbitrary objects, transformed by handing Spark a function.

```python
rdd.filter(lambda r: r["country"] == "FR").map(lambda r: r["latency_ms"])
```

Spark can distribute and rerun that function, but cannot *understand* it — the lambda is a black box.

The **DataFrame** is the modern API: a distributed table with named, typed columns described by a schema. You name columns and operations instead of supplying a function.

```python
df.filter(col("country") == "FR").select("latency_ms")
```

This is not cosmetic sugar: `col("country") == "FR"` is a *description of a comparison* that Spark can read, rewrite, and push into the file reader. The equivalent `spark.sql(...)` query compiles to the identical plan. A **Dataset** is the typed DataFrame in Scala and Java; Python has no static types, so there you just get DataFrames.

### Laziness, and why it is the point

**Transformations** — `select`, `filter`, `join`, `groupBy` — are **lazy**: they build a plan and do nothing. **Actions** execute it. Because nothing runs until you ask, Spark sees the *entire* computation first and can rewrite it.

The rewriting is done by **Catalyst**, the query optimizer. It will:

- push filters down to the file scan, so rows are eliminated before they are read;
- prune unreferenced columns, which combines with Parquet's column pruning (§4.4) to read a fraction of the bytes;
- reorder joins so the most selective runs first;
- fold constants and eliminate redundant work.

Underneath, **Tungsten** generates bytecode for whole stages, manages memory off-heap to avoid garbage collection, and reads columnar formats vectorially. Hence a rule with teeth:

> **Use DataFrames, Datasets, or SQL. Do not use RDDs, and avoid plain Python UDFs.**

An RDD lambda is **opaque**: Spark cannot push it down, reorder around it, or prune columns it touches, so the same logic can run five to ten times slower. Python UDFs are worse — every row is serialised from the JVM to Python and back. If you must write one, use a **pandas/Arrow UDF**, which moves columnar batches and is often an order of magnitude faster.

### Narrow, wide, and the shuffle

- **Narrow** transformations (`map`, `filter`, `union`) build each output partition from exactly one input partition. No data moves. Cheap.
- **Wide** transformations (`groupBy`, `join`, `distinct`, `repartition`, window functions) need data from *many* input partitions. Each task writes its output, split by destination, to local disk, and every next-stage task fetches its share from every previous one. That is the **shuffle**.

Serialization, disk writes, an all-to-all network transfer, disk reads: **the shuffle is the dominant cost in almost every Spark job**. Most Spark performance work is one sentence: *shuffle less, and shuffle less-skewed data.*

### Fault tolerance without replication

Spark records **lineage**: the transformations that produced each partition. If an executor dies, Spark **recomputes** the lost partitions from lineage on the survivors — no replica needed. Unlike Kafka's or OpenSearch's answer to §1.3, this works because batch input is immutable and transformations are deterministic (a non-deterministic one would make a recomputed partition disagree with its neighbours — §4.7).

Long lineages get expensive to replay, so **checkpointing** writes a partition to durable storage and truncates the lineage. In streaming, it is mandatory.

### The five things to tune

| Setting | What to do |
|---|---|
| **`spark.sql.shuffle.partitions`** | Defaults to **200**, which is wrong for almost everybody: 200 tiny tasks on small data, 200 spilling giants on large data. Target roughly **100–200 MB per partition**, or let AQE handle it. |
| **Adaptive Query Execution (AQE)** | On by default since Spark 3. Re-optimises at runtime from real stage statistics: coalesces small shuffle partitions, switches to a broadcast join when a side turns out small, and **splits skewed partitions automatically**. Leave it on. |
| **Broadcast joins** | If one side is small (tens of MB), ship it to every executor and join locally — **no shuffle**, usually the largest join speedup available. `spark.sql.autoBroadcastJoinThreshold` does it automatically; when Spark's size estimate is wrong, force it with `broadcast(smallDf)`. |
| **Caching** | `df.cache()` pays off only when a DataFrame is used **multiple times** in a job; otherwise it just occupies memory and evicts other things. |
| **Skew** | See below. |

**Skew**, this book's recurring villain: 199 tasks finish in a minute and the two-hundredth runs for four hours, because one key holds most of the rows. The fixes, in order:

1. Let AQE handle it.
2. Broadcast the small side to avoid the shuffle.
3. **Salt** the hot key — append a random suffix, aggregate in two phases, then combine (the two-phase aggregation of §6.8).
4. Filter the hot key out and process it separately.

Four hours becoming twelve minutes is a common outcome, and the whole change is a salt column.

## 7.3 The unbounded table

### One idea

Think of an input stream as a **table that keeps growing**:

```
   t = 1          t = 2          t = 3
 ┌───────┐      ┌───────┐      ┌───────┐
 │ row 1 │      │ row 1 │      │ row 1 │
 │ row 2 │      │ row 2 │      │ row 2 │
 └───────┘      │ row 3 │      │ row 3 │
                └───────┘      │ row 4 │
                               │ row 5 │
                               └───────┘
```

You write a query against that table — `df.groupBy("document_id").count()` — and Spark keeps the answer up to date as rows arrive, updating stored state incrementally rather than re-scanning.

The consequence: **the streaming API is the batch API.** Same DataFrame methods, same SQL; only `read` becomes `readStream` and `write` becomes `writeStream`. So the nightly reindex and the incremental update can be the same function, called twice — no Lambda-style drift (§4.3).

### How it actually runs

Underneath, the default engine runs a loop of small batch jobs:

```
forever:
  1. ask the source what offset range is available since the last batch
  2. write that range to the write-ahead log in the checkpoint directory
  3. run a batch query over exactly that range, reading and updating state
  4. write the output to the sink
  5. record the batch as committed in the checkpoint
```

That is the whole engine. Every guarantee comes from **steps 2 and 5**: offsets are recorded *before* processing and the batch is committed *after* the sink write (§7.6 explains why).

The price is latency. Per-batch overhead — planning, scheduling, state store access — keeps you above roughly a hundred milliseconds, and in practice you run at seconds. Flink's record-at-a-time model wins on latency; Spark wins on reusing your batch stack, throughput per core for heavy aggregation, and team familiarity. (A *continuous processing* mode offers millisecond latency but supports only map-like operations and has been experimental for years. Ignore it.)

### Triggers

```python
.trigger(processingTime="30 seconds")   # a micro-batch every 30s — most common
.trigger(availableNow=True)             # process everything available, then stop
# no trigger: run batches back to back as fast as possible
```

A longer interval means bigger batches, better throughput, fewer output files, and higher latency — §1.9's batching trade-off, for the fifth and final time in this book.

**`availableNow`** deserves more attention than it gets. It processes everything available and then **exits** — a batch job that uses the streaming checkpoint to track where it left off. Run it hourly from a scheduler and you get **incremental batch**: no always-on cluster, no offset bookkeeping, exactly-once. Lantern's chunk-embedding pipeline needs freshness in seconds, so it belongs in Flink; the hourly dashboard roll-up just needs `availableNow` on a schedule.

## 7.4 Reading and writing

### Sources

```python
views = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "document-views")
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", 500000)      # rate limit — read on
    .load())

parsed = (views
    .select(from_json(col("value").cast("string"), schema).alias("d"),
            col("timestamp").alias("kafka_ts"))
    .select("d.*", "kafka_ts"))
```

Kafka hands you `key`, `value`, `topic`, `partition`, `offset`, and `timestamp`; key and value are binary, so you parse them yourself. Other sources: **files**, **Delta and Iceberg tables**, `rate` (a synthetic generator, useful for testing), and `socket` (testing only).

For file sources, use `maxFilesPerTrigger` (see below), and beware that the default reader **lists the directory on every trigger** — minutes on a million-object S3 prefix. At volume, use a notification-based reader (Databricks Auto Loader's `cloudFiles`, or SQS/SNS) that learns of new files from events.

### The setting you must not forget

> **`maxOffsetsPerTrigger` is not optional in practice.**

Without it, a job started with `startingOffsets=earliest` reads **the entire topic in its first batch** — a week of retention at a hundred thousand records a second is sixty billion records. The job runs out of memory, restarts, and does it again.

This is also how Structured Streaming does backpressure, and the contrast matters more than any tuning tip: Flink propagates backpressure from sink to source automatically (§6.2), while Spark needs a **rate limit set by hand** — without one, nothing stops the source outrunning the job.

### Output modes

| Mode | Emits | Valid for |
|---|---|---|
| **`append`** | only rows that will never change | non-aggregated queries; windowed aggregations **with a watermark** |
| **`update`** | only rows that changed in this batch | aggregations; sink must support upsert |
| **`complete`** | the **entire** result table, every batch | aggregations only, and only if the result stays small |

`complete` is a trap: a `groupBy(document_id)` over two hundred million documents rewrites two hundred million rows every batch, forever. It exists for tiny dashboards ("counts by status", six rows) and is often chosen by accident because it "just works" in the console.

### Sinks, and the escape hatch

```python
(agg.writeStream
    .outputMode("update")
    .format("delta")
    .option("checkpointLocation", "s3://lantern/checkpoints/trending-v1")
    .trigger(processingTime="1 minute")
    .start())
```

Built-in sinks cover Kafka, files, Delta/Iceberg, console, and memory. For everything else there is **`foreachBatch`**, and you will use it constantly.

```python
def write_batch(df, batch_id):
    df.persist()
    # sink 1: OpenSearch
    (df.write.format("org.opensearch.spark.sql")
       .option("opensearch.mapping.id", "chunk_id")
       .mode("append").save("documents_v3"))
    # sink 2: an audit copy, idempotent by batch id
    (df.write.mode("overwrite")
       .save(f"s3://lantern/audit/batch={batch_id}"))
    df.unpersist()

query = stream.writeStream.foreachBatch(write_batch).start()
```

It hands you each micro-batch as an **ordinary batch DataFrame** plus a `batch_id`, which buys you:

- sinks with no streaming connector — JDBC, OpenSearch, an HTTP API;
- **multiple destinations** from one stream;
- **`MERGE INTO`** upserts against Delta or Iceberg;
- **idempotency via `batch_id`** — the same ID always maps to the same offset range, so skip IDs already written or overwrite keyed by ID. That is §4.7's idempotency key, handed to you for free.

### Checkpoints

The **checkpoint location** holds the source offsets (the write-ahead log), operator state, and the commit log; it makes the query restartable and exactly-once. Four rules, each a way people lose data:

1. **One checkpoint per query.** Two queries sharing one corrupt each other's state.
2. **Durable, consistent storage** — S3, HDFS, ABFS — not a local disk that dies with the container.
3. **Checkpoints are tied to the query plan.** Adding or removing a stateful operator, or changing aggregation keys, generally makes it unusable. **Version the path** (`/trending-v2`) and plan how to re-seed state — usually by replaying from Kafka (§5.1).
4. **Deleting it means starting over** from `startingOffsets` with empty state. Sometimes that is what you want; know that you are doing it.

## 7.5 State, watermarks, and joins

Chapter 6 covered the concepts; here is Spark's version, and where it differs.

### Windowed aggregation

```python
trending = (views
    .withWatermark("event_time", "10 minutes")
    .groupBy(window(col("event_time"), "5 minutes", "1 minute"),
             col("document_id"))
    .agg(count("*").alias("views"),
         approx_count_distinct("user_id").alias("uniques")))
```

`window(col, "5 minutes")` is tumbling; add a slide for sliding; `session_window(col, "30 minutes")` gives sessions. `withWatermark` does **two** jobs, and people miss the second:

- **It bounds lateness** (§6.5): an event more than ten minutes behind the maximum event time seen is dropped.
- **It lets Spark discard old state.** Without it, Spark cannot know the 10:00–10:05 window is finished, so it keeps that state **forever** and the job eventually dies. To §6.3's "what bounds this state?", the answer is "the watermark, or nothing".

The watermark also sets your latency: in `append` mode a window is emitted only once the watermark passes its end, so a ten-minute watermark means a dashboard **at least ten minutes behind**. To go faster, shorten the watermark and drop more late data, or use `update` mode and handle revised rows downstream.

### Where Spark and Flink differ on time

Spark's model is **simpler and less flexible**:

| | Spark | Flink |
|---|---|---|
| Watermark | one declarative watermark per stream, always `max_event_time − delay`; a single global watermark per query | custom watermark generators |
| Early / late results | — | per-window triggers for speculative results; allowed-lateness re-firing |
| Dropped late data | a *count*: `numRowsDroppedByWatermark` | the records themselves, via `sideOutputLateData` (§6.5) |

The last row is the practical loss: Spark tells you **how many** events you lost, not **which**.

### Stream–stream joins

Joining two streams buffers both sides in state until no match is possible, so **both sides need a watermark, and the join needs a time constraint:**

```python
(clicks.withWatermark("click_time", "3 hours")
  .join(impressions.withWatermark("imp_time", "2 hours"),
        expr("""
          imp_id = click_imp_id AND
          click_time >= imp_time AND
          click_time <= imp_time + interval 1 hour
        """)))
```

Without the time range, any future row could match any buffered one, so state is unbounded. Outer joins *require* the watermark, to know when to give up and emit a null.

### Stream–static joins

Joining a stream to a static dimension table is cheap and stateless; the static side is re-read or broadcast each micro-batch, so file- and table-based dimensions mostly update themselves. But it is *not* point-in-time correct: you join against the dimension as it is now, not as it was at event time (§4.7's backfill hazard). Spark has no equivalent of Flink's temporal join (§6.6), so for historical fidelity write SCD-2 logic yourself — join on the dimension version whose validity range contains the event time.

### Deduplication

```python
(views.withWatermark("event_time", "1 hour")
      .dropDuplicatesWithinWatermark(["view_id"]))
```

The watermark bounds how long IDs are remembered — §4.7's dedup window, with the watermark as TTL. Prefer this to plain `dropDuplicates`, whose state cleanup relies on event-time ordering assumptions rather than being tied explicitly to the watermark.

### Custom state and the state store

When built-ins don't fit, `flatMapGroupsWithState` (and `transformWithState` in newer versions) gives Flink-style custom state with timeouts — sessions, state machines, cross-record conditions.

State lives in a **state store**, HDFS-backed by default, which keeps it in the JVM heap. For anything substantial, switch to **RocksDB**, as in Flink (§6.3): large heap state means crushing garbage-collection pressure and eventually an unrecoverable job.

```
spark.sql.streaming.stateStore.providerClass =
  org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
```

Monitor state size. **Unbounded state growth is the most common cause of a job that works for a month and then degrades** — gradually, which makes it easy to miss.

## 7.6 The guarantee, and its three conditions

Structured Streaming provides **end-to-end exactly-once** through steps 2 and 5 of §7.3's loop. The batch's offset range is logged before processing and committed after the sink write; on restart, Spark finds the uncommitted batch and **re-executes it over exactly the same input**. Deterministic input plus an idempotent sink write equals exactly-once.

The three conditions:

1. **The source must be replayable** — Kafka, files, Delta, anything that can serve the same offset range twice. One more reason Chapter 5's log is the foundation.
2. **The sink must be idempotent or transactional.** Delta and Iceberg commit atomically per batch; the file sink keeps a transaction log. `foreachBatch` is **entirely your responsibility**: use the `batch_id`, an upsert, or overwrite-by-partition (§4.7).
3. **The checkpoint must be intact.**

One caveat catches people: **the Kafka sink is at-least-once.** It does not use Kafka's transactional producer (§5.4), so consumers may see duplicates; key the records so they can deduplicate.

## 7.7 Operating it

### The metric that matters most

Every micro-batch emits a progress report, via `query.lastProgress`, the Spark UI's Streaming tab, or a `StreamingQueryListener`.

> **`inputRowsPerSecond` versus `processedRowsPerSecond`.** If input exceeds processed, you are falling behind, and the gap compounds every batch.

Alongside it, watch:

| Metric | Why |
|---|---|
| **`batchDuration`** vs. trigger interval | Must stay comfortably below. Thirty-second triggers with forty-second batches means each batch carries more than the last, and lag grows without bound. |
| **`stateOperators.numRowsTotal`**, **`memoryUsedBytes`** | State growth over days and weeks — the slow failure. |
| **`numRowsDroppedByWatermark`** | Late data you are silently losing. Alert on it (§6.5). |
| **Kafka consumer lag** | An independent check. |

### Tuning, in order

1. **Cap the batch size** (`maxOffsetsPerTrigger`, `maxFilesPerTrigger`) so batch duration is stable.
2. **Lower `spark.sql.shuffle.partitions`.** In streaming the cost is paid *every micro-batch* — 200 tasks per shuffle, every thirty seconds, forever. Start at one to three times your total core count.
3. **Use the RocksDB state store** for any meaningful state.
4. **Watch for small files.** A ten-second trigger writes 8,640 times a day; with 200 shuffle partitions that is 1.7 million files, which §4.4 showed destroys read performance. Use longer triggers, `repartition` before writing, and compaction (`OPTIMIZE` on Delta, `rewrite_data_files` on Iceberg).
5. **Fix skew** as in batch: salting, two-phase aggregation, broadcast.
6. **Avoid Python UDFs** in the hot path; use built-ins or Arrow UDFs.
7. **Right-size executors.** Four or five cores each generally beats many single-core executors (scheduling overhead) or a few enormous ones (GC pauses that look exactly like §1.5's dead-versus-slow ambiguity).

### Symptoms and causes

| Symptom | Cause | Fix |
|---|---|---|
| Lag grows steadily | batch duration exceeds trigger interval | rate-limit input; more executors; fix skew; lower shuffle partitions |
| First batch never completes | `earliest` with no rate limit | `maxOffsetsPerTrigger` |
| Executor out of memory | large state, skew, or `complete` mode | RocksDB state store; watermarks; `update`/`append` mode |
| State grows forever | missing watermark on an aggregation, dedup, or join | add `withWatermark` |
| Aggregation emits nothing in `append` mode | watermark hasn't passed the window end — or event times are wrong | check timestamp parsing and timezone; use `update` mode to diagnose |
| Cannot restart after a code change | checkpoint incompatible with the new plan | new checkpoint path plus a state re-seeding plan |
| Millions of tiny output files | short trigger and high shuffle partitions | longer trigger; `repartition`; compaction |
| Duplicates in the sink | at-least-once sink | `batch_id`-keyed idempotent write, or `MERGE INTO` |

Two rows deserve a comment. "Emits nothing in `append` mode" is §6.5's stuck-watermark trap; switch temporarily to `update` mode, and if rows appear, the watermark is the issue. "Cannot restart after a code change" hurts most, because it arrives just as you need to deploy a fix. Version the checkpoint path from day one and document re-seeding — for Kafka, "replay from an earlier offset".

## 7.8 Choosing between Spark and Flink

Both do streaming well. Here is how I would decide.

| Choose Spark Structured Streaming when… | Choose Flink when… |
|---|---|
| your team is already on Spark and wants one engine for batch and streaming — familiarity is worth real latency | you need sub-second latency |
| latency is measured in seconds, not milliseconds | you need sophisticated event time: custom watermarks, early triggers, allowed-lateness re-firing, late-data side outputs, session logic, temporal joins, complex event processing |
| the work is aggregation- and join-heavy over a Delta or Iceberg lake | you need very large, long-lived keyed state with fine-grained control |
| the job is really **incremental batch** (`availableNow` on a schedule) | you want true end-to-end backpressure, not a rate limit you must remember |

**Choose Kafka Streams when** the processing belongs inside a JVM microservice, both ends are Kafka, and you would rather not operate a cluster at all.

And §6.8's point bears repeating: **Spark for the lake and Flink for the low-latency path is a coherent architecture, not a failure to standardise.** Lantern does exactly that; one engine for everything means being wrong about half your workloads.

## 7.9 Lantern gets a batch path

**The full reindex.** When the embedding model changes, a Spark batch job reads the compacted `document-changes` topic (§5.3) — every document's current version — chunks and embeds in large GPU-efficient batches, and writes to a new OpenSearch index, `documents_v4`. Embeddings are cached by `hash(text) + model_version`, so a rerun re-embeds only what changed; deterministic chunk IDs (`{document_id}:{chunk_index}`) make it idempotent; a rate limit keeps it from starving the live pipeline. Then §3.9's judgment set runs against both indexes, and if nDCG has not regressed, the alias swaps. The reindex takes eleven hours and inconveniences nobody.

**The backfill.** For six hours of bad chunks, a parameterized job reads that window of the raw Avro archive in S3, applies the fixed logic, and **overwrites the affected partitions** (§4.7), re-emitting to OpenSearch with the same deterministic IDs. Re-running it is harmless, which is the entire point.

**The analytics roll-up.** "Documents nobody has opened in two years", and a dozen like it, run against date-partitioned Parquet with predicate pushdown skipping row groups (§4.4). It runs hourly with `availableNow` — start, process what arrived since last time, write, exit.

And that third job **shares its transformation code with the streaming trending job**: one window-and-aggregate function, called from `readStream` in one place and `read` in another. That is §7.3's promise delivered, and why the dashboard and the weekly report agree.

---

## Key takeaways

- Spark splits a job into stages at shuffle boundaries; one task processes one partition.
- Laziness lets Catalyst see and rewrite the whole plan, so declarative DataFrames and SQL beat opaque RDDs and Python UDFs by large factors.
- The shuffle is the dominant cost: shuffle less and less-skewed — broadcast small sides, keep AQE on, salt hot keys.
- Batch fault tolerance is recomputation from lineage, which depends on immutable input and deterministic transformations.
- Structured Streaming treats a stream as an unbounded table, so the streaming API is the batch API.
- Exactly-once needs a replayable source, an idempotent sink, and an intact checkpoint; the Kafka sink is at-least-once.
- Always set `maxOffsetsPerTrigger`: Spark has no automatic backpressure.
- Watermarks bound state as well as lateness; an aggregation, dedup, or join without one will eventually kill the job.
- Version checkpoint paths from day one; plan changes can make old checkpoints unusable.
- `availableNow` on a schedule gives incremental batch without an always-on cluster.

## Where we are

Lantern now has every component it needs: source of truth, change stream, streaming and batch paths, lake, search index, and vectors. What it lacks is a way to answer questions *about* itself — how search quality is trending, whether last quarter beat the one before. The next chapter builds that: OLAP, the lakehouse, Delta Lake, and the medallion architecture, on the engine you have just learned.
