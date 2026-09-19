# Chapter 7 — The Unbounded Table

## 7.1 Two jobs, one engine

Lantern has a problem of a different shape.

Chapter 3 decided to upgrade the embedding model. Which means re-embedding every chunk of every document — two hundred million vectors — and building a new index. Chapter 4 warned that a bug might produce bad output for six hours, requiring a reprocess of that window. And the analytics team wants to know which documents nobody has opened in two years, across the full archive of view events sitting in S3.

None of these is a streaming problem. There is no latency requirement; nobody cares whether the reindex finishes at two in the morning or four. What matters is throughput: get through an enormous, *bounded* dataset as fast as the hardware allows. And each one needs to see all the data at once — a global aggregate, a full sort, a join against the complete history.

This is batch, and it is the case Chapter 4 said was the easier one (§4.3). The dominant engine for it is **Apache Spark**.

But Spark has a second, more interesting claim, which is why it gets a chapter rather than a section: **Structured Streaming**, its streaming API, built on an idea that lets you write *the same code* for the batch reindex and the incremental update. Given §4.3's warning about Lambda architecture — two implementations of the same logic, drifting apart — that is a significant offer.

We need the batch engine first, because Structured Streaming is built directly on it and cannot be understood without it.

## 7.2 How Spark runs a job

```
  Driver  (your program; holds the SparkSession — see below)
     │     builds a logical plan
     │     → Catalyst optimizer → physical plan → DAG of stages
     ▼
  Cluster manager (YARN / Kubernetes / standalone)
     │
     ├──► Executor 1 : [task][task][task]  + memory + cache
     ├──► Executor 2 : [task][task][task]
     └──► Executor 3 : [task][task][task]
```

The **driver** runs your code, builds the execution plan, schedules work, and collects results. It is a single process and therefore a single point of failure, and it is also the most common place people accidentally create a bottleneck — `collect()` pulls the entire dataset into the driver's memory, and on a large dataset it simply dies.

**Executors** are JVMs on worker machines. They run **tasks** and hold cached data.

A **partition** is a slice of the data, and the rule that governs everything is: **one task processes one partition**. So your parallelism equals your partition count, capped by the total number of executor cores. Too few partitions and most of your cluster is idle. Too many and you pay scheduling overhead for tasks that do almost nothing.

An **action** — `count()`, `write()`, `collect()` — triggers a **job**. The job is divided into **stages**, and the dividing lines are **shuffle boundaries**. Within a stage, every task runs independently with no data movement; between stages, data is redistributed across the network. Those stages form a **DAG** — a directed acyclic graph, meaning the arrows point one way and never loop back — which is what the Spark UI draws for you and what the driver walks when scheduling.

### What you actually write

Before the machinery, the thing you type. Spark has had three APIs over its life, and the difference between them is the single largest performance factor in this chapter — so they need naming properly rather than in passing.

Everything starts from a **SparkSession**, the object your program uses to talk to the cluster. `spark.read...` gives you data; `spark.sql("...")` runs a query. In a notebook it already exists as `spark`.

The **RDD** — Resilient Distributed Dataset — is the original API, from 2012. An RDD is a distributed collection of arbitrary Java or Python objects, and you transform it by handing Spark a function:

```python
rdd.filter(lambda r: r["country"] == "FR").map(lambda r: r["latency_ms"])
```

Spark distributes and runs that function and recovers it after a failure, which in 2012 was the whole point. What it cannot do is *understand* it. The lambda is a black box.

The **DataFrame** is the modern API, and it is a distributed table: named columns, known types, described by a schema. You transform it by naming columns and operations rather than by supplying a function:

```python
df.filter(col("country") == "FR").select("latency_ms")
```

That looks like cosmetic sugar for the RDD version. It is not, and the difference is the subject of the next subsection. `col("country") == "FR"` is not a function Spark must call; it is a *description of a comparison* that Spark can read, rewrite, reorder, and push down into the file reader. SQL — `spark.sql("SELECT latency_ms FROM views WHERE country = 'FR'")` — produces the identical plan, because SQL and DataFrames compile to the same thing.

A **Dataset** is the typed variant of a DataFrame, available in Scala and Java: compile-time type checking over the same optimised engine. In Python, the language has no static types to enforce, so `DataFrame` is what you get and the distinction does not arise.

Every code example in this chapter is DataFrames or SQL. RDDs appear once more, as something to avoid.

### Laziness, and why it is the point

**Transformations** — `select`, `filter`, `join`, `groupBy` — are **lazy**. They build a plan and do nothing. **Actions** execute it.

This is not an implementation quirk; it is the foundation of Spark's performance. Because nothing runs until you ask for a result, Spark sees your *entire* computation before executing any of it, and can rewrite it. That rewriting is done by **Catalyst**, the query optimizer, which will:

push your filters down to the file scan so that rows are eliminated before they are ever read; prune columns you never reference, which combines with Parquet's column pruning (§4.4) to read a fraction of the bytes; reorder joins so the most selective happens first; fold constants; and eliminate redundant work.

On top of that, **Tungsten** generates specialised Java bytecode for whole stages, manages memory off-heap to avoid garbage collection, and reads columnar formats vectorially.

Which produces a recommendation with real teeth:

> **Use DataFrames, Datasets, or SQL. Do not use RDDs, and avoid plain Python UDFs.**

The reason is Catalyst. A DataFrame expression is *declarative* — Spark knows you are filtering on a column and can push it into the scan. An RDD with a lambda is **opaque**: Spark sees a function pointer and can optimise nothing. It cannot push it down, cannot reorder around it, cannot prune columns it might reference. The same logic can be five to ten times slower purely because the optimiser was blindfolded.

Python UDFs are worse still, because each row must be serialised from the JVM to a Python process and back. If you must write one, use a **pandas/Arrow UDF**, which transfers batches columnar-wise and is often an order of magnitude faster.

### Narrow, wide, and the shuffle

**Narrow** transformations — `map`, `filter`, `union` — produce each output partition from exactly one input partition. No data moves between machines. Cheap.

**Wide** transformations — `groupBy`, `join`, `distinct`, `repartition`, window functions — produce output partitions that depend on *many* input partitions. Data must be redistributed: each task writes its output split by destination to local disk, and then every task in the next stage fetches its share from every task in the previous one. That is the **shuffle**.

The shuffle involves serialization, disk writes, an all-to-all network transfer, and disk reads. **It is the dominant cost in almost every Spark job.** Which makes most Spark performance work a single sentence: *shuffle less, and shuffle less-skewed data.*

### Fault tolerance without replication

Spark records **lineage**: the sequence of transformations that produced each partition. If an executor dies, Spark does not need a replica of the lost data — it **recomputes** the lost partitions from their lineage, in parallel, on surviving executors. The job carries on.

This is a different answer to §1.3's fault tolerance question than Kafka's or OpenSearch's, and it is available because batch input is immutable and transformations are deterministic. If you can recompute the answer, you do not need to store a copy of it. (Which, incidentally, is another argument for determinism from §4.7 — a non-deterministic transformation makes lineage recovery produce a *different* answer for the recomputed partition than for its neighbours.)

Long lineages become expensive to replay, so **checkpointing** writes a partition to durable storage and truncates the lineage behind it. In streaming, it is mandatory.

### The five things to tune

**`spark.sql.shuffle.partitions`** — the number of partitions produced by a shuffle. It defaults to **200**, and 200 is wrong for almost everybody. On a small dataset you get 200 tiny tasks whose scheduling overhead exceeds their work. On a large one you get 200 enormous partitions that spill to disk. Target roughly **100–200 MB per partition** and set it accordingly, or let AQE — the next item — handle it.

**Adaptive Query Execution (AQE)**, on by default in Spark 3 and later, and a genuine improvement rather than a marketing feature. It re-optimises the plan *at runtime* using real statistics gathered from completed stages. Specifically it coalesces small shuffle partitions after the fact, switches a sort-merge join to a broadcast join when a side turns out to be smaller than expected, and — the best part — **detects and splits skewed partitions automatically**. Leave it on.

**Broadcast joins.** If one side of a join is small — tens of megabytes — ship a copy to every executor and join locally. **No shuffle at all.** This is usually the single largest join speedup available, and `spark.sql.autoBroadcastJoinThreshold` triggers it automatically when Spark knows the size. When Spark's estimate is wrong (which happens with nested queries and stale statistics), force it with `broadcast(smallDf)`.

**Caching.** `df.cache()` is worth it only when a DataFrame is used **multiple times** in one job. Cached data occupies executor memory and evicts other things, so caching indiscriminately makes jobs slower. The common mistake is caching a DataFrame that is consumed exactly once, which pays the storage cost for no reuse.

**Skew.** The recurring villain of this book, and Spark is where it is most visible: 199 tasks finish in a minute and the two-hundredth runs for four hours, because one key holds most of the rows. The fixes, in order: let AQE handle it; broadcast the small side to avoid the shuffle; **salt** the hot key (append a random suffix, aggregate in two phases, then combine — exactly the two-phase aggregation of §6.8); or filter the hot key out and process it separately. A four-hour job that becomes a twelve-minute job is a common outcome, and the whole change is a salt column.

## 7.3 The unbounded table

Now the streaming half, and the idea it rests on.

### One idea

Think of an input stream as a **table that keeps growing**:

```
   t = 1              t = 2              t = 3
 ┌──────────┐       ┌──────────┐       ┌──────────┐
 │ row 1    │       │ row 1    │       │ row 1    │
 │ row 2    │       │ row 2    │       │ row 2    │
 └──────────┘       │ row 3    │       │ row 3    │
                    └──────────┘       │ row 4    │
                                       │ row 5    │
                                       └──────────┘
```

You write a query against that table — `df.groupBy("document_id").count()` — and Spark's job is to keep the answer up to date as rows arrive. It does not re-scan from the beginning each time; it keeps state and updates it incrementally.

The consequence is the thing that makes this interesting: **the streaming API is the batch API.** Not a similar API. The same DataFrame methods, the same SQL, the same functions. The only difference is `spark.read` versus `spark.readStream` at the top and `write` versus `writeStream` at the bottom.

Put that next to §4.3's Lambda architecture problem — two implementations of the same business logic, in two systems, drifting apart, until the dashboard disagrees with the report — and you can see the appeal. Here the logic is written once. The nightly reindex and the incremental update can genuinely be the same function, called twice.

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

That is the whole engine. And every guarantee Structured Streaming provides comes from **steps 2 and 5** — the offsets are durably recorded *before* processing and the batch is marked complete *after* the sink write. §7.6 unpacks why that ordering gives exactly-once.

The consequence of micro-batching is latency. There is per-batch overhead — planning, task scheduling, state store access — so you will not get below roughly a hundred milliseconds, and in practice you run at seconds. Flink's record-at-a-time model wins on latency; Spark wins on reusing your existing batch stack, on throughput per core for heavy aggregation, and on the fact that your team already knows it.

(There is also a *continuous processing* mode with millisecond latency. It supports only map-like operations and has been experimental for years. Ignore it.)

### Triggers

```python
.trigger(processingTime="30 seconds")   # a micro-batch every 30s — most common
.trigger(availableNow=True)             # process everything available, then stop
# no trigger: run batches back to back as fast as possible
```

A longer interval means bigger batches, better throughput, fewer output files, higher latency — §1.9's batching trade-off, for the fifth and final time in this book.

And then **`availableNow`**, which deserves more attention than it usually gets. It processes all currently available data and then **exits**. So it is a batch job — but one that uses Structured Streaming's checkpoint to track exactly where it left off.

Which means you can run it from a scheduler every hour and get **incremental batch**: no always-on cluster, no manual offset bookkeeping, no "which rows did I already process" query. Each run picks up precisely where the last one stopped, with exactly-once guarantees.

For a great many pipelines this is the correct answer and is reached for too rarely. Lantern's chunk-embedding pipeline needs freshness in seconds, so it belongs in Flink. But the analytics roll-up that feeds a dashboard refreshed hourly does not need a permanently running cluster — it needs `availableNow` on a schedule, at a fraction of the cost and complexity.

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

Kafka hands you `key`, `value`, `topic`, `partition`, `offset`, and `timestamp`, with key and value as binary — you parse them yourself.

Other sources: **files** (a directory that new files land in), **Delta and Iceberg tables**, `rate` (a synthetic generator, genuinely useful for testing), and `socket` (testing only).

Two warnings about file sources. First, use `maxFilesPerTrigger`, for the reason immediately below. Second, the default file source **lists the directory on every trigger** to find new files, and listing a directory with a million objects on S3 takes minutes. For any serious volume, use a notification-based reader — Databricks Auto Loader's `cloudFiles`, or an SQS/SNS-driven approach — which learns about new files from an event rather than by scanning.

### The setting you must not forget

> **`maxOffsetsPerTrigger` is not optional in practice.**

Without a rate limit, the first micro-batch of a job configured with `startingOffsets=earliest` tries to read **the entire topic in one batch**. A week of retention at a hundred thousand records a second is sixty billion records in one micro-batch. The job plans an enormous single batch, runs out of memory, restarts, and does it again.

This is also how Structured Streaming does backpressure, and the contrast with Flink is instructive. Flink propagates backpressure automatically from sink to source (§6.2); you cannot forget to configure it because there is nothing to configure. Spark requires you to **set a rate limit by hand**, and if you don't, there is no mechanism preventing the source from reading faster than the rest of the job can process. Knowing this difference is worth more than any single tuning tip in this chapter.

### Output modes

| Mode | Emits | Valid for |
|---|---|---|
| **`append`** | only rows that will never change | non-aggregated queries; windowed aggregations **with a watermark** |
| **`update`** | only rows that changed in this batch | aggregations; sink must support upsert |
| **`complete`** | the **entire** result table, every batch | aggregations only, and only if the result stays small |

`complete` mode is a trap worth flagging. It re-emits every row of the result every batch, forever. On a `groupBy(document_id)` over two hundred million documents that is two hundred million rows written every thirty seconds. It exists for small dashboards — "counts by status", six rows — and is frequently chosen by accident because it is the mode that makes a simple aggregation "just work" in the console.

### Sinks, and the escape hatch

```python
(agg.writeStream
    .outputMode("update")
    .format("delta")
    .option("checkpointLocation", "s3://lantern/checkpoints/trending-v1")
    .trigger(processingTime="1 minute")
    .start())
```

Built-in sinks cover Kafka, files, Delta/Iceberg, console, and memory. For everything else — and for several things the built-ins can do badly — there is **`foreachBatch`**, and you will use it constantly.

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

It hands you each micro-batch as an **ordinary batch DataFrame**, plus a `batch_id`. Which gives you four things:

Sinks with no streaming connector — JDBC, OpenSearch, an HTTP API. Writing to **multiple destinations** from one stream. **`MERGE INTO`** upserts against Delta or Iceberg, which the streaming sinks cannot express. And — the important one — **idempotency via `batch_id`**: because the same batch ID always corresponds to the same offset range, you can record which batch IDs you have written and skip repeats, or make the write an overwrite keyed by batch ID. That is §4.7's idempotency-key pattern with the key handed to you for free.

### Checkpoints

The **checkpoint location** holds the source offsets (the write-ahead log), the operator state, and the commit log. It is what makes the query restartable and exactly-once.

Four rules, each of which corresponds to a way people lose data.

**Every query needs its own checkpoint location.** Two queries sharing one directory will corrupt each other's state.

**It must be on durable, consistent storage** — S3, HDFS, ABFS — not on a local disk that vanishes with the container.

**Checkpoints are tied to the query plan.** Adding or removing a stateful operator, or changing the keys of an aggregation, generally makes the existing checkpoint unusable, because the stored state no longer corresponds to the operators that need it. Plan for this: **version the checkpoint path** (`/trending-v2`) and decide in advance how you will re-seed state — usually by replaying from Kafka, which is exactly what §5.1's replayability is for.

**Deleting the checkpoint means starting over**, from `startingOffsets`, with empty state. Sometimes that is precisely what you want. Know that it is what you are doing.

## 7.5 State, watermarks, and joins

Chapter 6 covered the concepts. Here is how Spark expresses them, and where it differs.

### Windowed aggregation

```python
trending = (views
    .withWatermark("event_time", "10 minutes")
    .groupBy(window(col("event_time"), "5 minutes", "1 minute"),
             col("document_id"))
    .agg(count("*").alias("views"),
         approx_count_distinct("user_id").alias("uniques")))
```

`window(col, "5 minutes")` is tumbling; add a slide for sliding; `session_window(col, "30 minutes")` for sessions.

`withWatermark` does **two** jobs, and the second is the one people miss.

The first is the one from §6.5: it declares how late an event may be and still be counted. Ten minutes here means an event whose timestamp is more than ten minutes behind the maximum seen is dropped.

The second is that **it is what allows Spark to discard old state.** Without a watermark, Spark cannot know that the 10:00–10:05 window will never receive another row, so it must keep that window's state **forever**. An aggregation without a watermark grows monotonically until the job dies. This is the same "what bounds this state?" discipline as §6.3, with the answer being "the watermark, or nothing".

A second interaction worth internalising: in `append` mode, a window's result is emitted only once the watermark has passed the window's end plus the lateness allowance. So **your output is delayed by the watermark duration**, exactly and unavoidably. A ten-minute watermark means a dashboard that is at least ten minutes behind. If that is unacceptable, either shorten the watermark and accept dropping more late data, or use `update` mode and handle revised rows downstream.

### Where Spark and Flink differ on time

Spark's model is **simpler and less flexible**, and it is worth being explicit about the gap so you are not surprised.

Spark gives you one declarative watermark per stream, always computed as `max_event_time − delay`, and the query has a single global watermark. Flink lets you write custom watermark generators, per-window triggers for early speculative results, allowed-lateness re-firing, and a side output stream of late records.

The last of those is the practical loss. Flink's `sideOutputLateData` gives you the dropped records to log and alert on (§6.5). Spark reports a *count* — `numRowsDroppedByWatermark` in the progress metrics — but does not hand you the rows. So you can know **how many** you lost, which you should absolutely monitor, but not **which**, and reconstructing them means a separate query against the source.

### Stream–stream joins

Joining two streams requires buffering both sides in state until no further match is possible — which means Spark needs to know when to stop waiting. So **both sides need a watermark, and the join needs a time constraint**:

```python
(clicks.withWatermark("click_time", "3 hours")
  .join(impressions.withWatermark("imp_time", "2 hours"),
        expr("""
          imp_id = click_imp_id AND
          click_time >= imp_time AND
          click_time <= imp_time + interval 1 hour
        """)))
```

Omit the time-range condition and state is unbounded, because any future row could still match any buffered row. Outer joins additionally *require* the watermark, since Spark must know when to give up waiting and emit a null.

### Stream–static joins

Joining a stream to a static DataFrame — a dimension table — is cheap and stateless. The static side is re-read or broadcast each micro-batch, which means that for file and table sources, slowly-changing dimensions mostly update themselves without you doing anything.

Note that this is *not* point-in-time correct: you join against the dimension as it is now, not as it was when the event happened. §4.7's backfill hazard applies in full, and Spark has no direct equivalent of Flink's temporal join (§6.6). If you need historical fidelity, you must write the SCD-2 logic yourself — join on the dimension version whose validity range contains the event time.

### Deduplication

```python
(views.withWatermark("event_time", "1 hour")
      .dropDuplicatesWithinWatermark(["view_id"]))
```

The watermark bounds how long IDs are remembered; without it, the dedup set grows forever. `dropDuplicatesWithinWatermark` is preferable to plain `dropDuplicates` because its state cleanup is tied explicitly to the watermark rather than to event-time ordering assumptions. This is §4.7's dedup window with the TTL supplied by the watermark.

### Arbitrary stateful processing and the state store

When the built-ins don't fit, `flatMapGroupsWithState` (and `transformWithState` in newer versions) gives you Flink-style custom state with timeouts — session logic, state machines, cross-record conditions.

State lives in a **state store**, which is HDFS-backed by default. For anything substantial, switch to **RocksDB**:

```
spark.sql.streaming.stateStore.providerClass =
  org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
```

The reason is the same as Flink's (§6.3): the default keeps state in the JVM heap, so large state means enormous garbage collection pressure and eventually an unrecoverable job. Monitor state row counts and size — **unbounded state growth is the most common cause of a streaming job that works for a month and then degrades**, and it degrades gradually, which makes it easy to miss.

## 7.6 The guarantee, and its three conditions

Structured Streaming provides **end-to-end exactly-once**, and it is worth stating the mechanism because it is simple and because the conditions are easy to violate.

The mechanism is steps 2 and 5 of §7.3's loop. Before processing, the batch's exact offset range is written to the write-ahead log. After the sink write succeeds, the batch is marked committed. On restart, Spark finds an uncommitted batch, reads its recorded offset range from the WAL, and **re-executes that batch over exactly the same input**. Deterministic input plus an idempotent sink write equals exactly-once.

The three conditions:

**One: the source must be replayable.** Kafka, files, Delta — anything that can serve the same offset range twice. A source that consumes destructively cannot support this, which is one more reason Chapter 5's log is the foundation of the architecture.

**Two: the sink must be idempotent or transactional.** Delta and Iceberg commit atomically per batch. The file sink maintains a transaction log. And `foreachBatch` is **entirely your responsibility** — use the `batch_id`, use an upsert, or use overwrite-by-partition (§4.7).

**Three: the checkpoint must be intact.**

And one specific caveat that catches people: **the Kafka sink is at-least-once**, not exactly-once. Structured Streaming does not use Kafka's transactional producer (§5.4). If you write to Kafka, downstream consumers may see duplicates, and the answer is to key the records so consumers can deduplicate — the same pattern, one hop further along.

## 7.7 Operating it

### The metric that matters most

Every micro-batch emits a progress report, available via `query.lastProgress`, the Spark UI's Streaming tab, or a `StreamingQueryListener`. Within it:

> **`inputRowsPerSecond` versus `processedRowsPerSecond`.** If input exceeds processed, you are falling behind, and the gap compounds every batch.

That single comparison is the health of a streaming query. Alongside it:

**`batchDuration` versus the trigger interval.** Batch duration must stay comfortably *below* the interval. If a thirty-second trigger produces forty-second batches, batches queue, and lag grows without bound — and it will not recover on its own, because each batch now has more data than the last.

**`stateOperators.numRowsTotal` and `memoryUsedBytes`** — state growth over days and weeks, which is the slow failure.

**`numRowsDroppedByWatermark`** — late data you are silently losing. Alert on it, per §6.5.

**Kafka consumer lag** at the source, as an independent check.

### Tuning, in order

**Cap the batch size.** `maxOffsetsPerTrigger` or `maxFilesPerTrigger`, so batch duration is stable and predictable rather than a function of how much happened to arrive.

**Lower `spark.sql.shuffle.partitions`.** This matters much more in streaming than in batch, because the cost is paid *every micro-batch*. The 200 default means 200 tasks per shuffle per batch — every thirty seconds, forever — and for a small batch, task scheduling overhead dominates the actual work. One to three times your total core count is a better starting point.

**Use the RocksDB state store** for any meaningful state.

**Watch for small files.** A ten-second trigger writes a set of output files 8 640 times a day. With 200 shuffle partitions, that is 1.7 million files a day. §4.4 explained why that destroys your read performance. Mitigate with longer trigger intervals, an explicit `repartition` before writing, and periodic compaction (`OPTIMIZE` on Delta, `rewrite_data_files` on Iceberg).

**Fix skew** exactly as in batch: salting, two-phase aggregation, broadcast the small side.

**Avoid Python UDFs** in the hot path; use built-ins or Arrow UDFs.

**Right-size executors.** A few medium executors — four or five cores each — generally beat many single-core ones (scheduling overhead per task) or a few enormous ones (garbage collection pauses that look exactly like §1.5's dead-versus-slow ambiguity).

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

Two of those rows deserve a comment. The "emits nothing in `append` mode" row is Spark's version of §6.5's stuck-watermark trap, and the diagnostic trick is genuinely useful: temporarily switch to `update` mode. If rows appear, your query is fine and the watermark is the issue. If nothing appears in `update` mode either, the problem is upstream.

And the "cannot restart after a code change" row is the one that hurts in production, because it arrives at the worst moment — you have a fix to deploy and discover you cannot deploy it without losing state. Version the checkpoint path from day one and have a documented re-seeding procedure, which for a Kafka source is simply "replay from an earlier offset".

## 7.8 Choosing between Spark and Flink

Both do streaming. Both do it well. Here is how I would actually decide.

**Choose Spark Structured Streaming when:**

Your team and platform are already on Spark, and you want one engine and one codebase for batch and streaming. This is not a small consideration — the operational familiarity is worth real latency.

Your latency requirement is measured in seconds, not milliseconds.

The work is aggregation- and join-heavy over a lake in Delta or Iceberg format.

Or the job is really **incremental batch**, in which case `availableNow` on a schedule is cheaper and simpler than anything always-on.

**Choose Flink when:**

You need sub-second latency.

You need sophisticated event-time handling — custom watermark generators, early triggers, allowed-lateness re-firing, side outputs of late data, session logic, temporal joins, complex event processing.

You need very large, long-lived keyed state with fine-grained control over it.

You want true end-to-end backpressure rather than a manual rate limit you have to remember.

**Choose Kafka Streams when** the processing belongs inside a JVM microservice, both ends are Kafka, and you would rather not operate a cluster at all.

And I want to repeat §6.8's point because it is the honest answer: **running Spark for the lake and Flink for the low-latency path is a coherent architecture, not a failure to standardise.** Lantern does exactly that. The engines are good at different things, and choosing one for everything means being wrong about half your workloads.

## 7.9 Lantern gets a batch path

Three jobs, all Spark, all solving problems streaming cannot.

**The full reindex.** When Chapter 3's embedding model changes, a Spark batch job reads the compacted `document-changes` topic (§5.3) — the current version of every document — chunks each one, embeds the chunks in large GPU-efficient batches, and writes to a new OpenSearch index `documents_v4`. Embeddings are cached keyed by `hash(text) + model_version`, so a subsequent rerun re-embeds only what actually changed. The job is idempotent because chunk IDs are deterministic (`{document_id}:{chunk_index}`). It is rate-limited so it does not starve the live pipeline. When it finishes, the judgment set from §3.9 runs against both indexes, and if nDCG has not regressed, the alias swaps. The reindex takes eleven hours and inconveniences nobody.

**The backfill.** When a bug produces bad chunks for six hours, a parameterized Spark job reads the raw Avro archive from S3 for that window, applies the fixed logic, and **overwrites the affected partitions** (§4.7) before re-emitting to OpenSearch with the same deterministic IDs. Re-running it is harmless, which is the entire point.

**The analytics roll-up.** The "documents nobody has opened in two years" question, and a dozen like it, run against Parquet in the lake, partitioned by date, with predicate pushdown skipping row groups wholesale (§4.4). It runs with `availableNow` on an hourly schedule, so there is no long-running cluster — it starts, processes exactly what arrived since last time using its checkpoint, writes, and exits.

And notice something about that third job: **it shares its transformation code with the streaming trending job.** The window-and-aggregate logic is one function, called from a `readStream` in one place and a `read` in another. That is the promise of §7.3 delivered, and it is why the dashboard and the weekly report agree.

---

## Where we are

Spark is a batch engine with a streaming API bolted on very carefully, and both halves are worth having.

The batch engine's performance comes from laziness — because nothing executes until you ask, Catalyst sees the whole plan and can rewrite it — which is why declarative DataFrames beat opaque RDDs and UDFs by large factors. Its dominant cost is the shuffle, so most tuning is about shuffling less and less-skewed data. Its fault tolerance is recomputation from lineage rather than replication, which works precisely because batch input is immutable and transformations are deterministic.

The streaming engine models a stream as an unbounded table and maintains the answer to a query over it incrementally, running small batch jobs in a loop. Exactly-once comes from recording offsets before processing and committing after, which requires a replayable source and an idempotent sink. Watermarks bound state as well as lateness, and an aggregation without one will eventually kill the job. And `availableNow` quietly turns the whole thing into incremental batch, which is the right answer more often than it is chosen.

We now have every component Lantern needs. A source of truth, a change stream, a low-latency streaming path, a high-throughput batch path, a lake, a search index, and vectors.

What we do not have is a way to answer questions *about* Lantern — how search quality is trending, which teams' documents go unfound, whether last quarter was better than the one before. That is a different workload with a different engine behind it, and it is where the lake finally becomes a table. The next chapter builds it: OLAP, the lakehouse, Delta Lake, and the medallion architecture, on the engine you have just learned.
