# Chapter 6 — Time

## 6.1 The question that has no good answer

Lantern's product team wants a new feature: a "trending documents" panel showing what the company is reading right now. Specifically, for each document, a count of how many people viewed it in each five-minute window.

There is a `document-views` Kafka topic. Each record has a document ID and a timestamp. The computation is a `GROUP BY document_id, window`. It is, on the face of it, the simplest aggregation in existence.

Now let me ask the question that makes it hard.

It is 10:05:00. A view event arrives with a timestamp of 10:04:58. Clearly it belongs to the 10:00–10:05 window. Should you emit that window's count now?

Your instinct says yes: the window is over, the clock has passed 10:05. But consider what is in flight. A mobile client that was in a lift has thirty events buffered from 10:02, and it will upload them in ninety seconds. One Kafka partition's consumer is lagging by forty seconds because of a garbage collection pause, and its records from 10:04 have not been read yet. A retry from a failed produce at 10:03 is about to be redelivered. And one client's clock is ninety seconds slow, so its 10:04 events are stamped 10:02.

If you emit now, your count is wrong — too low — and it will keep being wrong as more 10:00–10:05 events trickle in for the next several minutes. If you wait, how long? Ten seconds catches most of it. A minute catches nearly all. An hour catches the phone that was switched off. **There is no duration that is both correct and prompt.**

And notice you cannot escape by using the current clock time instead of the event timestamps. If you count "events I processed between 10:00 and 10:05", then the phone's thirty events from 10:02 land in the 10:05–10:10 bucket, the lagging consumer's events land wherever it caught up, and re-running the computation over the identical input produces a **different answer**, because consumer lag was different the second time. That is not a metric. It is an artefact of network conditions with a number attached.

This is the problem Apache Flink is built around, and it is why this chapter is called Time rather than Flink. Every streaming system must answer it. Flink answers it best, and once you understand the answer, the rest of Flink is comparatively ordinary.

## 6.2 What Flink is

**Flink** is a distributed stream processor. It processes records **one at a time**, as they arrive, with sub-second latency. It maintains large amounts of fault-tolerant state. And it has a first-class model of event time.

The one-line positioning, which is worth holding onto as we go: where Spark Streaming is *batch, repeated very quickly* (Chapter 7), Flink is *streaming, with batch treated as the special case of a stream that happens to end.*

### Operators: the one word you need first

Nearly everything in the rest of this chapter is said in terms of **operators**, so let us build the word from something concrete before anything else.

Go back to Lantern's trending panel. Written out as steps, the work is:

1. read view events from the `document-views` Kafka topic;
2. throw away the ones from internal test accounts;
3. group the survivors by document ID;
4. for each document, count the views falling in each five-minute window;
5. write each count to OpenSearch.

Each of those five steps is an **operator**. An operator is one processing step in a streaming pipeline: it takes records in, does one thing to each, and passes records out. That is the entire definition. If you have written `map` and `filter` over a list, you already have the idea; an operator is the same thing, except the list never ends and the step runs as a long-lived piece of a distributed program rather than as a loop.

Operators come in three flavours, and the vocabulary is worth fixing now because the chapter uses it constantly:

A **source** is an operator with no input inside the job — it pulls records in from the outside world. Step 1 is a source: a Kafka source.

A **sink** is an operator with no output inside the job — it pushes records out to the outside world. Step 5 is a sink: an OpenSearch sink.

Everything in between is a **transformation**: `filter` (step 2), `keyBy` (step 3), `window` plus an aggregate (step 4), and the familiar `map`, `flatMap`, `join`, and so on. Two of those deserve a word right now, because they appear in examples before their own sections. **`keyBy(document_id)`** does not compute anything; it declares "from here on, records are grouped by document ID", which is what lets the next operator keep a separate count per document (§6.3). **`window`** slices each of those per-key groups into five-minute buckets (§6.5).

A **job** is those operators wired together into a graph — a **dataflow** — with records flowing along the edges:

```
  Kafka source ──▶ filter ──▶ keyBy ──▶ window+count ──▶ OpenSearch sink
```

That graph is the unit you submit to a cluster, and it runs until you stop it. When the chapter says "the sink is slow", "give every operator a UID", or "Flink's UI shows backpressure per operator", it means one of those boxes.

### From one box to many machines

An operator is a logical step. On a cluster, each one actually runs as several identical copies working on different records simultaneously.

The number of copies is that operator's **parallelism**, and each copy is a **subtask**. A `filter` with a parallelism of eight is eight subtasks, each filtering roughly an eighth of the records, none of them aware of the others. Parallelism is set per operator: the source might run at twenty-four (one subtask per Kafka partition), the window at eight, the sink at four.

So the picture above, drawn honestly for a parallelism of three, is this:

```
   source-1 ──▶ filter-1 ──┐        ┌──▶ window-1 ──▶ sink-1
                           │        │
   source-2 ──▶ filter-2 ──┼─keyBy──┼──▶ window-2 ──▶ sink-2
                           │        │
   source-3 ──▶ filter-3 ──┘        └──▶ window-3 ──▶ sink-3
```

Note what `keyBy` does in that picture: every filter subtask may hold records for any document, but each window subtask must see *all* records for the documents it owns, or its counts would be split across machines. So records cross between subtasks there. That crossing is the expensive part of streaming, and it has a name in the next subsection.

### The two processes that run it

Only now does the cluster itself matter, and it is two kinds of process:

```
        ┌───────────────────────────────────────────┐
        │  JobManager                               │
        │   • builds and schedules the dataflow     │
        │   • coordinates checkpoints               │
        │   • restarts tasks after failure          │
        │   • HA via ZooKeeper or Kubernetes        │
        └────────────────────┬──────────────────────┘
                             │
        ┌────────────┬───────┴────────┬────────────┐
        ▼            ▼                ▼            ▼
   ┌─────────┐  ┌─────────┐     ┌─────────┐   ┌─────────┐
   │TaskMgr  │  │TaskMgr  │     │TaskMgr  │   │TaskMgr  │
   │slot slot│  │slot slot│     │slot slot│   │slot slot│
   │ + state │  │ + state │     │ + state │   │ + state │
   └─────────┘  └─────────┘     └─────────┘   └─────────┘
```

The **JobManager** is the coordinator. It takes your submitted dataflow, decides how many subtasks go where, triggers checkpoints (§6.4), and restarts things after a failure. One per job, and you make it highly available with ZooKeeper or Kubernetes because it is a single point of failure otherwise.

The **TaskManagers** are the workers. Each is a JVM process on some machine, and each offers a fixed number of **task slots** — a slot being one share of that worker's memory and threads. A subtask runs in a slot. Twenty-four subtasks need twenty-four slots somewhere in the cluster, and if the cluster has twenty, the job will not start.

One detail that pays off later: consecutive subtasks that do not need records to cross machines are **fused** into the same slot, so `source-1 → filter-1` becomes a single chain in which a record passes from one to the next as a plain method call, with no serialization and no network. This is why the UI often shows fewer boxes than you wrote.

### Forwarding versus redistribution

Between two operators, records move in one of two ways, and the distinction is the same one Spark will call narrow versus wide (§7.1).

**Forwarding** keeps a record in the same slot — `filter-1` hands to the operator fused after it. This is free.

**Redistribution** sends records across the network to whichever subtask owns the relevant key, with serialization on one end and deserialization on the other. A `keyBy` causes it, as does an explicit `rebalance`. This is the record-crossing in the diagram above, and it is where nearly all the cost is. A dataflow with four `keyBy` operations in a row is doing four network shuffles you may not have intended.

### Backpressure, handled properly

This is worth calling out early because it is one of Flink's genuine advantages and because §1.6 promised you would care about it.

Suppose the OpenSearch sink slows down. In Flink, the sink's input buffers fill. When they are full, the upstream operator cannot hand off its output, so it blocks. Its own input buffers then fill, so *its* upstream blocks. The effect propagates backwards, operator by operator, until it reaches the Kafka source, **which simply stops fetching records.**

Nothing is dropped. Nothing buffers without bound. No configuration was required. The system automatically runs at the speed of its slowest component, and when the sink recovers, everything resumes.

Compare this with the alternative, which is what you get in a system without end-to-end backpressure: the source keeps reading at full speed, records accumulate in an internal queue, memory grows, and eventually the process dies and takes the queue with it. Chapter 7 will show you Spark's approach, which is a manual rate limit you must remember to configure.

Practically, this also gives you the best diagnostic tool in the product. Flink's web UI shows **backpressure per operator**, which means when a job falls behind you can see immediately *which* operator is the bottleneck — the one that is busy while everything upstream of it is blocked. That is usually the entire investigation.

### Four ways to write a job

From most declarative to most manual:

**Flink SQL** — actual SQL over streams. Start here; §6.7 explains why.
**Table API** — the same engine, expressed as method chains.
**DataStream API** — imperative operator-by-operator construction. Full control.
**ProcessFunction** — raw access to state, timers, and side outputs. Where you go when the built-ins genuinely don't fit.

## 6.3 State

A **stateless** operation looks at one record and forgets it: `map`, `filter`. A **stateful** operation remembers something across records: a count, a running total, the last-seen value, a buffer of records waiting for a join partner, a set of IDs seen before.

Every interesting streaming computation is stateful, and state is what makes streaming hard, because it must survive machine failures while being far too large to hold in memory or to rebuild from scratch.

### Keyed state

The important kind. After a `keyBy(document_id)`, Flink partitions the keyspace across the operator's parallel subtasks, so each subtask owns a disjoint set of keys. State is then scoped **per key**, automatically.

This is worth appreciating, because it removes an entire category of concurrency problem. Inside your function you write what looks like single-threaded code accessing a single variable.

The snippet below is a **`KeyedProcessFunction`** — a class you write and hand to a `keyBy`-ed stream, and the most general way to express a stateful operator. Two methods matter: `open()` runs once when the subtask starts, and is where you register the state you intend to use; `processElement()` runs once per arriving record. `ValueState<Long>` is the registered state handle, and `Collector` is how you emit output.

```java
public class ViewCounter extends KeyedProcessFunction<String, View, Long> {
    private transient ValueState<Long> count;

    @Override
    public void open(Configuration c) {
        count = getRuntimeContext().getState(
            new ValueStateDescriptor<>("count", Long.class));
    }

    @Override
    public void processElement(View v, Context ctx, Collector<Long> out) {
        long n = Optional.ofNullable(count.value()).orElse(0L) + 1;
        count.update(n);
        out.collect(n);
    }
}
```

`count.value()` does not return *a* count. It returns the count **for the key of the record currently being processed**. Flink swaps the state context on every record. There are no locks, no maps you manage, no risk of two documents' counts interfering. You wrote code for one document and got a system that handles two hundred million.

There is also **operator state**, scoped per subtask rather than per key, which appears mostly inside connectors — a Kafka source remembering its offsets, for example. You will rarely write it yourself.

### The primitives

`ValueState<T>` holds one value per key. `ListState<T>` holds a list. `MapState<K,V>` holds a map. And `ReducingState` / `AggregatingState` apply a function on write, so that instead of accumulating a list of a million values you keep one accumulator. That last distinction matters more than it sounds: a `ListState` of every view event is unbounded; a `ReducingState` holding a count is eight bytes. When you have a choice, aggregate on write.

### Where state lives

**HashMapStateBackend** keeps state as Java objects on the JVM heap. Fastest possible access, and bounded by your heap, with all the garbage collection consequences that implies at scale.

**EmbeddedRocksDBStateBackend** keeps state in an embedded RocksDB instance on local disk, serialized. **RocksDB** is an embedded key-value store — a library, not a server — that Flink runs inside the TaskManager process; it stores sorted key-value pairs in immutable files on disk with an in-memory cache in front, which is why it can hold far more than the heap and why its files can be copied incrementally. Access is slower — you pay serialization and possibly a disk read — but state can be **far larger than memory**, into the terabytes, and it supports **incremental checkpoints**, which §6.4 will show is a large operational advantage.

For anything with substantial, long-lived state, use RocksDB. The access cost is real but rarely the bottleneck, and the alternative is a job that works in testing and dies in production when the keyspace grows.

In both cases, checkpoints are written to durable external storage — S3, HDFS — not to the local disk.

### The question you must always ask

Unbounded state is the single most common way to kill a long-running Flink job, and it is worth stating as a discipline rather than a warning.

> **For every piece of state you create, be able to say what bounds it.**

There are only three acceptable answers. A **window** that fires and clears its state. An explicit **timer** that deletes the state at a known future point. Or a **time-to-live**:

```java
StateTtlConfig ttl = StateTtlConfig.newBuilder(Time.days(7))
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
    .cleanupInRocksdbCompactFilter(1000)
    .build();
descriptor.enableTimeToLive(ttl);
```

If the answer is "nothing bounds it", you have written a job that works for a month. Lantern's per-document view counter, keyed by document ID, accumulates state for every document ever viewed — including the ones nobody has opened since 2019. Without a TTL, that grows forever, checkpoints get slower, and one day the job stops being able to checkpoint at all.

## 6.4 Checkpoints

Here is the problem in its sharpest form. A Flink job has been running for three weeks. It holds five hundred gigabytes of state spread over forty TaskManagers. One TaskManager's machine fails.

You cannot reprocess three weeks of input; it would take days. You cannot resume from nothing; the counts would be wrong. You cannot just restart that one subtask, because its state is gone and the other thirty-nine have moved on past it. And you must not double-count the records that were in flight.

What you need is a **globally consistent snapshot**: the state of every operator at one logically identical moment, plus the exact input positions corresponding to that moment.

The naive way to get one is to stop the world — pause every operator, wait for all in-flight records to settle, snapshot everything, resume. This is correct and unusable: on a job doing a million records a second with half a terabyte of state, the pause would be minutes long, and you need one every few minutes.

Flink's solution is genuinely clever, it is called **asynchronous barrier snapshotting**, and it descends from the Chandy–Lamport algorithm of 1985.

### Barriers

The JobManager periodically injects a special marker — a **checkpoint barrier** — into each source stream:

```
stream:  ...  e7  e6  ║BARRIER n║  e5  e4  e3  ...
                          ─────────►  flows with the records, in order
```

The barrier flows downstream *with the data*, in the ordinary record stream, keeping its position relative to the records around it. It is not a control message sent out of band; it is in the queue like everything else.

When an operator receives barrier `n` on all of its inputs, it does three things: it snapshots its own state **asynchronously**, so processing continues while the snapshot is written to S3 in the background; it forwards the barrier downstream; and it carries on.

When every operator has acknowledged barrier `n`, the JobManager records checkpoint `n` as **complete**.

Now look at what that completed checkpoint means. Because the barrier kept its place in the stream, the snapshot of each operator reflects *exactly* the state after processing every record before the barrier and no record after it. The snapshot is consistent across forty machines without any of them having stopped, because the barrier — not a clock — defined the moment.

**Recovery** is then simple: restart every operator from the last completed checkpoint's state, and **rewind the sources** to the offsets recorded in that checkpoint. The records after the barrier are read again and reprocessed. State and input position are consistent, so the recomputation produces exactly what the original would have.

### Alignment, and how to avoid paying for it

There is a subtlety in "receives the barrier on all of its inputs". An operator with two inputs may get barrier `n` on the first input while the second is still delivering pre-barrier records. To keep the snapshot exactly at the barrier, it must **buffer** the fast input's post-barrier records until the slow input's barrier arrives. This is **barrier alignment**.

Under heavy backpressure that wait can be long, and since checkpoints must complete before the next one starts, slow alignment is a common reason checkpoints begin to fail. Two escapes:

**`AT_LEAST_ONCE` mode** skips alignment entirely. Checkpoints are fast; on recovery, some records may be processed twice, so counts can be slightly high. Perfectly reasonable if your sink is idempotent.

**Unaligned checkpoints** let barriers overtake buffered in-flight records, and snapshot those in-flight records as part of the checkpoint. Checkpoints stay fast under backpressure at the cost of a larger snapshot. This is usually the right fix when checkpoint duration is your problem.

### Configuring it

```java
env.enableCheckpointing(60_000, CheckpointingMode.EXACTLY_ONCE);
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30_000);
env.getCheckpointConfig().setCheckpointTimeout(600_000);
env.getCheckpointConfig().enableExternalizedCheckpoints(RETAIN_ON_CANCELLATION);
```

The interval is a **recovery-time versus overhead** trade-off, and it is worth thinking about explicitly rather than accepting a default. A long interval means less snapshot overhead and more work to replay after a failure. A short interval means the opposite. One to five minutes is typical.

A useful rule: **if checkpoint duration approaches the checkpoint interval, the job is in trouble.** The checkpoints are not keeping up, and the cause is almost always either backpressure (fix the bottleneck operator) or state that has grown beyond what can be snapshotted in the time available (fix the TTL). Checkpoint duration creeping upward over weeks is the clearest early warning signal Flink gives you, and it is worth an alert.

With RocksDB you also get **incremental checkpoints**, which ship only the SST files (RocksDB's immutable on-disk data files) that changed since the last checkpoint, rather than the whole state. On a five-hundred-gigabyte state where a few gigabytes change per minute, this is the difference between a viable job and an impossible one.

### Savepoints, which are for you rather than for failures

A **savepoint** is a manually triggered, self-contained snapshot, and its purpose is different from a checkpoint's. Checkpoints are the system's private recovery mechanism. Savepoints are a tool for *humans* doing deliberate things:

Upgrade the job's code and resume with all state intact. **Rescale** — change the parallelism from twenty to forty, and Flink redistributes the keyed state across the new subtasks. Migrate to a different cluster. Run two versions side by side. Roll back after a bad deploy.

And one requirement that deserves emphasis, because it is cheap to satisfy now and impossible to retrofit later:

> **Give every operator an explicit, stable UID.**

```java
stream.keyBy(...).process(new ViewCounter()).uid("view-counter");
```

Flink matches state in a savepoint to operators by their ID. Without explicit UIDs, Flink generates them from the structure of the dataflow graph — so **adding a filter upstream changes the generated IDs of everything after it**, and the savepoint can no longer be restored. Your three weeks of state becomes unreachable at the precise moment you needed to deploy a fix.

Set `.uid()` on every stateful operator, from the first line of code, forever. It costs one method call and it is the difference between a job you can evolve and a job you can only restart from empty.

### Exactly-once, end to end

Checkpoints give exactly-once **state**. For exactly-once **output** the sink must participate, and there are two ways.

**An idempotent sink.** Writes are keyed on something deterministic, so a replay overwrites rather than duplicates. This is Lantern's approach — OpenSearch with `_id = "{document_id}:{chunk_index}"` — and it is the fourth appearance of this pattern in the book. Simple, cheap, and sufficient.

**A transactional sink**, implementing two-phase commit. The sink opens a transaction, writes into it, **pre-commits** when the barrier passes (durably promising it can commit), and **commits when the checkpoint completes**. Kafka's transactional producer and Flink's file sinks work this way. The checkpoint acts as the transaction coordinator, which neatly sidesteps §1.7's objection to two-phase commit — the coordinator's state is itself checkpointed, so a coordinator failure is recoverable rather than a permanent block.

There is a consequence of the transactional approach that surprises people and belongs in any design discussion:

> **With a transactional sink, downstream consumers see data only at checkpoint boundaries.** A five-minute checkpoint interval means five-minute output latency, regardless of how fast your processing is.

You bought exactly-once output with latency. If that is unacceptable, either shorten the checkpoint interval — accepting the overhead — or use an idempotent sink and accept at-least-once with idempotent effects.

## 6.5 Watermarks

Now we can answer §6.1.

### Three kinds of time

**Event time** is when the thing happened in the world, recorded in the event by whatever observed it. **Ingestion time** is when it entered Flink. **Processing time** is when an operator handled it.

Processing time is tempting: no waiting, lowest latency, and windows always close on schedule. And it produces results that are **not reproducible**, for the reason §6.1 gave — the same input replayed under different conditions gives different answers.

Use **event time** unless you have a specific reason not to. The exceptions are narrow and real: a monitoring alert that genuinely means "in the last minute of wall-clock time", or a rate limiter. For anything anyone will report on, event time.

### The mechanism

A **watermark** is a marker that flows in the stream alongside the records, carrying a timestamp `T` and making an assertion:

> "I believe no further record with an event time earlier than **T** will arrive."

```
records:  ─ e(10:04) ─ e(10:03) ─ ⟦W 10:02⟧ ─ e(10:06) ─ e(10:05) ─ ⟦W 10:04⟧ ─►
                                      ▲                                 ▲
                             "nothing before 10:02"           "nothing before 10:04"
```

When a watermark passes the end of a window, the window **fires**: its result is final and is emitted.

That is the whole idea. Notice what it does: it converts an unanswerable question ("is the window complete?") into an operational promise that the source makes and the rest of the job can act on. Completeness becomes a declared property rather than a guess, and the guess is made in exactly one place, where you can see and tune it.

### Generating them

The standard generator says: assume events may be out of order by up to a bounded amount.

```java
WatermarkStrategy
    .<View>forBoundedOutOfOrderness(Duration.ofSeconds(30))
    .withTimestampAssigner((view, ts) -> view.getEventTime())
    .withIdleness(Duration.ofMinutes(1));
```

This emits `watermark = (highest event time seen so far) − 30 seconds`.

That thirty seconds is the dial, and it is the most consequential number in a Flink job:

**Smaller** → windows fire sooner, results are fresher, and more genuinely late events arrive after their window closed and get dropped.
**Larger** → more events are captured, and **every window result waits that much longer**, permanently, for everyone.

Choose it from data rather than intuition. Measure the distribution of `processing_time − event_time` across your actual stream and pick something near the p99. Lantern's view events come from browsers on a corporate network and are rarely more than a couple of seconds late; thirty seconds is generous. A pipeline fed by mobile clients might need ten minutes, and would have to accept ten-minute-old dashboards as the price.

### The trap that catches everyone

Read this even if you skim the rest of the chapter.

An operator has many input subtasks — one per Kafka partition, in the common case. Each generates its own watermark. And the operator's watermark is **the minimum across all of its inputs**, because it can only promise what its *least* advanced input can promise.

Which means: **one idle or lagging input holds back the watermark for the entire job.**

Suppose Lantern has twenty-four partitions and one of them receives no events, because a `keyBy` upstream distributed unevenly, or because that partition's producer is down, or simply because it is three in the morning and traffic is low. That partition's watermark never advances. The minimum never advances. **No window anywhere in the job ever fires.**

The symptom is maddening and completely specific: *the job is running, the metrics show records flowing in and being processed, no errors appear anywhere, and no output is ever produced.* Everything looks healthy. Nothing comes out.

This is probably the single most common Flink support question, and there are three things to check. First, **`withIdleness()`**, as in the snippet above, which tells Flink to exclude an input that has been silent for a while from the minimum calculation. Second, whether some partitions genuinely have no producers — often a sign of a keying problem upstream. Third, whether your timestamp assigner is correct at all: if it returns a constant, or a wall-clock time, or a field that is null and silently becomes zero, the watermark logic will do something inexplicable.

### Late events

Events arriving after the watermark has passed their window are **late**. You have three options, and you should choose deliberately rather than inherit a default.

**Allowed lateness.** Keep the window's state for an extra period after firing, and **re-fire** with an updated result when a late event arrives. The window emits multiple times, so everything downstream must handle updates and retractions. Correct, and it pushes complexity outward.

**A side output.** Route late events to a separate stream:

```java
.sideOutputLateData(lateTag)
```

You can then log them, count them, alert on them, or feed them into a batch correction path.

**Drop them.** The default.

My recommendation: **always take the side output, even if you only count what lands in it.** A metric named `late_events_dropped` is nearly free and turns silent data loss into a number on a dashboard. Without it, you have no idea whether your thirty-second watermark delay is discarding 0.01% of events or 4% — and the difference matters enormously to whoever consumes the output. Silently dropping data is how a pipeline loses the trust of the people who depend on it, and once lost that is very hard to win back.

## 6.6 Windows and joins

### Windows

Windows chop an infinite stream into finite pieces you can aggregate.

**Tumbling** windows are fixed-size and non-overlapping. Every event belongs to exactly one.

```
[──10:00–10:05──][──10:05–10:10──][──10:10–10:15──]
```
`TumblingEventTimeWindows.of(Time.minutes(5))` — Lantern's trending panel.

**Sliding** windows are fixed-size with a shorter slide, so they overlap and each event belongs to `size / slide` of them.

```
[──10:00–10:05──]
      [──10:01–10:06──]
            [──10:02–10:07──]
```
`SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1))` — a five-minute moving average refreshed every minute. Note the arithmetic before you deploy: size over slide is five, so **five times the state and five times the output** of the equivalent tumbling window. A one-hour window sliding every second is 3 600 copies of everything, and people do write that by accident.

**Session** windows are defined by inactivity rather than by the clock: events group together until a gap of more than N minutes, then a new session starts. `EventTimeSessionWindows.withGap(Time.minutes(30))`. Naturally variable length, and they *merge* — an event arriving between two existing sessions joins them into one. Excellent for modelling user behaviour, and the hardest kind to bound in state, since a session has no predetermined end.

**Global** windows put everything in one window forever, with a custom trigger deciding when to fire. For count-based or condition-based windowing.

### Window functions, and one that will hurt you

`reduce` and `aggregate` are **incremental**: each arriving record updates a single accumulator. State per window is one value. Cheap. Use these by default.

`process(ProcessWindowFunction)` receives **every buffered record** when the window fires, along with window metadata. Powerful — you can compute a median, or inspect the window's key and boundaries — and it **buffers every record in state until the window closes.** A one-hour window over a high-volume stream with a `ProcessWindowFunction` is a memory problem waiting to happen.

The good news is that you can have both:

```java
.aggregate(new IncrementalCount(), new AddWindowMetadata())
```

The first function accumulates incrementally; the second runs once at firing time with access to window metadata and the accumulated result. Incremental state, full context. This is the pattern you want whenever you need to know *which* window you are emitting.

### Triggers and lateness

A **trigger** decides when a window fires; the default fires when the watermark passes the window end. A custom trigger can fire **early** — emitting a speculative result every ten seconds while the window is still open, then a final one when it closes. This is a genuinely good pattern for dashboards: you get low latency *and* eventual correctness, provided the consumer can handle a result being revised.

`allowedLateness(Time.minutes(5))` keeps the window alive after firing, as §6.5 described.

### Joins

Joining streams is harder than joining tables, because a stream has no end and so you never know whether a match is still coming. Flink offers several shapes.

**Window join** — join two streams within a shared window. Both sides must land in the same window, which makes boundary behaviour awkward.

**Interval join** — for each record on the left, join records on the right whose timestamps fall within `[t − lower, t + upper]`. This is the natural formulation for most real problems: "join each click to impressions from the preceding ten minutes". Much easier to reason about than a window join.

**Temporal join** — and this one deserves attention, because it solves a problem §4.7 raised and left open.

Recall the backfill hazard: enriching an event by joining against a dimension table that has since changed produces different answers on re-run. A temporal join fixes it by joining against the **version of the dimension as of the event's own event time**. "Enrich this document view with the document's title *as it was when the view happened*", not as it is now. Flink maintains the versioned dimension in state, keyed and time-indexed.

This is point-in-time correctness as a primitive, and it makes reprocessing deterministic. If you have ever had a backfill produce different numbers than the original run for reasons nobody could explain, this is very often the reason.

**`connect` and `CoProcessFunction`** — two streams of different types sharing state. The canonical use is a control stream: a low-volume stream of rules or configuration updates writes into state that a high-volume event stream reads. Combine with **broadcast state** when every parallel subtask needs the full rule set rather than a partition of it. This is how you build a streaming job whose behaviour can be changed without redeploying it.

**Async I/O** — deserves a specific warning. If your job enriches each record by calling an external service (a REST API, a database, an embedding model), a synchronous call inside `processElement` blocks the operator thread for the whole round trip. At ten milliseconds per call, one subtask manages a hundred records per second, and your carefully parallelised job is limited by network latency rather than by anything computational.

```java
AsyncDataStream.unorderedWait(stream, new AsyncEnricher(),
                              1000, TimeUnit.MILLISECONDS, 100);
```

`AsyncDataStream` issues many requests concurrently and processes responses as they arrive, taking throughput from a hundred per second to thousands. If your job calls anything external per record, this is not an optimisation; it is a requirement. Add a cache in front of it too.

## 6.7 Flink SQL

Most of what I have described can be expressed in SQL, and for most jobs it should be.

```sql
CREATE TABLE document_views (
  document_id  STRING,
  user_id      STRING,
  event_time   TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '30' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'document-views',
  'properties.group.id' = 'trending',
  'format' = 'avro-confluent',
  'scan.startup.mode' = 'group-offsets'
);

SELECT
  window_start,
  document_id,
  COUNT(*)                AS views,
  COUNT(DISTINCT user_id) AS unique_viewers
FROM TABLE(
  TUMBLE(TABLE document_views, DESCRIPTOR(event_time), INTERVAL '5' MINUTES))
GROUP BY window_start, window_end, document_id;
```

That is Lantern's trending panel, complete. The watermark is a declaration in the DDL. The window is a table function. State, checkpointing, and recovery are handled. This is thirty lines of SQL replacing several hundred lines of DataStream code, and the optimiser is good.

Three concepts you need in order to read streaming SQL without being confused.

**Dynamic tables.** A stream *is* a table that keeps changing, and a query over it produces another table that keeps changing. The duality is exact, and it is what makes SQL sensible here at all.

**Append-only versus changelog streams.** This is the one that trips people up. A *windowed* aggregation is append-only: once the window closes its result is final, so each row is emitted once. But a `GROUP BY` **without** a window — "total views per document, ever" — produces a result that changes with every new record. So Flink emits a **changelog**: a retraction of the old row followed by an insertion of the new one.

The practical consequence is that your **sink must support upserts** — `upsert-kafka`, a JDBC upsert, OpenSearch keyed by document ID. A plain append-only sink will reject a changelog stream, and the error message will be about changelog modes and will not obviously mean "you forgot the window".

**Unbounded aggregation state grows forever.** A non-windowed `GROUP BY document_id` keeps state for every document ID ever seen. This is §6.3's unbounded state problem arriving through SQL, where it is easier to write by accident. Bound it with `table.exec.state.ttl`.

My recommendation: **express what you can in SQL, and drop to DataStream or ProcessFunction only for logic that genuinely doesn't fit.** Less code, fewer bugs, and a planner that will often do better than you would by hand.

## 6.8 Operating it

### How a job actually gets deployed

Worth stating plainly, because the chapter has so far described what a job *is* without saying how it comes to be running.

You compile your job into a JAR (or a Python file, or a SQL script), and you submit it to a cluster:

```
flink run -d -c com.lantern.TrendingJob trending.jar
```

The JobManager receives the dataflow, asks the cluster manager for TaskManagers, places the subtasks in slots, and starts them. From then on the job runs until it fails or you stop it. `flink stop --savepointPath ...` stops it *and* takes a savepoint, which is how you deploy a new version: stop with a savepoint, submit the new JAR with `--fromSavepoint`, and the state carries over (§6.4).

There are two deployment shapes and the difference matters operationally. In **session mode** one long-lived cluster hosts many jobs, which is cheap and convenient and means one job's failure can disturb its neighbours. In **application mode** each job gets its own cluster, dedicated and isolated, which is what you want for anything in production. On Kubernetes the Flink Operator does this for you, and a job becomes a custom resource you deploy like any other workload.

Flink SQL has a third route: a SQL script submitted through the SQL client or a gateway, which the planner compiles into exactly the same dataflow of operators. Nothing below the SQL is different.

### The metrics that matter

**Backpressure and busy time per operator**, in the web UI. Start here, always. It names the bottleneck.

**Watermark lag** — `currentEmitEventTimeLag` — how far behind real time the job's notion of time has fallen. This is the streaming equivalent of consumer lag, expressed in the units that matter for correctness.

**Checkpoint duration, size, and failure count.** As §6.4 said, duration creeping up over weeks is the best early warning in the system.

**Kafka consumer lag** at the source. **Records in and out** per operator. **Restart count** — a job quietly restarting every ten minutes is a very different thing from a job running for a month. **State size on disk** per TaskManager.

### Symptoms and causes

| Symptom | Cause | Fix |
|---|---|---|
| Job consumes but emits nothing | **watermark stuck** — idle or lagging input | `withIdleness()`; check all partitions produce; verify the timestamp assigner |
| Checkpoints slow or timing out | backpressure, state growth, slow object store | unaligned checkpoints; incremental RocksDB; fix the bottleneck operator |
| State grows without bound | no TTL; session windows; unbounded SQL aggregation | state TTL; timers; bounded windows |
| One subtask much slower than the rest | **key skew** | better key, or two-phase aggregation on a salted key |
| Job restarts in a loop | a record that always throws | side-output bad records; sane restart strategy; DLQ |
| Cannot restore from a savepoint | operator UIDs changed | always set `.uid()` |
| Throughput collapses at an enrichment step | synchronous external calls | Async I/O plus caching |
| Duplicate output after a failure | non-idempotent sink | deterministic keys, or a 2PC sink |

The first row is there because it is the most common, and the sixth is there because it is the most expensive.

On skew: the fix worth knowing is **two-phase aggregation**. If one document ID accounts for a third of all views, append a random salt to the key, aggregate on `(document_id, salt)` to get a hundred partial counts computed in parallel, then aggregate those hundred partials by `document_id`. You have traded one serial hot key for two cheap parallel stages. The same trick appears in Spark under the name salting (§7.1).

### Flink, Kafka Streams, or Spark?

| | **Flink** | **Kafka Streams** | **Spark Structured Streaming** |
|---|---|---|---|
| Model | true streaming | true streaming | micro-batch |
| Latency | milliseconds | milliseconds | ~100 ms – seconds |
| Deployment | a cluster | **a library in your app** | a cluster |
| Sources | many | **Kafka only** | many |
| Event time | **strongest** | good | good, simpler |
| State | very large (RocksDB) | large (RocksDB) | large |
| Batch too? | yes, unified | no | **yes, same API** |
| Best for | complex, low-latency, event-time-correct stateful work | Kafka-native microservices | teams already on Spark; one engine for stream and batch |

There is no wrong answer, and a great deal of production data engineering runs Spark for the lake and Flink for the low-latency path, which is a perfectly coherent architecture rather than a failure to standardise.

## 6.9 Lantern gets a memory

The trending panel now exists, and it is worth stating precisely because every sentence is a decision from this chapter.

A Flink job reads `document-views` from Kafka with a watermark strategy of bounded out-of-orderness at thirty seconds, with `withIdleness` at one minute so that quiet partitions cannot stall the job. It keys by `document_id` and applies a five-minute tumbling event-time window, aggregating incrementally into a count plus a HyperLogLog sketch for unique viewers — approximate, deliberately, for the reasons §2.8 gave about distinct counts at scale.

Late events go to a side output, where they are counted and exported as a metric, and the count is on a dashboard next to the panel it affects. Results are written to OpenSearch keyed by `{document_id}:{window_start}`, which makes the sink idempotent, which means at-least-once processing is sufficient and no transactional sink is needed — so output appears immediately rather than at checkpoint boundaries.

Checkpointing runs every sixty seconds in exactly-once mode with unaligned checkpoints and an incremental RocksDB backend. Every operator has an explicit `uid`. Per-document state carries a seven-day TTL, so documents nobody has viewed in a week stop costing anything.

And the same job, with a different sink, computes a second output: a stream of documents whose view rate has risen by more than five times over their trailing average, which goes to a Kafka topic that the notification service consumes. That is the thing streaming makes possible and batch does not — not the dashboard, which could have been a query, but the *reaction*, in seconds, to something that just happened.

---

## Where we are

Flink is a true streaming engine, and its two contributions are state and time.

State is keyed, so you write single-key logic and get a partitioned system. It lives in RocksDB when it is large, and it must always be bounded by a window, a timer, or a TTL — the question "what bounds this state?" has no acceptable third answer.

Fault tolerance comes from checkpoint barriers that flow with the data, producing globally consistent snapshots without stopping the world, so that recovery is "restore state, rewind sources, reprocess". Savepoints let you upgrade, rescale, and roll back — provided you set operator UIDs, which you should do from the first line of code.

And time is handled by watermarks: an explicit, tunable promise that no more data will arrive before a given timestamp, converting the unanswerable question of completeness into a single number you can measure and adjust. The cost of that promise is latency, the price of setting it too low is dropped data, and you should always count what you drop.

What Flink does not give you is the *other* half of Lantern's needs. The nightly full reindex. Re-embedding two hundred million chunks when the model changes. Recomputing three days of chunks after a bug. Ad-hoc analysis over two years of archived events. These are bounded problems over enormous datasets, where latency is irrelevant and throughput is everything — and for those, a different engine has a much better answer, built on a completely different central idea.
