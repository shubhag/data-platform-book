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

Each of those five steps is an **operator**: one processing step that takes records in, does one thing to each, and passes records out. That is the entire definition. If you have written `map` and `filter` over a list you already have the idea, except the list never ends and the step runs as a long-lived piece of a distributed program rather than as a loop.

Three flavours, and the vocabulary matters because the chapter uses it constantly. A **source** has no input inside the job — it pulls records in from the outside world, like step 1's Kafka source. A **sink** has no output inside the job — it pushes records out, like step 5's OpenSearch sink. Everything between is a **transformation**: `filter` (step 2), `keyBy` (step 3), `window` plus an aggregate (step 4), and the familiar `map`, `flatMap`, `join`. Two of those deserve a word right now, because they appear in examples before their own sections. **`keyBy(document_id)`** does not compute anything; it declares "from here on, records are grouped by document ID", which is what lets the next operator keep a separate count per document (§6.3). **`window`** slices each of those per-key groups into five-minute buckets (§6.5).

A **job** is those operators wired together into a graph — a **dataflow** — with records flowing along the edges:

```
  Kafka source ──▶ filter ──▶ keyBy ──▶ window+count ──▶ OpenSearch sink
```

That graph is the unit you submit to a cluster, and it runs until you stop it. When the chapter says "the sink is slow", "give every operator a UID", or "Flink's UI shows backpressure per operator", it means one of those boxes.

### From one box to many machines

An operator is a logical step; on a cluster each one runs as several identical copies working on different records at once. The number of copies is its **parallelism** and each copy is a **subtask**. A `filter` with a parallelism of eight is eight subtasks, each handling roughly an eighth of the records, none aware of the others. It is set per operator: the source might run at twenty-four (one subtask per Kafka partition), the window at eight, the sink at four.

Drawn honestly for a parallelism of three:

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

The **JobManager** is the coordinator: it takes your dataflow, decides which subtasks go where, triggers checkpoints (§6.4), and restarts things after a failure. One per job, made highly available with ZooKeeper or Kubernetes because it is otherwise a single point of failure.

The **TaskManagers** are the workers — a JVM process per machine, each offering a fixed number of **task slots**, a slot being one share of that worker's memory and threads. A subtask runs in a slot, so twenty-four subtasks need twenty-four slots somewhere in the cluster; with twenty, the job will not start.

One detail that pays off later: consecutive subtasks that need no record to cross machines are **fused** into one slot, so `source-1 → filter-1` becomes a single chain passing records as plain method calls, with no serialization and no network. This is why the UI often shows fewer boxes than you wrote.

### Forwarding versus redistribution

Between two operators, records move in one of two ways, and the distinction is the same one Spark will call narrow versus wide (§7.1).

**Forwarding** keeps a record in the same slot — `filter-1` hands to the operator fused after it. This is free.

**Redistribution** sends records across the network to whichever subtask owns the relevant key, with serialization on one end and deserialization on the other. A `keyBy` causes it, as does an explicit `rebalance`. This is the record-crossing in the diagram above, and it is where nearly all the cost is. A dataflow with four `keyBy` operations in a row is doing four network shuffles you may not have intended.

### Backpressure, handled properly

This is worth calling out early because it is one of Flink's genuine advantages and because §1.6 promised you would care about it.

Suppose the OpenSearch sink slows down. In Flink, the sink's input buffers fill. When they are full, the upstream operator cannot hand off its output, so it blocks. Its own input buffers then fill, so *its* upstream blocks. The effect propagates backwards, operator by operator, until it reaches the Kafka source, **which simply stops fetching records.**

With numbers: the source can read 50,000 records/s, the window can process 50,000/s, and the OpenSearch cluster degrades to accepting 8,000/s.

```
Kafka source ──▶ filter ──▶ window ──▶ OpenSearch sink
   8,000/s        8,000/s    8,000/s      8,000/s      ← everything settles here
   (reading at the speed of the slowest stage, not its own)
```

Nothing is dropped. Nothing buffers without bound. No configuration was required. Kafka consumer lag grows, which is exactly right — the unprocessed records are sitting durably in Kafka rather than in a doomed JVM heap — and when OpenSearch recovers to 50,000/s the job drains the backlog at full speed. The system automatically runs at the speed of its slowest component, and when the sink recovers, everything resumes.

Compare this with the alternative, which is what you get in a system without end-to-end backpressure: the source keeps reading at full speed, records accumulate in an internal queue, memory grows, and eventually the process dies and takes the queue with it. Chapter 7 will show you Spark's approach, which is a manual rate limit you must remember to configure.

Practically, this also gives you the best diagnostic tool in the product. Flink's web UI shows **backpressure per operator**, which means when a job falls behind you can see immediately *which* operator is the bottleneck — the one that is busy while everything upstream of it is blocked. That is usually the entire investigation.

### Four ways to write a job

From most declarative to most manual:

**Flink SQL** — actual SQL over streams. Start here; §6.7 explains why.

```sql
SELECT document_id, COUNT(*) FROM TABLE(TUMBLE(TABLE views, DESCRIPTOR(ts), INTERVAL '5' MINUTES))
GROUP BY window_start, document_id;
```

**Table API** — the same engine as method chains: `views.window(Tumble.over(...)).groupBy(...).select(...)`.

**DataStream API** — imperative, operator by operator: `views.keyBy(v -> v.documentId).window(...).aggregate(new CountAggregate())`. Full control.

**ProcessFunction** — raw access to state, timers, and side outputs. Where you go when the built-ins genuinely don't fit: "emit an alert the first time a document is viewed by someone outside its owning team, then stay silent for 24 hours" is a rule no window expresses, and a `KeyedProcessFunction` with a `ValueState` and a timer expresses it in twenty lines.

All four compile down to the same graph of operators. The choice is about how much of the plan you want to write yourself, not about what the engine can do.

## 6.3 State

A **stateless** operation looks at one record and forgets it: `map`, `filter`. A **stateful** operation remembers something across records: a count, a running total, the last-seen value, a buffer of records waiting for a join partner, a set of IDs seen before.

Make that concrete with two operators over the same `document-views` stream.

"Drop views from internal test accounts" is stateless. The record either has `user_id` starting with `test_` or it does not; the decision needs nothing except the record in your hand. Restart the operator on a fresh machine and it behaves identically from the first record.

"Count views per document" is stateful. When the eleventh view of `doc_88` arrives, emitting `11` requires knowing that ten came before. That knowledge is not in the record. It lives in the operator, and if the machine dies, it dies with it.

Every interesting streaming computation is the second kind, and state is what makes streaming hard, because it must survive machine failures while being far too large to hold in memory or to rebuild from scratch.

### Keyed state

The important kind. After a `keyBy(document_id)`, Flink partitions the keyspace across the operator's parallel subtasks, so each subtask owns a disjoint set of keys. State is then scoped **per key**, automatically.

This is worth appreciating, because it removes an entire category of concurrency problem. Inside your function you write what looks like single-threaded code accessing a single variable.

The snippet below is a **`KeyedProcessFunction`**, the most general way to express a stateful operator. Two methods matter: `open()` runs once when the subtask starts and registers the state you intend to use; `processElement()` runs once per record. `ValueState<Long>` is the state handle, `Collector` emits output.

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

Watch it happen. Five records arrive at one subtask, interleaved across three documents:

| # | record | key Flink sets | `count.value()` reads | writes | emits |
|---|---|---|---|---|---|
| 1 | `doc_88, alice` | `doc_88` | `null` → 0 | `doc_88 → 1` | 1 |
| 2 | `doc_12, bob`   | `doc_12` | `null` → 0 | `doc_12 → 1` | 1 |
| 3 | `doc_88, carol` | `doc_88` | **1** | `doc_88 → 2` | 2 |
| 4 | `doc_40, dave`  | `doc_40` | `null` → 0 | `doc_40 → 1` | 1 |
| 5 | `doc_88, erin`  | `doc_88` | **2** | `doc_88 → 3` | 3 |

Record 3 read `1` and record 5 read `2` from the *same* Java field, because between records Flink swapped which key's slot that field points at — leaving three independent counters (`doc_88 → 3`, `doc_12 → 1`, `doc_40 → 1`) behind one variable. Notice also what the table does not contain: any record for a document owned by another subtask. `keyBy` guaranteed `doc_88` always lands here, which is what makes "the count for `doc_88`" a complete answer rather than a partial one needing combination later.

There is also **operator state**, scoped per subtask rather than per key, which appears mostly inside connectors — a Kafka source remembering its offsets. You will rarely write it yourself.

### The primitives

Five state types, each with a job it is right for:

| Primitive | Holds, per key | Lantern would use it for |
|---|---|---|
| `ValueState<T>` | one value | the running view count for a document |
| `ListState<T>` | a list | views buffered waiting for a join partner |
| `MapState<K,V>` | a map | views per country for one document: `{"FR": 12, "DE": 3}` |
| `ReducingState<T>` | one value, folded on write | the same count, but the fold is `(a,b) -> a+b` |
| `AggregatingState<I,O>` | one accumulator, folded on write | a HyperLogLog sketch of unique viewers |

The last two deserve emphasis, because choosing wrongly here is how a job dies slowly. Suppose you want the average view latency per document. The obvious implementation appends every latency to a `ListState` and averages when asked — for a document viewed a million times, a million longs, **8 MB of state for one key**, growing forever. The same answer needs two numbers folded on each write:

```java
ListState<Long> latencies;                   // 8 MB and climbing, for one document
AggregatingState<Long, Double> avgLatency;   // 16 bytes per key, forever: (sum, count)
```

Identical output. Across two hundred million documents, that is the difference between 3 GB of state and a job that cannot checkpoint. **When you have a choice, aggregate on write.**

### Where state lives

**HashMapStateBackend** keeps state as Java objects on the JVM heap. Fastest possible access, and bounded by your heap, with all the garbage collection consequences that implies at scale.

**EmbeddedRocksDBStateBackend** keeps state in an embedded RocksDB instance on local disk, serialized. **RocksDB** is an embedded key-value store — a library, not a server — that Flink runs inside the TaskManager process; it stores sorted key-value pairs in immutable files on disk with an in-memory cache in front, which is why it can hold far more than the heap and why its files can be copied incrementally. Access is slower — you pay serialization and possibly a disk read — but state can be **far larger than memory**, into the terabytes, and it supports **incremental checkpoints**, which §6.4 will show is a large operational advantage.

The choice is decided by one multiplication. Lantern keys by `document_id`: 200 million documents × 300 bytes of state each = **60 GB**, or 3 GB per TaskManager across twenty. Three gigabytes of live Java objects per JVM, permanently, is a heap that spends its life in garbage collection; the same 3 GB in RocksDB is a few files on local disk with a cache in front. So: RocksDB. Had the job keyed by `country` — two hundred keys, a few kilobytes — HashMap would be obvious and RocksDB's serialization pure waste.

**Estimate keys × bytes-per-key before choosing.** Under a gigabyte, use the heap. Above it, use RocksDB. The access cost is real but rarely the bottleneck, and the alternative is a job that works in testing and dies in production when the keyspace grows.

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

If the answer is "nothing bounds it", you have written a job that works for a month.

Here is that job's actual trajectory, which is worth seeing as numbers because the failure is so gradual that nobody notices it as a failure. Lantern's per-document counter keeps 300 bytes for every document ever viewed, and about 400,000 distinct documents are viewed for the first time each week:

| Week | Distinct keys | State | Checkpoint duration |
|---|---|---|---|
| 1 | 0.4 M | 120 MB | 3 s |
| 12 | 4.8 M | 1.4 GB | 34 s |
| 40 | 16 M | 4.8 GB | 140 s |

Nothing ever breaks and no error is logged. Then one afternoon the checkpoint interval is 60 seconds and the checkpoint takes 140, checkpoints overlap and time out, and — because a job that cannot checkpoint cannot recover — the next machine failure replays from a checkpoint hours old. The document viewed once in 2019 is still paying rent every sixty seconds. A seven-day TTL flattens the whole table: the keyspace stabilises at one week of activity, and week 40 looks like week 1.

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

### One failure, traced

Abstractly that is convincing; concretely it is convincing *and* memorable. Follow one document through a crash.

Checkpoint 41 completes at 10:00:00. What was written to S3 is two things that belong together:

```
checkpoint-41/
  sources:  document-views partition 0 → offset 8,300
  state:    doc_88 → 1,000
```

Read that pair carefully, because the whole mechanism is in it: **at the moment `doc_88`'s count was 1,000, the source had consumed exactly 8,300 records.** Neither number means anything alone; together they are a consistent position in the computation.

Processing continues. Offsets 8,301–8,500 are read, thirty of them views of `doc_88`, and the count climbs to 1,030. At 10:00:47 the TaskManager holding `doc_88` is killed, and that 1,030 no longer exists anywhere. Flink restarts and does exactly two things:

```
1. restore state from checkpoint-41   →  doc_88 = 1,000
2. rewind the source to offset 8,300
```

Now offsets 8,301 onward are read a second time. The same thirty views of `doc_88` arrive again, in the same order, and the count walks back up 1,001, 1,002, … 1,030. By the time the job catches up to where it was, the count is 1,030 — **the same number it had before the crash**, not 1,060.

The reason it is not 1,060 is the whole point. Restore the state but resume at offset 8,500 and the thirty views are lost, for 1,000. Rewind to 8,300 while keeping the in-memory count and they are counted twice, for 1,060. Exactly-once is not a promise that a record is delivered once; it is the promise that **state and input position always move together.**

What it does not promise: those thirty records genuinely *were* processed twice, so anything they did outside Flink happened twice. That is what the sink discussion below is about.

### Alignment, and how to avoid paying for it

There is a subtlety in "receives the barrier on all of its inputs". An operator with two inputs may get barrier `n` on the first input while the second is still delivering pre-barrier records. To keep the snapshot exactly at the barrier, it must **buffer** the fast input's post-barrier records until the slow input's barrier arrives. This is **barrier alignment**.

Picture the window operator reading from two upstream subtasks:

```
input A:  ─ e12 ─ e11 ─ ║B42║ ─ e10 ─ e9 ──►   barrier arrived at 10:00:03
input B:  ─────── e8 ── e7 ─── e6 ── e5 ──►   barrier still 4 s upstream
```

`e11` and `e12` arrived *after* barrier 42 on input A. Processing them would put post-barrier records from one input and pre-barrier records from the other into the same snapshot — not a single consistent moment, and on recovery the two inputs would rewind to positions that no longer agree.

So the operator buffers them and processes only input B until barrier 42 arrives there too, at 10:00:07. Those four seconds are **alignment time** — four seconds in which input A is stalled, visible in the UI as `checkpointAlignmentTime`. Under backpressure, where one input may be a minute behind, it is a minute of stall every checkpoint.

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

Flink matches state in a savepoint to operators by their ID. Without explicit UIDs, Flink generates them from the structure of the dataflow graph — so **adding a filter upstream changes the generated IDs of everything after it**, and the savepoint can no longer be restored.

In practice, and it happens at the worst possible moment. Monday's job has no UIDs, so the counter's ID is a hash of the graph's shape, and the savepoint records `{ 0xb4e2… : 500 GB of counts }`. On Friday you add one line — a filter for test accounts — upstream of it:

```
Monday:  source →          window+count → sink      id: 0xb4e2...
Friday:  source → filter → window+count → sink      id: 0x7a91...   ← shape changed, hash changed
```

Restoring from the savepoint, Flink looks for state belonging to `0x7a91…`, finds none, and either fails with `Cannot map checkpoint/savepoint state` or — if you pass `--allowNonRestoredState` to get past the error — starts the counter from **zero**. Three weeks of counts gone because of a one-line filter, and the counter's *code* never changed; only its position in the graph did. With `.uid("view-counter")` set from day one the ID is `view-counter` in both, and the filter is irrelevant.

Set `.uid()` on every stateful operator, from the first line of code, forever. It costs one method call and it is the difference between a job you can evolve and a job you can only restart from empty.

### Exactly-once, end to end

Checkpoints give exactly-once **state**. For exactly-once **output** the sink must participate, and there are two ways.

Why the sink must participate is the traced crash again: those thirty views were reprocessed, so if the operator wrote as it went, thirty writes were issued twice and the sink holds two rows per view. Flink cannot fix that alone — it has no way to un-issue a write.

**An idempotent sink.** Writes are keyed on something deterministic, so a replay overwrites rather than duplicates:

```
INSERT view(doc_88, carol, 10:04:11)        → two rows after a replay
PUT /views/_doc/doc_88:carol:10:04:11       → same _id both times, one row
```

This is Lantern's approach — OpenSearch with `_id = "{document_id}:{chunk_index}"` — and the fourth appearance of the pattern in the book. Note the requirement hiding in it: the ID must come from the *data*, never from a counter, a UUID, or the clock, because a replay must regenerate the identical value.

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

### Watching it work, record by record

This is the part to slow down on. Below is a real stream of nine view events arriving at Lantern's job, out of order as real streams are, with a watermark generator set to a bounded out-of-orderness of **30 seconds** — so `watermark = (highest event time seen) − 0:30`. The window is the five-minute tumbling window `10:00–10:05`.

Read the table one row at a time. "Max seen" is the highest event time encountered so far; the watermark trails it by thirty seconds.

| Arrives | Event time | Max seen | Watermark | What happens |
|---|---|---|---|---|
| 1 | 10:01:10 | 10:01:10 | 10:00:40 | window 10:00–10:05 created, count = 1 |
| 2 | 10:03:05 | 10:03:05 | 10:02:35 | count = 2 |
| 3 | **10:02:40** | 10:03:05 | 10:02:35 | *out of order — 25 s behind #2.* Still ≥ watermark, so it counts. count = 3 |
| 4 | 10:04:55 | 10:04:55 | 10:04:25 | count = 4 |
| 5 | 10:05:10 | 10:05:10 | 10:04:40 | belongs to the **next** window (10:05–10:10). First window still open — watermark is 10:04:40, not yet past 10:05 |
| 6 | **10:04:58** | 10:05:10 | 10:04:40 | 12 s out of order, and still ≥ watermark. Counts in the first window. count = 5 |
| 7 | 10:05:25 | 10:05:25 | 10:04:55 | next window. First window *still* open |
| 8 | 10:05:31 | 10:05:31 | **10:05:01** | **watermark passes 10:05 → window 10:00–10:05 fires, emitting count = 5, and its state is freed** |
| 9 | **10:04:20** | 10:05:31 | 10:05:01 | arrives after its window fired. Event time 10:04:20 < watermark 10:05:01: this record is **late** |

Four things in that trace are worth naming.

**Rows 3 and 6 are why watermarks exist.** Both arrived out of order, one by 25 seconds. With no grace at all — firing the moment a record stamped past 10:05 appeared, which is record 5 — the count would be 4 and record 6 would never have been seen.

**Row 8 is the firing rule.** The window did not fire because a clock struck 10:05; it fired because a *record* stamped 10:05:31 dragged the watermark past the window's end. Watermarks advance on data, not on time — so **if no more records arrive, the window never fires.** A stream that goes quiet at 10:04:59 leaves it open indefinitely, holding state and emitting nothing. That is the promise working as written, and the seed of the trap two subsections below.

**Row 9 is the cost of the promise.** The generator asserted "nothing before 10:05:01 will arrive" and something did. It will sometimes be wrong whatever number you pick, because §6.1 established no duration is both correct and prompt; what you choose is how often, and what happens then.

**And the table is reproducible.** Re-run this input tomorrow on a differently-loaded cluster and every row is identical, because nothing here consulted the wall clock — the property §6.1 said processing time could not give you.
### Generating them

The standard generator says: assume events may be out of order by up to a bounded amount.

```java
WatermarkStrategy
    .<View>forBoundedOutOfOrderness(Duration.ofSeconds(30))
    .withTimestampAssigner((view, ts) -> view.getEventTime())
    .withIdleness(Duration.ofMinutes(1));
```

This emits `watermark = (highest event time seen so far) − 30 seconds`, which is exactly the rule the trace above followed.

That thirty seconds is the dial, and it is the most consequential number in a Flink job:

**Smaller** → windows fire sooner, results are fresher, and more genuinely late events arrive after their window closed and get dropped.
**Larger** → more events are captured, and **every window result waits that much longer**, permanently, for everyone.

Take the same nine events and turn the dial, to see that this is not a small effect:

| Out-of-orderness | Window fires when | Result | Records lost |
|---|---|---|---|
| **0 s** | record 5 arrives (10:05:10) | count = **4** | records 6 and 9 — a third of the window, silently |
| **30 s** | record 8 arrives (10:05:31) | count = **5** | record 9 |
| **5 min** | a record stamped ≥ 10:10 arrives | count = **6** | none |

Three different answers to "how many people viewed this document between 10:00 and 10:05", from identical input, differing only in one configured duration. There is no setting that is simply correct — the 0-second job is fast and wrong, the 5-minute job is right and five minutes stale — and pretending otherwise is how teams end up with a dashboard nobody can reconcile.

Choose it from data rather than intuition: measure `kafka_ts − event_time` across a sample of your actual stream and pick something past the p99. For Lantern that distribution is p50 = 400 ms, p99 = 6 s, p999 = 22 s, max = 4 min (one laptop that had been asleep). Thirty seconds sits above p999 and below the pathological tail — cheap, because a panel 30 seconds behind is invisible to a user, and the 4-minute laptop is deliberately abandoned rather than delaying *every* window by four minutes to rescue one event. A pipeline fed by mobile clients would see p99 in the minutes and would have to accept ten-minute-old dashboards as the price.

### The trap that catches everyone

Read this even if you skim the rest of the chapter.

An operator has many input subtasks — one per Kafka partition, in the common case. Each generates its own watermark. And the operator's watermark is **the minimum across all of its inputs**, because it can only promise what its *least* advanced input can promise.

Which means: **one idle or lagging input holds back the watermark for the entire job.**

Suppose Lantern has twenty-four partitions and one of them receives no events, because a `keyBy` upstream distributed unevenly, or because that partition's producer is down, or simply because it is three in the morning and traffic is low.

```
partition 0   busy    watermark 10:42:15  ┐
partition 1   busy    watermark 10:42:09  │
partition 2   busy    watermark 10:42:18  │  min = 09:15:02
   ...                                    │
partition 17  SILENT  watermark 09:15:02  ┘  ← last record at 09:15:32
   ...
partition 23  busy    watermark 10:42:11
```

Twenty-three partitions are convinced it is 10:42. One last saw a record at 09:15 and has had nothing to raise its watermark since. The operator can only promise what its least advanced input promises, so the job's watermark is **09:15:02** — ninety minutes in the past. Every window since then sits open, accumulating state, waiting for a watermark that will never come. **No window anywhere in the job ever fires.**

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

The three options on the same late record — event 9 from the trace above, `doc_88` stamped 10:04:20, arriving after the window fired with count = 5:

| Choice | What the consumer of the trending panel sees |
|---|---|
| `allowedLateness(5 min)` | the window emits `5`, then a moment later emits `6` for the same window — a correction the dashboard must know how to apply |
| side output | the window's answer stays `5`; the record appears on a `late-views` stream, where a counter increments |
| drop (default) | the window's answer stays `5`; the record is gone and nothing anywhere records that it existed |

My recommendation: **always take the side output, even if you only count what lands in it.** A metric named `late_events_dropped` is nearly free and turns silent data loss into a number on a dashboard. Without it, you have no idea whether your thirty-second watermark delay is discarding 0.01% of events or 4% — and the difference matters enormously to whoever consumes the output. At Lantern's two million views a day, 0.01% is 200 records nobody will ever miss; 4% is 80,000, which is the difference between a trending panel and a wrong trending panel, and the two situations look *identical* from outside. Silently dropping data is how a pipeline loses the trust of the people who depend on it, and once lost that is very hard to win back.

## 6.6 Windows and joins

### Windows

Windows chop an infinite stream into finite pieces you can aggregate.

**Tumbling** — fixed-size, non-overlapping, every event in exactly one.

```
[──10:00–10:05──][──10:05–10:10──][──10:10–10:15──]
```
`TumblingEventTimeWindows.of(Time.minutes(5))` — Lantern's trending panel.

**Sliding** — fixed-size with a shorter slide, so they overlap and each event belongs to `size / slide` of them.

```
[──10:00–10:05──]
      [──10:01–10:06──]
            [──10:02–10:07──]
```
`SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1))` — a five-minute moving average refreshed every minute. Note the arithmetic before you deploy: size over slide is five, so **five times the state and five times the output** of the equivalent tumbling window. A one-hour window sliding every second is 3 600 copies of everything, and people do write that by accident.

**Session** — defined by inactivity rather than the clock: events group together until a gap of more than N minutes. `EventTimeSessionWindows.withGap(Time.minutes(30))`. Variable length, excellent for modelling user behaviour, and the hardest kind to bound in state since a session has no predetermined end.

**Global** — everything in one window forever, with a custom trigger deciding when to fire. For count- or condition-based windowing.

### The same events, four window types

The descriptions blur together until you see one input produce four different outputs. Here are five views of `doc_88` by one user:

```
 10:01   10:02        10:07                    10:44   10:46
   │       │            │                        │       │
   ▼       ▼            ▼                        ▼       ▼
   e1      e2           e3                       e4      e5
```

| | Windows emitted | Counts |
|---|---|---|
| **Tumbling** 5 min | `10:00–10:05`, `10:05–10:10`, `10:40–10:45`, `10:45–10:50` | 2, 1, 1, 1 |
| **Sliding** 5 min / 1 min | ~20 overlapping windows from `09:57–10:02` onward | e1 alone appears in five of them |
| **Session** 30 min gap | `10:01–10:07`, `10:44–10:46` | 3, 2 |
| **Global**, count trigger of 3 | fires once, at e3 | 3 |

Four correct answers — 4 rows, ~20 rows, 2 rows, 1 row — from identical input, and each is worth a sentence.

**Tumbling** counts every event exactly once, so the counts sum to 5. This is the trending panel.

**Sliding** counts every event *five* times, in five different rows, because `size / slide` = 5. That is right for a moving average and catastrophic if someone sums the column believing it is a total.

**Session** ignores the clock entirely: the boundaries came from the 37-minute silence between e3 and e4. Its state cannot be released until the gap has provably elapsed, which is why sessions are the hardest kind to bound. And they *merge* — a late record at 10:20 falls within 30 minutes of both, so Flink combines them into one session `10:01–10:46` with 6 events and retracts the two rows it already emitted.

**Global** ignores time completely; e4 and e5 sit in state until a sixth event arrives, whether that is in a minute or in a year.

**The window type is a modelling decision about what the question means**, not a performance setting, and choosing it by copying the nearest example is how a number ends up meaning something other than what its column header says.

### Window functions, and one that will hurt you

`reduce` and `aggregate` are **incremental**: each arriving record updates a single accumulator. State per window is one value. Cheap. Use these by default.

`process(ProcessWindowFunction)` receives **every buffered record** when the window fires, along with window metadata. Powerful — you can compute a median, or inspect the window's key and boundaries — and it **buffers every record in state until the window closes.**

The difference, for one popular document in one five-minute window receiving 50,000 views:

| | State held while the window is open | Why |
|---|---|---|
| `aggregate` | **8 bytes** | one running count, updated per record |
| `process` | **~10 MB** | all 50,000 view records, kept until firing |

Multiply the second row by every document open in every window at once and a one-hour window over a high-volume stream is a memory problem waiting to happen. The rule: reach for `process` only when the computation genuinely needs every record — a median or a p95 does, a count, sum, min, max or average does not.

The good news is that you can have both:

```java
.aggregate(new IncrementalCount(), new AddWindowMetadata())
```

The first function accumulates incrementally; the second runs once at firing time with access to window metadata and the accumulated result. Incremental state, full context. This is the pattern you want whenever you need to know *which* window you are emitting.

### Triggers and lateness

A **trigger** decides when a window fires; the default fires when the watermark passes the window end. A custom trigger can fire **early** — emitting a speculative result every ten seconds while the window is still open, then a final one when it closes.

For the 10:00–10:05 window, the default trigger emits one row, at 10:05:31 when the watermark crosses:

```
10:05:31   (10:00-10:05, doc_88, 5)   final
```

An early trigger firing every minute emits six rows for the same window — `1, 1, 3, 3, 4` as speculative results at 10:01 through 10:05, then `5` as the final one at 10:05:31.

The panel now updates every minute instead of once at the end, at the cost of showing 3 when the answer turns out to be 5. This is a genuinely good pattern for dashboards — low latency *and* eventual correctness — provided the consumer treats each row as a replacement for the last rather than something to add up. Give the output a `window_start` key and an upsert sink and that happens naturally.

`allowedLateness(Time.minutes(5))` keeps the window alive after firing, as §6.5 described — so record 9, the late one, would produce a seventh row at its arrival: `(10:00-10:05, doc_88, 6)`.

### Joins

Joining streams is harder than joining tables, because a stream has no end and so you never know whether a match is still coming. Flink offers several shapes.

**Window join** — join two streams within a shared window. Both sides must land in the same window, which makes boundary behaviour awkward, as below.

**Interval join** — for each record on the left, join records on the right whose timestamps fall within `[t − lower, t + upper]`. This is the natural formulation for most real problems: "join each click to impressions from the preceding ten minutes". Much easier to reason about than a window join.

Why it is easier shows up as a failure of the window join. Lantern wants to attribute each click to the search that produced it — a search at 10:04:58, a click five seconds later at 10:05:03. On five-minute tumbling windows, the search lands in `10:00–10:05` and the click in `10:05–10:10`, so **they do not join**: the click is attributed to nothing and the search looks like it produced no result. The pair was five seconds apart and a window boundary happened to fall between them, which befalls roughly one search in sixty purely as a function of where the clock's grid lands.

The interval join has no grid:

```java
searches.keyBy(s -> s.searchId)
    .intervalJoin(clicks.keyBy(c -> c.searchId))
    .between(Time.seconds(0), Time.minutes(10))   // clicks 0–10 min after the search
    .process(new AttributeClick());
```

The click is 5 seconds after the search, inside `[search + 0s, search + 10min]`, so they join. The condition is relative to each record rather than to a clock, which is what the business rule actually said — and the interval is also what bounds the state, since Flink can drop a search once the watermark passes its timestamp plus ten minutes.

**Temporal join** — and this one deserves attention, because it solves a problem §4.7 raised and left open.

Recall the backfill hazard: enriching an event by joining against a dimension table that has since changed produces different answers on re-run. A temporal join fixes it by joining against the **version of the dimension as of the event's own event time**.

Concretely: `doc_88` belonged to Support until it transferred to Engineering on 1 April, and a view of it happened on **15 March**. Which team gets credit?

| Join type | Run in February | Re-run today |
|---|---|---|
| ordinary join against current state | Support | **Engineering** |
| temporal join on event time | Support | **Support** |

The first row is the hazard in one line: same job, same input, two answers, and nothing in the code changed — only the world did. Last quarter's report stops being reproducible, and there is nothing to debug, because nothing was wrong.

```sql
SELECT v.doc_id, v.event_time, d.owner_team
FROM document_views v
JOIN documents FOR SYSTEM_TIME AS OF v.event_time AS d ON v.doc_id = d.doc_id;
```

Flink keeps the versioned dimension in state, keyed and time-indexed, and looks up the version whose validity range contains `v.event_time` — 15 March, therefore Support, today and forever.

This is point-in-time correctness as a primitive, and it makes reprocessing deterministic. If you have ever had a backfill produce different numbers than the original run for reasons nobody could explain, this is very often the reason.

**`connect` and `CoProcessFunction`** — two streams of different types sharing state. The canonical use is a control stream: a low-volume stream of rules writes into state that a high-volume event stream reads, with **broadcast state** when every subtask needs the full rule set rather than a partition of it. This is how you change a job's behaviour without redeploying it.

**Async I/O** — deserves a specific warning. If your job enriches each record by calling an external service, a synchronous call inside `processElement` blocks the operator thread for the whole round trip. At ten milliseconds per call one subtask manages a hundred records per second, and your carefully parallelised job is limited by network latency rather than by anything computational.

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

Which emits, as each window closes, one row per document per window:

```
window_start          document_id   views   unique_viewers
2026-09-19 10:00:00   doc_88        5       4
2026-09-19 10:00:00   doc_12        1       1
   ... (nothing for the 10:05 window until the watermark passes 10:10)
```

That is Lantern's trending panel, complete, and every mechanism in this chapter is present in it without being written by hand. `WATERMARK FOR event_time AS event_time - INTERVAL '30' SECOND` is the §6.5 generator. `TUMBLE` is the §6.6 window. `GROUP BY window_start, document_id` is the `keyBy` and the keyed state. Checkpointing, recovery, and state cleanup are the planner's problem. This replaces several hundred lines of DataStream code, and the optimiser is good.

Three concepts you need in order to read streaming SQL without being confused.

**Dynamic tables.** A stream *is* a table that keeps changing, and a query over it produces another table that keeps changing. The duality is exact, and it is what makes SQL sensible here at all.

**Append-only versus changelog streams.** This is the one that trips people up. A *windowed* aggregation is append-only: once the window closes its result is final, so each row is emitted once. But a `GROUP BY` **without** a window — "total views per document, ever" — produces a result that changes with every new record. So Flink emits a **changelog**: a retraction of the old row followed by an insertion of the new one.

Side by side, for three views of `doc_88`. The windowed query emits one row, once: `+I (10:00-10:05, doc_88, 3)`. Drop the window and the same three views emit five:

```
+I (doc_88, 1)   -U (doc_88, 1)   +U (doc_88, 2)   -U (doc_88, 2)   +U (doc_88, 3)
```

Five rows to say "the count is 3", because a consumer that saw only `+U (doc_88, 3)` without the retraction of `2` would double-count if it were summing. A million views produce two million rows.

The practical consequence is that your **sink must support upserts** — `upsert-kafka`, a JDBC upsert, OpenSearch keyed by document ID. A plain append-only sink will reject a changelog stream with:

```
Table sink 'default_catalog.default_database.trending' doesn't support
consuming update changes which is produced by node GroupAggregate(...)
```

which is accurate and does not obviously mean "you forgot the window" — which is what it means about ninety percent of the time.

**Unbounded aggregation state grows forever.** A non-windowed `GROUP BY document_id` keeps state for every document ID ever seen. This is §6.3's unbounded state problem arriving through SQL, where it is easier to write by accident. Bound it with `table.exec.state.ttl`.

My recommendation: **express what you can in SQL, and drop to DataStream or ProcessFunction only for logic that genuinely doesn't fit.** Less code, fewer bugs, and a planner that will often do better than you would by hand.

## 6.8 Operating it

### How a job actually gets deployed

Worth stating plainly, because the chapter has described what a job *is* without saying how it comes to be running. You compile it into a JAR (or a Python file, or a SQL script) and submit it:

```
flink run -d -c com.lantern.TrendingJob trending.jar
```

The JobManager receives the dataflow, asks the cluster manager for TaskManagers, places the subtasks in slots, and starts them. From then on the job runs until it fails or you stop it. `flink stop --savepointPath ...` stops it *and* takes a savepoint, which is how you deploy a new version: stop with a savepoint, submit the new JAR with `--fromSavepoint`, and the state carries over (§6.4).

There are two deployment shapes and the difference matters operationally. In **session mode** one long-lived cluster hosts many jobs, which is cheap and convenient and means one job's failure can disturb its neighbours. In **application mode** each job gets its own cluster, dedicated and isolated, which is what you want for anything in production. On Kubernetes the Flink Operator does this for you, and a job becomes a custom resource you deploy like any other workload.

Flink SQL has a third route: a SQL script submitted through the SQL client or a gateway, which the planner compiles into exactly the same dataflow of operators. Nothing below the SQL is different.

### The metrics that matter

**Backpressure and busy time per operator**, in the web UI. Start here, always. It names the bottleneck.

**Watermark lag** — `currentEmitEventTimeLag` — how far behind real time the job's notion of time has fallen. This is the streaming equivalent of consumer lag, expressed in the units that matter for correctness. A healthy Lantern job sits at roughly 30 seconds, because that is the configured out-of-orderness and nothing more. Seeing 1,200 seconds means the job's clock is twenty minutes behind the world's, so the 10:40 window will not fire until 11:00 — and seeing it climb steadily rather than hold flat is the signal that the job will never catch up on its own.

**Checkpoint duration, size, and failure count.** As §6.4 said, duration creeping up over weeks is the best early warning in the system — 3 s in week 1, 11 s in week 4, 34 s in week 12 is a job with no TTL and about six months to live, and each individual reading looks fine.

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

On skew: the fix worth knowing is **two-phase aggregation**. Lantern's onboarding handbook is read by every new employee and accounts for a third of all views, so `keyBy(document_id)` sends 16,000 records/s to one subtask while its neighbours handle 400. More parallelism does not help, because one key cannot be split across subtasks — so split it artificially:

```java
.keyBy(v -> v.documentId + "#" + rnd.nextInt(100))   // phase 1: 100 partials, spread wide
.window(...).aggregate(new Count())
.keyBy(p -> p.documentId)                            // phase 2: combine the 100
.window(...).aggregate(new Sum())
```

The hot key's 16,000/s becomes a hundred streams of 160/s, and phase two handles a hundred records per window instead of sixteen thousand per second. One serial hot key traded for two cheap parallel stages. The same trick appears in Spark under the name salting (§7.1).

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
