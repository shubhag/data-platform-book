# Chapter 6 — Time

A streaming job has to decide when a time window is "done", even though delayed and out-of-order events keep arriving after the clock has moved on. It also has to remember things across billions of records and survive machine failures without losing or double-counting any of them. This chapter shows how Apache Flink handles both, using state, checkpoints, and watermarks.

**By the end you'll be able to:**

- Read a Flink job as a graph of operators and spot its expensive shuffles.
- Choose a state type and backend, and say what bounds every piece of state.
- Trace how a checkpoint lets a job recover from a crash without losing or double-counting.
- Set a watermark delay from measured data, and diagnose a stuck watermark.
- Pick the right window, join, and sink for a streaming question, in SQL where possible.

## 6.1 The question that has no good answer

Lantern's product team wants a "trending documents" panel: for each document, how many people viewed it in each five-minute window. A `document-views` Kafka topic carries a document ID and a timestamp per view. The computation is a `GROUP BY document_id, window`, which looks like the simplest aggregation in existence.

Here is what makes it hard. It is 10:05:00, and a view stamped 10:04:58 arrives. Should you emit the 10:00–10:05 count now? Consider what is still in flight:

- a phone that was in a lift, holding thirty events from 10:02, which it will upload in ninety seconds;
- a Kafka consumer running forty seconds behind after a garbage-collection pause;
- a failed produce from 10:03, about to be retried;
- a client whose clock is ninety seconds slow, stamping its 10:04 events as 10:02.

Emit now and the count is too low, and it stays wrong as stragglers arrive. Ten seconds of waiting catches most of them, a minute nearly all, and an hour catches the phone that was switched off. **There is no duration that is both correct and prompt.**

Counting by wall clock ("events I processed between 10:00 and 10:05") doesn't escape the problem. The phone's 10:02 events land in the 10:05–10:10 bucket, and re-running over identical input gives a **different answer** because consumer lag differed. That is not a metric. It is network conditions with a number attached.

Every streaming system has to answer this. Flink answers it best, which is why this chapter is called Time rather than Flink.

## 6.2 What Flink is

**Flink** is a distributed stream processor. It processes records **one at a time** as they arrive, with sub-second latency, holds large fault-tolerant state, and has a first-class model of event time. Spark Streaming is *batch, repeated very quickly* (Chapter 7); Flink is *streaming, with batch as a stream that happens to end.*

### Operators and jobs

The trending panel, as steps: (1) read views from Kafka, (2) drop test accounts, (3) group by document ID, (4) count per document per five-minute window, (5) write counts to OpenSearch.

Each step is an **operator**: one processing step that takes records in, does one thing, and passes records out. Think `map` and `filter` over a list that never ends. A **source** pulls records in from outside (step 1), a **sink** pushes them out (step 5), and everything between is a **transformation**.

Two transformations need a word now. **`keyBy(document_id)`** computes nothing; it declares "from here on, records are grouped by document", so the next operator can keep a count per document (§6.3). **`window`** slices each group into five-minute buckets (§6.5).

A **job** is the operators wired into a graph, a **dataflow**, which runs until you stop it:

```
  Kafka source ──▶ filter ──▶ keyBy ──▶ window+count ──▶ OpenSearch sink
```

### From one box to many machines

Each operator runs as several identical copies. The number of copies is its **parallelism**, and each copy is a **subtask**. It is set per operator: say twenty-four for the source (one per Kafka partition), eight for the window, and four for the sink.

```
   source-1 ──▶ filter-1 ──┐        ┌──▶ window-1 ──▶ sink-1
   source-2 ──▶ filter-2 ──┼─keyBy──┼──▶ window-2 ──▶ sink-2
   source-3 ──▶ filter-3 ──┘        └──▶ window-3 ──▶ sink-3
```

Each window subtask must see *all* records for the documents it owns, so at `keyBy` records cross between subtasks. That crossing is where the cost is.

The cluster runs two kinds of process:

```
        JobManager  (schedules, coordinates checkpoints, restarts; HA via ZooKeeper/K8s)
            │
   ┌────────┼────────┐
TaskMgr  TaskMgr  TaskMgr   (workers: slots + state)
```

The **JobManager** is the coordinator. It places subtasks, triggers checkpoints (§6.4), and restarts things after failure, and it is made highly available because otherwise it is a single point of failure. **TaskManagers** are the workers: one JVM per machine, each offering a fixed number of **task slots** (a share of its memory and threads). Twenty-four subtasks need twenty-four slots, so with twenty the job won't start.

Consecutive subtasks that need no network hop are **fused** into one slot, so `source-1 → filter-1` becomes plain method calls with no serialization. That's why the UI often shows fewer boxes than you wrote.

### Forwarding versus redistribution

Spark calls this split narrow versus wide (§7.1).

- **Forwarding** keeps the record in the same slot. It is free.
- **Redistribution** serializes the record and sends it to the subtask owning its key (`keyBy`, `rebalance`). It is nearly all the cost, so four `keyBy`s in a row means four network shuffles you may not have intended.

### Backpressure, handled properly

This is one of Flink's real advantages, and §1.6 promised you'd care. If the OpenSearch sink slows, its input buffers fill and its upstream operator blocks. Then that operator's buffers fill, and so on back to the Kafka source, **which simply stops fetching.** If the source can read 50,000/s and OpenSearch drops to 8,000/s, every stage settles at 8,000/s:

```
Kafka source ──▶ filter ──▶ window ──▶ OpenSearch sink
   8,000/s        8,000/s    8,000/s      8,000/s
```

Nothing is dropped, nothing buffers without bound, and nothing needed configuring. Consumer lag grows, and that is correct, because the unread records sit durably in Kafka instead of a doomed JVM heap. Without end-to-end backpressure, an internal queue grows until the process dies with it. Spark's answer (Chapter 7) is a manual rate limit.

It also gives you the best diagnostic in the product. The web UI shows **backpressure per operator**, and the bottleneck is the busy operator with everything upstream blocked. That is usually the whole investigation.

### Four ways to write a job

From most declarative to most manual:

- **Flink SQL**: SQL over streams. Start here (§6.7).
- **Table API**: the same engine as method chains.
- **DataStream API**: operator by operator, `views.keyBy(...).window(...).aggregate(...)`, for full control.
- **ProcessFunction**: raw state, timers, and side outputs, for when nothing built-in fits.

One job for ProcessFunction: "alert the first time someone outside a document's team views it, then stay quiet for 24 hours". No window expresses that, but a `KeyedProcessFunction` with a `ValueState` and a timer does it in twenty lines. All four APIs compile to the same operator graph.

## 6.3 State

A **stateless** operation looks at one record and forgets it. A **stateful** operation remembers something across records, such as a count, a join buffer, or a set of IDs already seen.

"Drop test accounts" is stateless, because the record in your hand is all you need. "Count views per document" is stateful: emitting `11` for `doc_88` means knowing ten came before, and that knowledge dies with the machine. Every interesting streaming job is the second kind.

### Keyed state

After `keyBy(document_id)`, each subtask owns a disjoint set of keys, and state is scoped **per key** automatically. You write single-threaded code over one variable, with no locks. Here is a **`KeyedProcessFunction`**: `open()` registers state once, and `processElement()` runs per record.

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

`count.value()` returns the count **for the current record's key**, because Flink swaps the state context on every record:

| # | record | `count.value()` reads | writes | emits |
|---|---|---|---|---|
| 1 | `doc_88, alice` | `null` → 0 | `doc_88 → 1` | 1 |
| 2 | `doc_12, bob` | `null` → 0 | `doc_12 → 1` | 1 |
| 3 | `doc_88, carol` | **1** | `doc_88 → 2` | 2 |
| 4 | `doc_40, dave` | `null` → 0 | `doc_40 → 1` | 1 |
| 5 | `doc_88, erin` | **2** | `doc_88 → 3` | 3 |

One Java field holds three independent counters. And because `keyBy` sends every `doc_88` record here, the count is complete rather than partial. You wrote code for one document and got a system for two hundred million.

There is also **operator state**, scoped per subtask rather than per key. It lives mostly inside connectors (a Kafka source's offsets), and you will rarely write it.

### The primitives

| Primitive | Holds, per key | Lantern would use it for |
|---|---|---|
| `ValueState<T>` | one value | a document's view count |
| `ListState<T>` | a list | views awaiting a join partner |
| `MapState<K,V>` | a map | views per country: `{"FR": 12, "DE": 3}` |
| `ReducingState<T>` | one value, folded on write | the count, folded with `(a,b) -> a+b` |
| `AggregatingState<I,O>` | one accumulator, folded on write | a HyperLogLog sketch of unique viewers |

The wrong pick kills a job slowly. For average view latency, appending every latency to a `ListState` costs **8 MB for one key** after a million views, growing forever. Folding on write needs two numbers:

```java
ListState<Long> latencies;                   // 8 MB and climbing, for one document
AggregatingState<Long, Double> avgLatency;   // 16 bytes per key, forever: (sum, count)
```

Across two hundred million documents, that is 3 GB of state versus a job that cannot checkpoint. **When you have a choice, aggregate on write.**

### Where state lives

- **HashMapStateBackend** keeps Java objects on the heap. It is fastest, but bounded by the heap, with garbage-collection pain at scale.
- **EmbeddedRocksDBStateBackend** keeps serialized state on local disk. It is slower (serialization, disk reads) but holds terabytes and supports **incremental checkpoints**.

**RocksDB** is an embedded key-value store, a library running inside the TaskManager rather than a server. It keeps sorted key-value pairs in immutable disk files behind an in-memory cache, which is why it can outgrow the heap and copy its files incrementally.

One multiplication decides. Lantern: 200 million documents × 300 bytes = **60 GB**, or 3 GB per TaskManager across twenty. That much live Java per JVM means a heap that lives in garbage collection, so the answer is RocksDB. Keyed by `country` (a few kilobytes), the heap is obvious. **Under a gigabyte of keys × bytes, use the heap; above it, RocksDB.** Either way, checkpoints go to durable storage like S3 or HDFS.

### What bounds it?

Unbounded state is the most common way to kill a long-running job:

> **For every piece of state you create, be able to say what bounds it.**

There are three acceptable answers: a **window** that fires and clears, a **timer** that deletes it, or a **time-to-live**:

```java
StateTtlConfig ttl = StateTtlConfig.newBuilder(Time.days(7))
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
    .cleanupInRocksdbCompactFilter(1000)
    .build();
descriptor.enableTimeToLive(ttl);
```

Without one, you have a job that works for a month. Lantern's counter keeps 300 bytes per document ever viewed, with about 400,000 new documents a week:

| Week | Keys | State | Checkpoint |
|---|---|---|---|
| 1 | 0.4 M | 120 MB | 3 s |
| 12 | 4.8 M | 1.4 GB | 34 s |
| 40 | 16 M | 4.8 GB | 140 s |

No error is ever logged. Then a 140-second checkpoint stops fitting a 60-second interval, checkpoints time out, and the next failure replays from hours ago. The document viewed once in 2019 is still paying rent every sixty seconds. A seven-day TTL caps the keyspace at a week of activity, so week 40 looks like week 1.

## 6.4 Checkpoints

A job has run for three weeks with 500 GB of state across forty TaskManagers, and one machine dies. You can't reprocess three weeks, can't restart from nothing, and can't restart just the dead subtask, because the other thirty-nine have moved on. You also must not double-count in-flight records.

You need a **globally consistent snapshot**: every operator's state at one logical moment, plus the matching input positions. Stopping the world to take one would pause a busy job for minutes. Flink uses **asynchronous barrier snapshotting** instead, a descendant of the 1985 Chandy–Lamport algorithm.

### Barriers

The JobManager injects a **checkpoint barrier** into each source stream, and it flows downstream *in order with the data*:

```
stream:  ...  e7  e6  ║BARRIER n║  e5  e4  e3  ...  ──►
```

When an operator has barrier `n` on all inputs, it snapshots its state **asynchronously** (writing to S3 while processing continues) and forwards the barrier. When every operator has acknowledged, checkpoint `n` is **complete**. Each snapshot covers exactly the records before the barrier, so forty machines agree without any of them stopping: the barrier, not a clock, defines the moment.

**Recovery** means restoring every operator from the last completed checkpoint and **rewinding the sources** to its recorded offsets.

### One failure, traced

Checkpoint 41 completes at 10:00:00 and stores two numbers that belong together:

```
checkpoint-41/
  sources:  document-views partition 0 → offset 8,300
  state:    doc_88 → 1,000
```

**When `doc_88`'s count was 1,000, the source had read exactly 8,300 records.** Offsets 8,301–8,500 then bring thirty more `doc_88` views, taking it to 1,030. At 10:00:47 that TaskManager dies. Flink restarts and does two things:

```
1. restore state from checkpoint-41   →  doc_88 = 1,000
2. rewind the source to offset 8,300
```

The same thirty views replay, and the count climbs back to 1,030. Getting either half wrong breaks it:

| Recovery | Result |
|---|---|
| restore state, resume at 8,500 | 1,000 (thirty lost) |
| keep in-memory count, rewind to 8,300 | 1,060 (thirty doubled) |
| restore state **and** rewind to 8,300 | 1,030 |

Exactly-once doesn't mean each record is delivered once. It means **state and input position always move together.** Those thirty records really were processed twice, so anything they did *outside* Flink happened twice. That is the sink's problem, covered below.

### Alignment, and how to avoid paying for it

An operator with two inputs may get barrier `n` on one while the other still carries pre-barrier records. To snapshot exactly at the barrier, it **buffers** the fast input until the slow input's barrier arrives. This is **barrier alignment**.

```
input A:  ─ e12 ─ e11 ─ ║B42║ ─ e10 ─ e9 ──►   barrier arrived at 10:00:03
input B:  ─────── e8 ── e7 ─── e6 ── e5 ──►   barrier still 4 s upstream
```

Processing `e11` now would mix post-barrier A with pre-barrier B, and on recovery the two inputs would rewind to positions that disagree. So input A stalls for four seconds of **alignment time** (`checkpointAlignmentTime` in the UI). Under backpressure, that can be a minute per checkpoint, which is a common reason checkpoints start failing. There are two escapes:

| Escape | How | Cost |
|---|---|---|
| **`AT_LEAST_ONCE` mode** | skip alignment | some records processed twice on recovery; fine with an idempotent sink |
| **Unaligned checkpoints** | barriers overtake buffered records, which go into the checkpoint | bigger snapshots; usually the right fix for slow checkpoints |

### Configuring it

```java
env.enableCheckpointing(60_000, CheckpointingMode.EXACTLY_ONCE);
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30_000);
env.getCheckpointConfig().setCheckpointTimeout(600_000);
env.getCheckpointConfig().enableExternalizedCheckpoints(RETAIN_ON_CANCELLATION);
```

The interval trades **recovery time against overhead**, and one to five minutes is typical. **If checkpoint duration approaches the interval, the job is in trouble.** The cause is almost always backpressure (fix the bottleneck) or oversized state (fix the TTL). Duration creeping up over weeks is Flink's clearest early warning, so alert on it.

RocksDB adds **incremental checkpoints**, which ship only the SST files (its immutable data files) changed since the last one. On 500 GB of state with a few gigabytes changing per minute, that separates a viable job from an impossible one.

### Savepoints, which are for you rather than for failures

A **savepoint** is a manually triggered, self-contained snapshot. Checkpoints are the system's private recovery mechanism. Savepoints are for deliberate human work: upgrading code with state intact, **rescaling** (twenty subtasks to forty, with keyed state redistributed), migrating clusters, running two versions side by side, and rolling back a bad deploy.

> **Give every operator an explicit, stable UID.**

```java
stream.keyBy(...).process(new ViewCounter()).uid("view-counter");
```

Flink matches savepoint state to operators by ID. Without explicit UIDs it hashes the graph's shape, so **adding a filter upstream changes every downstream ID**:

```
Monday:  source →          window+count → sink      id: 0xb4e2...
Friday:  source → filter → window+count → sink      id: 0x7a91...
```

Friday's job finds no state for `0x7a91…`. It either fails with `Cannot map checkpoint/savepoint state` or, with `--allowNonRestoredState`, starts the counter from **zero**. Three weeks of counts are lost to a one-line filter, and the counter's code never changed. Set `.uid()` on every stateful operator from day one.

### Exactly-once, end to end

Checkpoints give exactly-once **state**. Exactly-once **output** needs the sink, because in the traced crash thirty writes went out twice and Flink cannot un-issue a write.

**An idempotent sink** keys writes deterministically, so a replay overwrites:

```
INSERT view(doc_88, carol, 10:04:11)        → two rows after a replay
PUT /views/_doc/doc_88:carol:10:04:11       → same _id both times, one row
```

This is Lantern's approach, OpenSearch with `_id = "{document_id}:{chunk_index}"`, and the fourth time the pattern appears in the book. The ID must come from the *data*, never from a counter, UUID, or clock, because a replay must regenerate it exactly.

**A transactional sink** uses two-phase commit. It writes into a transaction, **pre-commits** as the barrier passes (a durable promise it can commit), and **commits when the checkpoint completes**. Kafka's transactional producer and Flink's file sinks work this way. The checkpoint is the coordinator, and its state is itself checkpointed, so §1.7's objection to two-phase commit goes away: a coordinator failure becomes recoverable instead of a permanent block.

> **With a transactional sink, consumers see data only at checkpoint boundaries.** A five-minute interval means five-minute output latency.

If that is too slow, shorten the interval and pay the overhead, or use an idempotent sink with at-least-once processing.

## 6.5 Watermarks

Now we can answer §6.1.

### Three kinds of time

**Event time** is when it happened, as recorded in the event. **Ingestion time** is when it entered Flink, and **processing time** is when an operator handled it. Processing time is tempting because nothing waits, but as §6.1 showed, replays give different answers. Use **event time** for anything anyone will report on. The narrow exceptions are rate limiters and alerts that really mean "the last minute of wall-clock time".

### The mechanism

A **watermark** is a marker flowing with the records, carrying a timestamp `T` and an assertion:

> "I believe no further record with an event time earlier than **T** will arrive."

```
records:  ─ e(10:04) ─ e(10:03) ─ ⟦W 10:02⟧ ─ e(10:06) ─ e(10:05) ─ ⟦W 10:04⟧ ─►
```

When a watermark passes a window's end, the window **fires**, and its result is final. The unanswerable "is it complete?" becomes a declared promise, and the guess is made in one place where you can see and tune it.

### Watching it work, record by record

Slow down here. Nine events arrive out of order, and the generator allows **30 seconds** of out-of-orderness: `watermark = (max event time seen) − 0:30`. The window is `10:00–10:05`.

| # | Event time | Max seen | Watermark | What happens |
|---|---|---|---|---|
| 1 | 10:01:10 | 10:01:10 | 10:00:40 | window created, count = 1 |
| 2 | 10:03:05 | 10:03:05 | 10:02:35 | count = 2 |
| 3 | **10:02:40** | 10:03:05 | 10:02:35 | 25 s out of order, still ≥ watermark: count = 3 |
| 4 | 10:04:55 | 10:04:55 | 10:04:25 | count = 4 |
| 5 | 10:05:10 | 10:05:10 | 10:04:40 | next window; first still open |
| 6 | **10:04:58** | 10:05:10 | 10:04:40 | 12 s out of order: count = 5 |
| 7 | 10:05:25 | 10:05:25 | 10:04:55 | next window; first *still* open |
| 8 | 10:05:31 | 10:05:31 | **10:05:01** | **watermark passes 10:05: fires count = 5, frees state** |
| 9 | **10:04:20** | 10:05:31 | 10:05:01 | window already fired: **late** |

What to notice:

- **Rows 3 and 6 are why watermarks exist.** Firing at record 5, the first past 10:05, would give 4.
- **Row 8 is the firing rule.** A *record* dragged the watermark past 10:05; no clock struck. Watermarks advance on data, so **if no more records arrive, the window never fires.** Remember that for the trap below.
- **Row 9 is the cost.** The promise was wrong. Any delay will sometimes be wrong; you choose how often, and what happens then.
- **It is reproducible.** Nothing consulted the wall clock, so a re-run gives identical rows.

### Generating them

```java
WatermarkStrategy
    .<View>forBoundedOutOfOrderness(Duration.ofSeconds(30))
    .withTimestampAssigner((view, ts) -> view.getEventTime())
    .withIdleness(Duration.ofMinutes(1));
```

That thirty seconds is the most consequential number in a Flink job. Make it smaller and windows fire sooner but more late events are dropped. Make it larger and more events are caught, but **every window result waits that much longer**, for everyone. Here are the same nine events with the dial turned:

| Delay | Fires when | Result | Lost |
|---|---|---|---|
| **0 s** | record 5 (10:05:10) | **4** | records 6 and 9, silently |
| **30 s** | record 8 (10:05:31) | **5** | record 9 |
| **5 min** | a record ≥ 10:10 arrives | **6** | none |

One input, one setting changed, three answers. The 0-second job is fast and wrong; the 5-minute job is right and five minutes stale.

Set the delay from data: measure `kafka_ts − event_time` and pick a value past the p99. Lantern's numbers are p50 = 400 ms, p99 = 6 s, p999 = 22 s, and max = 4 min (one laptop that had been asleep). Thirty seconds sits above p999 and below the tail. The panel runs 30 seconds behind, which nobody notices, and the laptop is abandoned rather than delaying every window by four minutes. A mobile-fed pipeline might see a p99 of minutes, and would have to accept ten-minute-old dashboards.

### The trap that catches everyone

Read this even if you skim the rest.

An operator's watermark is **the minimum across all its inputs**, since it can only promise what its slowest input promises. So **one idle or lagging input holds back the whole job.** Say one of Lantern's twenty-four partitions goes quiet, because of uneven keying upstream, a dead producer, or just 3 a.m.

```
partition 0   busy    watermark 10:42:15  ┐
   ...                                    │  min = 09:15:02
partition 17  SILENT  watermark 09:15:02  ┘  ← last record at 09:15:32
   ...
partition 23  busy    watermark 10:42:11
```

Twenty-three partitions say 10:42, but the job's watermark is **09:15:02**. Every window since then sits open, accumulating state. **No window anywhere ever fires.**

The symptom is specific: *the job is running, records are flowing in, there are no errors, and nothing ever comes out.* It is probably the most common Flink support question. Check:

1. **`withIdleness()`**, which drops a long-silent input from the minimum;
2. whether some partitions have no producers at all (often a keying problem upstream);
3. the timestamp assigner. If it returns a constant, wall-clock time, or a null that becomes zero, the watermark does inexplicable things.

### Late events

Events arriving after their window's watermark has passed are **late**. Pick what happens to them deliberately. Here are the options applied to record 9 (arriving after the window fired with 5):

| Choice | How | The panel sees |
|---|---|---|
| **Allowed lateness** | `allowedLateness(5 min)` keeps state and **re-fires** | `5`, then `6`: downstream must apply corrections |
| **Side output** | `.sideOutputLateData(lateTag)` routes late records to their own stream | `5`; the record lands on `late-views` and a counter ticks |
| **Drop** | the default | `5`; the record is gone without a trace |

**Always take the side output, even if you only count what lands in it.** At Lantern's two million views a day, 0.01% late is 200 records nobody misses, while 4% is 80,000 and a wrong trending panel. From outside, the two look identical. Silently dropped data is how a pipeline loses the trust of the people who depend on it.

## 6.6 Windows and joins

### Windows

Windows chop an infinite stream into finite pieces.

| Type | Shape | Example |
|---|---|---|
| **Tumbling** | fixed, non-overlapping; each event in one | `TumblingEventTimeWindows.of(Time.minutes(5))`, the trending panel |
| **Sliding** | fixed size, shorter slide; each event in `size / slide` | `SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1))`, a moving average |
| **Session** | closes after a gap of inactivity | `EventTimeSessionWindows.withGap(Time.minutes(30))`, user behaviour |
| **Global** | one window forever; a custom trigger fires it | count- or condition-based windows |

Check the sliding arithmetic before deploying. Five minutes sliding by one is **five times the state and output** of a tumbling window, and one hour sliding by one second is 3,600 copies, which people do write by accident.

Here are five views of `doc_88` by one user, fed to all four types:

```
 10:01   10:02        10:07                    10:44   10:46
   e1      e2           e3                       e4      e5
```

| | Windows emitted | Counts |
|---|---|---|
| **Tumbling** 5 min | `10:00–05`, `10:05–10`, `10:40–45`, `10:45–50` | 2, 1, 1, 1 |
| **Sliding** 5 / 1 min | ~20 windows from `09:57–10:02` on | e1 alone in five |
| **Session** 30 min gap | `10:01–10:07`, `10:44–10:46` | 3, 2 |
| **Global**, count trigger 3 | fires once, at e3 | 3 |

- **Tumbling** counts each event once, so the counts sum to 5.
- **Sliding** counts each event five times. That's right for a moving average and disastrous if someone sums the column.
- **Session** boundaries come from the 37-minute silence, and state stays until the gap provably elapses, so sessions are the hardest to bound. They also *merge*: a late record at 10:20 is within 30 minutes of both, so Flink joins them into `10:01–10:46` with 6 events and retracts the two emitted rows.
- **Global** ignores time, so e4 and e5 wait for a sixth event, whether it comes in a minute or a year.

**The window type is a modelling decision about what the question means**, not a performance setting.

### Window functions, and one that will hurt you

`reduce` and `aggregate` are **incremental**: each record updates one accumulator. `process(ProcessWindowFunction)` gets **every buffered record** plus window metadata at firing time, so it **keeps every record in state until the window closes**. For one document with 50,000 views in a window, `aggregate` holds **8 bytes** (one count) while `process` holds **~10 MB**. Multiply that across open windows and it becomes a memory problem.

Use `process` only when you need every record, as a median or p95 does; a count, sum, min, max, or average does not. To get incremental state *and* window metadata, combine them:

```java
.aggregate(new IncrementalCount(), new AddWindowMetadata())
```

### Triggers

A **trigger** decides when a window fires. The default fires once, when the watermark passes the end: `(10:00-10:05, doc_88, 5)` at 10:05:31. An **early** trigger firing every minute emits `1, 1, 3, 3, 4` as speculative results from 10:01 to 10:05, then `5` as the final. That gives a dashboard low latency and eventual correctness, as long as each row *replaces* the last; a `window_start` key and an upsert sink make that happen naturally. With `allowedLateness(Time.minutes(5))` (§6.5), late record 9 adds one more row: `(10:00-10:05, doc_88, 6)`.

### Joins

Stream joins are hard because a stream never ends, so you never know whether a match is still coming.

**Window join**: both sides must land in the same window. Lantern attributes clicks to searches, with a search at 10:04:58 and a click at 10:05:03. Five-minute tumbling windows put them in different windows, so **they don't join**. That happens to about one search in sixty, purely from where the grid falls.

**Interval join**: each left record joins right records within `[t − lower, t + upper]`. There is no grid:

```java
searches.keyBy(s -> s.searchId)
    .intervalJoin(clicks.keyBy(c -> c.searchId))
    .between(Time.seconds(0), Time.minutes(10))   // clicks 0–10 min after the search
    .process(new AttributeClick());
```

The condition is relative to each record, which is what the business rule says. It also bounds state, because a search can be dropped once the watermark passes its timestamp plus ten minutes.

**Temporal join**: this solves the backfill hazard §4.7 left open, where enriching against a changed dimension gives different answers on re-run. `doc_88` moved from Support to Engineering on 1 April. Who gets credit for a view on **15 March**?

| Join type | Run in February | Re-run today |
|---|---|---|
| ordinary join, current state | Support | **Engineering** |
| temporal join, event time | Support | **Support** |

```sql
SELECT v.doc_id, v.event_time, d.owner_team
FROM document_views v
JOIN documents FOR SYSTEM_TIME AS OF v.event_time AS d ON v.doc_id = d.doc_id;
```

Flink keeps the versioned dimension in keyed, time-indexed state and picks the version valid at `v.event_time`, so the answer is Support, forever. This is point-in-time correctness as a primitive. When a backfill disagrees with the original run for no visible reason, this is very often why.

**`connect` and `CoProcessFunction`**: two streams of different types share state. The classic case is a low-volume rules stream writing state that a high-volume event stream reads, using **broadcast state** when every subtask needs all the rules. It changes a job's behaviour without a redeploy.

**Async I/O**: a synchronous external call in `processElement` blocks the thread for the round trip, so at 10 ms per call a subtask manages a hundred records per second.

```java
AsyncDataStream.unorderedWait(stream, new AsyncEnricher(),
                              1000, TimeUnit.MILLISECONDS, 100);
```

This issues requests concurrently and takes throughput from a hundred per second to thousands. For per-record external calls it is a requirement, not an optimisation. Put a cache in front of it too.

## 6.7 Flink SQL

Most of this chapter can be written in SQL, and for most jobs it should be.

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

```
window_start          document_id   views   unique_viewers
2026-09-19 10:00:00   doc_88        5       4
2026-09-19 10:00:00   doc_12        1       1
   ... (nothing for the 10:05 window until the watermark passes 10:10)
```

That is the whole trending panel. `WATERMARK` is the §6.5 generator, `TUMBLE` is the §6.6 window, and `GROUP BY` is the `keyBy` plus keyed state. Checkpointing, recovery, and cleanup are the planner's problem, and it replaces several hundred lines of DataStream code.

Three ideas make streaming SQL readable:

**Dynamic tables.** A stream *is* a table that keeps changing, and a query over it yields another changing table.

**Append-only versus changelog.** This is the one that trips people up. A *windowed* aggregate is append-only, because a closed window's row is final. A `GROUP BY` **without** a window ("total views ever") changes with every record, so Flink emits a **changelog** of retractions and updates. Three views of `doc_88` emit one windowed row, `+I (10:00-10:05, doc_88, 3)`, or without the window:

```
+I (doc_88, 1)   -U (doc_88, 1)   +U (doc_88, 2)   -U (doc_88, 2)   +U (doc_88, 3)
```

A summing consumer that missed the retraction of 2 would double-count, and a million views produce two million rows. So the **sink must support upserts** (`upsert-kafka`, JDBC upsert, OpenSearch keyed by ID). An append-only sink rejects the stream:

```
Table sink 'default_catalog.default_database.trending' doesn't support
consuming update changes which is produced by node GroupAggregate(...)
```

About ninety percent of the time, that message means "you forgot the window".

**Unbounded aggregation state.** A non-windowed `GROUP BY document_id` keeps state for every ID ever seen. It's §6.3's problem, easier to write by accident. Bound it with `table.exec.state.ttl`.

**Write what you can in SQL, and drop to DataStream or ProcessFunction only for logic that doesn't fit.** You get less code, fewer bugs, and a planner that often beats you.

## 6.8 Operating it

### How a job gets deployed

Compile to a JAR (or Python file, or SQL script) and submit it:

```
flink run -d -c com.lantern.TrendingJob trending.jar
```

The JobManager gets TaskManagers, places subtasks in slots, and starts them. To upgrade, `flink stop --savepointPath ...` stops the job and takes a savepoint, and you resubmit with `--fromSavepoint` (§6.4). **Session mode** runs many jobs on one long-lived cluster, which is cheap but lets one job's failure disturb its neighbours. **Application mode** gives each job its own cluster, which is what you want in production. On Kubernetes, the Flink Operator makes a job a custom resource. SQL submitted through the SQL client or a gateway compiles to the same dataflow.

### The metrics that matter

- **Backpressure and busy time per operator.** Start here, because it names the bottleneck.
- **Watermark lag** (`currentEmitEventTimeLag`): how far the job's clock trails the world. Healthy Lantern sits near 30 s, its configured delay. At 1,200 s, the 10:40 window fires at 11:00, and a steady climb means the job won't catch up on its own.
- **Checkpoint duration, size, and failures.** Readings of 3 s, then 11 s, then 34 s over twelve weeks describe a job with no TTL and about six months to live.
- **Kafka consumer lag, records in and out, state size, and restart count.** A job quietly restarting every ten minutes is not healthy.

### Symptoms and causes

| Symptom | Cause | Fix |
|---|---|---|
| Job consumes but emits nothing | **watermark stuck**: idle or lagging input | `withIdleness()`; check all partitions produce; verify the timestamp assigner |
| Checkpoints slow or timing out | backpressure, state growth, slow object store | unaligned checkpoints; incremental RocksDB; fix the bottleneck operator |
| State grows without bound | no TTL; session windows; unbounded SQL aggregation | state TTL; timers; bounded windows |
| One subtask much slower than the rest | **key skew** | better key, or two-phase aggregation on a salted key |
| Job restarts in a loop | a record that always throws | side-output bad records; sane restart strategy; DLQ |
| Cannot restore from a savepoint | operator UIDs changed | always set `.uid()` |
| Throughput collapses at an enrichment step | synchronous external calls | Async I/O plus caching |
| Duplicate output after a failure | non-idempotent sink | deterministic keys, or a 2PC sink |

The first row is the most common, and the sixth is the most expensive.

For skew, use **two-phase aggregation**. Lantern's onboarding handbook gets a third of all views, so one subtask takes 16,000 records/s while its neighbours take 400. Adding parallelism can't split one key, so you split it yourself:

```java
.keyBy(v -> v.documentId + "#" + rnd.nextInt(100))   // phase 1: 100 partials, spread wide
.window(...).aggregate(new Count())
.keyBy(p -> p.documentId)                            // phase 2: combine the 100
.window(...).aggregate(new Sum())
```

The hot key becomes a hundred streams of 160/s. Spark calls this salting (§7.1).

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
| Best for | complex, low-latency, event-time-correct stateful work | Kafka-native microservices | teams on Spark; one engine for both |

There is no wrong answer. Running Spark for the lake and Flink for the low-latency path is a coherent architecture, not a failure to standardise.

## 6.9 Lantern gets a memory

Every part of the finished trending panel is a decision from this chapter:

- **Time:** 30-second bounded out-of-orderness, with `withIdleness` at one minute so quiet partitions can't stall the job.
- **State:** keyed by `document_id` into a five-minute tumbling event-time window, aggregating incrementally into a count plus a HyperLogLog sketch of unique viewers. The sketch is deliberately approximate, for the reasons in §2.8.
- **Late events:** sent to a side output, with their count on a dashboard beside the panel.
- **Sink:** OpenSearch keyed by `{document_id}:{window_start}`. Because it is idempotent, at-least-once processing is enough, and output appears immediately rather than at checkpoint boundaries.
- **Recovery:** exactly-once checkpoints every sixty seconds, unaligned, on incremental RocksDB, with a `uid` on every operator and a seven-day state TTL.

With a second sink, the same job flags documents whose view rate has jumped fivefold over their trailing average, sending them to a Kafka topic the notification service reads. The dashboard could have been a query. What streaming adds is the *reaction*, in seconds, to something that just happened.

---

## Key takeaways

- Flink's two contributions are **state** and **time**.
- Every `keyBy` is a network shuffle, and that is where the cost is.
- Backpressure flows from sink to source automatically, and the UI names the bottleneck.
- Keyed state gives you single-key code at scale. Use RocksDB beyond about a gigabyte.
- Bound every piece of state with a window, a timer, or a TTL.
- A checkpoint restores state and rewinds sources together; that is exactly-once.
- Set `.uid()` on every stateful operator, or savepoints break.
- Exactly-once output needs an idempotent or transactional sink.
- Set the watermark delay from measured lateness, and always count late events.
- Output stopped with no errors? Suspect a stuck watermark first.

## Where we are

Flink covers Lantern's low-latency half. The other half is the nightly reindex, re-embedding two hundred million chunks, recomputing after a bug, and ad-hoc analysis over two years of events. Those are bounded problems where throughput is everything, and the next chapter's engine handles them with a completely different central idea.
