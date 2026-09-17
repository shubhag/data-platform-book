# Chapter 5 — The Log

## 5.1 The wrong mental model

Kafka is usually introduced as a message queue, and that introduction does more harm than good. So let us start by noticing what is wrong with it.

A queue is a place where messages wait to be collected. A producer puts a message in; a consumer takes it out; **the message is now gone**. This is a perfectly good abstraction — it is what RabbitMQ and SQS and every job queue you have used provide — and it has two properties baked into it. First, reading is destructive: once consumed, the message no longer exists. Second, a message has one consumer; if two systems both need it, you need two queues and a fan-out mechanism.

Kafka is not that. Kafka is **a file that many people read**.

Specifically, it is an append-only log — the write-ahead log of §1.6, promoted from an implementation detail to the entire product. Writers append to the end. Readers read from wherever they like, at their own pace, without affecting each other or the data. Nothing is removed on read. Records are deleted on a schedule, or never.

That shift sounds academic and changes everything downstream, so let me draw it:

```
Topic "document-changes", partition 0:

offset:   0     1     2     3     4     5     6     7   ← next append here
        ┌─────┬─────┬─────┬─────┬─────┬─────┬─────┐
        │ e0  │ e1  │ e2  │ e3  │ e4  │ e5  │ e6  │
        └─────┴─────┴─────┴─────┴─────┴─────┴─────┘
                  ▲                       ▲
           indexer group             analytics group
            (at offset 2)             (at offset 5)
```

Two independent consumers, at different positions, reading the same records. Neither knows the other exists. Neither can affect the other. And the records at offsets 0 and 1, which both have passed, are **still there** — because deletion is governed by a retention policy, not by consumption.

Three properties do all the work in this chapter, and everything else follows from them.

**One: appends are immutable and sequential.** Nothing is ever modified in place. This is the same immutability that made Lucene segments fast in §2.3, for the same reasons — no locking, aggressive caching, and purely sequential disk writes. It is worth appreciating how fast sequential writes are: on spinning disks, sequential throughput is hundreds of times better than random, and even on SSDs the difference is large. Kafka handles millions of messages per second on ordinary hardware not because of clever tricks but because appending to a file is the fastest thing a storage device does.

**Two: reading does not consume.** Many consumers read the same data independently. For Lantern this means one stream of document changes can feed the search indexer, an analytics pipeline, an audit archive, and a cache invalidator, with no coordination between them and no extra cost to the producer. Adding a fifth consumer is free and requires no change to anything that already exists.

**Three: consumers control their own position.** You can rewind. Rewind an hour to reprocess after a bug. Rewind to the beginning to rebuild an index from scratch. Skip forward to abandon a backlog you have decided doesn't matter. This is what makes Kafka a *replayable* source, and it is the mechanism that makes Kappa architecture (§4.3) and routine backfills (§4.7) possible rather than aspirational.

So Kafka is simultaneously several things: a message bus, a buffer that decouples a fast producer from a slow consumer, a durable store of recent history, and the integration backbone that lets systems exchange data without knowing about each other. All of that from a log.

## 5.2 Topics, partitions, and the two rules

A **topic** is a named stream of records — `document-changes`, `search-queries`. It is purely a logical name.

A **partition** is a physical append-only log on a broker, and a topic is made of one or more of them. Partitioning is exactly the mechanism of §1.3, appearing here for the third time in this book.

And now the two rules that govern all Kafka design. I would like you to be able to recite them.

> **Rule one: ordering is guaranteed within a partition, and nowhere else.**
>
> **Rule two: parallelism is bounded by partition count — a partition is read by at most one consumer in a group.**

Everything you do with Kafka is a consequence of those two sentences in tension: rule one wants records together, rule two wants them spread apart.

### The key decides everything

When you produce a record you may attach a **key**, and the key decides the partition:

```
if key is null:   spread across partitions (sticky, then round-robin)
else:             partition = murmur2(key) % number_of_partitions
```

So **every record with the same key lands in the same partition**, and by rule one, those records are strictly ordered relative to each other.

This is the central design lever. Consider Lantern's options.

**Key by `document_id`.** Every change to a given document goes to one partition, in order. Two edits to the same document are never processed out of sequence. Meanwhile, edits to *different* documents are spread across all partitions and processed fully in parallel. With a few hundred million document IDs, the distribution is beautifully even. This is the right answer, and it is right for a reason worth naming: we identified the ordering requirement (per document) and chose the key to match it exactly — no tighter, which would cost parallelism, and no looser, which would lose correctness.

**Key by `team`.** Now there are perhaps two hundred distinct keys for twelve partitions, and the platform team — which owns a third of all documents — sends a third of all traffic to a single partition. That is skew (§1.3), and its consequences here are specific and unpleasant: one partition's consumer falls behind while eleven idle, and because that partition is a single serial log, **you cannot add capacity to fix it.** More consumers will not help; rule two says only one consumer may read that partition. Re-keying means reprocessing.

**No key at all.** Maximum throughput, perfectly even distribution, zero ordering. Correct for logs and metrics where each record stands alone. Wrong for anything where two records about the same thing could conflict.

The general principle: **key by whatever entity needs ordered processing, and nothing coarser.**

### Choosing the partition count

Two considerations pull in opposite directions.

Upward: **you cannot have more active consumers in a group than partitions.** Partition count is your ceiling on consumer parallelism, permanently. Twelve partitions means at most twelve consumers doing work, no matter how many you deploy. If you need to process ten thousand records a second and one consumer manages a thousand, you need at least ten partitions and preferably more for headroom.

Downward: partitions are not free. Each is a set of files with open handles, memory for buffers, and replication traffic. Every partition lengthens leader elections and consumer rebalances. Clusters with tens of thousands of partitions start to suffer in ways that are hard to diagnose.

And the asymmetry that makes this decision matter: **increasing the partition count later breaks your key-to-partition mapping.** `murmur2(key) % 12` and `murmur2(key) % 16` disagree for most keys, so after the change a document's history is split across two partitions, and the ordering guarantee you were relying on is broken across the boundary. The old records do not move — Kafka will not rewrite history — so the discontinuity is permanent.

The practical advice is therefore to **over-provision modestly at creation**: two to three times your current need. Not tenfold, which imports the costs above, but enough that you are not forced into a breaking change within the year.

### What a record contains

A record is a key, a value, a timestamp, and **headers** — a small map of metadata. Use the headers for cross-cutting concerns: a trace ID for distributed tracing, a schema ID, a source system identifier, the name of the service that produced it. This keeps operational metadata out of your business payload, where it would otherwise end up in your schema and then in your analytics tables forever.

## 5.3 Brokers, replication, and the three settings that matter

A **broker** is one Kafka server; a **cluster** is a set of them. Partitions are distributed across brokers, and each partition is replicated.

```
Topic with 3 partitions, replication factor 3

 Broker 1          Broker 2          Broker 3
 P0 leader         P0 follower       P0 follower
 P1 follower       P1 leader         P1 follower
 P2 follower       P2 follower       P2 leader
```

Each partition has one **leader**, which handles all reads and writes for it. **Followers** continuously fetch from the leader and append the same records in the same order. This is leader–follower replication (§1.4) again, and note that leadership is spread deliberately across brokers so that no single machine handles all the write traffic.

The set of followers that are sufficiently caught up is called the **ISR** — the **in-sync replicas**. It is a dynamic set: a follower that falls too far behind is *removed* from the ISR, and rejoins when it catches up. When a leader fails, the controller elects a new leader **from the ISR**, which is the guarantee that matters: an ISR member has all the acknowledged records, so nothing committed is lost.

### The durability triad

Chapter 1 (§1.4) described the choice between synchronous and asynchronous replication in the abstract and promised you would meet it as a configuration flag. Here it is.

**The producer's `acks` setting** determines what the leader waits for before acknowledging:

| Setting | Leader waits for | Result |
|---|---|---|
| `acks=0` | nothing at all | fastest; loses data on any hiccup |
| `acks=1` | its own log write | fast; **loses data if the leader dies before followers catch up** |
| `acks=all` | all in-sync replicas | slower; no loss while at least one ISR member survives |

**The topic's `min.insync.replicas`** sets a floor. With `acks=all`, if fewer than this many replicas are in sync, the write is **rejected** rather than accepted with weaker guarantees. This is the crucial complement to `acks=all`, and here is why: on its own, `acks=all` means "all *in-sync* replicas", and if two of your three replicas have fallen out of the ISR, then "all in-sync replicas" means one — the leader. Your `acks=all` has silently degraded to `acks=1` at the exact moment you most needed it. `min.insync.replicas` closes that hole by refusing the write instead.

So the canonical safe configuration, which you should treat as the default and deviate from only deliberately:

```
replication.factor    = 3
min.insync.replicas   = 2
acks                  = all
```

Read it as a sentence: three copies exist, at least two must confirm each write, and the producer waits for that confirmation. **You survive the loss of any one broker with zero data loss and zero interruption to writes** — because you only ever needed two of the three, so the third's death changes nothing. Lose a second broker and writes stop, which is the system correctly refusing to accept data it cannot protect.

Compare the alternatives to see why this one is chosen. `min.insync.replicas=3` with RF=3 blocks writes the instant any broker restarts, which is too strict for a system that has deployments. `min.insync.replicas=1` gives you no real guarantee, as we just saw. Two out of three is the point where durability and availability are both satisfied.

### The setting that will lose your data

One more, and it deserves its own space because the default has changed over time and because its name does not convey its consequences.

**`unclean.leader.election.enable`.** Suppose every ISR member for a partition is down, and only an out-of-sync replica remains — one that is missing the last few thousand records. Two choices: leave the partition offline until an ISR member returns, or promote the stale replica and continue.

If this setting is `true`, Kafka promotes the stale replica. The partition comes back. And **those few thousand acknowledged records are gone forever**, silently, with the log now continuing from an earlier point. Consumers that had read past that point see the offsets rewind, which breaks things in creative ways.

If it is `false`, the partition stays offline until a replica with the data returns.

This is the CAP choice of §1.4 reduced to a single boolean. `true` is availability; `false` is consistency. Keep it `false` unless you have explicitly decided that for this particular topic, being up matters more than being correct — which is a legitimate decision for some data and should be made consciously.

### How the data is stored

A partition is a directory. Inside it are **segment** files — by default one gigabyte or seven days each — plus index files mapping offsets and timestamps to positions in those segments.

Retention deletes whole segments, never individual records, which is why retention is coarse: `retention.ms` (a week by default) or `retention.bytes`.

### Compaction: turning a log into a table

There is a second retention policy, and it is conceptually the most interesting feature in Kafka.

Set `cleanup.policy=compact` and Kafka stops deleting by age. Instead, a background process keeps **the most recent record for each key** and discards the older ones. A record with a `null` value is a **tombstone**, meaning "this key is deleted", and is itself removed after a grace period.

Think about what that gives you. The topic is no longer a history of changes that expires; it is a **complete, durable, replayable snapshot of current state**, keyed by ID, that you can read from the beginning to reconstruct everything. It is a table, stored as a log, with the log's replay properties intact.

This is not a niche feature. Kafka stores consumer offsets in a compacted topic. Kafka Streams and Flink back their state stores with compacted changelog topics, which is how their state survives a total cluster loss. And for Lantern it is exactly the right shape for the document-change stream: if the topic is compacted on `document_id`, then reading it from the beginning gives you the current version of every document — precisely what you need to rebuild the search index from scratch, which is precisely what a model change in Chapter 3 requires.

### Two mechanisms worth knowing by name

**Zero-copy.** When a consumer fetches records, Kafka uses the `sendfile` system call to transfer bytes directly from the filesystem cache to the network socket, without copying them into the application's memory and back. This is a large part of its throughput, and it has a consequence you might not predict: consumers that are reading *recent* data are served entirely from the operating system's page cache and never touch the disk at all. Which is why Kafka wants most of the machine's RAM left free for the OS rather than given to its JVM heap — the same reasoning as OpenSearch's filesystem cache in §2.11.

**Tiered storage.** Modern Kafka can offload older segments to object storage, keeping only recent data on broker disks. This makes very long retention affordable, which in turn makes Kappa architecture practical — you can genuinely keep a year of history and replay it.

### A note on ZooKeeper

Older Kafka kept cluster metadata in Apache ZooKeeper, a separate system that had to be deployed and operated alongside. Modern Kafka uses **KRaft**, in which a quorum of controller nodes runs Raft (§1.6) internally. Fewer moving parts, much faster failover, and far better metadata scalability. If you encounter documentation discussing ZooKeeper ensembles, it is describing the legacy mode.

## 5.4 Producing

```java
props.put("acks", "all");
props.put("enable.idempotence", true);
props.put("compression.type", "zstd");
props.put("linger.ms", 20);
props.put("batch.size", 65536);

producer.send(new ProducerRecord<>("document-changes", documentId, payload),
              (metadata, exception) -> { /* handle result */ });
```

### The send is not a send

The most common misunderstanding: `send()` does not send anything. It places the record in an in-memory accumulator, organised into per-partition batches, and returns immediately. A background I/O thread takes full or expired batches and ships them to brokers. The callback fires when the broker acknowledges.

Which leads to a performance trap worth naming explicitly. `send()` returns a `Future`, and it is tempting to call `.get()` on it to make sure the record landed. Doing so per record makes every send a blocking round trip and **collapses throughput by roughly two orders of magnitude**, because you have eliminated all batching and all pipelining. Use the callback. Block only when you genuinely need synchronous semantics for a specific record.

### The batching knob, again

**`linger.ms`** is how long the producer waits to accumulate more records before sending a batch. At `0`, records go out as soon as the I/O thread can take them: lowest latency, smallest batches. At `20`, you accept up to twenty extra milliseconds of latency and get substantially larger batches.

This is §1.9's batching trade-off, and I want to point out that the gain is not merely "fewer requests". Compression in Kafka is applied **per batch**, so a batch of two hundred similar records compresses dramatically better than two hundred individually compressed records — often three to five times better. Larger batches thus reduce network traffic, disk usage, and replication bandwidth simultaneously. A small `linger.ms` is frequently the cheapest improvement available to a Kafka producer, and `zstd` or `lz4` compression is nearly always worth the CPU.

### When the buffer fills

`buffer.memory` bounds the accumulator. When brokers are slow or unreachable, the buffer fills, and then `send()` **blocks** for up to `max.block.ms` before throwing.

This is not a malfunction. **It is backpressure** (§1.6), working correctly: the system is telling you it cannot accept more data right now. The important thing is what your application does with it. If a blocked `send()` propagates a pause back to whatever is generating the data, you have a correctly-behaving pipeline. If it throws an exception that your code catches and logs, you are silently dropping records under load — which is the exact condition in which you can least afford to.

### Exactly-once writes, for free

Set `enable.idempotence=true` — the default in current clients — and something rather elegant happens.

The producer is assigned a **producer ID** and attaches a monotonically increasing **sequence number** to each record, per partition. The broker remembers the highest sequence number it has seen for each producer and partition. When a retry arrives carrying a sequence number it has already accepted, **the broker discards it** and reports success.

Duplicates from producer retries are eliminated, at the broker, with no application code. This is §1.7's idempotency-key pattern implemented inside the protocol.

It also fixes a subtler problem. Without idempotence, `retries > 0` combined with more than one in-flight request per connection can **reorder** records: batch two succeeds while batch one is being retried, so batch one's records land after batch two's. Your carefully-designed per-key ordering is quietly broken by a transient network error. With idempotence enabled, the broker enforces sequence ordering and rejects out-of-order batches, so ordering survives retries for up to five in-flight requests.

Leave it on. There is no reason not to.

### Transactions

Idempotence handles duplicate *writes*. Transactions handle something larger: atomicity across multiple partitions, and — the reason they exist — atomicity between consuming and producing.

```java
producer.initTransactions();
producer.beginTransaction();
producer.send(record1);
producer.send(record2);
producer.sendOffsetsToTransaction(offsets, groupMetadata);   // ← the important line
producer.commitTransaction();
```

That third-from-last line is the whole point. The consumer's **offset commit is written inside the same transaction as the output records.** Either both happen or neither does. There is no window in which you have produced output but not recorded that you consumed the input, or vice versa — which is exactly the window that produces duplicates and gaps in a consume-transform-produce pipeline.

Consumers configured with `isolation.level=read_committed` do not see records from uncommitted or aborted transactions.

This is Kafka's exactly-once processing, and it is what Kafka Streams and Flink use under the hood. It costs perhaps ten to twenty percent of throughput plus additional latency at commit boundaries, and it applies to Kafka-to-Kafka pipelines — a point §5.6 returns to, because it is the most commonly misunderstood limitation in the ecosystem.

## 5.5 Consuming

### Consumer groups

Consumers that share a `group.id` cooperate. Kafka assigns each partition to **exactly one** consumer in the group.

```
Topic with 4 partitions

Group "indexer", 2 consumers:        Group "analytics", 1 consumer:
   C1 → P0, P1                          C1 → P0, P1, P2, P3
   C2 → P2, P3
```

Two things to notice. Within a group, the partitions are divided — the group as a whole processes each record once. Across groups, everything is independent — each group receives **all** the records. So Lantern's single `document-changes` topic feeds the indexer group, the analytics group, and the audit archive group, each at its own pace, each with its own position, with no coordination and no cost to the producer.

And rule two from §5.2 shows its teeth: with four partitions, a fifth consumer in a group **sits completely idle.** Not slower — idle, doing nothing, because there is no partition left to assign. This is the single most common surprise for people new to Kafka, and it is why partition count is a capacity decision rather than a cosmetic one.

### Offsets, and where you commit them

An **offset** is a group's position in a partition: the offset of the next record it intends to read. Committed offsets are stored in the internal compacted topic `__consumer_offsets` — which is a nice illustration of §5.3's compaction, since what you want is the latest offset per group-partition, not the history.

Now the setting that silently determines your delivery guarantee.

`enable.auto.commit` defaults to **true**, and commits the current position every five seconds **on a timer, from a background thread, regardless of whether you have finished processing.**

Trace the failure. At second zero, `poll()` returns records at offsets 100 through 130. You begin processing. At second five, the background thread commits offset 130. At second six, you have processed up to offset 115 and the process crashes. On restart, the group resumes from the committed position — **offset 130** — and offsets 115 to 130 are never processed. They are not delayed. They are gone, and nothing anywhere records that they were skipped.

Auto-commit gives you **at-most-once** semantics with silent data loss, from a default setting, with no warning. For metrics it is fine. For Lantern's indexer it would mean documents randomly missing from search.

The fix is short:

```java
props.put("enable.auto.commit", false);

while (running) {
    var records = consumer.poll(Duration.ofMillis(500));
    process(records);            // must be idempotent
    consumer.commitSync();       // commit AFTER processing
}
```

Process, then commit. Now a crash before the commit means the records are redelivered, which is at-least-once, which is harmless because `process` is idempotent (§4.7). This is §1.7's ack-placement diagram made concrete, and it is three lines of code.

### Where to start when there is no position

`auto.offset.reset` determines behaviour when a group has no committed offset — a brand-new group, or one whose stored offset has aged out of retention. `latest` (the default) skips everything that already exists and reads only new records. `earliest` reads the topic from the beginning.

Both are reasonable and both are dangerous by accident. A new consumer group deployed with `earliest` against a topic holding a year of data will start reprocessing a year of data, usually at a moment nobody expected. A group deployed with `latest` when you *intended* a full rebuild silently skips all history and you discover the gap weeks later. Set it explicitly and know which you meant.

### Rebalancing, and why it hurts

When a consumer joins, leaves, or is presumed dead, the group **rebalances**: partitions are reassigned among the surviving members.

With the classic eager protocol, a rebalance is stop-the-world — **every** consumer in the group revokes **all** its partitions and stops consuming while the new assignment is computed. On a large group with many partitions this can take tens of seconds, during which nothing is processed. And frequent rebalances are one of the top two or three operational complaints about Kafka, so it is worth knowing their causes.

**Slow processing** is the most common. Kafka considers a consumer alive partly by whether it calls `poll()` within `max.poll.interval.ms`, five minutes by default. If one batch of records takes longer than that to process — because a downstream service got slow, or because `max.poll.records` was large and each record is expensive — the group concludes you are dead and rebalances. Your consumer then finishes its work, tries to commit, and discovers it no longer owns those partitions. The fix is to reduce `max.poll.records` so a batch is comfortably fast, or to make processing faster.

**Missed heartbeats.** A background thread sends heartbeats every `heartbeat.interval.ms`; miss them for `session.timeout.ms` (45 seconds) and you are evicted. This is §1.5's failure detection, with exactly the trade-off described there — too short and you evict healthy consumers, too long and you tolerate dead ones.

**Deployments.** Every rolling restart triggers a rebalance per instance replaced. The mitigation is **static membership** via `group.instance.id`: a consumer with a stable identity that restarts within its session timeout **reclaims its own partitions** without a group-wide rebalance. This turns a rolling deploy from a series of stop-the-world events into a non-event, and it is one of the highest-value settings in the client.

Also use the **cooperative sticky assignor**, the default in recent versions, which revokes only the partitions that actually need to move rather than all of them. And implement a `ConsumerRebalanceListener` so you can commit offsets and flush buffers in `onPartitionsRevoked`, rather than discovering the revocation when your commit fails.

### Lag, the metric that tells you everything

```
lag = log end offset (latest produced)  −  committed offset (consumer position)
```

How many records behind the consumer is. If you monitor one thing about a Kafka pipeline, monitor this, because its *shape over time* diagnoses most problems.

**Steadily increasing** means consumers cannot keep up. Add consumers, up to the partition count; beyond that, add partitions or make processing faster.

**Spiky** — rising and falling — means intermittent stalls: GC pauses, a slow downstream dependency, rebalances.

**Uneven across partitions**, with one partition's lag growing while others stay flat, means **key skew** (§5.2). No amount of added capacity helps; the key must change.

**Flat and high** means you are keeping up but permanently behind, usually because a backlog was never cleared.

One refinement: monitor lag in **time** as well as in records. "Forty thousand records behind" means nothing without knowing the rate — it could be two seconds or two hours. Time lag, measured as the difference between now and the timestamp of the next unprocessed record, is directly comparable against your freshness SLA (§4.6), which makes it the more useful alert.

## 5.6 What you actually get, end to end

Assembling everything:

| | Producer | Consumer | Result |
|---|---|---|---|
| **At-most-once** | `acks=0/1` | auto-commit | fast; loses records |
| **At-least-once** | `acks=all`, idempotent | commit after processing | **the default choice**; duplicates possible |
| **Exactly-once** | transactions | `read_committed`, offsets in transaction | correct; ~10–20% slower; Kafka→Kafka |

And now the limitation that matters most, because it is where people's understanding usually goes wrong.

> **Kafka's exactly-once guarantee covers Kafka → process → Kafka. The moment you write to an external system, it does not apply.**

Kafka's transactions work by atomically committing records into Kafka's own log alongside the offset. OpenSearch is not participating in that transaction. Neither is PostgreSQL, nor S3, nor an HTTP API. There is no distributed transaction spanning Kafka and OpenSearch — and, per §1.7, building one with two-phase commit would be slow and would block on coordinator failure.

So for Lantern's indexer, which reads Kafka and writes OpenSearch, exactly-once is not available from Kafka. What is available is the pattern we have now built three times:

**At-least-once delivery from Kafka, plus an idempotent write to OpenSearch, keyed on `{document_id}:{chunk_index}`.**

A crash redelivers records. The redelivered records overwrite the identical documents already in the index. The final state is correct. The cost is zero — no transactions, no coordination, no throughput penalty — and this is what the great majority of production pipelines actually do. When someone tells you their pipeline is exactly-once, this is usually, and quite properly, what they mean.

## 5.7 The ecosystem

Kafka rarely arrives alone. Five things you should recognise.

**Kafka Connect** is a framework for moving data in and out of Kafka using pluggable connectors — JDBC, S3, OpenSearch, MongoDB, and many more — with offset management, scaling, retry, and error handling already solved. The advice here is unambiguous: **for straightforward ingest or egress, use a connector rather than writing a consumer.** A hand-written consumer that reads Kafka and writes S3 will, over eighteen months, grow offset handling, backpressure, retry logic, schema handling, and a DLQ, at which point you have reimplemented Kafka Connect with fewer tests. Do configure `errors.tolerance` and a **dead-letter queue** topic; the defaults are stricter than you want.

**Debezium and change data capture.** This is how Lantern's pipeline actually begins, so it deserves proper attention.

The naive way to get PostgreSQL changes into Kafka is to have the application write to both — a **dual write**. This is broken, and it is worth seeing why clearly: the two writes are not atomic, so any failure between them leaves the systems permanently inconsistent, with no record of the divergence. The database has the edit and the search index does not, forever, and nothing anywhere knows.

The second naive approach is **polling**: `SELECT * FROM documents WHERE updated_at > ?` every few seconds. Better, and it has three real flaws. It misses deletes entirely, since a deleted row cannot be selected. It misses intermediate states — if a row changes twice between polls you see only the final value, which is fine for a search index and wrong for an audit log. And it depends on `updated_at` being maintained correctly by every code path that writes, which it will not be.

**Change data capture** reads the database's own **write-ahead log** — PostgreSQL's WAL, MySQL's binlog — which is the authoritative, ordered record of every committed change. Debezium turns each change into a Kafka record containing the before and after images. You get every insert, update, and delete, in commit order, with no polling, no missed states, no application changes, and no dependence on developer discipline.

Notice what this is: the WAL of §1.6, which exists so the database can recover from crashes, repurposed as an integration point. It was already there, already ordered, already durable. CDC just reads it.

**Kafka Streams** is a Java library — not a cluster — for stateful stream processing. `KStream` and `KTable` abstractions, joins, windowed aggregations, with state held in a local RocksDB instance backed by a compacted changelog topic (§5.3), so that state survives losing the machine. It is an excellent fit when your processing belongs inside a JVM microservice and both ends are Kafka. It is a poor fit for heavy analytics or non-Kafka sources. **ksqlDB** puts SQL on top of it.

**Schema Registry** — see §4.5. Use it.

**MirrorMaker 2** replicates topics between clusters for disaster recovery and geographic distribution. **Cruise Control** automates partition rebalancing and broker capacity management, and becomes worth deploying somewhere around the point where you have more brokers than you can think about individually.

## 5.8 Operating it

### What to watch

Beyond consumer lag, which §5.5 covered:

**Under-replicated partitions** should be **zero**. Any sustained non-zero value means a follower has fallen out of an ISR, which means your durability margin is gone — you are running with fewer effective copies than you designed for, and the next failure may not be survivable. Treat it as urgent rather than informational.

**Offline partitions** should be **zero**, always. A non-zero value means some data is unavailable right now.

**ISR shrink and expand rate.** Frequent flapping indicates brokers that are struggling — slow disks, network saturation, GC.

**Request latency** at p99 for both produce and fetch. **Disk usage and its growth rate**, since retention is only as good as the disk it fits on. **Active controller count**, which must be exactly one.

### Resource shape

Kafka is **disk-I/O and network bound**, and almost never CPU bound. This shapes your choices: fast disks matter, plentiful page cache matters, and co-locating brokers with CPU-hungry neighbours is less harmful than you might assume.

And, as §5.3 noted, **leave most of the RAM to the operating system's page cache** rather than the JVM heap. A heap of around six gigabytes is typical even on a large broker. Recent data is then served from cache with zero disk reads, which is where Kafka's throughput comes from. This is the same principle as OpenSearch's filesystem cache (§2.11), for the same underlying reason: both systems rely on the OS to cache memory-mapped files, and both are harmed by a greedy heap.

### A table of symptoms

| Symptom | Likely cause | Fix |
|---|---|---|
| Lag grows on one partition only | key skew | change the key; repartition |
| Constant rebalances | slow processing, deploys | lower `max.poll.records`; static membership; cooperative assignor |
| Duplicates downstream | at-least-once with non-idempotent sink | deterministic keys, upsert |
| Records out of order | multiple partitions, or retries without idempotence | key correctly; `enable.idempotence` |
| `send()` blocking or timing out | buffer full, brokers slow | that is backpressure — scale brokers, or handle it |
| Data lost after a broker failure | `acks=1`, or unclean leader election | `acks=all`, `min.insync.replicas=2`, unclean=false |
| Disk filling | retention too long | tune retention; tiered storage |
| One bad record stalls a partition | no error handling | DLQ and skip; never block indefinitely |

That last row deserves a sentence. A record that always throws — a malformed payload, a schema violation — will be retried, fail, be retried, and fail, forever, and **everything behind it in the partition stops.** This is head-of-line blocking, and because the partition is a strict serial log there is no way around the bad record. It must be routed to a dead-letter queue and skipped. A pipeline without a DLQ is a pipeline with an unhandled halt condition.

### A checklist for a new topic

Seven questions, and having written them down you will not regret it:

1. **What is the key**, what ordering does that buy, and is there skew risk?
2. **How many partitions** — consumer parallelism plus headroom?
3. **Retention**: delete or compact, and for how long?
4. **Durability**: RF=3, `min.insync.replicas=2`, `acks=all`, unclean election off?
5. **Schema** registered, with which compatibility mode?
6. **Who consumes it**, and is their processing idempotent?
7. **Is there a DLQ**, and an alert on lag?

## 5.9 Lantern gets a nervous system

Here is what the pipeline now looks like.

Debezium reads PostgreSQL's write-ahead log and publishes every document insert, update, and delete to a Kafka topic called `document-changes`, **keyed by `document_id`**. Twenty-four partitions, replication factor three, `min.insync.replicas=2`, `acks=all`, unclean leader election disabled. Records are Avro, with schemas in a registry configured for backward compatibility. Retention is seven days for the change stream; a parallel **compacted** topic keyed by `document_id` holds the current version of every document, so the whole corpus can be replayed from the beginning to rebuild the index.

A consumer group called `indexer` reads `document-changes` with twelve consumers, auto-commit disabled. For each batch it chunks the document text, calls the embedding service, and issues an OpenSearch `_bulk` request keyed on `{document_id}:{chunk_index}` — idempotent, so a redelivery is a no-op. Only then does it commit offsets. `429` responses from OpenSearch are retried with backoff. Records that fail deterministically go to `document-changes-dlq` with the exception attached. Static membership means deploys don't rebalance the group. Lag is monitored in both records and seconds, and alerts when time lag exceeds thirty seconds.

A second, entirely independent group archives the raw change stream to S3 as Avro, which is the raw layer of §4.2 — the thing everything can be re-derived from.

Look at what this bought us. The database no longer knows the search index exists; Debezium reads a log the database was writing anyway. The indexer can be stopped for an hour, redeployed, or rewound three hours, and nothing upstream notices. The nightly analytics job and the audit archive read the same stream without any coordination. When Chapter 3's embedding model is upgraded, the replay path already exists: read the compacted topic from offset zero into a new index, verify, swap the alias.

And we can now be precise about the latency budget:

```
PostgreSQL commit → Debezium reads WAL        ~100 ms – 1 s
produce + replicate to ISR                    ~5 – 50 ms   (mostly linger.ms)
indexer consumes and chunks                   ~10 – 50 ms
embedding inference (batched)                 ~10 – 50 ms
OpenSearch bulk write                         ~20 – 100 ms
OpenSearch refresh interval                   ~1 s          ← the largest term
                                              ─────────────
end to end                                    ~1.5 – 3 s
```

Which meets the requirement, and — more usefully — tells us that if we ever need it faster, the refresh interval is where to look first, not the pipeline.

---

## Where we are

Kafka is a log, and the log's three properties — immutable sequential appends, non-destructive reads, consumer-controlled position — give us durability, fan-out to unlimited independent consumers, and the ability to rewind. Partitions provide parallelism, keys provide per-entity ordering, and the tension between those two is where the design work lives. Three settings (`acks=all`, `min.insync.replicas=2`, RF=3) buy zero-loss durability through any single failure. Idempotent producers eliminate retry duplicates for free. And the honest end-to-end guarantee is at-least-once delivery combined with an idempotent sink, which is entirely sufficient.

But look again at what the indexer consumer actually does. It processes each record independently: chunk it, embed it, write it. It holds no memory of anything. It never needs to know what happened five minutes ago, or to count something over a window, or to join two streams together.

That is a *stateless* transformation, and it is the easy case. The moment you want to count events per document per hour, or detect that a document has been edited five times in ten minutes, or join a document change against a stream of view events — you need to remember things. Across crashes. For months. While events arrive late and out of order and you must decide when an hour is finally over.

That is a much harder problem, and it has the best answer in Chapter 6.
