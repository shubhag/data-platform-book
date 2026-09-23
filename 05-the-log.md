# Chapter 5 — The Log

Lantern's documents change hundreds of times a second, and several systems need each change: the search indexer, analytics, an audit archive. Wiring each one to the database, or having the application write to all of them, breaks the first time anything fails. This chapter is about Kafka, the durable log that sits in the middle and lets every consumer read the same changes at its own pace.

**By the end you'll be able to:**

- Explain why Kafka is a log rather than a queue, and what that buys you.
- Choose a partition key and a partition count for a new topic.
- Configure producers and topics so a single broker failure loses no data.
- Commit consumer offsets correctly and read consumer lag.
- State what "exactly-once" really covers, and build the at-least-once plus idempotent-sink pattern that most pipelines actually use.

## 5.1 The wrong mental model

Kafka is usually introduced as a message queue, and that does more harm than good. In a **queue** (RabbitMQ, SQS, any job queue), a consumer takes a message out and **the message is now gone**. Reading is destructive, and each message has one consumer; if two systems need it, you need two queues and a fan-out.

Kafka is not that. Kafka is **a file that many people read**. Specifically, it is an append-only log — the write-ahead log of §1.6, promoted from an implementation detail to the entire product. Writers append to the end; readers read from wherever they like. Nothing is removed on read; records are deleted on a schedule, or never.

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

Two consumers at different positions read the same records, unaware of each other. Offsets 0 and 1 are **still there**: deletion follows retention, not consumption.

Three properties do all the work in this chapter:

1. **Appends are immutable and sequential.** Nothing is modified in place — the same immutability that made Lucene segments fast in §2.3, for the same reasons: no locking, aggressive caching, purely sequential disk writes. Kafka handles millions of messages per second on ordinary hardware because appending to a file is the fastest thing a storage device does.
2. **Reading does not consume.** One stream of Lantern's document changes can feed the indexer, analytics, an audit archive, and a cache invalidator, at no extra cost to the producer.
3. **Consumers control their own position.** Rewind to reprocess after a bug or rebuild an index; skip forward to abandon a backlog. This makes Kafka a *replayable* source, which is what makes Kappa architecture (§4.3) and routine backfills (§4.7) practical.

So Kafka is at once a message bus, a buffer, a store of recent history, and an integration backbone. All from a log.

## 5.2 Topics, partitions, and the two rules

A **topic** is a named stream of records, such as `document-changes`. It is purely a logical name. A **partition** is a physical append-only log on a broker, and a topic is made of one or more of them. This is the partitioning of §1.3, again.

Two rules govern all Kafka design. Learn them by heart:

> **Rule one: ordering is guaranteed within a partition, and nowhere else.**
>
> **Rule two: parallelism is bounded by partition count — a partition is read by at most one consumer in a group.**

Rule one wants related records together; rule two wants records spread apart. Most Kafka design is managing that tension.

### The key decides everything

When you produce a record you may attach a **key**, and the key picks the partition:

```
if key is null:   spread across partitions (sticky, then round-robin)
else:             partition = murmur2(key) % number_of_partitions
```

So **every record with the same key lands in the same partition**, and by rule one those records stay in order. Here are Lantern's options:

| Key | Ordering | Distribution | Verdict |
|---|---|---|---|
| `document_id` | per document | even across hundreds of millions of IDs | **right** |
| `team` | per team | ~200 keys; the platform team sends a third of traffic to one partition | skewed |
| none | none | perfectly even | fine for logs and metrics only |

`document_id` matches the real ordering requirement exactly: edits to one document stay in order, different documents run in parallel. Keying by `team` is skew (§1.3): one partition's consumer falls behind while eleven idle, and **you cannot add capacity to fix it**, because rule two allows one consumer per partition. Re-keying means reprocessing.

The principle: **key by whatever entity needs ordered processing, and nothing coarser.**

### Choosing the partition count

Two forces pull in opposite directions:

- **Up:** you cannot have more active consumers in a group than partitions. Twelve partitions means at most twelve consumers doing work.
- **Down:** each partition costs files, open handles, buffer memory, and replication traffic, and lengthens leader elections and rebalances. Tens of thousands of partitions hurt in ways that are hard to diagnose.

The catch: **increasing the partition count later breaks your key-to-partition mapping.** `murmur2(key) % 12` and `murmur2(key) % 16` disagree for most keys, so a document's history splits across two partitions, and old records never move. **Over-provision modestly at creation**: two to three times current need, not tenfold.

### What a record contains

A record is a key, a value, a timestamp, and **headers**, a small map of metadata. Put cross-cutting metadata there (trace ID, schema ID, producing service) so it stays out of your payload, and out of your analytics tables forever.

## 5.3 Brokers, replication, and the three settings that matter

A **broker** is one Kafka server; a **cluster** is a set of them. Partitions are spread across brokers, and each partition is replicated.

```
Topic with 3 partitions, replication factor 3

 Broker 1          Broker 2          Broker 3
 P0 leader         P0 follower       P0 follower
 P1 follower       P1 leader         P1 follower
 P2 follower       P2 follower       P2 leader
```

Each partition has one **leader**, which handles all reads and writes; **followers** fetch from it and append the same records in order. This is leader–follower replication (§1.4), with leadership spread across brokers.

The followers that are sufficiently caught up form the **ISR** (**in-sync replicas**). Laggards drop out and rejoin once caught up. When a leader fails, the controller elects a new leader **from the ISR**. Every ISR member has all the acknowledged records, so nothing committed is lost.

### The durability triad

Here is §1.4's sync-versus-async replication choice as configuration. **The producer's `acks` setting** controls what the leader waits for before acknowledging:

| Setting | Leader waits for | Result |
|---|---|---|
| `acks=0` | nothing at all | fastest; loses data on any hiccup |
| `acks=1` | its own log write | fast; **loses data if the leader dies before followers catch up** |
| `acks=all` | all in-sync replicas | slower; no loss while at least one ISR member survives |

**The topic's `min.insync.replicas`** sets a floor: with `acks=all`, if fewer than this many replicas are in sync, the write is **rejected**. You need it because `acks=all` means "all *in-sync* replicas". If two of three replicas have dropped out of the ISR, that means just the leader, and your `acks=all` has quietly become `acks=1` exactly when you needed it most.

The canonical safe configuration, to deviate from only deliberately:

```
replication.factor    = 3
min.insync.replicas   = 2
acks                  = all
```

Three copies, two must confirm each write, and the producer waits. **You survive the loss of any one broker with zero data loss and no interruption to writes.** Lose a second and writes stop, which is the system correctly refusing data it cannot protect. (`min.insync.replicas=3` blocks writes whenever any broker restarts; `1` guarantees nothing.)

### The setting that will lose your data

**`unclean.leader.election.enable`** decides what happens when every ISR member for a partition is down and only an out-of-sync replica remains, one missing the last few thousand records.

| Value | Behaviour | CAP choice (§1.4) |
|---|---|---|
| `true` | promote the stale replica; partition comes back, **those acknowledged records are gone forever**, and consumers see offsets rewind | availability |
| `false` | partition stays offline until a replica with the data returns | consistency |

Keep it `false` unless you have consciously decided that, for this topic, being up matters more than being correct.

### How the data is stored

A partition is a directory of **segment** files (by default one gigabyte or seven days each), plus index files mapping offsets and timestamps to positions. Retention deletes whole segments, never individual records, so it is coarse: `retention.ms` (a week by default) or `retention.bytes`.

### Compaction: turning a log into a table

Set `cleanup.policy=compact` and Kafka stops deleting by age. Instead, **compaction** keeps **the most recent record for each key** and discards older ones. A record with a `null` value is a **tombstone**, meaning "this key is deleted", and is itself removed after a grace period.

The topic is now a **replayable snapshot of current state**: a table stored as a log. Kafka keeps consumer offsets this way, and Kafka Streams and Flink back their state stores with compacted changelog topics so state survives losing the cluster. For Lantern, a `document-changes` topic compacted on `document_id` gives you the current version of every document when read from the start — exactly what rebuilding the index after a Chapter 3 model change requires.

### Two mechanisms worth knowing by name

- **Zero-copy.** Kafka uses the `sendfile` system call to move bytes from the filesystem cache straight to the network socket, never copying them through application memory. Consumers reading *recent* data are served from the OS page cache without touching disk. That is why Kafka wants most RAM left to the OS rather than its JVM heap — the same reasoning as OpenSearch in §2.11.
- **Tiered storage.** Modern Kafka can offload older segments to object storage, keeping only recent data on broker disks. Long retention becomes affordable, so Kappa architecture becomes practical: keep a year of history and replay it.

### A note on ZooKeeper

Older Kafka kept metadata in a separate ZooKeeper cluster. Modern Kafka uses **KRaft**, where controller nodes run Raft (§1.6) internally: fewer moving parts, faster failover, better metadata scalability. Docs about ZooKeeper ensembles describe the legacy mode.

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

`send()` does not send anything. It adds the record to an in-memory batch per partition and returns; a background thread ships batches, and the callback fires on acknowledgement.

The trap: `send()` returns a `Future`, and calling `.get()` on it per record makes every send a blocking round trip. That **cuts throughput by roughly two orders of magnitude**, because it kills all batching and pipelining. Use the callback.

### The batching knob, again

**`linger.ms`** is how long the producer waits to fill a batch: `0` means lowest latency and smallest batches; `20` trades up to twenty milliseconds for much larger ones.

This is §1.9's batching trade-off, with a bonus: compression is applied **per batch**. A batch of two hundred similar records often compresses three to five times better than two hundred records compressed one by one, which cuts network, disk, and replication traffic together. A small `linger.ms` is often the cheapest producer win available, and `zstd` or `lz4` compression is nearly always worth the CPU.

### When the buffer fills

`buffer.memory` bounds the accumulator. When brokers are slow or unreachable, it fills, and `send()` **blocks** for up to `max.block.ms` before throwing.

That is **backpressure** (§1.6) working correctly. If the pause propagates back to the data source, the pipeline is healthy; if you catch and log the exception, you are silently dropping records under load.

### Exactly-once writes, for free

Set `enable.idempotence=true` (the default in current clients). The producer gets a **producer ID** and attaches an increasing **sequence number** to each record, per partition. The broker remembers the highest sequence it has seen per producer and partition, and when a retry arrives with a sequence it already accepted, **the broker discards it** and reports success. That is §1.7's idempotency-key pattern, built into the protocol.

It also fixes reordering: without it, retries plus multiple in-flight requests can land batch two before a retried batch one. With it, ordering survives retries for up to five in-flight requests. Leave it on.

### Transactions

Idempotence handles duplicate *writes*. **Transactions** give atomicity across multiple partitions and, the real reason they exist, between consuming and producing.

```java
producer.initTransactions();
producer.beginTransaction();
producer.send(record1);
producer.send(record2);
producer.sendOffsetsToTransaction(offsets, groupMetadata);   // ← the important line
producer.commitTransaction();
```

The marked line writes the consumer's **offset commit inside the same transaction as the output records**. Either both happen or neither does, closing the window that causes duplicates and gaps. Consumers with `isolation.level=read_committed` never see records from uncommitted or aborted transactions.

This is Kafka's exactly-once processing, used under the hood by Kafka Streams and Flink. It costs perhaps ten to twenty percent of throughput plus latency at commit boundaries, and it only covers Kafka-to-Kafka pipelines — see §5.6.

## 5.5 Consuming

### Consumer groups

Consumers sharing a `group.id` form a **consumer group**, and Kafka assigns each partition to **exactly one** consumer in it.

```
Topic with 4 partitions

Group "indexer", 2 consumers:        Group "analytics", 1 consumer:
   C1 → P0, P1                          C1 → P0, P1, P2, P3
   C2 → P2, P3
```

Within a group, partitions are divided, so each record is processed once. Across groups, each receives **all** the records at its own pace.

Rule two from §5.2 bites here: with four partitions, a fifth consumer in a group **sits completely idle**. Partition count is a capacity decision.

### Offsets, and where you commit them

An **offset** is a group's position in a partition: the next record it intends to read. Committed offsets live in the internal compacted topic `__consumer_offsets`, since what matters is the latest offset per group and partition (§5.3).

`enable.auto.commit` defaults to **true**. It commits the current position every five seconds **on a timer, from a background thread, whether or not you have finished processing.** Trace the failure:

- At 0 s, `poll()` returns offsets 100–130 and you start processing.
- At 5 s, the background thread commits offset 130.
- At 6 s, you have reached offset 115 and the process crashes.
- On restart, the group resumes from **130**. Offsets 115–130 are never processed, and nothing records that they were skipped.

Auto-commit gives you **at-most-once** delivery with silent loss, from a default. For Lantern's indexer, that means documents randomly missing from search. The fix:

```java
props.put("enable.auto.commit", false);

while (running) {
    var records = consumer.poll(Duration.ofMillis(500));
    process(records);            // must be idempotent
    consumer.commitSync();       // commit AFTER processing
}
```

Process, then commit. A crash before the commit redelivers the records — at-least-once — which is harmless because `process` is idempotent (§4.7). This is §1.7's ack-placement diagram in three lines of code.

### Where to start when there is no position

`auto.offset.reset` applies when a group has no committed offset: a new group, or one whose offset has aged out of retention.

| Value | Behaviour | Accidental failure |
|---|---|---|
| `latest` (default) | read only new records | you meant a full rebuild and silently skipped all history |
| `earliest` | read from the beginning | a new group starts reprocessing a year of data nobody expected |

Set it explicitly, and know which one you meant.

### Rebalancing, and why it hurts

When a consumer joins, leaves, or is presumed dead, the group **rebalances**: partitions are reassigned among the members. With the classic eager protocol it is stop-the-world: **every** consumer revokes **all** its partitions, sometimes for tens of seconds.

| Cause | What happens | Fix |
|---|---|---|
| **Slow processing** (most common) | a batch takes longer than `max.poll.interval.ms` (5 min default) between `poll()` calls; the group declares you dead, and your later commit fails because you no longer own the partitions | lower `max.poll.records`, or process faster |
| **Missed heartbeats** | no heartbeat for `session.timeout.ms` (45 s) and you are evicted — §1.5's failure-detection trade-off | tune the timeouts; too short evicts healthy consumers, too long tolerates dead ones |
| **Deployments** | every instance restarted in a rolling deploy triggers a rebalance | **static membership** via `group.instance.id` |

**Static membership** gives a consumer a stable identity, so if it restarts within its session timeout it **reclaims its own partitions** without a group-wide rebalance. Rolling deploys become non-events. Also use the **cooperative sticky assignor** (the default in recent versions), which revokes only the partitions that must move. And implement a `ConsumerRebalanceListener` to commit and flush in `onPartitionsRevoked`.

### Lag, the metric that tells you everything

```
lag = log end offset (latest produced)  −  committed offset (consumer position)
```

**Lag** is how far behind the consumer is. If you monitor one thing, monitor this; its shape diagnoses most problems.

| Shape | Meaning | Action |
|---|---|---|
| steadily increasing | consumers can't keep up | add consumers up to partition count; then add partitions or speed up processing |
| spiky | intermittent stalls: GC, slow downstream, rebalances | find the stall |
| one partition growing, others flat | **key skew** (§5.2) | change the key; capacity won't help |
| flat and high | keeping up, but a backlog was never cleared | clear the backlog |

Also monitor lag in **time**: "forty thousand records behind" could be two seconds or two hours. Time lag (now minus the timestamp of the next unprocessed record) compares directly with your freshness SLA (§4.6), so it makes the better alert.

## 5.6 What you actually get, end to end

| | Producer | Consumer | Result |
|---|---|---|---|
| **At-most-once** | `acks=0/1` | auto-commit | fast; loses records |
| **At-least-once** | `acks=all`, idempotent | commit after processing | **the default choice**; duplicates possible |
| **Exactly-once** | transactions | `read_committed`, offsets in transaction | correct; ~10–20% slower; Kafka→Kafka |

> **Kafka's exactly-once guarantee covers Kafka → process → Kafka. The moment you write to an external system, it does not apply.**

Kafka's transactions commit records into Kafka's own log alongside the offset; OpenSearch, PostgreSQL, S3, and HTTP APIs are not part of them. Spanning them would need two-phase commit, which (§1.7) is slow and blocks on coordinator failure.

So Lantern's indexer, which reads Kafka and writes OpenSearch, uses the pattern this book has now built three times: **at-least-once delivery from Kafka, plus an idempotent write to OpenSearch keyed on `{document_id}:{chunk_index}`.** A crash redelivers records, which overwrite identical documents, so the final state is correct, at no cost. Most production pipelines do this. When someone says their pipeline is exactly-once, this is usually, and quite properly, what they mean.

## 5.7 The ecosystem

**Kafka Connect** moves data in and out of Kafka using pluggable connectors (JDBC, S3, OpenSearch, MongoDB, and many more), with offset management, scaling, retry, and error handling already solved. **For straightforward ingest or egress, use a connector rather than writing a consumer.** A hand-written Kafka-to-S3 consumer eventually grows offsets, retries, schema handling, and a DLQ: Connect, reimplemented with fewer tests. Do configure `errors.tolerance` and a **dead-letter queue** topic; the defaults are stricter than you want.

**Debezium and change data capture.** Three ways to get PostgreSQL changes into Kafka:

| Approach | How | Problem |
|---|---|---|
| **Dual write** | application writes to both the database and Kafka | not atomic; any failure between the writes leaves the systems permanently inconsistent, with no record of it |
| **Polling** | `SELECT * FROM documents WHERE updated_at > ?` every few seconds | misses deletes; misses intermediate states (fine for search, wrong for audit); relies on every code path maintaining `updated_at` |
| **Change data capture** | read the database's **write-ahead log** (PostgreSQL WAL, MySQL binlog) | none of the above |

**Change data capture (CDC)** reads the authoritative, ordered record of every committed change. **Debezium** turns each change into a Kafka record with before and after images: every insert, update, and delete, in commit order, with no application changes. It is the WAL of §1.6, built for crash recovery, repurposed as an integration point; CDC just reads it.

**Kafka Streams** is a Java library — not a cluster — for stateful stream processing: `KStream` and `KTable`, joins, windowed aggregations, with state in local RocksDB backed by a compacted changelog topic (§5.3). Good when processing lives in a JVM microservice and both ends are Kafka; poor for heavy analytics or non-Kafka sources. **ksqlDB** puts SQL on top of it.

**Schema Registry** — see §4.5. Use it.

**MirrorMaker 2** replicates topics between clusters for disaster recovery and geographic distribution. **Cruise Control** automates partition rebalancing and broker capacity management, worth it once you have more brokers than you can think about individually.

## 5.8 Operating it

### What to watch

Beyond consumer lag (§5.5):

| Metric | Healthy | Why |
|---|---|---|
| **Under-replicated partitions** | **zero** | non-zero means a follower left an ISR and your durability margin is gone; treat as urgent |
| **Offline partitions** | **zero**, always | some data is unavailable right now |
| ISR shrink/expand rate | low | flapping means struggling brokers: slow disks, network saturation, GC |
| Produce and fetch p99 latency | stable | — |
| Disk usage and growth rate | headroom | retention is only as good as the disk it fits on |
| Active controller count | exactly one | — |

### Resource shape

Kafka is **disk-I/O and network bound**, almost never CPU bound, so fast disks and page cache matter most. As §5.3 noted, **leave most RAM to the OS page cache**; around six gigabytes of heap is typical even on a large broker. Same principle as OpenSearch (§2.11): both rely on the OS to cache memory-mapped files, and a greedy heap hurts both.

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

That last row is **head-of-line blocking**. A record that always throws — a malformed payload, a schema violation — is retried and fails forever, and **everything behind it in the partition stops**, because a partition is a strict serial log. Route it to a dead-letter queue and skip it.

### A checklist for a new topic

1. **What is the key**, what ordering does it buy, and is there skew risk?
2. **How many partitions** — consumer parallelism plus headroom?
3. **Retention**: delete or compact, and for how long?
4. **Durability**: RF=3, `min.insync.replicas=2`, `acks=all`, unclean election off?
5. **Schema** registered, with which compatibility mode?
6. **Who consumes it**, and is their processing idempotent?
7. **Is there a DLQ**, and an alert on lag?

## 5.9 Lantern gets a nervous system

**Ingest.** Debezium reads PostgreSQL's write-ahead log and publishes every document insert, update, and delete to `document-changes`, **keyed by `document_id`**: twenty-four partitions, RF=3, `min.insync.replicas=2`, `acks=all`, unclean leader election disabled. Records are Avro, with schemas in a registry set to backward compatibility. The change stream keeps seven days; a parallel **compacted** topic keyed by `document_id` holds every document's current version for replay.

**Indexing.** A consumer group called `indexer` runs twelve consumers with auto-commit disabled. For each batch it chunks the text, calls the embedding service, and sends an OpenSearch `_bulk` request keyed on `{document_id}:{chunk_index}`, so a redelivery is a no-op. Only then does it commit offsets. OpenSearch `429`s are retried with backoff; records that fail deterministically go to `document-changes-dlq` with the exception attached. Static membership keeps deploys from rebalancing the group, and an alert fires when time lag exceeds thirty seconds.

**Archive.** A second, independent group archives the raw change stream to S3 as Avro: the raw layer of §4.2, from which everything can be re-derived.

The database no longer knows the search index exists. The indexer can be stopped, redeployed, or rewound three hours without anything upstream noticing. When Chapter 3's embedding model is upgraded, the replay path already exists: read the compacted topic from offset zero into a new index, verify, swap the alias.

The latency budget:

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

That meets the requirement, and if we ever need it faster, look at the refresh interval first, not the pipeline.

---

## Key takeaways

- Kafka is an append-only log, not a queue: reads don't consume, and consumers control their own position, so data fans out and can be replayed.
- Ordering holds only within a partition; parallelism is capped at one consumer per partition per group.
- Key by the entity that needs ordering, and nothing coarser; a skewed key cannot be fixed by adding capacity.
- Over-provision partitions 2–3× at creation, because adding partitions later breaks key-to-partition mapping.
- `replication.factor=3`, `min.insync.replicas=2`, `acks=all`, and unclean leader election off survive any single broker loss with zero data loss.
- Compacted topics keep the latest record per key, turning a log into a replayable table.
- Leave `enable.idempotence` on; it removes retry duplicates and reordering for free.
- Disable auto-commit and commit after processing; otherwise a crash silently skips records.
- Watch lag in records and in time; its shape diagnoses most problems.
- Exactly-once covers only Kafka to Kafka; for external sinks, use at-least-once plus an idempotent write.

## Where we are

Lantern's indexer is stateless: it chunks, embeds, and writes each record on its own, with no memory of what came before. The moment you want to count edits per document per hour, or join document changes against view events, you need state that survives crashes while late, out-of-order events arrive. That harder problem gets its best answer in Chapter 6.
