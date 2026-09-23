# Chapter 9 — The Whole Machine

This chapter puts Lantern back together: the full architecture, one edit traced from commit to search result, and the eight ideas that kept reappearing across the book. Then it tests you and gives you a plan.

**By the end you'll be able to:**

- Explain why each component of Lantern sits where it does, and which section justified it.
- Trace a single edit end to end, naming the guarantee at each hop and the biggest term in the latency budget.
- Explain how idempotency, ordering, failure, consistency, and backpressure run through the whole system.
- Recognise the eight recurring ideas in a system you have never used.
- Test yourself with 48 questions and plan four weeks of hands-on practice.

## 9.1 Lantern, complete

Eight chapters ago Lantern was one server running `SELECT * FROM documents WHERE body LIKE '%kafka%'`. Here is what it is now.

```
┌──────────────┐                    ┌─────────────────────────────────────┐
│ PostgreSQL   │   Debezium reads   │              KAFKA                  │
│ source of    │───the WAL─────────►│  document-changes                   │
│ truth        │      §5.7          │   key = document_id          §5.2   │
└──────────────┘                    │   24 partitions, RF=3               │
┌──────────────┐                    │   min.insync=2, acks=all     §5.3   │
│ App events   │───────────────────►│  document-views                     │
└──────────────┘                    │  document-changes-compacted  §5.3   │
                                    └──────┬────────┬─────────┬───────────┘
                                           │        │         │
              ┌────────────────────────────┘        │         └──────────────┐
              │                                     │                        │
    ┌─────────▼──────────┐          ┌───────────────▼────────┐   ┌───────────▼─────────┐
    │ FLINK        §6    │          │ SPARK            §7    │   │ Kafka Connect  §5.7 │
    │ chunk, embed,      │          │ full reindex,          │   │ → S3 raw (bronze)   │
    │ index; trending    │          │ backfill, analytics    │   │ Avro          §4.2  │
    │ windows, watermarks│          │ availableNow           │   └───────────┬─────────┘
    └─────────┬──────────┘          └───────────┬────────────┘               │
              │                                 │                            ▼
              │                                 │              ┌──────────────────────────┐
              │                                 └─────────────►│ S3 LAKEHOUSE       §4.4  │
              │                                                │ Iceberg / Parquet        │
              │                                                │ bronze → silver → gold   │
              │                                                └──────────────────────────┘
              ▼
┌───────────────────────────────────────────────┐
│               OPENSEARCH            §2, §3    │
│  documents_v3   (alias: documents)     §2.10  │
│   ├── text fields, custom analyzers     §2.5  │
│   ├── keyword fields (filters, aggs)    §2.4  │
│   └── knn_vector 768d HNSW int8         §3.5  │
│  12 primaries × 1 replica, ~25 GB each  §2.11 │
└──────────────────────┬────────────────────────┘
                       │
        BM25 ∪ k-NN → RRF → cross-encoder rerank    §3.7
                       │
                       ▼
                 Search API → users
                       │
                       ▼
          click logs → Kafka → judgment set → CI eval   §3.9
```

Every box was a decision, with a section number, because none was arbitrary.

### Why each piece is where it is

**PostgreSQL is the source of truth; OpenSearch is derived (§2.1).** The most important structural decision in the system. The index holds nothing that cannot be rebuilt from the database, so a corrupted index or a botched model upgrade is recoverable: you rebuild. Where the index is the only copy, every index problem is a data-loss problem.

| Piece | Why it is there | Section |
|---|---|---|
| CDC, not dual writes | Two non-atomic writes drift apart silently. Debezium reads the WAL PostgreSQL writes anyway: every change, in commit order, with no app changes. | §5.7 |
| Kafka keyed by `document_id` | Changes to one document are strictly ordered; different documents run fully in parallel. | §5.2 |
| Flink on the hot path | Low latency, stateful, event-time correct. An edit is searchable in seconds. | §6 |
| Spark on the cold path | The eleven-hour full reindex, backfills, two years of analytics. Throughput is everything; latency is irrelevant. | §7 |
| S3 raw layer | Everything can be re-derived from it. It turns a bug into a reprocess, not an incident. | §4.2 |
| Lakehouse | Analytics never touch PostgreSQL (§8.2). Columnar files, bronze → silver → gold (§8.8), a transaction log making a folder act like a table (§8.6). Gold flows back as ranking features (§8.11). | §8 |
| Alias in front of the index | A reindex, mapping change, or model upgrade is an atomic swap with instant rollback. | §2.10 |

## 9.2 One edit, end to end

An employee fixes a typo in *Diagnosing CrashLoopBackOff* and clicks save.

| # | Step | What happens | Guarantee |
|---|---|---|---|
| 1 | PostgreSQL commits | The change is written to the WAL and `fsync`ed before the commit returns. | Durable: survives a crash (§1.6). |
| 2 | Debezium reads the WAL | Produces a before/after record to `document-changes`, keyed by `document_id`. Tracks its WAL position, so it resumes after a crash. | At-least-once, in commit order (§5.7). |
| 3 | Kafka accepts | Appended to partition `murmur2(document_id) % 24`. With `acks=all` and `min.insync.replicas=2`, the leader waits for two of three replicas. | Survives any single broker loss; ordered within the partition (§5.3). |
| 4 | Flink consumes | The subtask owning that partition reads it; the offset is in every checkpoint. | Position is recoverable (§6.4). |
| 5 | Flink processes | Six overlapping chunks (§3.3), embedded via Async I/O so the thread doesn't block (§6.6). Six 768-d vectors return, normalised and int8-quantized. | None yet: in-flight work. |
| 6 | Flink writes to OpenSearch | Bulk-indexes `_id`s `{document_id}:0` to `{document_id}:5`, deletes chunks ≥ 6 in case the document shrank, checks per-item errors (§2.10). | Idempotent: a replay of 4–6 changes nothing (§4.7). |
| 7 | OpenSearch indexes | Routed to its shard, written to the translog, replicated before acknowledging. | Durable and crash-safe (§2.6). |
| 8 | Flink checkpoints | At the next barrier, state and Kafka offset are snapshotted. Only now is the work "done". | A crash before this replays 4–6, which step 6 makes safe (§6.4). |
| 9 | OpenSearch refreshes | Within one second, buffered documents become a searchable segment. | Visible to search (§2.6). |
| 10 | A user searches | BM25 over title and body plus k-NN, both pre-filtered by access control. RRF fuses the lists; the top 150 are reranked by a cross-encoder; the top 10 return. | (§3.7) |

```
PostgreSQL commit → Debezium              ~100 ms – 1 s
Kafka produce + replicate                 ~5 – 50 ms    (mostly linger.ms)
Flink consume + chunk                     ~10 – 50 ms
embedding inference (batched)             ~10 – 50 ms
OpenSearch bulk write                     ~20 – 100 ms
OpenSearch refresh interval               ~1 s           ← largest term
                                          ──────────────
end to end                                ~1.5 – 3 s
```

The largest term is a *configuration default*, not the pipeline: what you assume is slow often isn't. You only learn that by measuring, which also shows where to push if the requirement tightens.

## 9.3 How the concerns thread through

Some concerns live in no single component; they run through all of them.

### Idempotency, at every hop

| Hop | Mechanism | Section |
|---|---|---|
| Producer → Kafka | producer ID + sequence number; broker drops retries | §5.4 |
| Kafka → Flink | checkpoints record offsets; sources rewind on restart | §6.4 |
| Flink → OpenSearch | `_id = {document_id}:{chunk_index}` — replay overwrites | §2.10, §4.7 |
| Spark → lake | overwrite the partition, never append | §4.7 |
| Lake → OpenSearch | deterministic `_id`, per-item error checks | §2.10 |

Notice what is missing: no distributed transaction anywhere, no two-phase commit, no coordinator that can fail and block everything. Every hop is at-least-once, every write is idempotent, and the composition is **exactly-once in effect**.

This is the most valuable pattern in the book. It turns §1.7's impossible problem, exactly-once delivery over a network, into an ordinary one at no throughput cost. When someone proposes a distributed transaction to fix duplicates, offer this instead.

### Ordering

Kafka orders within a partition, so keying by `document_id` gives per-document order (§5.2). Flink keeps it through `keyBy(document_id)`, since one subtask owns each key.

But OpenSearch is last-write-wins per document, so a retry crossing a newer edit could let a stale version overwrite a fresh one. The guard is external versioning: index with `version_type: external` and `version = source_updated_at_millis`, and the engine **rejects** any write with a lower version than it holds. This is §1.6's fencing token applied to documents, in one line of configuration.

### Failure, component by component

| What fails | What happens | Section |
|---|---|---|
| A Kafka broker | A new leader is elected from the ISR; nothing acknowledged is lost. Producers retry; the idempotent producer drops duplicates. | §5.3, §5.4 |
| A Flink TaskManager | Restart from the last checkpoint; offsets rewind up to a minute; idempotent writes absorb the replay. | §6.4 |
| An OpenSearch node | Replicas are promoted, the cluster goes yellow, copies rebuild in the background. Searches continue. | §2.3, §2.11 |
| The embedding service | Calls retry with backoff; backpressure stops the Kafka source; lag grows and alerts. **Nothing is lost**: records wait in Kafka and drain on recovery. | §6.2 |
| A bad deploy corrupts three days of index | Replay `document-changes` from three days ago into a *new* index (or rebuild from S3 raw or the compacted topic if retention has passed). Verify with the judgment set. Swap the alias. | §4.7, §2.10 |

That last row is the payoff for the whole book. Elsewhere it is an all-hands incident; here it is a scheduled job and an alias swap, because raw data was kept, writes were idempotent, and the index was never the source of truth.

### Consistency, chosen deliberately

Lantern is **eventually consistent** by design: an edit is durable in PostgreSQL immediately and searchable about two seconds later. Those seconds buy batched writes, twelve shards indexing in parallel, uncoordinated replica reads, and full availability through a machine failure. That is §1.4's trade-off and §1.8's PACELC "else" branch.

It is a good trade only because it was **decided** consciously, the window is **written down** ("searchable within five seconds at p99"), and it is **monitored**, so a four-minute backlog is caught before a user reports it. The failure mode is choosing it by accident and learning the window from a support ticket.

### Backpressure, at every stage

- **Flink** propagates it automatically from sink to source (§6.2).
- **Kafka's producer buffer** blocks when brokers are slow (§5.4).
- **Spark** uses `maxOffsetsPerTrigger` as a manual rate limit (§7.4).
- **OpenSearch** returns `429` when write queues fill, and the client backs off (§2.10).

Everywhere, overload means *slow down*, never *drop data silently*: every component was built by people who had read §1.6.

### Where the cost is

Cost drives more architecture decisions than performance does.

- **HNSW vectors in RAM:** 640 GB before quantization, ~180 GB after int8 (§3.5). That often decides whether a project is approved.
- **Embedding inference:** two hundred million chunks, re-billed on every model change. Caching by `hash(text) + model_version` means a reindex only re-embeds what changed (§3.9).
- **OpenSearch node count**, driven by shard sizing and filesystem cache needs (§2.11).
- **Always-on streaming clusters**, which is why analytics uses `availableNow` on a schedule rather than a permanent job (§7.3).

## 9.4 The eight ideas

This is what I most want you to keep. Across four very different systems, the same few ideas kept reappearing under different names. Once you see them, new systems stop being new: you recognise the furniture.

### 1. Partitioning gives parallelism, and brings skew

Kafka *partitions*, OpenSearch *shards*, Spark *partitions*, Flink *keyed subtasks*, lake *partition directories*. Split the data so many machines work at once. The failure is always **skew**: one partition gets far more than its share, and the system runs at the speed of its unluckiest piece. The secondary cost is scatter-gather, which runs at the speed of the *slowest* partition, so more partitions is not always better (§1.3).

For any new system, ask: *what is the unit of partitioning, how is it chosen, and what is the characteristic skew?*

### 2. Replication gives durability, and forces a choice

Kafka's ISR, OpenSearch replica shards, HDFS blocks, S3's internals. Keep N copies so losing one loses nothing. The knob is always **how long the leader waits**: synchronous is safe and slow, asynchronous fast and lossy. The real answer is a quorum: `acks=all` with `min.insync.replicas=2` at RF=3 survives one failure without waiting on the slowest replica (§1.4, §5.3).

### 3. A durable log is the foundation of recovery

PostgreSQL's WAL, OpenSearch's translog, Spark's write-ahead log, Flink's checkpoints, and **Kafka**, which made the idea the entire product. **Write down what you are about to do, durably, before you do it**, and any crash is recoverable by replay. Debezium reusing PostgreSQL's WAL is one log serving two masters (§1.6, §5.7).

### 4. Batching trades latency for throughput

Kafka's `linger.ms`, OpenSearch's `_bulk` size, Spark's trigger interval, Flink's checkpoint interval, lake target file size. One curve, five times: wait longer, amortise overhead, get more throughput, and make every item wait. No setting is fast in both senses (§1.9).

### 5. At-least-once plus idempotency equals exactly-once, cheaply

Exactly-once *delivery* over a network is impossible. Exactly-once *effect* is easy: write to a deterministic key so a replay overwrites rather than accumulates. §9.3 traced this through every hop of Lantern. **When in doubt, make the write idempotent.**

### 6. Event time is not processing time, and clocks lie

Watermarks in Flink and Spark, late data, temporal joins, reprocessing determinism. Clocks drift and jump, so you cannot order events by comparing timestamps from different machines (§1.5). *When it happened* differs from *when you found out*, so every window needs an explicit choice of how long to wait and what to do with late arrivals (§6.5).

### 7. Approximation is a legitimate engineering choice

BM25 relevance, `terms` aggregation accuracy, HyperLogLog distinct counts, t-digest percentiles, HNSW recall. At scale exactness is often unaffordable, and nobody needs exactly 8 431 947 distinct viewers. What matters is that the approximation is **explicit, bounded, and understood**, so nobody builds billing on a HyperLogLog estimate (§2.8, §3.5).

### 8. Everything must be rebuildable

Keep raw data. Make writes idempotent. Put an alias in front of the index. Version the checkpoint path. Build alongside, verify, then atomically swap. That last pattern appeared four times (alias swaps, Kappa reprocessing, Iceberg's metadata pointer, the shadow-table backfill): it is the general shape of safe change.

The payoff is cultural too. A bug becomes *"reprocess Tuesday"*, not an incident with a postmortem, and over years that shapes how much risk your team will take and how fast it moves.

## 9.5 Forty-eight questions

Reading feels like understanding; questions test it. If you can answer these unaided, you have what this book set out to give you. If not, follow the section reference.

**The machine that isn't one**
1. Why can you never distinguish a crashed node from a slow one, and what do real systems do instead? (§1.5)
2. What does `R + W > N` guarantee, and why does the arithmetic work? (§1.4)
3. State CAP correctly. Now state PACELC and explain why it governs more of your decisions. (§1.8)
4. Why does the p99 of a scatter-gather query matter more than the median? (§1.9)
5. Give three ways to make a non-idempotent operation idempotent. (§1.7)
6. Why are consensus clusters always an odd number of nodes? (§1.6)
7. What is a fencing token, and what disaster does it prevent? (§1.6)

**Finding things**
8. Why can't a database answer "documents about Kafka" well, for four distinct reasons? (§2.1)
9. Why can't you change the number of primary shards after index creation? (§2.6)
10. A `term` query on a `text` field returns nothing. Why? (§2.4)
11. Name the three "write to disk" operations in OpenSearch and what each guarantees. (§2.6)
12. Why is a `filter` clause faster than an equivalent `must` clause? (§2.8)
13. Your cluster is yellow. What does that mean, and how urgent is it? What about red? (§2.11)
14. Why does BM25 saturate term frequency, and what breaks without saturation? (§2.9)
15. How do you change a field's mapping type with zero downtime? (§2.10)
16. Why is a `terms` aggregation's top-10 only approximate? (§2.8)
17. Why is the JVM heap capped at 31 GB, and why only half of RAM? (§2.11)

**Meaning as geometry**
18. Why does cosine similarity ignore magnitude, and when is that the wrong choice? (§3.4)
19. Explain HNSW in two sentences using the express-train analogy. (§3.5)
20. Why do k-d trees fail at 768 dimensions? (§3.5)
21. Why can a selective metadata filter *destroy* ANN recall, and what are the three fixes? (§3.8)
22. Why is RRF usually preferred to a weighted score sum? (§3.7)
23. What is the difference between a bi-encoder and a cross-encoder, and why can't the cross-encoder do retrieval? (§3.7)
24. Which metric do you optimise for retrieval, and which for final ranking? Why the difference? (§3.9)

**Moving data**
25. Why is Parquet ten times faster than JSON for `SELECT avg(x)`? Give both reasons. (§4.4)
26. What is backward versus forward compatibility, and which lets you deploy producers first? (§4.5)
27. Name the four data-quality dimensions and one concrete check for each. (§4.6)
28. Why is "overwrite the partition" better than "append" in a batch job? (§4.7)
29. List three things that make a job non-deterministic and therefore unsafe to reprocess. (§4.7)
30. What is the small files problem, and what number should you target? (§4.4)

**The log**
31. Why is "Kafka is a message queue" the wrong mental model, and what changes? (§5.1)
32. What exactly does `acks=all` + `min.insync.replicas=2` + RF=3 buy, and what does each part do? (§5.3)
33. You have 4 partitions and 6 consumers in one group. What happens? (§5.5)
34. Why does auto-commit give you at-most-once on crash? Trace the failure. (§5.5)
35. When would you use a compacted topic rather than time-based retention? (§5.3)
36. Why is dual-writing to a database and a search index broken, and what replaces it? (§5.7)

**Time**
37. Explain checkpoint barriers. Why don't they stop the world? (§6.4)
38. Your Flink job consumes records but emits nothing. What is the most likely cause? (§6.5)
39. What does the watermark delay actually trade off, and how should you pick it? (§6.5)
40. Why does a transactional sink mean downstream latency equals the checkpoint interval? (§6.4)

**The unbounded table**
41. Why do DataFrames beat RDDs, and what is the mechanism? (§7.2)
42. Why is `spark.sql.shuffle.partitions = 200` usually wrong, and worse in streaming? (§7.2, §7.7)
43. Why must a streaming aggregation have a watermark — for two separate reasons? (§7.5)
44. What does `foreachBatch` give you that the built-in sinks don't? (§7.4)
45. Name the three conditions for Structured Streaming's exactly-once guarantee. (§7.6)

**The whole machine**
46. Trace a single document edit from a PostgreSQL commit to a user seeing it, naming the guarantee at each hop. (§9.2)
47. A bug corrupted three days of your search index. Walk through recovery with zero downtime. (§9.3)
48. Lantern has no distributed transactions anywhere. Why doesn't it need any? (§9.3)

## 9.6 How to actually learn this

The essential advice first: **break things on purpose, somewhere safe.** The understanding that matters comes from watching a system misbehave and working out why, not from prose.

| Week | Read | Do |
|---|---|---|
| 1 — foundations | Chapters 1 and 4 | Every other chapter reuses these. Don't skip Chapter 4: §4.7 on idempotency is the most practically useful section in the book. |
| 2 — search | Chapter 2 | `docker run opensearchproject/opensearch`, index a few thousand documents, and spend an hour with `_analyze`, `_explain`, `bool` queries, and aggregations. Make mistakes on purpose: a `term` query on a `text` field; searching for a document right after indexing it. |
| 3 — semantic | Chapter 3 | Run `sentence-transformers` over ten thousand documents, index as `knn_vector`, and compare BM25, pure vector, and RRF on twenty queries of your own, including a product code and a paraphrase. Watching BM25 win on one and lose on the other makes §3.6 obvious. |
| 4 — streaming | Chapter 5, then 6 or 7 | Run a local Kafka. Produce, consume, **kill the consumer mid-batch**, and watch duplicates appear. Make the write idempotent and watch them stop mattering. Fifteen minutes; teaches §1.7, §4.7, §5.5, and §5.6 at once. |

**Then re-read Chapter 9**, especially §9.4. Every production incident you see for the next two years will be an instance of one of those eight ideas, and recognising which one is most of the diagnosis.

## 9.7 Where to go next

- ***Designing Data-Intensive Applications*, Martin Kleppmann.** The best book on Chapters 1 and 4's material, going much deeper on replication, consistency, and distributed transactions. If you read one thing next, read this.
- **OpenSearch documentation**, especially search and tuning, and the older *Elasticsearch: The Definitive Guide*: dated APIs, still the clearest take on analysis and relevance.
- ***Kafka: The Definitive Guide*** (O'Reilly), especially the chapters on reliability and exactly-once semantics, which cover the cases §5.6 summarised.
- **The Flink documentation's "Concepts" section.** Rarely, docs that are excellent: its event-time and watermark pages are the best explanation of that material in existence, including mine.
- ***Spark: The Definitive Guide*** plus the Structured Streaming Programming Guide.
- **Pinecone, Weaviate, and Qdrant engineering blogs:** unusually, the clearest writing on ANN and hybrid retrieval.

And **read the original papers** for the systems you use most: Dynamo, Kafka, Chandy–Lamport, Raft. They are shorter and clearer than most writing about them, and they say what problem the authors were solving, which is usually what makes the design make sense.

---

## A closing thought

We started with a program that worked and broke it by adding machines. Partitioning, replication, quorums, write-ahead logs, watermarks, idempotency: all scaffolding around the fact that a distributed system cannot tell dead from slow, its clocks lie, and its messages arrive twice or not at all.

Look at what it holds up. Lantern searches two hundred million documents in forty milliseconds, understands a question phrased in words that appear nowhere in the answer, stays up when machines die, reflects an edit in under three seconds, and can rebuild itself on request. None of that was possible on the one server we started with, at any price.

The constraints are real and unavoidable. What makes the field worth working in is how much you can build once you stop fighting them and design for them.

## Key takeaways

- PostgreSQL is the source of truth and OpenSearch is derived, so every index problem is a rebuild, not a data loss.
- CDC replaces dual writes; keying by `document_id` gives per-document order with full parallelism.
- An edit is searchable in about 1.5–3 seconds, and the biggest term is the refresh interval, a config default.
- At-least-once at every hop plus idempotent writes gives exactly-once effect with no distributed transactions.
- Everywhere in Lantern, overload means *slow down*, never *drop data*.
- Eventual consistency is fine when it is decided, written down, and monitored.
- Eight ideas (partitioning, replication, the log, batching, idempotency, event time, approximation, rebuildability) explain most systems.
- Build alongside, verify, then swap atomically: that is the general shape of safe change.
