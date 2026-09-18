# Chapter 9 — The Whole Machine

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

Every box in that diagram was a decision, and every decision has a section number attached because none of them was arbitrary.

### Why each piece is where it is

**PostgreSQL is the source of truth; OpenSearch is derived.** This is the most important structural decision in the whole system and it was made in §2.1. The search index holds no information that cannot be rebuilt from the database. Which means a corrupted index, a bad mapping, a botched model upgrade, or a six-hour bug are all *recoverable* rather than catastrophic — you rebuild. Systems where the search index is the only copy of something are systems where every index problem is a data-loss problem.

**Changes reach Kafka through CDC, not dual writes.** §5.7 explained why the obvious approach is broken: two non-atomic writes leave the systems permanently inconsistent with no record of the divergence. Debezium reads the write-ahead log PostgreSQL was writing anyway, so we get every insert, update, and delete, in commit order, with no application changes and no dependence on developers remembering to publish an event.

**Kafka is keyed by `document_id`.** All changes to one document are strictly ordered; different documents proceed fully in parallel (§5.2). The ordering requirement was identified first and the key chosen to match it exactly.

**Flink handles the hot path.** Low latency, stateful, event-time correct (§6). An edit is searchable in seconds.

**Spark handles the cold path.** The eleven-hour full reindex, backfills, and analytics over two years of history (§7). Latency is irrelevant; throughput is everything.

**S3 holds the raw layer.** The thing everything can be re-derived from (§4.2). It is the reason a bug is a reprocess rather than an incident.

**The lakehouse answers questions about the system.** Analytical queries never touch PostgreSQL (§8.2); they run over columnar files in the lake, organised bronze → silver → gold (§8.8), with a transaction log making the folder behave like a table (§8.6). And gold flows *back* into the product as document features for ranking (§8.11), which is the loop that makes the analytics layer worth paying for.

**An alias sits in front of the index.** So a reindex, a mapping change, or an embedding model upgrade is an atomic swap with instant rollback (§2.10).

## 9.2 One edit, end to end

Trace a single change. An employee fixes a typo in a document titled *Diagnosing CrashLoopBackOff*, and clicks save.

**1. PostgreSQL commits.** The row is updated. The change is written to PostgreSQL's write-ahead log and `fsync`ed before the commit returns. *Guarantee: durable. If the database crashes now, the edit survives (§1.6).*

**2. Debezium reads the WAL.** It sees the update, constructs a record containing the before and after images, and produces it to `document-changes` keyed by `document_id`. Debezium tracks its own WAL position, so if it crashes it resumes from where it stopped. *Guarantee: at-least-once, in commit order (§5.7).*

**3. Kafka accepts the write.** The record is appended to the partition determined by `murmur2(document_id) % 24`. The leader replicates to its followers and waits for two of three to acknowledge, because `acks=all` and `min.insync.replicas=2`. *Guarantee: durable through the loss of any single broker; strictly ordered within the partition (§5.3).*

**4. Flink consumes it.** The record is read by whichever subtask owns that partition. Flink's Kafka source tracks the offset in its state, which is included in every checkpoint. *Guarantee: the position is recoverable (§6.4).*

**5. Flink processes it.** The document text is chunked into six overlapping pieces (§3.3). Each is sent to the embedding service via Async I/O so the operator thread is not blocked on network latency (§6.6). Six 768-dimensional vectors come back, normalised and int8-quantized. *Guarantee: none yet — this is in-flight work.*

**6. Flink writes to OpenSearch.** A bulk request indexes six documents with `_id` values of `{document_id}:0` through `{document_id}:5`. A seventh operation deletes any chunks with index ≥ 6, in case the document shrank. Per-item errors in the bulk response are checked (§2.10). *Guarantee: idempotent. A replay of steps 4–6 overwrites identical content and changes nothing (§4.7).*

**7. OpenSearch indexes the documents.** Each goes to its routed shard, is appended to the translog, and is replicated to the replica shard synchronously before being acknowledged. *Guarantee: durable and crash-safe (§2.6).*

**8. Flink checkpoints.** At the next barrier, Flink snapshots its state including the Kafka offset. Only now is the work "recorded as done". *Guarantee: if Flink crashes before this, steps 4–6 are replayed — and step 6 being idempotent is what makes that safe (§6.4).*

**9. OpenSearch refreshes.** Within one second, the buffered documents become a searchable segment. *Guarantee: now visible to search (§2.6). This is the largest single term in the latency budget.*

**10. A user searches.** The query runs BM25 across title and body and a k-NN search over the vectors, both filtered by access control as a pre-filter during graph traversal. The two result lists are fused with RRF, the top 150 go to a cross-encoder, and the top 10 are returned (§3.7).

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

Two observations about that budget worth carrying into your own systems. First, the largest term is a *configuration default*, not the pipeline — which is a useful reminder that the thing you assume is slow is often not the thing that is slow. Second, you cannot know any of this without measuring it, and having measured it you know exactly where to push if the requirement tightens.

## 9.3 How the concerns thread through

The interesting thing about an assembled system is that certain concerns are not located in any one component. They run through all of them, and this is where a whiteboard diagram stops being enough.

### Idempotency, at every hop

| Hop | Mechanism | Section |
|---|---|---|
| Producer → Kafka | producer ID + sequence number; broker drops retries | §5.4 |
| Kafka → Flink | checkpoints record offsets; sources rewind on restart | §6.4 |
| Flink → OpenSearch | `_id = {document_id}:{chunk_index}` — replay overwrites | §2.10, §4.7 |
| Spark → lake | overwrite the partition, never append | §4.7 |
| Lake → OpenSearch | deterministic `_id`, per-item error checks | §2.10 |

Read down that column and notice what is *not* there: there is no distributed transaction anywhere in this system. No two-phase commit spanning Kafka and OpenSearch. No coordinator that can fail and block everything. Every hop is at-least-once, and every write is idempotent, and the composition is **exactly-once in effect**.

This is the single most valuable pattern in the book. It converts §1.7's impossible problem — exactly-once delivery over a network — into an ordinary one, at no cost in throughput. When someone proposes a distributed transaction to solve a duplicate problem, this is the alternative to offer.

### Ordering

Kafka guarantees order within a partition, and keying by `document_id` means per-document order (§5.2). Flink preserves it through `keyBy(document_id)`, since one subtask owns that key.

But OpenSearch is last-write-wins per document, so if two updates somehow arrived out of order — say from a retry crossing with a newer edit — a stale version could overwrite a fresh one. The guard is OpenSearch's external versioning: index with `version_type: external` and `version = source_updated_at_millis`, and the engine **rejects** any write whose version is lower than what it already holds. This is §1.6's fencing token, applied to documents rather than leaders, and it is one line of configuration.

### Failure, component by component

**A Kafka broker dies.** A new leader is elected from the ISR. Because two replicas acknowledged every write, nothing acknowledged is lost. Producers experience a brief blip and retry; the idempotent producer discards the resulting duplicates (§5.3, §5.4).

**A Flink TaskManager dies.** The job restarts from the last completed checkpoint. Kafka offsets rewind by up to a minute, and that minute of records is reprocessed. The idempotent OpenSearch write absorbs the duplicates (§6.4).

**An OpenSearch node dies.** Replica shards are promoted to primary. The cluster goes yellow. The allocator rebuilds the missing copies in the background. Searches continue throughout (§2.3, §2.11).

**The embedding service is down.** Flink's Async I/O calls time out and retry with backoff. If they keep failing, backpressure propagates to the Kafka source, which stops reading (§6.2). Lag grows and alerts. **Nothing is lost** — the records are still in Kafka, and when the service recovers, the pipeline drains the backlog. Compare this with a system that drops on failure, or one that buffers unboundedly until it dies.

**A bad deploy corrupts three days of the index.** Replay `document-changes` from the offset three days ago into a *new* index, or — if Kafka retention has passed — rebuild from the S3 raw layer or the compacted topic. Verify with the judgment set. Swap the alias. Nothing was lost because nothing authoritative lived in the index (§4.7, §2.10).

That last one is worth dwelling on, because it is the payoff for every design decision in the book. A three-day corruption of a search index — which in many systems is a genuine emergency involving an all-hands incident channel — is here a scheduled job and an alias swap. Not because anything clever was done at the moment of the incident, but because the raw data was kept, the writes were idempotent, and the index was never the source of truth.

### Consistency, chosen deliberately

Lantern is **eventually consistent**, by design. A document edit is durable in PostgreSQL immediately and findable in search about two seconds later.

In exchange for those two seconds we get: batched writes, twelve shards indexing in parallel, reads served from replicas without coordination, and complete availability during a machine failure. That is §1.4's trade-off and §1.8's PACELC "else" branch, and it is a good trade.

But it is only a good trade because three things are true. Someone **decided** it consciously. The staleness window is **written down** — "an edit is searchable within five seconds at p99". And it is **monitored**, so when a backlog pushes it to four minutes, we know before a user tells us. The failure mode is not choosing eventual consistency; it is choosing it by accident and discovering the window from a support ticket.

### Backpressure, at every stage

Flink propagates it automatically from sink to source (§6.2). Kafka's producer buffer blocks when brokers are slow (§5.4). Spark uses `maxOffsetsPerTrigger` as a manual rate limit (§7.4). OpenSearch returns `429` when write queues fill, and the client backs off (§2.10).

At every single stage the response to overload is *slow down*. Nowhere is it *drop data silently*. That consistency is not an accident — it is what you get when every component was built by people who had read §1.6.

### Where the cost is

Worth knowing, because cost drives more architectural decisions than performance does:

**HNSW vectors in RAM.** 640 GB before quantization, ~180 GB after int8 (§3.5). Quantization is often the difference between an approved project and a rejected one.

**Embedding inference.** Two hundred million chunks is a real bill, and re-embedding on every model change multiplies it. Caching by `hash(text) + model_version` means a reindex only re-embeds what changed (§3.9).

**OpenSearch node count**, driven by shard sizing and the filesystem cache requirement (§2.11).

**Always-on streaming clusters** — which is exactly why the analytics path uses `availableNow` on a schedule rather than a permanently running job (§7.3).

## 9.4 The eight ideas

Here is the thing I most want you to take from this book.

Across eight chapters and four completely different systems, the same small set of ideas has kept reappearing under different names. Once you see them, new systems stop being new. You read the documentation for something you have never used and find yourself recognising the furniture.

### 1. Partitioning gives parallelism, and brings skew

Kafka *partitions*. OpenSearch *shards*. Spark *partitions*. Flink *keyed subtasks*. Lake *partition directories*.

Same idea every time: divide the data so many machines can work at once. And the same failure every time — **skew**, where one partition gets far more than its share, and the system runs at the speed of its unluckiest piece. Also the same secondary cost: scatter-gather, where a query touching all partitions runs at the speed of the *slowest* one, so more partitions is not monotonically better (§1.3).

When you meet a new system: *what is the unit of partitioning, how is it chosen, and what is the characteristic skew?*

### 2. Replication gives durability, and forces a choice

Kafka's ISR. OpenSearch's replica shards. HDFS blocks. S3's internals.

Same idea: keep N copies so that losing one loses nothing. And always the same knob — **how long does the leader wait?** Synchronous is safe and slow, asynchronous is fast and lossy, and the real answer is a quorum in between: `acks=all` with `min.insync.replicas=2` at RF=3, which survives one failure without waiting on the slowest (§1.4, §5.3).

### 3. A durable log is the foundation of recovery

PostgreSQL's WAL. OpenSearch's translog. Spark's write-ahead log. Flink's checkpoints. And **Kafka**, which took the idea and made it the entire product.

Same principle: **write down what you are about to do, durably, before you do it.** Then any crash is recoverable by replay. It is one of the few genuinely universal techniques in systems engineering, and once you notice it you find it everywhere — including in PostgreSQL's WAL being repurposed by Debezium as an integration point, which is the same log serving two completely different masters (§1.6, §5.7).

### 4. Batching trades latency for throughput

Kafka's `linger.ms`. OpenSearch's `_bulk` size. Spark's trigger interval. Flink's checkpoint interval. Target file size in a lake.

The same curve, five times, in five different unit systems. Wait longer, amortise more overhead, compress better, achieve higher throughput, and make every individual item wait. There is no setting that is fast in both senses (§1.9).

### 5. At-least-once plus idempotency equals exactly-once, cheaply

Exactly-once *delivery* over a network is impossible. Exactly-once *effect* is easy: write to a deterministic key so that a replay overwrites rather than accumulates.

This is the pattern §9.3 traced through every hop of Lantern, and it is the answer to a startling proportion of the hard questions in this field. **When in doubt, make the write idempotent.**

### 6. Event time is not processing time, and clocks lie

Watermarks in Flink and Spark. Late-data handling. Temporal joins. Reprocessing determinism.

Machine clocks drift and jump, so you cannot order events by comparing timestamps from different machines (§1.5). And *when something happened* is a different question from *when you found out*, which means every aggregation over a window requires an explicit, tunable decision about how long to wait — and an explicit decision about what to do with whatever arrives too late (§6.5).

### 7. Approximation is a legitimate engineering choice

BM25 relevance. `terms` aggregation accuracy. HyperLogLog distinct counts. t-digest percentiles. HNSW recall.

At scale, exactness is often unaffordable, and frequently nobody wants it. Nobody needs to know there were exactly 8 431 947 distinct viewers. What matters is that the approximation is **explicit, bounded, and understood** — so that nobody builds a billing system on a HyperLogLog estimate (§2.8, §3.5).

### 8. Everything must be rebuildable

Keep raw data. Make writes idempotent. Put an alias in front of the index. Version the checkpoint path. Build alongside, verify, then atomically swap.

That last pattern appeared four separate times — OpenSearch alias swaps, Kappa reprocessing, Iceberg's metadata pointer, and the shadow-table backfill — which is a strong hint that it is the general shape of safe change in a data platform.

The payoff is cultural as much as technical. In a system built this way, a bug is *"reprocess Tuesday"* rather than an incident with a postmortem. That difference compounds over years, in how much risk your team is willing to take and therefore how fast it can move.

## 9.5 Forty questions

Reading produces a comfortable feeling of understanding that questions dispel very quickly. If you can answer these without looking things up, you have what this book set out to give you. If you cannot answer one, the section reference tells you where to go.

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

(There are forty-eight. I was not going to cut eight good questions to make the heading accurate.)

## 9.6 How to actually learn this

A reading plan, and then the only advice in this book I would call essential.

**Week 1 — foundations.** Chapters 1 and 4. These are the concepts every other chapter reuses, and time spent here pays back four times over. Do not skip Chapter 4 because it sounds administrative; §4.7 on idempotency is the most practically useful section in the book.

**Week 2 — search.** Chapter 2, and then *run it*. `docker run opensearchproject/opensearch`, index a few thousand documents, and spend an hour with `_analyze`, `_explain`, `bool` queries, and aggregations. Reading about analyzers teaches you a fraction of what watching `_analyze` tokenize a sentence teaches you. Deliberately make the mistakes: run a `term` query on a `text` field and watch it return nothing. Index a document and search for it immediately and watch it not be there.

**Week 3 — semantic.** Chapter 3. Run `sentence-transformers` locally over ten thousand documents, index them as `knn_vector`, and compare BM25, pure vector, and RRF over twenty queries you write yourself. Include a product code and a paraphrased question among them. The moment you watch BM25 win decisively on the code and lose completely on the paraphrase, §3.6 stops being a table and becomes obvious.

**Week 4 — streaming.** Chapter 5, then 6 or 7 depending on what your team uses. Run a local Kafka. Produce, consume, and then **kill the consumer mid-batch** and watch duplicates appear in your output. Then make the write idempotent and watch them stop mattering. That single exercise teaches §1.7, §4.7, §5.5, and §5.6 simultaneously, in about fifteen minutes, in a way no amount of reading does.

**Then re-read Chapter 9.** Especially §9.4. Every production incident you see for the next two years will be an instance of one of those eight ideas, and recognising which one is most of the diagnosis.

And the essential advice: **break things on purpose, in an environment where it is safe.** The understanding that matters in this field is not the kind you get from prose. It is the kind you get from having watched a system misbehave and worked out why. Every section in this book that felt sharp to you was probably sharp because somebody, once, watched it go wrong.

## 9.7 Where to go next

The books and documentation that are genuinely worth your time, rather than a comprehensive list.

***Designing Data-Intensive Applications*, Martin Kleppmann.** The best book available on the material in Chapters 1 and 4, and one of the best technical books of the last decade. If you read one thing after this, read this. It goes considerably deeper than I have on replication, consistency models, and distributed transactions, and it is a pleasure to read.

**OpenSearch documentation**, particularly the search and tuning sections. Also the older *Elasticsearch: The Definitive Guide* — it is out of date on APIs and still has the clearest explanations of analysis and relevance anywhere.

***Kafka: The Definitive Guide*** (O'Reilly), especially the chapters on reliability guarantees and exactly-once semantics, which go carefully through the cases §5.6 summarised.

**The Flink documentation's "Concepts" section.** Genuinely excellent, which is rare. The pages on event time and watermarks are the best explanation of that material in existence, including mine.

***Spark: The Definitive Guide*** plus the Structured Streaming Programming Guide.

**Engineering blogs from Pinecone, Weaviate, and Qdrant** for vector search. The clearest accessible writing on ANN algorithms and hybrid retrieval is being done in vendor blogs, which is unusual and useful.

And one broader suggestion: **read the original papers** for the systems you use most. The Dynamo paper, the Kafka paper, the Chandy–Lamport paper, the Raft paper. They are shorter than you expect and considerably clearer than most of the writing about them, and they tell you what problem the authors thought they were solving — which is usually the thing that makes the design make sense.

---

## A closing thought

We started with a program that worked and broke it by adding machines, and I said that everything afterwards would be about coping with what we lost.

In a sense that is what happened. Partitioning, replication, quorums, write-ahead logs, watermarks, idempotency — every one of them is scaffolding erected around the hole left by the fact that a distributed system cannot tell dead from slow, and that its clocks lie, and that its messages arrive twice or not at all.

But it is worth noticing what we built with the scaffolding. Lantern searches two hundred million documents in forty milliseconds. It understands a question phrased in words that appear nowhere in the answer. It stays up when machines die, reflects an edit in under three seconds, and can rebuild itself completely from first principles if we ask it to. None of that was possible on the one server we started with, at any price.

The constraints are real and there is no way around them. The interesting part — the reason this field is worth working in — is how much can be built once you stop fighting them and start designing for them.
