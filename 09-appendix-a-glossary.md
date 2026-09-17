# Appendix A — Glossary

Terms are grouped by where they first matter, with a section reference to the fuller explanation.

---

## Distributed systems (Chapter 1)

**Amdahl's law** — Speedup from parallelism is capped by the fraction of work that is inherently serial. 5% serial means 20× is your ceiling. §1.9

**At-least-once** — Acknowledge after processing. Never loses data, may duplicate. The right default. §1.7

**At-most-once** — Acknowledge before processing. Never duplicates, may lose data. §1.7

**Backpressure** — Signalling upstream to slow down instead of buffering without bound. The correct response to overload. §1.6

**Bulkhead** — Separate resource pools per dependency, so one flooded compartment doesn't sink the ship. §1.6

**CAP theorem** — During a network **partition**, choose **consistency** or **availability**. Partition tolerance is not optional. §1.8

**Causal consistency** — Causally related operations are seen in order everywhere; concurrent ones may be seen differently. Cheap and close to what humans expect. §1.8

**Circuit breaker** — After N consecutive failures, stop calling a dependency and fail fast. §1.6

**Consistent hashing** — Map keys and nodes onto a ring so that adding a node moves only ~1/N of keys, rather than nearly all. §1.3

**CRDT** — Conflict-free replicated data type: a structure that merges deterministically, so concurrent writes need no arbitration. §1.4

**Eventual consistency** — Replicas converge if writes stop. Says nothing about when, or what you read meanwhile. §1.8

**Fencing token** — A monotonically increasing number attached to writes so storage can reject a stale leader's writes. Defeats the "zombie leader" after a GC pause. §1.6

**FLP impossibility** — In an asynchronous network you cannot reliably distinguish a crashed process from a slow one. The theoretical root of most distributed systems difficulty. §1.5

**Head-of-line blocking** — One stuck item blocking everything behind it in an ordered queue. §5.8

**Hedged request** — Send a duplicate request to another replica after p95 elapses; take the first answer. Cuts tail latency. §1.9

**Idempotent** — Doing it N times has the same effect as doing it once. The central practical concept in this book. §1.7

**Lamport timestamp** — A per-node counter, max'd on message receipt. Gives an order consistent with causality, but cannot detect concurrency. §1.7

**Linearizability** — The strongest consistency model: the system behaves as if there were one copy and each operation took effect at a single instant. §1.8

**Little's Law** — `L = λ × W`; concurrency equals arrival rate times time in system. Also explains why queueing explodes near full utilisation. §1.9

**PACELC** — If **P**artitioned choose **A** or **C**; **E**lse choose **L**atency or **C**onsistency. The second clause governs daily decisions. §1.8

**Partitioning / sharding** — Splitting one dataset across machines for capacity and parallelism. §1.3

**Quorum** — A majority (or configured count) of replicas. Overlapping quorums are what make agreement possible. §1.4, §1.6

**Raft** — A consensus algorithm designed to be understandable. Terms, elections, and majority voting. §1.6

**Replication factor** — Total number of copies (RF=3 means three copies, not one plus three). §1.4

**Scatter-gather** — A query sent to all partitions and merged. Latency equals the *slowest* partition's, not the average. §1.3

**Skew / hotspot** — Uneven load across partitions. The characteristic failure of partitioning. §1.3

**Split brain** — Two nodes both believe they are leader; data diverges. §1.5

**Straggler** — The one slow task that makes a whole parallel operation wait. §1.3

**Tail amplification** — With N parallel calls each 1% slow, the aggregate is 1−0.99ᴺ slow. Twenty calls → 18%. §1.9

**Thundering herd** — Synchronised retries flattening a recovering service. Prevented by jitter. §1.6

**USE method** — For each resource check **U**tilisation, **S**aturation, **E**rrors. §1.9

**Vector clock** — A counter per node; can detect genuine concurrency (and hence conflicts), unlike Lamport timestamps. §1.7

**Write-ahead log (WAL)** — Append the intended change durably *before* applying it. The foundation of recovery everywhere. §1.6

---

## Search (Chapter 2)

**Alias** — A name pointing to one or more indexes; swappable atomically. Applications should only ever reference aliases. §2.10

**Analyzer** — The pipeline turning text into indexed terms: character filters → tokenizer → token filters. §2.5

**BM25** — The default relevance function. TF-IDF plus term-frequency saturation and tunable length normalisation. §2.9

**Cluster manager** — The node role maintaining cluster state; elected by quorum. Formerly "master". §2.3

**Doc values** — Columnar per-document values used for sorting, aggregating, and scripting. The inverse of the inverted index. §2.2

**Flood stage** — The 95% disk watermark at which OpenSearch makes indexes read-only. §2.11

**Inverted index** — Term → list of documents containing it. The structure that makes search fast. §2.2

**`keyword` field** — Not analysed; stored as one exact term. For filters, sorting, aggregations. §2.4

**Mapping** — The schema: field types and how each is analysed and stored. §2.4

**Mapping explosion** — Dynamic mapping creating thousands of fields from arbitrary JSON keys, destabilising the cluster. §2.4

**Merge** — Background combination of small immutable segments into larger ones; when deleted documents are actually removed. §2.3, §2.6

**Multi-field** — Indexing one source field several ways (e.g. `title` as text and `title.keyword` as keyword). §2.4

**nDCG** — Normalised discounted cumulative gain. The standard headline metric for ranked retrieval. §2.9

**Near real-time** — Documents are durable on write but not searchable until the next refresh (default 1 s). §2.6

**Postings list** — The list of documents (with frequencies and positions) for one term. §2.2

**Query context vs filter context** — Scored and uncached, versus boolean and cached. Put yes/no conditions in `filter`. §2.8

**Refresh / flush / translog fsync** — Three different "write to disk" operations guaranteeing visibility, segment durability, and crash safety respectively. §2.6

**Routing** — `hash(key) % primary_shards` determines a document's shard. Why the primary count is immutable. §2.6

**`search_after`** — Cursor-based deep pagination. Constant cost per page, unlike `from`/`size`. §2.7

**Segment** — An immutable Lucene file. Updates write new versions and mark old ones deleted. §2.3

**Shard** — A slice of an index, and a complete independent Lucene index in its own right. §2.3

**`text` field** — Analysed into terms. For full-text search; not sortable or aggregatable. §2.4

---

## Semantic search (Chapter 3)

**ANN** — Approximate nearest neighbour. Trades exactness for speed, measured by recall@k. §3.5

**Bi-encoder** — Query and document embedded independently, so document vectors can be precomputed and indexed. §3.7

**Chunking** — Splitting documents into pieces small enough to embed meaningfully. 300–800 tokens with overlap. §3.3

**Cosine similarity** — The angle between vectors, ignoring magnitude. The default metric. §3.4

**Cross-encoder** — Query and document scored together in one model pass. Far more accurate, impossible to precompute. §3.7

**Curse of dimensionality** — Above ~20 dimensions, distances converge and spatial indexes stop pruning. §3.5

**Dense vector** — Few hundred dimensions, all non-zero, no dimension individually interpretable. §3.3

**Embedding** — A fixed-length vector representing content such that similar meanings are nearby. §3.3

**`ef_search` / `ef_construction` / `m`** — HNSW's three tuning parameters: query-time candidates, build-time candidates, links per node. §3.5

**HNSW** — Hierarchical Navigable Small World. Layered graph, express-train navigation, O(log N) search. §3.5

**Hybrid search** — Running keyword and vector retrieval and fusing the results. §3.7

**MMR** — Maximal marginal relevance: trading a little relevance for diversity, to avoid near-duplicate results. §3.9

**Position bias** — Click logs can only teach you about documents you already ranked highly. §3.9

**Product quantization** — Compressing vectors via per-subspace codebooks; ~32× smaller. §3.5

**Recall@k** — Of the true top-k, how many did the approximate search find? The metric for the retrieval stage. §3.5, §3.9

**RRF** — Reciprocal rank fusion: `Σ 1/(60 + rank)`. Combines retrievers without normalisation or tuning. §3.7

**Scalar quantization** — float32 → int8 (4× smaller, near-free in quality) or binary (32×). §3.5

**Sparse vector** — One dimension per vocabulary word, nearly all zeros, each dimension interpretable. What BM25 operates on. §3.3

---

## Data engineering (Chapter 4)

**Avro** — Row-oriented binary format with embedded schema and best-in-class schema evolution. For Kafka and raw events. §4.4

**Backward compatible** — A new reader can read old data. Lets you upgrade consumers first. §4.5

**Bronze / silver / gold** — Raw, cleaned, curated layers of a data platform. §4.2

**Data contract** — A versioned agreement between producer and consumers: schema, semantics, SLA, ownership. §4.5

**Dead-letter queue (DLQ)** — Where unprocessable records go, so one poison message can't block a pipeline forever. §1.7, §5.8

**Forward compatible** — An old reader can read new data. Lets you upgrade producers first. §4.5

**Kappa architecture** — One streaming implementation; reprocess by replaying the log. §4.3

**Lakehouse** — A data lake with a table format (Iceberg/Delta/Hudi) providing ACID, time travel, and row-level updates. §4.4

**Lambda architecture** — Separate streaming and batch paths merged at serving time. Correct, and prone to logic drift. §4.3

**Medallion** — Another name for bronze/silver/gold layering. §4.2

**Micro-batch** — Streaming implemented as small batch jobs in a loop. Spark's model. §4.3

**Parquet** — Column-oriented format with column pruning, predicate pushdown, and strong compression. The lake standard. §4.4

**Partition pruning** — Skipping directories or row groups entirely based on a filter and stored statistics. §4.4

**Predicate pushdown** — Using per-chunk min/max statistics to skip data without decompressing it. §4.4

**Schema registry** — Central schema store that rejects incompatible changes at registration, turning a 3 a.m. outage into a failed build. §4.5

**Small files problem** — Many tiny files destroying read performance. Target 128 MB–1 GB. §4.4

**Upsert** — Insert or update on a key. The primary mechanism for idempotent writes. §4.7

---

## Kafka (Chapter 5)

**`acks`** — What the producer waits for: `0` nothing, `1` the leader, `all` the in-sync replicas. §5.3

**Broker** — One Kafka server. §5.3

**CDC** — Change data capture: reading a database's own write-ahead log to publish every change. §5.7

**Compaction** — Retention that keeps the latest record per key forever, turning a topic into a replayable table. §5.3

**Consumer group** — Consumers sharing a `group.id`; each partition assigned to exactly one member. §5.5

**Consumer lag** — Latest produced offset minus committed offset. The single most important Kafka metric. §5.5

**ISR** — In-sync replicas: those caught up enough to be eligible for leader election. §5.3

**KRaft** — Kafka's built-in Raft-based metadata quorum, replacing ZooKeeper. §5.3

**`linger.ms`** — How long the producer waits to fill a batch. The latency/throughput dial. §5.4

**`min.insync.replicas`** — The floor below which `acks=all` writes are rejected rather than silently weakened. §5.3

**Offset** — A consumer group's position in a partition. §5.5

**Rebalance** — Reassignment of partitions when group membership changes. Stop-the-world unless cooperative. §5.5

**Static membership** — `group.instance.id`, letting a restarting consumer reclaim its own partitions without a group-wide rebalance. §5.5

**Tombstone** — A null-valued record in a compacted topic, meaning "this key is deleted". §5.3

**Unclean leader election** — Promoting an out-of-sync replica. Availability at the cost of silent data loss. §5.3

---

## Flink (Chapter 6)

**Allowed lateness** — Keeping a window alive after firing to incorporate late events, re-firing with updates. §6.5

**Async I/O** — Issuing external calls concurrently instead of blocking the operator thread per record. §6.6

**Barrier / checkpoint barrier** — A marker flowing with the data; when it passes, each operator snapshots its state. §6.4

**Checkpoint** — An automatic, periodic, globally consistent snapshot for failure recovery. §6.4

**Event time** — When something happened in the world. The basis of reproducible results. §6.5

**Incremental checkpoint** — Shipping only changed RocksDB files rather than the full state. §6.4

**Keyed state** — State scoped per key, with Flink swapping the context per record. §6.3

**Processing time** — When an operator handled a record. Low latency, non-reproducible. §6.5

**Savepoint** — A manually triggered snapshot for upgrades, rescaling, and rollback. Requires stable operator UIDs. §6.4

**Session window** — A window defined by a gap of inactivity; windows merge as events fill gaps. §6.6

**Side output** — A secondary output stream, typically for late or invalid records. §6.5

**State TTL** — Time-to-live on state, one of only three acceptable answers to "what bounds this state?". §6.3

**Temporal join** — Joining against the version of a dimension as of the event's own time. Point-in-time correctness. §6.6

**Two-phase commit sink** — Pre-commit at the barrier, commit when the checkpoint completes. Exactly-once output, at checkpoint-interval latency. §6.4

**Unaligned checkpoint** — Letting barriers overtake in-flight records, snapshotting those too. Keeps checkpoints fast under backpressure. §6.4

**Watermark** — An assertion that no further event earlier than time T will arrive. Converts completeness from a guess into a declared promise. §6.5

---

## Spark (Chapter 7)

**Action** — An operation that triggers execution (`count`, `write`, `collect`). §7.2

**AQE** — Adaptive Query Execution: runtime re-optimisation, including automatic skew handling. Leave it on. §7.2

**`availableNow`** — A trigger that processes all available data then exits. Incremental batch with exactly-once bookkeeping. §7.3

**Broadcast join** — Shipping a small table to every executor to avoid a shuffle. Usually the biggest join win. §7.2

**Catalyst** — Spark's query optimizer. The reason DataFrames beat RDDs. §7.2

**Driver / executor** — The coordinating process and the worker JVMs. §7.2

**`foreachBatch`** — Access to each micro-batch as a batch DataFrame plus a `batch_id`. The escape hatch. §7.4

**Lineage** — The recorded sequence of transformations, used to recompute lost partitions instead of replicating them. §7.2

**`maxOffsetsPerTrigger`** — The manual rate limit. Spark's substitute for automatic backpressure. §7.4

**Narrow / wide transformation** — One input partition per output, versus many. Wide means a shuffle. §7.2

**Output mode** — `append`, `update`, or `complete`. What gets emitted each batch. §7.4

**Salting** — Adding a random suffix to a hot key and aggregating in two phases, to defeat skew. §7.2

**Shuffle** — All-to-all redistribution of data between stages. The dominant cost in most Spark jobs. §7.2

**`spark.sql.shuffle.partitions`** — Post-shuffle partition count, defaulting to a usually-wrong 200. §7.2

**State store** — Where streaming state lives. Use RocksDB for anything substantial. §7.5

**Tungsten** — Whole-stage code generation, off-heap memory, vectorized reads. §7.2

**Unbounded table** — The model: a stream is a table that keeps growing, and a query over it is maintained incrementally. §7.3
