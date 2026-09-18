# Appendix B — Reference Tables

Things to look up rather than read. Every number here is *the shape of the right answer*, not a constant — the reasoning is in the referenced section.

---

## B.1 Ten questions for any distributed system

Use these on anything you meet for the first time. Twenty minutes with the documentation and this list will get you further than a week of tutorials.

1. What is the **unit of partitioning**, and how is a record's partition chosen?
2. What is the **unit of replication**, and is it synchronous or asynchronous?
3. Who is the **leader**, and what happens when it dies?
4. What **consistency model** do reads get?
5. What happens during a **network partition** — does it choose C or A?
6. Where is the **durable log**, and how does recovery use it?
7. What **delivery guarantee** does it provide, and where exactly is the acknowledgement?
8. How does it apply **backpressure** when overloaded?
9. What is the **scatter-gather cost** of a typical read?
10. What is its characteristic **skew or hotspot** failure?

---

## B.2 Delivery guarantees

```
receive → acknowledge → process                    at-most-once   (loses data)
receive → process → acknowledge                    at-least-once  (duplicates)
receive → (process and acknowledge atomically)     exactly-once
```

| | Producer side | Consumer side | Use when |
|---|---|---|---|
| At-most-once | `acks=0/1` | ack before processing | metrics, debug logs |
| **At-least-once** | `acks=all` + idempotence | ack after processing | **almost everything** |
| Exactly-once | transactions | offsets inside the transaction | financial counts, billing |

**The rule:** at-least-once delivery + idempotent sink = exactly-once *effect*, at no throughput cost. §1.7, §5.6, §9.3

**Four ways to make a write idempotent** (§4.7):
1. **Upsert on a natural key** — best option
2. **Overwrite the whole partition** — best batch option
3. **Commit output and position atomically** — needs sink cooperation
4. **Dedup window with a TTL** — the fallback

---

## B.3 Sizing rules of thumb

### OpenSearch (§2.11)
| Thing | Target | Why |
|---|---|---|
| Shard size | **10–50 GB** | below: overhead per shard; above: slow recovery and rebalancing |
| Shards per node | ≤ ~20 per GB of heap | each shard is a Lucene index with fixed cost |
| JVM heap | **≤ 31 GB**, ≤ 50% of RAM | above 31 GB loses compressed oops; the other half must be filesystem cache |
| Primaries | `ceil(total_GB / 30)`, multiple of data node count | even distribution |
| Replicas | ≥ 1 in production | changeable live; add to scale reads |
| Bulk request | **5–15 MB** by bytes | not by document count |
| Bulk concurrency | 2–4 threads per data node | |

### Kafka (§5.3, §5.8)
| Thing | Target |
|---|---|
| Replication factor | **3** |
| `min.insync.replicas` | **2** |
| `acks` | **all** |
| `unclean.leader.election.enable` | **false** |
| Partitions | ≥ max consumer parallelism, 2–3× current need |
| JVM heap | ~6 GB — leave the rest to page cache |
| Under-replicated partitions | **0**, always |

### Data lake (§4.4)
| Thing | Target |
|---|---|
| File size | **128 MB – 1 GB** |
| Partition size | 100 MB – 1 GB |
| Partition key | appears in nearly every WHERE clause; usually a date |
| Never partition by | high-cardinality keys (`user_id`) |

### Lakehouse / Delta (§8.6, §8.9)
| Thing | Target |
|---|---|
| Compacted file size | **~1 GB** (`OPTIMIZE` default 1 GB; 128 MB–1 GB acceptable) |
| Minimum partition size | **1 GB** — below this, do not partition at all |
| Don't partition below | ~1 TB total table size; cluster instead |
| Clustering columns | the **1–3** columns in most `WHERE` clauses |
| `OPTIMIZE` cadence | nightly on any streaming-written table, or enable auto-compaction |
| `VACUUM` retention | 7 days default; raise **before** you need older time travel |
| Cluster auto-termination | **15–30 min**, on every interactive cluster, always |

### Spark (§7.2, §7.7)
| Thing | Target |
|---|---|
| Partition size | 100–200 MB |
| `spark.sql.shuffle.partitions` (batch) | sized to the above, or let AQE coalesce |
| `spark.sql.shuffle.partitions` (streaming) | 1–3× total cores |
| Executor size | 4–5 cores each |
| Broadcast threshold | tens of MB |

### Vector search (§3.3, §3.5)
| Thing | Target |
|---|---|
| Chunk size | 300–800 tokens, 10–20% overlap |
| HNSW `m` | 16 (32–48 for high recall) |
| HNSW `ef_construction` | 100–512 |
| HNSW `ef_search` | 100–512, ≥ k, tunable per query |
| Memory | `n × (dims × 4 + m × 8)` bytes, before quantization |
| First optimization | **int8 quantization** — 4× smaller, near-free |

### General (§1.9)
| Thing | Target |
|---|---|
| Utilisation for latency-sensitive systems | **60–70%** — queueing explodes as ρ→1 |
| Always report | p50, p95, p99, p99.9, max — never averages |

---

## B.4 The batching dial, in five costumes

All the same trade-off. Wait longer → bigger batches → better throughput and compression → higher latency. §1.9

| System | Setting | Typical |
|---|---|---|
| Kafka producer | `linger.ms` | 5–100 ms |
| OpenSearch | `_bulk` request size | 5–15 MB |
| Spark Streaming | trigger interval | seconds to minutes |
| Flink | checkpoint interval | 1–5 minutes |
| Data lake | target file size | 128 MB – 1 GB |

---

## B.5 Diagnostic first moves

| Symptom | First thing to check | Section |
|---|---|---|
| Distributed job slow, one task lagging | **skew** — is one key dominant? | §1.3, §7.2 |
| Search returns nothing but should match | `_analyze` both the document text and the query text | §2.5 |
| Search slow | `"profile": true`; are yes/no conditions in `filter`? | §2.11 |
| Just-indexed document not found | refresh interval — it's near-real-time | §2.6 |
| OpenSearch shard unassigned | `GET _cluster/allocation/explain` | §2.11 |
| Vector search recall poor with filters | selective pre-filter degrading graph traversal | §3.8 |
| Kafka lag growing on one partition only | key skew — no amount of consumers helps | §5.5 |
| Kafka constant rebalances | `max.poll.interval.ms` vs processing time | §5.5 |
| Records silently missing after a crash | auto-commit — it acks before you process | §5.5 |
| Flink runs but emits nothing | **watermark stuck** on an idle/lagging input | §6.5 |
| Flink checkpoints slowly growing | state without a TTL, or backpressure | §6.4 |
| Flink can't restore a savepoint | operator UIDs changed | §6.4 |
| Spark first batch never finishes | no `maxOffsetsPerTrigger` | §7.4 |
| Spark streaming state grows forever | missing `withWatermark` | §7.5 |
| Millions of tiny files | short trigger × high shuffle partitions | §4.4, §7.7 |
| Duplicates in a sink | at-least-once with a non-idempotent write | §4.7, §9.3 |

---

## B.6 Useful commands and endpoints

### OpenSearch
```
# health and topology
GET _cluster/health?level=indices
GET _cat/nodes?v&h=name,heap.percent,disk.used_percent,cpu
GET _cat/indices?v&s=store.size:desc
GET _cat/shards/my-index?v&s=state
GET _cluster/allocation/explain          ← why is this shard unassigned

# mapping and analysis
GET  /my-index/_mapping
POST /my-index/_analyze { "field": "body", "text": "Running servers" }

# search and relevance
GET /my-index/_search { "query": {...}, "size": 10 }
GET /my-index/_search { "explain": true, ... }
GET /my-index/_explain/42 { "query": {...} }    ← why did THIS doc score that
GET /my-index/_search { "profile": true, ... }  ← per-shard timing
POST /my-index/_rank_eval { ... }               ← relevance metrics

# write and maintain
POST /_bulk                                     (NDJSON, 5–15 MB, check per-item errors)
POST /_reindex?slices=auto&wait_for_completion=false
POST /my-index/_update_by_query?conflicts=proceed
POST /_aliases                                  ← atomic swap
GET  _tasks?detailed  /  POST _tasks/<id>/_cancel
POST /my-index/_forcemerge?max_num_segments=1   ← read-only indexes only
PUT  /my-index/_settings { "index": { "refresh_interval": "30s" } }
```

### Kafka
```
kafka-topics.sh --describe --topic T
kafka-consumer-groups.sh --describe --group G      ← lag per partition
kafka-consumer-groups.sh --reset-offsets --to-earliest --group G --topic T --execute
kafka-configs.sh --describe --entity-type topics --entity-name T
```

### Flink
```
# web UI is the primary tool:
#   backpressure per operator   ← finds the bottleneck
#   watermark per operator      ← finds the stuck input
#   checkpoint duration/size/failures
flink savepoint <jobId> s3://path
flink run -s s3://path/savepoint-xxx job.jar
```

### Spark
```python
query.lastProgress          # inputRowsPerSecond vs processedRowsPerSecond
query.status
df.explain(True)            # confirm pushdown, broadcast, partition pruning
spark.sql("SET -v")         # all current configuration
```

---

## B.7 Checklists

### A new Kafka topic (§5.8)
1. What is the **key**, what ordering does it buy, is there skew risk?
2. How many **partitions** (consumer parallelism + headroom)?
3. **Retention**: delete or compact, how long?
4. RF=3, `min.insync.replicas=2`, `acks=all`, unclean election off?
5. **Schema** registered, compatibility mode set?
6. Who **consumes** it, and is their processing idempotent?
7. Is there a **DLQ** and an alert on lag?

### A zero-downtime OpenSearch reindex (§2.10)
1. Create the new index with the new mapping.
2. Dual-write, or confirm the pipeline is replayable.
3. `_reindex` the history with `slices=auto`.
4. **Verify**: counts, spot checks, and `_rank_eval` against both.
5. Atomically swap the alias.
6. Keep the old index for a rollback window, then delete.

### A safe backfill (§4.7)
1. Can you get the **input** back?
2. Is the computation **deterministic**? (`now()`, `random()`, changed dimension tables)
3. Is the write **idempotent**? (overwrite the partition, or upsert)
4. **Blast radius** — shadow table, diff, then swap.
5. **Throttle** it; run off-peak.
6. Notify **downstream** consumers (lineage).
7. Reset the **bookmark** explicitly and completely.

### A new Delta table (§8.8, §8.9)
- [ ] Grain written in the table comment, in one sentence
- [ ] Layer decided: bronze (no logic), silver (true), or gold (shaped for one named consumer)
- [ ] Partitioned only if partitions exceed ~1 GB; otherwise `CLUSTER BY`
- [ ] Clustered on the columns queries actually filter by
- [ ] Write is idempotent: `MERGE` on a key, or `CREATE OR REPLACE` over a bounded window
- [ ] `OPTIMIZE` and `VACUUM` scheduled; retention chosen deliberately
- [ ] Quality expectations defined, with drop-vs-fail decided per rule
- [ ] Grants applied in Unity Catalog; PII not readable by default

### A new streaming job
1. What **bounds the state**? (window, timer, or TTL — there is no fourth answer)
2. Event time or processing time, and **what is the watermark delay**?
3. Where do **late events** go? (side output, always)
4. Is the **sink idempotent**, and what is the key?
5. Is there a **rate limit** (Spark) or is backpressure automatic (Flink)?
6. Are **operator UIDs** set (Flink) / is the **checkpoint path versioned** (Spark)?
7. What alerts exist on **lag**, **checkpoint duration**, and **dropped late records**?

---

## B.8 The eight recurring ideas

The compressed form of §9.4. When a new system confuses you, one of these is usually the key.

1. **Partitioning gives parallelism, and brings skew.**
2. **Replication gives durability, and forces the sync/async choice.**
3. **A durable log is the foundation of recovery** — write the intent before the act.
4. **Batching trades latency for throughput** — the same dial everywhere.
5. **At-least-once + idempotency = exactly-once, cheaply.**
6. **Event time ≠ processing time, and clocks lie.**
7. **Approximation is a legitimate engineering choice** — if it is explicit and bounded.
8. **Everything must be rebuildable** — build alongside, verify, swap atomically.
