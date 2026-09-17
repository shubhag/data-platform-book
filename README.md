# Storing, Processing, and Finding Things

### A book about how large data platforms actually work

---

## Contents

| | | Pages |
|---|---|---|
| | **[Preface](00-preface.md)** — what this is, how to read it, and a fictional system called Lantern | ~3 |
| **1** | **[The Machine That Isn't One](01-the-machine-that-isnt-one.md)** | ~25 |
| | *Partitioning · replication · the dead-versus-slow problem · consensus and quorums · retries, duplicates, and idempotency · consistency, CAP and PACELC · latency, throughput, and finding bottlenecks* | |
| **2** | **[Finding Things](02-finding-things.md)** | ~35 |
| | *Why a database can't search · the inverted index · shards, segments, and immutability · mappings, `text` vs `keyword` · analyzers · the write path and near-real-time · query vs filter context · aggregations and approximation · BM25 · aliases, bulk indexing, reindexing · sizing and troubleshooting* | |
| **3** | **[Meaning as Geometry](03-meaning-as-geometry.md)** | ~30 |
| | *Embeddings and what 768 dimensions mean · chunking · cosine, dot product, L2 · the curse of dimensionality · HNSW as an express-train network · keyword vs semantic, honestly compared · hybrid search and RRF · bi-encoders vs cross-encoders · filtering without destroying recall · measuring relevance* | |
| **4** | **[Moving Data](04-moving-data.md)** | ~22 |
| | *Platform layers and the raw-data rule · batch vs stream vs micro-batch · Lambda's drift problem · rows vs columns, JSON/Avro/Parquet · table formats and the lakehouse · schema evolution and registries · the four quality dimensions · idempotency, deduplication, and safe reprocessing* | |
| **5** | **[The Log](05-the-log.md)** | ~24 |
| | *Why "message queue" is the wrong model · topics, partitions, and the two rules · keys and ordering · ISR and the durability triad · compaction as a table · producers, batching, and idempotence · consumer groups, offsets, and the auto-commit trap · rebalancing · lag · CDC and the ecosystem* | |
| **6** | **[Time](06-time.md)** | ~26 |
| | *The question with no good answer · Flink's architecture and real backpressure · keyed state and what bounds it · checkpoint barriers and consistent snapshots without stopping · savepoints and operator UIDs · event time and watermarks · the stuck-watermark trap · windows, triggers, and joins · Flink SQL* | |
| **7** | **[The Unbounded Table](07-the-unbounded-table.md)** | ~23 |
| | *Spark's execution model · laziness, Catalyst, and why RDDs lose · the shuffle · skew and salting · the unbounded-table idea · micro-batches and triggers · `availableNow` as incremental batch · `foreachBatch` · watermarks that bound state · exactly-once and its three conditions · choosing between Spark and Flink* | |
| **8** | **[The Whole Machine](08-the-whole-machine.md)** | ~18 |
| | *The complete architecture · one edit traced end to end with the guarantee at every hop · how idempotency, ordering, failure, and backpressure thread through · **the eight ideas that recur everywhere** · 48 self-test questions · a four-week learning plan* | |
| **A** | **[Glossary](09-appendix-a-glossary.md)** — every term, grouped by chapter, with section references | ~16 |
| **B** | **[Reference Tables](10-appendix-b-reference.md)** — sizing rules, diagnostic first moves, commands, checklists | ~13 |

**Total: roughly 220 pages, 3–4 hours of reading.**

---

## How to read it

**Straight through**, if you have the time. The chapters build deliberately: Chapter 1 supplies the vocabulary, Chapters 2 and 3 build something with it, Chapter 4 supplies the discipline, Chapters 5–7 supply the machinery, and Chapter 8 assembles it.

**If you have one hour:** Chapter 1, then §8.4 (the eight recurring ideas) and §8.5 (the self-test). Chapter 1 gives you the concepts; §8.4 shows you that they are the *only* concepts, wearing different hats; the questions show you what you actually retained.

**If you need one specific thing:** every chapter opens with the problem it solves and closes with a troubleshooting table. Appendix B is the lookup layer — sizing numbers, first diagnostic moves, and checklists.

**Cross-references** are written as §3.4, meaning chapter 3, section 4. Follow them when something feels underexplained; it usually means it was introduced properly elsewhere.

---

## The one piece of advice

**Reading is not enough, and the gap is larger than it feels.**

These are operational systems, and the understanding that matters comes from watching them misbehave. Somewhere in Chapter 2, stop and start a local OpenSearch; run `_analyze` on a sentence and watch the terms come out. In Chapter 5, run a local Kafka, kill a consumer mid-batch, and watch duplicates appear in your output — then make the write idempotent and watch them stop mattering.

That single exercise teaches four sections at once, in fifteen minutes, in a way no amount of prose does. §8.6 lays out a four-week version.

---

## The shortest possible summary

Large datasets are handled by **splitting** them, **copying** them, and writing every change to a **durable log** first, so anything can be recovered or replayed.

Data flows through a **log-centric backbone** into **stream** processors for fresh answers and **batch** processors for complete ones, landing in columnar files in a lakehouse and in an inverted index for search.

Because networks fail and retries are unavoidable, everything is delivered **at least once** — and correctness comes not from preventing duplicates but from making every write **idempotent**.

Search combines **keyword** matching (precise on the exact words typed) with **semantic** matching (robust to paraphrase), fused, reranked, and — critically — **measured** against a judgment set rather than tuned by intuition.

And everything is a trade-off between latency, throughput, cost, and correctness. The job is to make those trade-offs **explicitly**, rather than by accident.
