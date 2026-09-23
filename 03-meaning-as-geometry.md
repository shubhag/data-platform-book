# Chapter 3 — Meaning as Geometry

Keyword search matches strings, so it misses documents that say the right thing in different words. This chapter turns meaning into coordinates, so related ideas land near each other, and shows how to search those coordinates fast at Lantern's scale.

**By the end you'll be able to:**

- Choose an embedding model, a chunking strategy, and a similarity metric.
- Explain how HNSW finds approximate nearest neighbours, what it costs, and how to tune it.
- Combine keyword and vector results with RRF, then rerank them.
- Pick a filtering strategy that neither wrecks recall nor leaks data.
- Build an evaluation loop that tells you whether any of it worked.

## 3.1 The gap we cannot close with words

Chapter 2 ended with two failing queries, and they fail in different ways.

A user types **"why does my deployment keep restarting"**. The answer is titled *Diagnosing CrashLoopBackOff*, and the two share zero words, so stemming and boosting have nothing to work with. A synonym could map "restarting" to "CrashLoopBackOff", until tomorrow someone types "pod won't stay up".

A user types **"kubernetes pod memory limits"**. The right document, *k8s container resource constraints*, matches because "kubernetes" is in its body, but it ranks fourteenth, below documents that mention Kubernetes more often and are about something else.

The first failure is **recall**: the answer is not in the results. The second is **ranking**: it is there, buried. Both have one root cause:

> **An inverted index matches strings. It has no representation of meaning. Two documents that say the same thing in different words are, to the index, completely unrelated.**

Stemming, synonyms, and ngrams each patch this one word at a time, and no finite list of rules covers how humans phrase things. We need a representation in which "restarting" and "CrashLoopBackOff" are close *without anyone writing that down*.

## 3.2 An old idea: meaning is context

Linguists had the key idea long before neural networks: *you shall know a word by the company it keeps.* Words used in similar contexts have similar meanings.

Hand someone ten thousand sentences containing "wug": "the wug barked", "I walked the wug", "the wug chased a cat". Nobody told them what a wug is, but they can see it is used almost exactly where "dog" is.

Scale that to every sentence on the internet and give words with similar contexts similar representations. The machine still has no idea what a dog *is*, but similar things end up represented similarly, learned from usage with no hand-written rules. That representation is a list of numbers.

## 3.3 Embeddings: meaning with coordinates

An **embedding** is a fixed-length list of floating-point numbers (a **vector**) that represents content so that similar meanings sit near each other.

```
"senior java engineer"        →  [ 0.021, −0.418,  0.093, ...,  0.177 ]
"experienced JVM developer"   →  [ 0.019, −0.402,  0.101, ...,  0.169 ]
"chocolate cake recipe"       →  [−0.331,  0.284, −0.556, ...,  0.042 ]
```

A typical model produces 768 numbers. The first two vectors are nearly identical; the third resembles neither. Nobody wrote a rule linking "java" to "JVM"; it fell out of the training.

### How to picture 768 dimensions

Picture three instead. Every sentence is a point: cooking in one region, Kubernetes in another, and within Kubernetes, networking a little apart from storage. "Why does my deployment keep restarting" and "Diagnosing CrashLoopBackOff" land almost on top of each other. With 768 axes the geometry is the same; you just can't draw it.

No single dimension means anything. Dimension 412 is not "formality"; the meaning lives in the pattern across all 768. That is why these are **dense** vectors: every dimension carries some signal, none an interpretable one.

The inverted index is also a vector, with one dimension per vocabulary word. It is **sparse**: fifty thousand dimensions, almost all zero, each interpretable (dimension 4 411 is exactly "kubernetes"). Side by side:

| | **Sparse** (BM25) | **Dense** (embeddings) |
|---|---|---|
| Dimensions | ~50 000+ | 384 – 3 072 |
| Mostly zeros? | Yes, 99.9% | No, all non-zero |
| One dimension means | One specific word | Nothing on its own |
| Matches on | Exact terms | Meaning |
| Synonyms | Only if configured | Automatically |
| Rare codes, IDs, names | Excellent | Poor |
| Interpretable | Yes | No |
| Storage | Small | Large |

Almost everything in §3.6 and §3.7 follows from this table.

### Where embeddings come from

A neural network, usually a transformer, is trained with **contrastive learning**: show it pairs that should be close (a question and its answer, two paraphrases) and pairs that should be far apart (usually random text). Pull the first together, push the second apart, and repeat a few hundred million times.

| Family | Examples | Trade-off |
|---|---|---|
| **Open, self-hosted** | `all-MiniLM-L6-v2` (384-d, tiny, fast, a good first default); `all-mpnet-base-v2` (768-d, noticeably better); `BGE`, `E5`, `GTE` (strong, with good multilingual variants) | You run the infrastructure |
| **Hosted APIs** | OpenAI `text-embedding-3-small` / `-large`, Cohere Embed, Voyage | Trivial to adopt; the per-token bill gets interesting at two hundred million documents |
| **Fine-tuned** | A mid-sized model tuned on *your* query-to-document pairs from click logs | Usually beats a much larger generic model on your data. The highest-leverage quality gain in this chapter, but it needs §3.9's evaluation setup first |

### Five practical facts that will bite you

**1. The dimension count is part of your schema.** A 768-dimensional model always produces 768 numbers, and the index declares that number. So **changing models means re-embedding and reindexing everything.** Put the model in the index name (`documents_v3_bge_base`), keep it behind an alias (§2.10), and expect to do this within the first year.

**2. Models silently truncate.** Maximum input is often 256 to 8192 tokens, and anything past it is dropped without an error. Embed a forty-page spec as one vector and you have embedded page one.

**3. Chunking is the most important design decision here.** Long documents are split and each piece embedded. Too large gives *semantic mush*, a vector averaging five topics and close to nothing. Too small loses context: "it supports up to 500 concurrent users" is useless if nobody knows what "it" is.

Start at **300 to 800 tokens with 10–20% overlap**, split on paragraphs, headings, and sentence ends rather than a fixed character count. Overlap catches answers that straddle a boundary. From day one, **prepend the title and section heading** to each chunk and **store the parent document ID**, so you can match on small chunks and return the parent (*small-to-big*).

**4. Symmetric and asymmetric search differ.** Like-with-like comparison (duplicate tickets) is **symmetric**; a short question against a long passage is **asymmetric**. Most top models are asymmetric and expect prefixes like `"query: ..."` and `"passage: ..."`. Omit them and you get plausible vectors with quietly worse quality, so read the model card.

**5. Index and query with the same model, always.** Two models produce unrelated coordinate systems, so distances between them are meaningless. A half-and-half index is broken with no errors to show for it.

## 3.4 Measuring closeness

Given two vectors, how close are they? There are three common answers.

### Cosine similarity

The default. It measures the **angle** between vectors and ignores their lengths.

```
                A · B          Σ aᵢbᵢ
cos(A,B) = ───────────── = ────────────────────
            ‖A‖ · ‖B‖      √(Σaᵢ²) · √(Σbᵢ²)
```

It runs from **−1** (opposite) through **0** (unrelated) to **1** (same direction). Length is discarded because it mostly reflects text length, not meaning. A paragraph and a page on the same topic point the same way, so cosine calls them similar; Euclidean distance would call them far apart.

**Calibration warning.** Unrelated sentences typically score 0.3 to 0.6, not 0, because the model's output space is not centred; related pairs score 0.7 to 0.9. So **0.75 does not mean "75% relevant"**. Calibrate any threshold on your own data, per model.

### Dot product

```
A · B = Σ aᵢbᵢ
```

Cosine without the normalisation, so cheaper. **For unit-length vectors, dot product and cosine are identical**, so normalise once at index time and use dot product forever. Unnormalised, it favours large-magnitude vectors; a few models encode importance that way, most don't, so check the model card.

### Euclidean (L2) distance

```
d(A,B) = √( Σ (aᵢ − bᵢ)² )
```

Straight-line distance, where **smaller is better**, unlike the other two. Mix it in carelessly and your search returns the least relevant results first. For unit-length vectors `d² = 2 − 2·cos`, so it ranks identically to cosine.

### The rule, and the trap

> Use **cosine**. If you normalise at index time, use **dot product** and get the same ranking faster.

> **The metric must be consistent everywhere: index configuration, query, and any reranking or fusion downstream.**

An HNSW index built for L2 and queried with cosine was built for a different notion of "near". It still returns results, just worse ones, and no error will tell you.

## 3.5 Finding the nearest vector among two hundred million

Lantern has two hundred million 768-dimensional chunks. A query arrives, and we need the ten nearest vectors within 200 milliseconds.

### Why the obvious approaches fail

**Exact k-nearest-neighbour search** compares the query to every vector. It is 200 000 000 × 768 ≈ 154 billion multiply-accumulates per query: at an optimistic ten billion per second per core, fifteen core-seconds. It is Chapter 2's `LIKE '%kafka%'` in a new costume.

Spatial indexes (k-d trees, R-trees, quadtrees) prune whole regions of space. They shine in two or three dimensions, are *slower than a brute-force scan* by fifteen or twenty, and are hopeless at 768.

### The curse of dimensionality

Pruning needs "everything in this region is far away". In high dimensions, **almost all distances between random points are nearly the same**; the nearest-to-farthest ratio approaches one. Nothing can be confidently pruned, and a tree that can't prune just visits everything slowly.

Almost all of a high-dimensional cube's volume sits in its 2⁷⁶⁸ corners, so the space is unimaginably empty and three-dimensional intuition misleads. So, as Chapter 2 did with `cardinality`, **we give up on exactness, on purpose.**

### Approximate nearest neighbour search

**ANN** search returns *most* of the true nearest neighbours, most of the time, fast. It is measured by **recall@k**: of the true top ten, how many did we find? Typical targets are 95% to 99%.

That is fine because **relevance is already fuzzy**: nobody skimming ten results notices a missing seventh-best, and two people wouldn't agree on "seventh best" anyway. It is not fine for exact-match work such as deduplication or compliance lookups; use exact search over a small pre-filtered set there.

### HNSW, which you should understand properly

**HNSW** (Hierarchical Navigable Small World) is the default in OpenSearch, Lucene, FAISS, pgvector, Qdrant, Weaviate, and Milvus. Understand it and you understand the landscape.

It builds on **greedy graph traversal**: link each vector to its, say, sixteen nearest neighbours, then search by repeatedly moving to whichever neighbour is closest to the query. This works well, but from far away it takes many small steps, like crossing a country on foot, and can stall in a local minimum. HNSW adds express routes as **layers**:

```
 Layer 2:   A ─────────────────────────────────── F          few nodes, very long links
             ╲                                  ╱
 Layer 1:   A ────── C ────── D ────── E ────── F            more nodes, medium links
             ╲      ╱ ╲      ╱ ╲      ╱ ╲      ╱
 Layer 0:   A─B─C─D─E─F─G─H─I─J─K─L─M─N─O─P─Q─R              every node, short local links
```

Layer 0 holds every vector. Each higher layer holds a random sample of the one below, so its nearest-neighbour links span long distances. A search enters at the top, walks greedily, **drops a layer** when it stops improving, and finishes with a careful local search at layer 0.

**It is an express train network.** The top layer is the intercontinental flight, the middle layers are high-speed rail, and layer 0 is walking the last few blocks. You match the transport to the distance remaining. "Navigable small world" means even layer 0 has a few long-range links, the six-degrees property that makes greedy routing converge quickly.

Search costs roughly **O(log N)**, like a tree without a tree; building costs roughly O(N log N). The catch is **memory**, since HNSW assumes vectors *and* graph sit in RAM:

```
200 000 000 vectors × 768 dims × 4 bytes (float32)  ≈  614 GB   of raw vectors
plus m=16 links × 8 bytes × 200 000 000             ≈   26 GB   of graph
                                                       ────────
                                                       ~640 GB
```

That is an infrastructure decision, which is why the compression options below matter.

### The three parameters you tune

| Parameter | Controls | Higher means | Start at |
|---|---|---|---|
| **`m`** | Links per node per layer | Higher recall; more memory, slower build | 16; 32–48 for high recall |
| **`ef_construction`** | Candidate list while *building* | Better links and recall; slower build, free at query time | 100–512; be generous |
| **`ef_search`** | Candidate list while *searching* | Higher recall and latency; **settable per query** | At least `k` |

Since `ef_search` is per query, one index can serve both a fast path and a thorough one. To tune it, brute-force the exact neighbours of a few hundred sample queries offline as ground truth. Sweep `ef_search`, plot recall against latency, and pick the knee of the curve. Rebuild with higher `m` or `ef_construction` only if that can't reach your target.

### The alternatives, in one pass

| Method | How it works | Notes |
|---|---|---|
| **IVF** | k-means clusters vectors into `nlist` groups; queries search the `nprobe` nearest | Far less memory than HNSW; needs training; recall depends on `nprobe`. A FAISS staple at very large scale |
| **Product quantization (PQ)** | Split each vector into 96 sub-vectors of 8 dims; replace each with a one-byte ID among 256 centroids | 3 072 bytes becomes 96, **32× smaller**, at a real but often acceptable recall cost. Usually combined: `IVF-PQ`, `HNSW-PQ` |
| **Scalar quantization** | float32 to int8 (4×) or one bit (32×) | **int8 is nearly free in quality; make it your first optimization** |
| **LSH** | Hash so nearby vectors collide | Older and generally worse; still useful for streaming dedup |
| **DiskANN** and relatives | Graph index on SSD | Billion-scale corpora where RAM is not an option |

Pair int8 with **rescoring**: retrieve five times as many candidates with compressed vectors, then re-score those with full-precision vectors from disk, for near-exact ranking at quantized memory cost. For Lantern, int8 takes 640 GB to about 180 GB: an ordinary cluster instead of an awkward one.

### In OpenSearch

For Lantern, the vectors live in the same index as the text.

```json
PUT /documents_v3
{
  "settings": {
    "index.knn": true,
    "index.knn.algo_param.ef_search": 200
  },
  "mappings": {
    "properties": {
      "title":    { "type": "text" },
      "body":     { "type": "text" },
      "team":     { "type": "keyword" },
      "body_vec": {
        "type": "knn_vector",
        "dimension": 768,
        "method": {
          "name": "hnsw",
          "space_type": "cosinesimil",
          "engine": "lucene",
          "parameters": { "m": 16, "ef_construction": 256 }
        }
      }
    }
  }
}
```

and the query:

```json
GET /documents/_search
{
  "size": 10,
  "query": {
    "knn": {
      "body_vec": {
        "vector": [0.021, -0.418, ...],
        "k": 50,
        "filter": { "term": { "team": "platform" } }
      }
    }
  }
}
```

For the `engine`: **`lucene`** filters most cleanly and needs no separate native memory pool, so it is easiest to operate; **`faiss`** supports the quantization options above and scales further; **`nmslib`** is legacy. The *Neural Search* plugin can run the embedding model in the cluster so you send text, at the cost of putting inference in the search critical path.

## 3.6 An honest comparison

"Which is better?" is the wrong question, and seeing why is the most valuable thing in this chapter.

| | **Keyword (BM25)** | **Semantic (vectors)** |
|---|---|---|
| Synonyms, paraphrase | ✗ unless hand-configured | ✓ inherent |
| Exact IDs, codes, SKUs | ✓ excellent | ✗ often poor |
| Rare or unseen terms | ✓ that is what IDF is for | ✗ model never saw them |
| Typos | ~ needs fuzzy matching | ✓ fairly robust |
| Cross-language | ✗ | ✓ with a multilingual model |
| Long natural questions | ✗ weak | ✓ strong |
| Negation ("not java") | ~ needs `must_not` | ✗ **also bad** |
| Numeric and date filters | ✓ range queries | ✗ needs metadata filter |
| Explainability | ✓ "matched these terms" | ✗ "the vectors were near" |
| Index cost | Low | High (RAM-resident) |
| New content | Instantly searchable | Needs embedding first |
| How you improve it | Analysers, boosts | Model, chunking, fine-tuning |

**There is barely a row where both are good or both bad**, which follows from §3.3's sparse-versus-dense table. Search `ERR_CONN_REFUSED_0x5f` and BM25 ranks the four documents containing it first, while the embedding model, which has never seen the token, returns twenty vague networking pages. Search "why does my deployment keep restarting" and the result flips.

> **They fail on disjoint sets of queries. Which is exactly why you run both.**

Combining the two is strictly better than either, and in 2026 essentially every serious search system does it. The question is how.

## 3.7 Hybrid search

**Hybrid search** runs a keyword query and a vector query and merges the results.

### Why you cannot simply add the scores

BM25 is **unbounded**: a one-word common query might score 2, a five-word rare one 45. Cosine is **bounded** to [−1, 1] and, per §3.4, in practice clusters in a narrow band well above zero. Add them and BM25 dominates arbitrarily. So either use ranks, or normalise.

### Reciprocal Rank Fusion

Start here. **RRF** ignores scores and uses only *positions*.

```
                        1
RRF(d) =    Σ      ─────────────          k = 60 by convention
         retrievers   k + rankᵢ(d)
```

| Document | Score |
|---|---|
| Rank 1 in BM25, rank 3 in vectors | `1/61 + 1/63 ≈ 0.0323` |
| Rank 2 in both | `2/62 ≈ 0.0323` |
| Rank 1 in one retriever only | `1/61 ≈ 0.0164` |

**A document both retrievers liked moderately beats one that only one retriever loved.** Agreement between two systems that fail differently is the strongest evidence available. RRF is the default because it:

- needs **no normalisation**, so the scale problem never arises;
- needs **no tuning**, since results are insensitive to `k=60`;
- is **robust** across queries and corpora, because ranks are more stable than scores;
- **extends trivially** to more retrievers, such as learned-sparse or editorially promoted lists.

Its weakness: at rank one, "overwhelmingly best" and "mildly best" look the same.

### Normalisation and weighted sum

Normalise each list (min-max within the result set, or z-scores), then blend:

```
final = α · norm(bm25) + (1 − α) · norm(cosine)
```

Pick α between 0.3 and 0.7 by measuring (§3.9), leaning towards BM25 for technical identifiers and vectors for prose. The fragility is min-max: it normalises against the maximum *in this result set*, so a document's score depends on what else was retrieved. It needs more care than RRF. OpenSearch does it with a **search pipeline**:

```json
PUT /_search/pipeline/hybrid
{
  "phase_results_processors": [
    { "normalization-processor": {
        "normalization": { "technique": "min_max" },
        "combination":   { "technique": "arithmetic_mean",
                           "parameters": { "weights": [0.3, 0.7] } }
    }}
  ]
}
```

Then send a `hybrid` query with both sub-queries and `?search_pipeline=hybrid`.

### Retrieve, then rerank

Reranking comes *after* fusion, and it is where the largest quality gains live.

```
     200 000 000 chunks
            │
            │  BM25 ∪ vector search, fused with RRF
            │  cost: milliseconds
            ▼
      ~150 candidates
            │
            │  expensive, accurate reranking model
            │  cost: tens to hundreds of milliseconds
            ▼
        top 10  →  user
```

A model a hundred times more accurate and ten thousand times more expensive is unusable on two hundred million documents and affordable on a hundred and fifty.

### Bi-encoders and cross-encoders

Everything in §3.3 was a **bi-encoder**: query and document are embedded separately and compared. The document vector **does not depend on the query**, which is what lets you precompute it and index it with HNSW.

A **cross-encoder** reads query and document **concatenated into one input**, so attention can compare them token by token and see that "restarting" corresponds to "CrashLoopBackOff". It is far more accurate and *impossible to precompute*, because the score belongs to the pair. One inference per candidate limits it to short lists.

```
BI-ENCODER   (retrieval)                CROSS-ENCODER   (reranking)

 query ──► [model] ──► vector            query ─┐
                            ╲                   ├─► [model] ──► score
 doc   ──► [model] ──► vector             doc  ─┘
        (precomputed)      ╱
                    similarity            one inference per pair,
                                          nothing precomputable
```

Reranking is typically the largest relevance gain in this chapter, often ten to twenty percent nDCG over pure retrieval. Options include Cohere Rerank, the open `bge-reranker` family, and the `ms-marco` cross-encoders.

A **feature-based reranker** is often better in production: a gradient-boosted model over BM25 score, vector score, recency, click-through rate, author authority, document type, and whether the searcher's team wrote it. Less impressive as machine learning, more useful as a product. Lantern's users almost always want their own team's documents, and no cross-encoder will ever discover that.

## 3.8 Filters, and a subtlety that catches everyone

In Lantern, a result must not be archived, must be visible to the user, and must match any team filter. These are requirements, not preferences: **a semantically perfect result the user isn't allowed to see is a security incident.** There are three ways to apply them.

### Pre-filtering

Restrict to matching documents, then find nearest neighbours among them. In principle you get the best `k` valid results.

The subtlety: **a naive pre-filter can destroy ANN recall.** HNSW's greedy walk (§3.5) uses neighbours as stepping stones. If the filter excludes 99% of documents, most stepping stones vanish, and the walk wanders a graph of holes, returning results nowhere near the true nearest valid ones.

Real engines, including OpenSearch and pgvector, counter this two ways:

- **Filtered graph traversal**: excluded nodes can be walked *through* but not *returned*, preserving connectivity.
- **Exact fallback**: when few documents pass the filter, brute-force just those, which is fast.

So filtered ANN handles moderately and very selective filters well, and is weakest in an awkward middle band.

### Post-filtering

Retrieve the top `k`, then drop failures. Simple, but **structurally unable to guarantee results**: ask for ten from a team holding 1% of documents and you typically get zero. Over-fetching (`k × 20`) is a guess dressed up as a parameter. Fine for soft preferences; **never** for tenancy or access control.

### Partitioning by the filter

Give each tenant, team, or category its own index, or use OpenSearch routing to put each tenant on a known shard. The filter becomes *which index you query*: exact, free, and more secure, since a leak needs the wrong index rather than a dropped filter clause. The cost is §2.3's per-index overhead, so it suits tens or hundreds of partitions, not millions.

> **Rule of thumb**
>
> | Situation | Strategy |
> |---|---|
> | Highly selective filter, or hard isolation required | **Partition**, or pre-filter with engine support |
> | Broad filter, more than a fifth of the corpus | The engine's filtered ANN is fine |
> | Correctness-critical constraint | **Never** post-filter |

## 3.9 Knowing whether any of this worked

Relevance work without measurement is not engineering (§2.9), and here the knobs (model, chunking, `ef_search`, fusion, reranker, filters) interact in ways that defeat intuition. A new chunk size can make a better model perform worse; a reranker can hurt when recall is poor. Without measurement you are doing a random walk and calling it iteration.

### Building a judgment set

You need queries paired with the documents that should answer them.

| Source | Strength | Watch out for |
|---|---|---|
| **Click logs** | Cheap, abundant | **Position bias**: clicks only cover what you already ranked high, so they flatter your current ranking |
| **Human raters** | Unbiased gold standard, with graded labels (perfect, good, acceptable, irrelevant) | Expensive, but **two to three hundred well-chosen queries** are enough. Span head, tail, natural-language, identifier, and currently failing queries |
| **Language model** | Generate questions a document answers, check you retrieve it; great before you have traffic | Circular: you measure against a model's idea of a good question |

### What to measure, and where

- **Retrieval: recall@100 or @200.** A reranker can't rescue what retrieval never returned; if recall@100 is 70%, 30% of queries are unanswerable. Fix retrieval first.
- **Final ranking: nDCG@10**, plus **MRR** when there is one right answer.
- **Latency at p50, p95, p99**, in the same table as quality. Three percent more nDCG for ten times the latency is usually a bad trade.

Offline metrics are a proxy. What matters is user behaviour: click-through, click position, **zero-result rate**, reformulations (each a silent failure), session success, and the business outcome. **A/B test anything you ship.** I have seen an nDCG improvement reduce click-through because the new ranking was more "correct" and less interesting.

### An evaluation loop that works

1. Commit the judgment set to version control, next to the code.
2. Script it: run 250 queries, report recall@100, nDCG@10, and p95 latency.
3. Run it in CI on every search change; fail the build on regression.
4. Keep a leaderboard of configurations.
5. **Change one thing at a time.**

Build this *before* tuning. Guesswork about machine learning is still guesswork.

### The failure modes to watch for

| Failure | What happens | Mitigation |
|---|---|---|
| **Stale vectors** | Edited text, old vector | Store a hash of the embedded text; re-embed on change |
| **Mixed model versions** | An interrupted migration leaves two models in one index | Reindex fully into a new index behind an alias, never in place |
| **Chunk-boundary misses** | The answer spans two chunks | Overlap and parent-document retrieval (§3.3) |
| **Near-duplicate flooding** | Top ten are chunks of one document or boilerplate copies | Deduplicate by parent, or use **maximal marginal relevance** for diversity |
| **Cold-start terms** | A new codename or error code the model never saw | Keep BM25 on |
| **Cost surprise** | Embedding two hundred million chunks, again on every model change | Cache embeddings by `hash(text) + model_version`; quantize (§3.5: 640 GB to ~180 GB) |

## 3.10 Lantern, revisited

**Indexing.** Documents are chunked at about 500 tokens with 15% overlap on paragraph boundaries, each chunk prefixed with title and section heading and tagged with its parent ID. Chunks are embedded with a 768-dimensional model, normalised, and int8-quantized.

The vectors live in the **same OpenSearch index** as the text, so one query does keyword matching, vector matching, and filtering in a single round trip, against one consistent view of the data. A separate vector database is reasonable, but it means two systems to sync and two versions of the truth.

**Querying.** BM25 runs over title (boosted) and body, alongside k-NN with `k=100` and `ef_search=200`. Access control and archival status are **pre-filters** applied during traversal. RRF fuses the lists, and a cross-encoder reranks the top 150 into the final ten. A 250-query judgment set runs in CI.

"Why does my deployment keep restarting" now returns *Diagnosing CrashLoopBackOff* first. `ERR_CONN_REFUSED_0x5f` still returns its four documents, because BM25 never stopped working. Both work, for different reasons, and that is the whole point.

## 3.11 What to decide, and in what order

1. **Build the evaluation harness** first, with an achievable judgment set.
2. **Ask whether you need semantic search at all.** Short keyword lookups on structured data may be fine with BM25 and good analysers. Measure it.
3. **Decide chunking**: the biggest quality lever, cheapest to iterate, and it constrains everything downstream.
4. **Pick an embedding model**: a solid open 768-dimensional one, with fine-tuning on click data planned for later.
5. **Add hybrid fusion** with RRF. Don't tune weights yet.
6. **Settle filtering** (pre-filter or partition) *before* building the index; tenancy is painful to retrofit.
7. **Add reranking** only once recall@100 is good. Reranking bad retrieval just adds latency.

---

## Key takeaways

- Inverted indexes match strings; embeddings put similar meanings near each other with no hand-written rules.
- Chunking is the biggest quality lever: 300–800 tokens, 10–20% overlap, natural boundaries, title and parent ID attached.
- The model is schema: same model for index and query, and a model change means a full reindex.
- Use cosine (or dot product on normalised vectors), keep the metric consistent, and calibrate thresholds per model.
- HNSW's layered "express train" graph finds almost-nearest neighbours in O(log N), at a large RAM cost.
- int8 quantization with rescoring is nearly free and cut Lantern from ~640 GB to ~180 GB.
- Keyword and vector search fail on disjoint queries: run both, fuse with RRF, rerank the top ~150.
- Never post-filter for access control; partition, or pre-filter with engine support.
- Measure recall@100, nDCG@10, and latency in CI, changing one thing at a time.

## Where we are

We replaced string matching with geometry and found it complements keyword search rather than replacing it. But we have assumed documents simply *arrive* in the index: every PostgreSQL edit becoming a chunk, a vector, and a searchable segment, reliably, in order, without duplicates, within seconds. That pipeline is a larger problem than search, and Chapter 4 begins with the principles it rests on.
