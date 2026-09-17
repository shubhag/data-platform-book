# Chapter 3 — Meaning as Geometry

## 3.1 The gap we cannot close with words

Chapter 2 ended with two failing queries, and they are worth looking at again, because they fail in different ways and the difference matters.

A user types **"why does my deployment keep restarting"**. The document that answers this is titled *Diagnosing CrashLoopBackOff*. The overlap between query and document is zero words. Not "few" — zero. No amount of stemming helps, because there is no shared root. No amount of field boosting helps, because there is nothing to boost. A synonym list could in principle map "restarting" to "CrashLoopBackOff", and someone could add that entry, and then tomorrow somebody types "pod won't stay up" and we are back where we started.

A user types **"kubernetes pod memory limits"**. The right document is titled *k8s container resource constraints*. Here there is one shared word — "kubernetes" is not in the title but appears in the body — so the document does match, and it ranks fourteenth, below several documents that mention Kubernetes far more often but are about something else entirely.

The first failure is **recall**: the right answer is not in the result set at all. The second is **ranking**: it is there, buried. Both come from the same root cause, and it is the defining limitation of everything in Chapter 2:

> **An inverted index matches strings. It has no representation of meaning. Two documents that say the same thing in different words are, to the index, completely unrelated.**

Every technique in Chapter 2 is an attempt to patch this one word at a time. Stemming handles inflection. Synonyms handle a hand-written list of equivalences. Ngrams handle typos. Each is a small, curated exception to the rule that only identical strings match, and there is no finite list of exceptions that covers how humans phrase things. The approach does not scale, and more importantly it does not *generalise* — every new phrasing requires a new rule written by a person.

What we need is a representation in which "restarting" and "CrashLoopBackOff" are *inherently* close, without anybody having written that down. And it turns out such a representation exists, and the idea behind it is genuinely one of the more surprising things in computing.

## 3.2 An old idea: meaning is context

Long before neural networks, linguists had a hypothesis, usually stated as: *you shall know a word by the company it keeps.* The claim is that a word's meaning is determined by the contexts it appears in, and therefore that two words appearing in similar contexts have similar meanings.

Think about how you would learn the meaning of a word you had never seen. Someone hands you ten thousand sentences containing "wug". They say "the wug barked", "I walked the wug", "my wug needs feeding", "the wug chased a cat". You have not been told what a wug is. But you now know a great deal, and in particular you know that "wug" is used almost exactly where "dog" is used. You would not be far wrong to treat them as near-synonyms.

Now scale that up. Take every sentence on the internet. For every word, record the distribution of contexts it appears in. Words with similar context distributions get similar representations. What you get is not a dictionary definition — the machine has no idea what a dog *is* — but something operationally more useful: a system in which similar things are represented similarly, derived entirely from observed usage, with no human writing any rules.

The representation that comes out of this is a list of numbers. And that is the idea we now need to look at properly.

## 3.3 Embeddings: meaning with coordinates

An **embedding** is a fixed-length list of floating-point numbers — a **vector** — that represents a piece of content in a way that places similar meanings near each other.

```
"senior java engineer"        →  [ 0.021, −0.418,  0.093, ...,  0.177 ]
"experienced JVM developer"   →  [ 0.019, −0.402,  0.101, ...,  0.169 ]
"chocolate cake recipe"       →  [−0.331,  0.284, −0.556, ...,  0.042 ]
```

768 numbers each, in a typical model. Look at the first two: almost identical, number by number. The third bears no resemblance to either. Nobody wrote a rule saying that "java" relates to "JVM". It fell out of the training.

### How to picture 768 dimensions

You cannot, and you should not try. But you can reason about it by analogy, and the analogy holds up well enough to be genuinely useful.

Imagine a space with three axes, and imagine that every possible sentence is a single point somewhere in it. Sentences about cooking cluster in one region. Sentences about Kubernetes cluster in another, far away. Within the Kubernetes region, sentences about networking sit a little apart from sentences about storage. "Why does my deployment keep restarting" and "Diagnosing CrashLoopBackOff" land almost on top of each other, because they are about the same thing, even though they share no words.

Now replace three axes with 768. Everything about the picture stays the same — points, regions, clusters, distances — you just cannot draw it. The geometry is what matters, and the geometry is unchanged.

A reasonable next question is: what does each of the 768 numbers *mean*? And the honest answer is **nothing individually.** Dimension 412 is not "formality" or "technicality" or anything you could name. The meaning lives in the *pattern* across all 768 together. This is why these are called **dense** vectors: every dimension carries a little bit of signal, and none carries an interpretable one.

That word "dense" is a contrast with something, and the contrast is illuminating. The inverted index of Chapter 2 also represents a document as a vector — one dimension per word in the vocabulary, with a number in the slot for each word the document contains. But that vector has fifty thousand dimensions and all but a few dozen are zero. It is **sparse**. And critically, each of its dimensions *does* have an interpretable meaning: dimension 4 411 is precisely and only the word "kubernetes".

Lay them side by side and the entire chapter's trade-offs become visible:

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

Keep that table in mind. Almost everything in §3.6 and §3.7 is a consequence of it.

### Where embeddings come from

A neural network, usually a transformer, trained so that related texts end up near each other. The dominant recipe is **contrastive learning**: show the model pairs that *should* be close — a question and its correct answer, a title and its article, two paraphrases — and pairs that should be far apart, usually just random text. Adjust the weights to pull the first kind together and push the second kind apart. Repeat a few hundred million times.

The families you will encounter:

**Open models you can self-host.** `all-MiniLM-L6-v2` produces 384-dimensional vectors, is tiny and very fast, and is a genuinely good default for getting started. `all-mpnet-base-v2` gives 768 dimensions and noticeably better quality. The `BGE`, `E5`, and `GTE` families are current, strong, and include good multilingual variants.

**Hosted APIs.** OpenAI's `text-embedding-3-small` and `-large`, Cohere Embed, Voyage. Trivial to adopt, no infrastructure, and a per-token bill that becomes interesting when you are embedding two hundred million documents.

**Fine-tuned models.** A mid-sized model fine-tuned on *your* query-to-document pairs — harvested from your own click logs — will usually beat a much larger generic model on your own data. This is the single highest-leverage quality improvement available in this chapter, and it is also the one that requires you to have built the evaluation infrastructure in §3.8 first.

### Five practical facts that will bite you

**One: the dimension count is part of your schema.** A 768-dimensional model always produces exactly 768 numbers. Your index declares that number. Which means **changing embedding models means re-embedding and reindexing every document.** Treat the model identity as schema: put it in the index name (`documents_v3_bge_base`), keep it behind an alias (§2.10), and plan model upgrades as reindexes. This is not an obscure edge case — you will want to change models within the first year.

**Two: models silently truncate.** Every model has a maximum input length, often between 256 and 8192 tokens. Feed it more and it does not error; it processes the beginning and discards the rest. If you embed a forty-page specification as one vector, you have embedded page one and thrown away thirty-nine. Which leads directly to:

**Three: chunking is a real design decision, and the most important one here.** You must split long documents into pieces and embed each piece.

Get the size wrong in either direction and quality collapses. Chunks that are too large produce what I think of as *semantic mush*: a vector that is the average of five unrelated topics, close to nothing in particular and vaguely close to everything. Chunks that are too small lose the context that makes them meaningful — a chunk reading "it supports up to 500 concurrent users" is useless if the vector has no idea what "it" refers to.

A sensible starting point is **300 to 800 tokens with 10–20% overlap**, split on natural boundaries: paragraph breaks, section headings, sentence ends. Not at a fixed character count, which cheerfully cuts sentences in half. The overlap exists because an answer may straddle a boundary, and a little redundancy is much cheaper than a miss.

Two refinements are worth applying from the start. **Prepend context to each chunk** — the document title and the section heading — so that a chunk from deep inside a page still knows what it is about. And **store the parent document ID** with every chunk, so that after matching a chunk you can retrieve its neighbours or the full document to give the user something coherent to read. This pattern is sometimes called *small-to-big*: match precisely on small chunks, return generously from the parent.

**Four: symmetric and asymmetric search are different problems.** Comparing two things of the same kind — finding duplicate tickets, finding similar products — is *symmetric*. Comparing a short question to a long document is *asymmetric*, and the two are not interchangeable. Models trained for asymmetric search, which includes most of the current best ones, expect you to mark which side is which, using instruction prefixes like `"query: ..."` and `"passage: ..."`. Omit the prefixes and the model still works, produces plausible-looking vectors, and quietly loses a significant chunk of its quality. Read the model card. This is a very easy mistake to make and a very hard one to notice.

**Five: the same model must be used for indexing and for querying, always.** Two different models produce vectors in unrelated coordinate systems. Distances between them are not merely inaccurate; they are meaningless. If half your index is embedded with one model and half with another, your search is broken in a way that does not throw errors and does not show up in any obvious metric.

## 3.4 Measuring closeness

We have vectors. Now: given two of them, how close are they? There are three answers in common use, and choosing correctly matters more than you would expect.

### Cosine similarity

This is the default, and the one to use unless something tells you otherwise. It measures the **angle** between two vectors and completely ignores their lengths.

```
                A · B          Σ aᵢbᵢ
cos(A,B) = ───────────── = ────────────────────
            ‖A‖ · ‖B‖      √(Σaᵢ²) · √(Σbᵢ²)
```

The result runs from **−1** (pointing in opposite directions) through **0** (perpendicular, unrelated) to **1** (pointing the same way). The numerator is the dot product; the denominator divides out both magnitudes, leaving only direction.

Why throw away magnitude? Because magnitude usually encodes something you don't care about. A vector's length tends to reflect text length, or how many high-frequency words it contains — artefacts of the input rather than its meaning. Two documents about the same topic, one a paragraph and one a page, *point in the same direction* but have different lengths. Cosine says they are similar, which is correct. Euclidean distance would say they are far apart, which is not.

One calibration warning, because it causes real product mistakes. Cosine scores from a modern embedding model do **not** spread nicely across the range. Two completely unrelated sentences typically score 0.3 to 0.6, not 0.0, because the model's output space is not centred. Genuinely related pairs score 0.7 to 0.9. So **0.75 does not mean "75% relevant"** — it might mean "barely related" or "essentially identical" depending on the model. If you need a threshold, you must calibrate it empirically on your own data, per model. A hard-coded 0.8 that somebody chose because it sounded confident is a bug waiting to surface.

### Dot product

```
A · B = Σ aᵢbᵢ
```

Cosine without the normalisation. Cheaper to compute, since you skip two square roots.

The relationship between the two is worth stating precisely, because it saves work: **if your vectors are normalised to unit length, dot product and cosine similarity are identical.** So the standard optimisation is to normalise once at index time and then use dot product forever, getting cosine's ranking at dot product's cost.

If vectors are *not* normalised, dot product rewards longer vectors, which means it will favour documents whose embeddings happen to have large magnitude. Some models deliberately exploit this to encode a notion of importance or confidence in the magnitude. Most don't. Check the model card.

### Euclidean (L2) distance

```
d(A,B) = √( Σ (aᵢ − bᵢ)² )
```

Straight-line distance in the space. Intuitive, and carrying one property that causes bugs: **smaller is better.** Every other measure here is "higher is better", so mixing L2 into a pipeline inverts your sort order somewhere, and the failure looks like "my search returns the least relevant results first", which is at least an easy symptom to spot.

For unit-length vectors, L2 and cosine produce *identical rankings*, because `d² = 2 − 2·cos`. So the choice only matters for unnormalised vectors.

### The rule, and the trap

> Use **cosine**. If you normalise at index time, use **dot product** and get the same ranking faster.

And then the trap, which is worth stating as a warning because it produces silently degraded results rather than errors:

> **The metric must be consistent everywhere — in your index configuration, in your query, and in any reranking or fusion logic downstream.**

If you build an HNSW index configured for L2 and then score with cosine, the index's internal graph was constructed to optimise for a different notion of "near" than the one you are querying with. It will return results. They will be worse than they should be, by an amount you cannot easily measure. This is a configuration mismatch that no error message will tell you about, so check it deliberately.

## 3.5 Finding the nearest vector among two hundred million

Here is the problem. Lantern has two hundred million chunks, each a 768-dimensional vector. A query arrives, gets embedded, and we need the ten nearest vectors.

### Why the obvious approach fails

Compare the query against every indexed vector, keep the best ten. This is **exact k-nearest-neighbour search**, and it is trivially correct.

Count the arithmetic: 200 000 000 vectors × 768 dimensions = about 154 billion multiply-accumulate operations, per query. Even at a very optimistic ten billion operations per second per core, that is fifteen core-seconds. For one search. Against a target of 200 milliseconds.

Notice also that this is a *scan*. There is no index helping us — we are looking at every single vector. It is the `LIKE '%kafka%'` of Chapter 2 all over again, in a new costume.

So the natural next thought is: use a spatial index. There are well-known data structures for "find the nearest point" — k-d trees, R-trees, quadtrees — which recursively partition space so that you can prune whole regions without examining them.

They do not work here, and the reason is interesting enough to have a name.

### The curse of dimensionality

Classic spatial indexes work beautifully in two or three dimensions and degrade until, somewhere around fifteen or twenty dimensions, they become *slower than a brute-force scan*. At 768 dimensions they are hopeless.

The intuition: pruning works when you can say "everything in this region is far away, skip it". But as dimensions increase, something strange happens to distances. Consider points scattered randomly in a high-dimensional cube. Compute the distance from one point to every other. In two dimensions, those distances vary a lot — some points are clearly near, others clearly far. In 768 dimensions, **almost all the distances are nearly the same.** The ratio between the nearest and the farthest neighbour approaches one. Everything is roughly equidistant from everything else.

If all points are about equally far away, there is no region you can confidently prune, and a tree that cannot prune is just an expensive way to visit everything.

Another way to feel it: in high dimensions, essentially all of a cube's volume is in its corners, and there are 2⁷⁶⁸ corners. Space is unimaginably empty and your points are unimaginably sparse within it. Geometric intuition built in three dimensions is not merely imprecise here; it is actively misleading.

So we need something fundamentally different. And since exact search is impossible and clever exact indexes don't exist, we do what Chapter 2 did with `cardinality` aggregations: **we give up on exactness, on purpose.**

### Approximate nearest neighbour search

**ANN** search returns *most* of the true nearest neighbours, most of the time, very fast. The quality measure is **recall@k**: of the true top ten, how many did we actually find? Typical operating points are 95% to 99%.

And the crucial observation that makes this acceptable: **relevance is already fuzzy.** If the true seventh-best result is missing from a list of ten that a human will skim in four seconds, nobody notices. Nobody can even define "seventh best" consistently — ask two people to rank ten documents and they will disagree. Spending a hundred times the compute to guarantee you found a ranking that nobody agrees with anyway is not a good trade.

Where it *is* a bad trade: exact-match tasks. Deduplication ("is this identical to something we have?"), compliance lookups, anything where a miss has consequences. For those, use exact search over a small pre-filtered candidate set, and be explicit about the difference.

### HNSW, which you should understand properly

There are several ANN algorithms. One dominates: **HNSW**, Hierarchical Navigable Small World. It is the default in OpenSearch, in Lucene, in FAISS, in pgvector, in Qdrant, Weaviate, and Milvus. If you understand HNSW you understand the vector search landscape.

Start with the simpler idea it builds on. Suppose you connect every vector to its, say, sixteen nearest neighbours, forming a graph. Now to search: start at any node, look at its neighbours, move to whichever is closest to your query, repeat. Stop when no neighbour is closer than where you are. This is **greedy graph traversal**, and it works remarkably well — you walk "downhill" towards your query.

It has one serious flaw. If you start far from the target, you take a great many small local steps to get there, like crossing a country entirely on foot. And you can get stuck in a local minimum, arriving at a point where no immediate neighbour is closer even though much better matches exist elsewhere.

HNSW's fix is to add express routes. Build **several layers** of graph:

```
 Layer 2:   A ─────────────────────────────────── F          few nodes, very long links
             ╲                                  ╱
 Layer 1:   A ────── C ────── D ────── E ────── F            more nodes, medium links
             ╲      ╱ ╲      ╱ ╲      ╱ ╲      ╱
 Layer 0:   A─B─C─D─E─F─G─H─I─J─K─L─M─N─O─P─Q─R              every node, short local links
```

The bottom layer contains every vector, connected to its close neighbours. Each layer above contains a random sample of the layer below — roughly one node in `m` — connected to *its* nearest neighbours, which, because the layer is sparse, are much further apart in absolute terms. The top layer has a handful of nodes joined by enormous hops.

A search starts at the single entry point in the top layer and greedily walks towards the query. Each hop covers a vast distance. When no neighbour in that layer improves things, it **drops down a layer** and continues with the denser, shorter links. And so on down to layer 0, where it does a careful local search and collects the best candidates.

The analogy that makes this stick: **it is an express train network.** The top layer is the intercontinental flight, crossing the map in one hop. The middle layers are high-speed rail between cities. Layer 0 is walking the last few blocks. You would never walk from London to Tokyo, and you would never fly between two adjacent buildings — matching the granularity of your transport to the distance remaining is exactly what the hierarchy does.

That is the "hierarchical" part. The "navigable small world" part refers to a property of the graph: even at layer 0 there are a few unexpectedly long-range links, giving the graph the six-degrees-of-separation character that makes greedy routing converge quickly instead of wandering.

Search costs roughly **O(log N)** — the same shape as a tree, achieved without any tree. Build costs roughly O(N log N), since each inserted vector must find its own neighbours by searching the partially-built graph.

The catch is **memory**. The graph's links are as essential as the vectors themselves, and HNSW is designed on the assumption that the whole thing sits in RAM. Walk the arithmetic for Lantern:

```
200 000 000 vectors × 768 dims × 4 bytes (float32)  ≈  614 GB   of raw vectors
plus m=16 links × 8 bytes × 200 000 000             ≈   26 GB   of graph
                                                       ────────
                                                       ~640 GB
```

That is a genuine infrastructure decision, not a rounding error. It is why §3.5's final subsection exists.

### The three parameters you tune

**`m`** — how many links each node keeps per layer. Higher means a better-connected graph, which means higher recall, and also more memory and slower builds. 16 is a good default; 32 to 48 for high-recall requirements.

**`ef_construction`** — how large a candidate list to consider while *inserting* each vector during the build. Higher means better-quality links, hence higher recall at query time, at the cost of a slower build. It costs nothing at query time, so it is the cheapest knob to be generous with. 100 to 512.

**`ef_search`** — how large a candidate list to maintain while *searching*. Higher means higher recall and higher latency. Crucially, this one is **adjustable per query at runtime**, so you can serve a fast-and-approximate path and a slow-and-thorough path from the same index. It must be at least `k`.

The tuning method is worth doing properly rather than guessing, and it is not much work. Take a sample of a few hundred queries. Compute the *exact* nearest neighbours by brute force — slow, but it is offline and one-time, and it gives you ground truth. Then sweep `ef_search` across a range, measure recall against ground truth and latency at each point, and plot the two against each other. You will see a curve that rises steeply and then flattens. Pick the knee. Only if `ef_search` alone cannot reach your recall target should you rebuild with a higher `m` or `ef_construction`.

### The alternatives, in one pass

Worth recognising even if HNSW is what you use.

**IVF** (inverted file, or cell probing) clusters all vectors into `nlist` groups with k-means, and at query time searches only the `nprobe` clusters nearest the query. Much lower memory than HNSW, requires a training step, and recall depends directly on `nprobe`. A FAISS staple, and a reasonable choice at very large scale.

**Product quantization (PQ)** compresses vectors aggressively. Split each 768-dimensional vector into, say, 96 sub-vectors of 8 dimensions; cluster each sub-space into 256 centroids; replace each sub-vector with the one-byte ID of its nearest centroid. A 3 072-byte vector becomes 96 bytes — **thirty-two times smaller** — at a real but often acceptable cost in recall. Usually combined with an index rather than used alone: `IVF-PQ`, `HNSW-PQ`.

**Scalar quantization** is the simpler, blunter version: store each float32 as an int8, for a 4× reduction, or as a single bit, for 32×. And here is the practically important fact: **int8 quantization costs almost nothing in quality and should usually be your first optimization.** Four times less memory for a fraction of a percent of recall is not a trade-off so much as a free lunch. It pairs beautifully with a **rescoring** pass: retrieve five times as many candidates using the compressed vectors, then re-score just those few hundred with full-precision vectors read from disk. You get near-exact ranking at quantized memory cost. Applied to Lantern, int8 takes our 640 GB down to around 180 GB, which is the difference between an awkward cluster and an ordinary one.

**LSH**, locality-sensitive hashing, is the older approach: hash vectors so that nearby ones collide. Simpler than HNSW, generally worse on the recall-versus-latency curve, still useful for streaming deduplication.

**DiskANN** and its relatives are graph indexes designed to live on SSD rather than in RAM, for corpora at billion scale where memory is simply not an option.

### In OpenSearch

The nice thing about all of this, for Lantern, is that it lives in the same index as the text.

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

A note on the `engine` choice, since it is not obvious. **`lucene`** integrates most cleanly with filtering and needs no separate native memory pool, which makes it the easiest to operate. **`faiss`** supports the quantization options above and scales further. **`nmslib`** is the legacy option. OpenSearch also offers a *Neural Search* plugin that runs the embedding model inside the cluster, letting you send text rather than vectors — convenient, at the cost of putting model inference in your search cluster's critical path.

## 3.6 An honest comparison

We now have two retrieval systems. It is tempting to ask which is better. That is the wrong question, and seeing why is the most valuable thing in this chapter.

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

Read down the two columns and notice the pattern: **there is barely a row where they are both good or both bad.** Where one is strong the other is weak, almost systematically.

That is not a coincidence; it follows from §3.3's sparse-versus-dense table. BM25 operates on exact tokens, so it is superb precisely when the user's exact tokens matter and useless when they don't. Embeddings operate on a learned, smoothed, generalised representation, so they are superb when meaning matters and useless when the literal string is the point.

Two concrete cases make it vivid. A user searches for the error code `ERR_CONN_REFUSED_0x5f`. BM25 finds it instantly — the term appears in four documents, its IDF is enormous, and those four documents rank first. The embedding model has almost certainly never seen that token, tokenises it into meaningless fragments, produces a vector that is a vague average of "error" and "connection", and returns twenty documents about networking, none of which is the one. Conversely, a user searches "why does my deployment keep restarting". The embedding model lands within a whisker of the CrashLoopBackOff document. BM25 returns nothing.

> **They fail on disjoint sets of queries. Which is exactly why you run both.**

This is not a compromise or a hedge. Running both and combining the results is strictly better than either, and in 2026 essentially every serious search system does it. The question is how to combine them.

## 3.7 Hybrid search

**Hybrid search** means running a keyword query and a vector query and merging the two result lists.

### Why you cannot simply add the scores

It seems like it should work: take the BM25 score, add the cosine similarity, sort.

It does not, because the two numbers are not on remotely the same scale. BM25 is **unbounded** — its value depends on how many query terms matched, how rare those terms are, and how large the corpus is. A one-word query on a common word might score 2; a five-word query on rare terms might score 45. Cosine similarity is **bounded** to [−1, 1] and, as §3.4 noted, in practice clusters in 0.6 to 0.9.

Add them and BM25 dominates completely and arbitrarily. The vector contribution becomes a rounding error on long queries and nearly the whole score on short ones. You have not combined two signals; you have added noise to one.

So there are two legitimate approaches: normalise the scores, or ignore them and use ranks.

### Reciprocal Rank Fusion

Start here. **RRF** throws away the scores entirely and uses only the *positions*.

```
                        1
RRF(d) =    Σ      ─────────────          k = 60 by convention
         retrievers   k + rankᵢ(d)
```

A document ranked first by BM25 and third by the vector search scores `1/61 + 1/63 ≈ 0.0323`. A document ranked second by both scores `2/62 ≈ 0.0323` as well. A document that only one retriever found at rank one scores `1/61 ≈ 0.0164`.

Look at what that last comparison encodes. **A document that both retrievers liked moderately beats a document that only one retriever loved.** And that is exactly right, because agreement between two systems that fail in different ways is the strongest evidence available. Two independent methods converging on the same document is much better evidence than one method being very confident.

RRF is the default recommendation for four reasons. It needs no normalisation, so the scale problem simply does not arise. It needs no tuning — the `k=60` is a convention that works, and results are not sensitive to it. It is robust across query types and corpora, because ranks are more stable than scores. And it extends trivially to three or more retrievers: add a learned-sparse retriever, or a business-rules list of editorially-promoted documents, and just sum another term.

Its weakness is the flip side of its strength: by discarding magnitude, it treats "overwhelmingly the best match" and "mildly the best match" identically when both are at rank one. Sometimes that information was worth keeping.

### Normalisation and weighted sum

The alternative: normalise each list onto a common scale — min-max within the result set, or z-scores — and then blend.

```
final = α · norm(bm25) + (1 − α) · norm(cosine)
```

with α somewhere between 0.3 and 0.7, chosen by measurement on your evaluation set (§3.8). This gives you a dial: turn it towards BM25 for a corpus of technical identifiers, towards vectors for a corpus of prose.

The fragility is in the normalisation. Min-max normalises against the maximum *in this result set*, which means the same document can get a different normalised score depending on what else happened to be retrieved alongside it. Scores shift as the corpus changes. It works, and it needs more care than RRF.

OpenSearch implements this natively through a **search pipeline**:

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

You then issue a `hybrid` query containing the two sub-queries and pass `?search_pipeline=hybrid`.

### Retrieve, then rerank

The third approach is not an alternative to the first two but a stage after them, and it is where the largest quality gains live.

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

The logic is economic. A model that is a hundred times more accurate and ten thousand times more expensive is unusable on two hundred million documents and entirely affordable on a hundred and fifty. So use the cheap system to find candidates and the expensive system to order them.

### Bi-encoders and cross-encoders

To understand why reranking works so much better, you need one architectural distinction.

Everything in §3.3 was a **bi-encoder**. The query is embedded on its own; the document was embedded, separately, months ago. The two vectors are then compared. The essential property is that the document's embedding **does not depend on the query** — which is exactly what lets you precompute it and build an HNSW index over it. The model never sees the query and the document together; it only ever sees each in isolation and hopes the geometry lines up.

A **cross-encoder** works differently. It takes the query and the document **concatenated into one input**, and runs the whole thing through the model together. Now the attention mechanism can compare them token by token — it can notice that "restarting" in the query corresponds to "CrashLoopBackOff" in the document, and that the document's mention of "memory limits" answers the query's implicit question. It has access to the interaction, not just to two independent summaries.

This is far more accurate. And it is *impossible to precompute*, because the score is a property of the pair. You must run one model inference per candidate document, which is why it can only be applied to a short list.

```
BI-ENCODER   (retrieval)                CROSS-ENCODER   (reranking)

 query ──► [model] ──► vector            query ─┐
                            ╲                   ├─► [model] ──► score
 doc   ──► [model] ──► vector             doc  ─┘
        (precomputed)      ╱
                    similarity            one inference per pair,
                                          nothing precomputable
```

In practice, reranking typically delivers the single largest relevance improvement of anything in this chapter — often ten to twenty percent nDCG over pure retrieval. Options include Cohere Rerank, the open `bge-reranker` family, and the `ms-marco` cross-encoders.

There is also a feature-based alternative that is often better in production: a gradient-boosted model over engineered features — BM25 score, vector score, recency, click-through rate, author authority, document type, whether the searcher's own team wrote it. It is less impressive as machine learning and more useful as a product, because it can incorporate **business** signals that no text model has access to. Lantern's users, for instance, almost always want documents from their own team, and no cross-encoder will ever discover that.

## 3.8 Filters, and a subtlety that catches everyone

Almost every real query has hard constraints. In Lantern: the document must not be archived; the user must have permission to see it; if they selected a team filter, it must match.

These are not preferences to be traded against relevance. They are requirements. **A semantically perfect result that the user is not allowed to see is worse than no result** — it is a security incident.

There are three ways to combine filters with vector search, and the differences are not cosmetic.

### Pre-filtering

Restrict to the documents matching the filter, then find the nearest neighbours among those. Conceptually right: you always get exactly `k` valid results, and they are the best valid results.

But here is the subtlety, and it is the most important thing in this section. **A naive pre-filter can destroy ANN recall**, and understanding why requires remembering how HNSW works.

HNSW's graph was built assuming every node is reachable. The greedy traversal of §3.5 works by hopping from neighbour to neighbour towards the query. Now suppose your filter excludes 99% of documents. The traversal starts at the top-layer entry point and tries to walk towards the query — but almost every node it wants to step to is excluded. It cannot use those nodes as stepping stones. The connectivity it depended on is gone. It wanders through a graph that is mostly holes, and the ten results it eventually returns may be nowhere near the true nearest valid neighbours.

Real engines handle this, and it is worth knowing how, because the behaviour differs by selectivity. They implement **filtered graph traversal**, where the filter is evaluated during the walk and excluded nodes can still be traversed *through* even though they cannot be *returned* — which preserves connectivity. And when the filtered set is small enough, they abandon the graph entirely and **fall back to exact brute-force search** over just those documents, which is fast precisely because there are few of them. OpenSearch and pgvector both do this. The result is that filtered ANN works well for moderately selective filters and for very selective ones, and is weakest in an awkward middle band.

### Post-filtering

Retrieve the top `k` by vector similarity, then discard those that fail the filter.

Simple, requires no engine support, and **structurally unable to guarantee results.** If you ask for ten and filter for a team that holds 1% of documents, you will typically get zero. The mitigation is to over-fetch — ask for `k × 20` and hope — which is a guess dressed up as a parameter.

Post-filtering is acceptable for soft preferences. It must **never** be used for correctness-critical constraints such as tenancy or access control, because "we asked for more candidates and hoped enough would be permitted" is not an authorization model.

### Partitioning by the filter

Put each tenant, or team, or category in its own index — or use OpenSearch's routing so that each tenant's documents live on a known shard.

Now the filter is not a filter at all. It is *which index you query*. There is no recall degradation because there is nothing to filter: every document in the index is already permitted. It is exact, it is free, and it is also a much stronger security posture, because cross-tenant leakage requires querying the wrong index rather than a filter clause being accidentally dropped from a query builder.

The cost is index proliferation, with the per-index overhead §2.3 warned about, so it works for tens or hundreds of partitions and not for millions.

> **The rule of thumb.** Highly selective filters, or hard isolation requirements: **partition**, or pre-filter with engine support. Broad filters covering more than a fifth of the corpus: the engine's filtered ANN is fine. Correctness-critical constraints: never post-filter.

## 3.9 Knowing whether any of this worked

I said in §2.9 that relevance work without measurement is not engineering. In this chapter the point becomes acute, because you now have a dozen interacting knobs — model, chunk size, overlap, `ef_search`, fusion method, fusion weight, reranker, filter strategy — and they interact in ways that defeat intuition. Changing chunk size can make a better model perform worse. Adding a reranker can hurt if retrieval recall is poor. Without measurement you are performing a random walk and calling it iteration.

### Building a judgment set

You need queries paired with the documents that should answer them. Three ways to get them, and they complement each other.

**Implicit, from click logs.** If a user searched something and clicked the fourth result, that result was probably relevant. This is cheap and abundant, and it carries a bias you must remember: it can only tell you about documents you already ranked highly enough to be clicked. This is **position bias**, and it means click data will happily confirm that your current ranking is excellent.

**Explicit, from human raters.** Present query-document pairs and have people rate them, ideally on a graded scale (perfect, good, acceptable, irrelevant) rather than a binary one. Expensive, unbiased, the gold standard. And the required volume is smaller than people fear: **two to three hundred well-chosen queries** will detect the differences that matter. Choose them to span your traffic — head queries, tail queries, natural-language questions, exact identifier lookups, queries that currently fail.

**Synthetic, from a language model.** Take a document, ask a model to generate the questions this document answers, and check whether your system retrieves that document for those questions. This is excellent for bootstrapping when you have no traffic at all, and it has an obvious circularity to be aware of — you are measuring against a model's idea of a good question.

### What to measure, and where

The metrics were introduced in §2.9; what is new here is knowing which one belongs to which stage.

For the **retrieval** stage, measure **recall@k** with a generous k — recall@100 or recall@200. The only question that matters for retrieval is: *is the right answer anywhere in the candidate set?* Because a reranker cannot rescue a document that retrieval never returned. Recall is the ceiling on everything downstream, and if recall@100 is 70%, then 30% of your queries are unanswerable no matter how good your reranker is. Fix retrieval first.

For the **final ranking**, measure **nDCG@10**, plus **MRR** if there is essentially one right answer per query. These reward putting the best thing first, which is what the user experiences.

And measure **latency at p50, p95, and p99** alongside quality, every time, in the same table. A three percent nDCG gain for ten times the latency is usually a bad trade, and quality-only reporting hides that.

### Online, which is what actually matters

Offline metrics are a proxy. The real measures are what users do: click-through rate, the position of the click, the **zero-result rate**, how often users reformulate their query (a reformulation is a failure you did not otherwise record), session success, and whatever the business outcome is — tickets resolved, documents found, time to answer.

**A/B test anything you intend to ship.** Offline metrics correlate with user satisfaction; they do not equal it. I have seen an nDCG improvement ship and reduce click-through, because the new ranking was more "correct" and less interesting.

### An evaluation loop that works

1. Commit the judgment set to version control, next to the code.
2. Write a script: run these 250 queries, report recall@100, nDCG@10, and p95 latency.
3. Run it in CI on every change to anything touching search. Fail the build on regression beyond a threshold.
4. Keep a leaderboard of configurations with their numbers.
5. **Change one thing at a time.** This is the discipline that makes the rest work.

Build this *before* you start tuning, not after. Everything before it is guesswork, and guesswork that happens to be about machine learning is still guesswork.

### The failure modes to watch for

A few specific ways semantic search goes wrong in production, each of which I have seen surprise a competent team.

**Stale vectors.** A document is edited but not re-embedded. The text and the vector now describe different things, and keyword search and semantic search disagree about the same document. Guard against it by storing a hash of the embedded text alongside the vector and re-embedding when it changes.

**Mixed model versions.** Half the index embedded with model v1, half with v2, because a migration was interrupted. Distances across the two halves are meaningless, so results are subtly and unfixably wrong. Always reindex fully into a new index behind an alias, never in place.

**Chunk-boundary misses.** The answer spans two chunks and neither one alone matches. Overlap and parent-document retrieval (§3.3) are the mitigations.

**Near-duplicate flooding.** The top ten results are ten chunks of the same document, or ten copies of the same boilerplate paragraph. Technically correct, practically useless. Deduplicate by parent document, or apply **maximal marginal relevance**, which explicitly trades a little relevance for diversity.

**Cold-start terms.** A new product, a new internal codename, a new error code. The embedding model has never seen it and cannot represent it. Keyword search handles it perfectly. One more argument for hybrid, and a good reason not to switch off BM25 once vectors are working.

**Cost surprise.** Embedding two hundred million chunks is a real bill, and re-embedding on every model change multiplies it. Cache embeddings keyed by `hash(text) + model_version`, so that a reindex re-embeds only what actually changed. And note from §3.5 that quantization takes Lantern's memory from 640 GB to about 180 GB, which may be the difference between a project that is approved and one that is not.

## 3.10 Lantern, revisited

Here is where the system now stands.

Documents are chunked at roughly 500 tokens with 15% overlap, split on paragraph boundaries, each chunk prefixed with its document title and section heading and tagged with its parent document ID. Each chunk is embedded with a 768-dimensional model, normalised to unit length, and int8-quantized. The vectors live in the **same OpenSearch index** as the text, alongside the keyword fields — which is worth pausing on, because it means one query can do keyword matching, vector matching, and metadata filtering in a single round trip, against one consistent view of the data, with one set of shards to operate. Keeping vectors in a separate specialised database is a reasonable choice, but it means two systems to keep in sync, and two versions of the truth about which documents exist.

A query runs BM25 across title and body, with title boosted, and simultaneously a k-NN search with `k=100` and `ef_search=200`. Access control and archival status are **pre-filters**, evaluated during graph traversal. The two result lists are fused with RRF. The top 150 go to a cross-encoder reranker, which returns the final ten. A judgment set of 250 queries runs in CI reporting recall@100, nDCG@10, and p95 latency.

"Why does my deployment keep restarting" now returns *Diagnosing CrashLoopBackOff* in first position. `ERR_CONN_REFUSED_0x5f` still returns the four documents containing it, because BM25 never stopped working. Both work, and they work for different reasons, and that is the whole point.

## 3.11 What to decide, and in what order

If you are building this from scratch, the order matters more than people assume, so here is the sequence I would defend.

**First, build the evaluation harness.** Yes, before anything else, with a judgment set small enough to be achievable. Every subsequent decision is measurable or it is a guess.

**Second, ask whether you need semantic search at all.** If your queries are short keyword lookups against structured data, BM25 with good analysers and field boosting may genuinely be enough, and you will have saved yourself a great deal of infrastructure. Measure it. This chapter is not free, and some systems do not need it.

**Third, decide chunking.** Biggest quality lever, cheapest to iterate on, and it constrains everything downstream.

**Fourth, pick an embedding model.** A solid open 768-dimensional model to start. Plan to fine-tune later on your own click data, because that is where the next big gain will come from.

**Fifth, add hybrid fusion.** RRF. Do not tune weights yet.

**Sixth, settle filtering.** Decide pre-filter versus partition *before* you build the index, because tenancy is very painful to retrofit.

**Seventh, add reranking** — but only once retrieval recall@100 is good. A reranker applied to bad retrieval improves nothing and costs latency.

---

## Where we are

We have replaced string matching with geometry. Text becomes a vector; similar meanings become nearby points; nearest-neighbour search finds them; and because exact nearest-neighbour search is impossible at scale, HNSW navigates a layered graph to find *almost* the nearest, fast — the same deliberate, bounded approximation that OpenSearch makes for distinct counts and percentiles.

We also found that the new method is not a replacement for the old one but a complement, failing on almost exactly the queries where keyword search succeeds, which is why hybrid retrieval with rank fusion and a cross-encoder reranker is the modern architecture.

But notice what we have been quietly assuming for two chapters. We have assumed that documents *arrive* in the index. That an edit in PostgreSQL becomes a chunk, becomes a vector, becomes a bulk request, becomes a searchable segment — reliably, in order, without duplicates, within a couple of seconds, for hundreds of edits a second, forever, and recoverably when any part of it breaks.

That pipeline is a larger engineering problem than either of the two chapters we have just finished. It is the subject of the rest of the book, and Chapter 4 begins with the principles it rests on.
