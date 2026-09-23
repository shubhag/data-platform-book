# Chapter 2 — Finding Things

Lantern has two hundred million documents and a search box, and a database can tell you which documents match a pattern but not which are *about* something. This chapter builds the structure that can, the inverted index, and follows it through OpenSearch across many machines. Every rule is shown on a ten-document lab you can run on a laptop.

**By the end you'll be able to:**

- Explain how an inverted index answers a query without scanning, and why doc values exist alongside it.
- Choose between `text` and `keyword`, and between query and filter context, without guessing.
- Diagnose "this should match and doesn't" in seconds with `_analyze`.
- Read a BM25 score, improve relevance, and measure whether you did.
- Operate an index safely: bulk writes, aliases, reindexing, cluster health and sizing.

---

## 2.0 The lab

Start a single-node OpenSearch. One command, about thirty seconds:

```bash
docker run -d --name lantern -p 9200:9200 \
  -e "discovery.type=single-node" \
  -e "DISABLE_SECURITY_PLUGIN=true" \
  -e "OPENSEARCH_JAVA_OPTS=-Xms1g -Xmx1g" \
  opensearchproject/opensearch:2
```

Create the index with one shard and no replicas, which makes every score in this chapter reproducible (§2.7 explains why).

```bash
curl -s -X PUT localhost:9200/documents -H 'Content-Type: application/json' -d '
{
  "settings": { "number_of_shards": 1, "number_of_replicas": 0 },
  "mappings": {
    "properties": {
      "title":      { "type": "text" },
      "body":       { "type": "text" },
      "team":       { "type": "keyword" },
      "status":     { "type": "keyword" },
      "updated_at": { "type": "date" }
    }
  }
}'
```

Load the corpus. Without `?refresh=wait_for` the documents would be indexed but not yet findable (§2.6).

```bash
curl -s -X POST 'localhost:9200/_bulk?refresh=wait_for' -H 'Content-Type: application/x-ndjson' --data-binary '
{"index":{"_index":"documents","_id":"d1"}}
{"title":"Kafka partitions are ordered","body":"Within a partition Kafka guarantees order. Across partitions it guarantees nothing.","team":"platform","status":"published","updated_at":"2026-03-01"}
{"index":{"_index":"documents","_id":"d2"}}
{"title":"Ordered delivery in Kafka","body":"Ordered delivery requires a single partition or a partition key.","team":"platform","status":"published","updated_at":"2025-11-12"}
{"index":{"_index":"documents","_id":"d3"}}
{"title":"Spark partitions and tasks","body":"A Spark partition maps to one task on one core.","team":"data","status":"published","updated_at":"2024-06-02"}
{"index":{"_index":"documents","_id":"d4"}}
{"title":"Kafka consumer groups rebalance","body":"When a consumer joins or leaves, the group rebalances and partitions move.","team":"platform","status":"published","updated_at":"2026-01-20"}
{"index":{"_index":"documents","_id":"d5"}}
{"title":"The Kafka log is append only","body":"Kafka never modifies a record in place. The log is append only, and that is what makes it fast.","team":"platform","status":"archived","updated_at":"2023-08-30"}
{"index":{"_index":"documents","_id":"d6"}}
{"title":"Tuning Spark shuffle partitions","body":"The default of 200 shuffle partitions is wrong for almost every job.","team":"data","status":"published","updated_at":"2026-02-14"}
{"index":{"_index":"documents","_id":"d7"}}
{"title":"Diagnosing CrashLoopBackOff","body":"A container that exits immediately enters a back-off loop, and the kubelet waits longer between each attempt.","team":"infra","status":"published","updated_at":"2026-03-10"}
{"index":{"_index":"documents","_id":"d8"}}
{"title":"Kubernetes pod memory limits","body":"A pod exceeding its memory limit is killed by the kernel OOM killer.","team":"infra","status":"published","updated_at":"2025-09-01"}
{"index":{"_index":"documents","_id":"d9"}}
{"title":"The novelist Franz Kafka","body":"Franz Kafka wrote The Trial. He has nothing to do with distributed systems.","team":"misc","status":"published","updated_at":"2019-01-01"}
{"index":{"_index":"documents","_id":"d10"}}
{"title":"Postgres replication basics","body":"Streaming replication ships the write-ahead log to a standby.","team":"data","status":"published","updated_at":"2025-05-05"}
'
curl -s 'localhost:9200/documents/_count'
```

```json
{"count":10,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0}}
```

That is the whole lab. Every response printed in this chapter is the real one from this setup, and `docker rm -f lantern` tears it down.

> **A note on the corpus.** d9, about the novelist, will match "kafka" perfectly and be useless every time: the problem of relevance in one document. d7, "Diagnosing CrashLoopBackOff", will be unfindable by the people who need it most: the problem of Chapter 3 in one document.

---

## 2.1 Why the database can't do this

The obvious implementation of Lantern's search box is hopeless:

```sql
SELECT * FROM documents WHERE body LIKE '%kafka%';
```

### It is slow, and an index cannot save it

A B-tree index is sorted by the **beginning** of the value, like a phone book. `LIKE 'Kafka%'` is fast: binary-search to the K's, read forward, stop. `LIKE '%kafka%'` is not, because the matches are scattered through the sort order:

```
"Kafka consumer groups rebalance"  ← matches, filed under K
"Ordered delivery in Kafka"        ← matches, filed under O
"Postgres replication basics"
"The novelist Franz Kafka"         ← matches, filed under T
```

There is nothing to skip, so the database reads all forty terabytes. The index is **sorted by the wrong thing**; §2.2 is the structure sorted by the right thing.

### It is wrong, four times

Run the SQL mentally against the ten documents:

1. **Case.** `'%kafka%'` misses every Kafka document here, because they all capitalise it.
2. **Word forms.** `streaming` misses a document that says `streams`.
3. **Multiple words.** `'%kafka ordering%'` matches nothing, though d1 and d2 are exactly what the user wanted.
4. **Spurious matches.** It cheerfully returns d9, about the novelist.

### And there is no "best"

Even fixed, the query returns eleven thousand rows in no order. The user wants the *best* ten, and **relevance is not a predicate**: there is no `WHERE` clause for "about".

| | Database retrieval | Search |
|---|---|---|
| The question | a precise predicate | a fuzzy intention |
| The answer | the set of matching rows | an *ordered* list, best first |
| "Correct" means | exactly the matching rows | the user found what they wanted |

**Search has no provably correct answer**, which is why §2.9 spends so long on measuring relevance.

---

## 2.2 The inverted index

### Turn the arrow around

A database maps *document → contents*: perfect for "show me d2", useless for "who mentions Kafka?". So invert it, and for each word list the documents containing it. Part of the real thing, from your ten titles:

```
delivery    → [d2]
kafka       → [d1, d2, d4, d5, d9]
ordered     → [d1, d2]
partitions  → [d1, d3, d6]
spark       → [d3, d6]
...
```

That is an **inverted index**. The sorted left column is the **term dictionary**, and each right-hand list is a **postings list**. It is the index at the back of a textbook.

### Why this makes queries fast

**One term.** Binary-search the dictionary to `kafka` and read the list. The cost depends on the list's length, not the corpus size.

**Two terms, AND.** "kafka AND partitions" is the **intersection** of the two sorted lists. Walk them with two fingers, always advancing whichever is behind:

```
A on d1,  B on d1   → equal! emit d1, advance both
A on d2,  B on d3   → advance A
A on d4,  B on d3   → advance B
A on d4,  B on d6   → advance A
A on d5,  B on d6   → advance A
A on d9,  B on d6   → advance B; B runs out → done

result: [d1]
```

Real engines skip ahead, so the work is closer to the length of the *shorter* list. That is why a five-word query over a billion documents takes milliseconds: **nothing is scanned**. This is the most important mechanical idea in the chapter.

### What is in a postings entry

Real postings also carry **term frequency (tf)**, how often the term appears in the document (for ranking, §2.9), and **positions**, its word offsets in the field. Positions make **phrase search** possible. In d2, "Ordered delivery in Kafka", `ordered` is at position 0 and `delivery` at 1, so d2 contains `"ordered delivery"`; "delivery was ordered" would fail the check. Try it:

```bash
curl -s 'localhost:9200/documents/_search?filter_path=hits.hits._id' -H 'Content-Type: application/json' -d '
{ "query": { "match_phrase": { "title": "ordered delivery" } } }'
# → {"hits":{"hits":[{"_id":"d2"}]}}
```

Change `match_phrase` to `match` and you get d2 and d1: `match` wants either word, `match_phrase` uses positions to keep only the adjacent pair. That check runs after the intersection has done the heavy filtering.

### Doc values: the other direction

Now ask the opposite: "what is d4's `updated_at`?" Sorting and aggregating need that, and an inverted index is oriented the wrong way. So the engine also builds **doc values**: for each field, every document's value laid out contiguously in document order (`team`: platform, platform, data, platform, …).

```
inverted index:   term → [documents]     "who has this value?"
doc values:       document → value       "what is this document's value?"
```

**Two structures, opposite orientations, both built at index time.** You *search* with the inverted index and *sort and aggregate* with doc values. A `text` field was shredded into terms, so it has no single value to store, which is why **you cannot sort or aggregate on a `text` field**:

```bash
curl -s 'localhost:9200/documents/_search' -H 'Content-Type: application/json' -d '
{ "sort": [ { "title": "asc" } ] }'
```

```json
{"error":{"root_cause":[{"type":"illegal_argument_exception","reason":
"Text fields are not optimised for operations that require per-document
field data like aggregations and sorting ... Please use a keyword field instead..."}]}}
```

---

## 2.3 The shape of the system

**Lucene** is a Java *library* that implements everything in §2.2, in one process, over files on one disk. **OpenSearch** wraps Lucene in a distributed system: HTTP API, data split across machines, replication, failure handling.

> *Lucene does the searching. OpenSearch does the distributing.*

(OpenSearch is a 2021 fork of Elasticsearch 7.10, so most Elasticsearch documentation applies verbatim.)

### Splitting it up

Lantern's two hundred million documents are roughly six terabytes, too much for one machine, so the index is split as §1.3 described:

```
Cluster          the whole deployment
 └── Node        one OpenSearch process on one machine
   └── Index     a named collection of documents ("documents")
     └── Shard   a slice of the index, itself a complete Lucene index
       └── Segment    an immutable file of indexed data
         └── Document one JSON object (kept verbatim in `_source`)
```

Keep `_source` on: without it you cannot reindex or use the update API. And an index is "like a table" but far more expensive: "one index per customer" across ten thousand customers is a known way to kill a cluster. The idea that takes longest to sink in:

> **A shard is not a piece of a search engine. A shard IS a search engine.**

Split the lab across three shards and shard 2 might hold d3, d6 and d9, with its *own* term dictionary, postings and doc values, answering any query correctly about its own three documents. Three consequences:

1. **Searching an index means searching every shard and merging**: §1.3's scatter-gather, as slow as the slowest shard.
2. **Relevance scores differ slightly between shards** (§2.7), because term rarity is computed per shard.
3. **`terms` aggregations are approximate** (§2.8), for the same reason.

### Primaries and replicas

A **primary** shard accepts writes. A **replica** is an exact copy on a *different* node that serves reads and is promoted if the primary's node dies (§1.4's leader–follower replication).

```
3 primaries, 1 replica each = 6 shards on 3 nodes

    Node A            Node B            Node C
  ┌──────────┐     ┌──────────┐     ┌──────────┐
  │ P0  R2   │     │ P1  R0   │     │ P2  R1   │
  └──────────┘     └──────────┘     └──────────┘
```

No shard shares a node with its own copy. Kill node A and R0 on node B is promoted; the cluster keeps serving and quietly rebuilds a replica elsewhere.

> **The primary count is fixed when the index is created. The replica count can change at any time.** §2.6 shows why.

### Segments: nothing is ever modified

Inside a shard, Lucene writes **segments**: files **never modified** once written. So what does an update do?

```bash
curl -s -X POST 'localhost:9200/documents/_update/d4?refresh=wait_for' -H 'Content-Type: application/json' -d '
{ "doc": { "status": "archived" } }' > /dev/null
curl -s 'localhost:9200/documents/_stats/docs?filter_path=_all.primaries.docs'
```

```json
{"_all":{"primaries":{"docs":{"count":10,"deleted":1}}}}
```

Ten live documents and **one deleted**, from an update. The old version stays on disk, marked deleted, and the new one goes into a new segment; the bytes come back only when a background **merge** skips the dead documents. Deletes work the same way. Now set d4 back to `published` with the same request: the count reads `"deleted":2`, because undoing a change is another write.

Tombstones are not inert: **deleted documents still count in the corpus statistics** BM25 uses (§2.9) until merges catch up.

Why accept this? Immutable files need **no locking**, are **cached without invalidation**, **copy** for recovery without pausing, and are written **sequentially**. The price is late space reclamation and merge I/O. Kafka's log (§5.1) is fast for the same reasons.

### Node roles

- **Cluster manager** (*master* in older docs): keeps cluster state (indexes, mappings, shard locations), holds no data, elected by quorum (§1.6). Run **three** dedicated ones: three survive one failure, a fourth adds nothing.
- **Data nodes** hold shards and do the work. **Ingest, warm and cold** nodes run pre-indexing pipelines and hold older data cheaply.
- **Coordinating node** is a *role in a request*: whichever node receives the query fans it out and merges results.

A background **allocator** keeps replicas off their primary's node, balances disk, and honours **awareness** rules (copies spread across availability zones). Moving shards moves real bytes, one reason §2.11 wants shards in the tens of gigabytes.

---

## 2.4 The schema: mappings

A **mapping** is OpenSearch's schema: per field, its type, how it is analysed, and whether it is searchable, sortable and aggregatable.

### Dynamic mapping, and two ways it bites

Index a field OpenSearch has never seen and it **guesses** a type, permanently:

```bash
curl -s -X POST 'localhost:9200/documents/_doc/d11?refresh=wait_for' -H 'Content-Type: application/json' -d '
{ "title": "Sharding strategies", "views": "417" }' > /dev/null
curl -s 'localhost:9200/documents/_mapping?filter_path=documents.mappings.properties.views'
# → ..."views":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}}...
```

The producer forgot to strip the quotes, so `views` is a **text** field forever; the fix is a full reindex. Failure mode one: **the first document decides.**

Failure mode two is worse. Index user-supplied keys such as `utm_campaign_spring_2026` and dynamic mapping adds a field for **every distinct key**. The mapping lives in the cluster state, replicated to every node on every change, so the cluster manager drowns: **mapping explosion**. Defend with an explicit mapping via an **index template**, and either:

- `"dynamic": "strict"`: reject documents with unknown fields; or
- `"dynamic": false`: keep them in `_source` but do not index them.

Now delete d11 (`curl -s -X DELETE 'localhost:9200/documents/_doc/d11?refresh=wait_for'`). The `views` field stays: deleting the document does not un-guess the type.

### `text` vs `keyword`

A **`text`** field is **analysed** into separate terms (lowercased, maybe stemmed). A **`keyword`** field is **not analysed**: the whole value is exactly one term, byte for byte. Take `"Senior Java Engineer"`:

| | As `text` (terms `senior`, `java`, `engineer`) | As `keyword` (one term) |
|---|---|---|
| Search `java engineer` | matches | no match |
| Exact lookup `"Senior Java Engineer"` | **no match**: the whole was never stored | matches |
| Sort / aggregate | not possible | works (it has doc values) |

In the lab, `title` is `text` and `team` is `keyword`:

```bash
curl -s 'localhost:9200/documents/_search?filter_path=hits.total' -H 'Content-Type: application/json' -d '
{ "query": { "term": { "title": "Kafka partitions are ordered" } } }'
# → {"hits":{"total":{"value":0,"relation":"eq"}}}
```

Zero hits, no error: that is what "OpenSearch is broken" usually looks like. The same `term` query on `team` for `platform` returns 4.

**Which type you want depends on the query, not the field**, so have both through a **multi-field**:

```json
"title": {
  "type": "text",
  "fields": { "keyword": { "type": "keyword", "ignore_above": 256 } }
}
```

Search `title`; sort and aggregate on `title.keyword`. Dynamic mapping produces exactly this for strings (hence `views.keyword`), which is why things often break *after* you write an explicit mapping: you removed a multi-field you did not know was there.

> **Diagnostic shortcut: a search returns nothing and you are certain it should match → check for a `term` query against a `text` field.**

### The nested-object trap

This one gives **silently wrong answers**. A document lists its contributors:

```json
{ "contributors": [ { "name": "asha",  "edits": 120 },
                    { "name": "priya", "edits": 3   } ] }
```

As a plain `object`, it is flattened into `contributors.name → [asha, priya]` and `contributors.edits → [120, 3]`. **The pairing is gone**, so "a contributor named priya with more than 100 edits" matches. The fix is `"type": "nested"`, which stores each sub-object as a hidden document and needs a `nested` query. It is slower, so use it only when you need the correlation.

### The rule

> **You cannot change the type of an existing field.** Changing a mapping means a new index and a copy, which is why aliases (§2.10) exist.

---

## 2.5 Analysis: how text becomes terms

Search the lab for "ordering". d1 is titled "Kafka partitions are **ordered**".

```bash
curl -s 'localhost:9200/documents/_search?filter_path=hits.total' -H 'Content-Type: application/json' -d '
{ "query": { "match": { "title": "ordering" } } }'
```

```json
{"hits":{"total":{"value":0,"relation":"eq"}}}
```

Nothing. This is the most common bug in search.

**Analysis** is the pipeline that turns a string into indexed terms. The crucial fact:

> It runs **twice**, on the document at index time and on the query at query time, and both must produce **compatible terms**, because matching happens on terms and nothing else.

### Three stages

```
  "The Café's WiFi-router is <b>broken</b>!"
        │  1. CHARACTER FILTERS (zero or more, on the raw string)
        ▼     html_strip   → "The Café's WiFi-router is broken!"
        │  2. TOKENIZER (exactly one)
        ▼     standard     → [The, Café's, WiFi, router, is, broken]
        │  3. TOKEN FILTERS (zero or more, in order)
        ▼     lowercase    → [the, café's, wifi, router, is, broken]
              asciifolding → [the, cafe's, wifi, router, is, broken]
              stemmer      → [the, cafe, wifi, router, is, broken]
```

**Tokenizers** split, one per analyzer:

| Tokenizer | Behaviour | Use |
|---|---|---|
| `standard` | Unicode word boundaries | essentially all prose |
| `whitespace` | split on spaces only | when `WiFi-router` should stay one token |
| `ngram` | every substring: `java` (2–4) → `ja, av, va, jav, ava, java` | substring match; index grows several times |
| `edge_ngram` | prefixes only: `java` → `j, ja, jav, java` | autocomplete |

The `edge_ngram` trap: if the query side uses it too, `java` becomes `[j, ja, jav, java]`, and `j` matches every word starting with j. So set `"analyzer"` to the edge-ngram analyzer (index time: all prefixes) and `"search_analyzer"` to `standard` (query time: the word as typed). **This is the canonical reason `search_analyzer` exists.**

### Token filters

- **`lowercase`**: always. **`asciifolding`**: `café → cafe`; users do not type accents.
- **`stop`**: removes "the", "a", "of". Mostly a mistake now: BM25 (§2.9) already weights common words near zero, and stopwords **break phrase queries** (`"to be or not to be"` becomes the empty set).
- **`stemmer`**: `running`, `runs` → `run`. This raises **recall** (of everything relevant, how much you returned) at some cost to **precision** (of what you returned, how much was relevant): an aggressive stemmer merges `universal` and `university`. Strengths run from `porter_stem` to `minimal_english` (plurals only).
- **`synonym_graph`**: make `js` and `javascript` interchangeable. Apply it at query time, in a `search_analyzer`: at index time, every change means reindexing and the expansion distorts IDF.

### `_analyze`, and the five-second diagnosis

The most useful endpoint in the product shows you the terms:

```bash
curl -s 'localhost:9200/documents/_analyze?filter_path=tokens.token' -H 'Content-Type: application/json' -d '
{ "field": "title", "text": "Kafka partitions are ordered" }'
```

Run it on the document text and on the query, and compare:

```
document terms:  [kafka, partitions, are, ordered]
query terms:     [ordering]
                  ↑ no term in common. Not ranked low: not a candidate.
```

The default analyzer does not stem. **The recipe** when a document should match and does not: `_analyze` the document text, `_analyze` the query, compare. The mismatch is almost always visible: a one-sided stemmer, a stopword, case, an accent, a hyphen.

### Fixing it: a custom analyzer

Build one that strips HTML, lowercases, folds accents and stems:

```bash
curl -s -X PUT localhost:9200/documents_v2 -H 'Content-Type: application/json' -d '
{
  "settings": {
    "number_of_shards": 1, "number_of_replicas": 0,
    "analysis": {
      "analyzer": {
        "lantern_text": {
          "type": "custom",
          "char_filter": ["html_strip"],
          "tokenizer": "standard",
          "filter": ["lowercase", "asciifolding", "porter_stem"]
        }
      }
    }
  },
  "mappings": {
    "properties": {
      "title":  { "type": "text", "analyzer": "lantern_text",
                  "fields": { "keyword": { "type": "keyword", "ignore_above": 256 } } },
      "body":   { "type": "text", "analyzer": "lantern_text" },
      "team":   { "type": "keyword" },
      "status": { "type": "keyword" },
      "updated_at": { "type": "date" }
    }
  }
}'
```

The `title.keyword` multi-field fixes §2.2's sort error. Check the analyzer before trusting it:

```bash
curl -s 'localhost:9200/documents_v2/_analyze?filter_path=tokens.token' -H 'Content-Type: application/json' -d '
{ "analyzer": "lantern_text", "text": "Ordered delivery, ordering, and the Café'"'"'s <b>servers</b>" }'
```

```json
{"tokens":[{"token":"order"},{"token":"deliveri"},{"token":"order"},
           {"token":"and"},{"token":"the"},{"token":"cafe'"},{"token":"server"}]}
```

`Ordered` and `ordering` both became `order`. `deliveri` is not a word and need not be: stems only have to be *consistent*. But `cafe'` is a bug: the tokenizer kept `Café's` whole, the stemmer stripped the `s`, and a user typing `cafe` will never match. Found in four seconds; fixed with an `apostrophe` token filter before the stemmer, or a `char_filter` removing `'`.

Copy the corpus across and retry:

```bash
curl -s -X POST 'localhost:9200/_reindex?refresh=true' -H 'Content-Type: application/json' -d '
{ "source": { "index": "documents" }, "dest": { "index": "documents_v2" } }' > /dev/null

curl -s 'localhost:9200/documents_v2/_search?filter_path=hits.hits._id' -H 'Content-Type: application/json' -d '
{ "query": { "match": { "title": "ordering" } } }'
```

```json
{"hits":{"hits":[{"_id":"d1"},{"_id":"d2"}]}}
```

Both documents, from a word neither contains. **Use `documents_v2` from here on**; §2.10 shows how to swap without the application noticing.

---

## 2.6 What happens when you write

Two mysteries: why the primary count is immutable, and why a document you just indexed cannot be found.

### Routing, and the immutable shard count

```
1. Any node receives the document and becomes the coordinator.
2. It picks the shard:  shard = hash(routing) % number_of_primary_shards
   (routing defaults to _id), and forwards to that primary.
3. Primary writes to an in-memory buffer and appends to the TRANSLOG.
4. Primary forwards to all in-sync replicas in parallel.
5. Replicas acknowledge → primary acknowledges to the client.
```

**Step 2 explains the first mystery.** Lantern has 12 primaries; suppose `hash("d-1001") = 90211`:

```
90211 % 12 = 7     → d-1001 lives on shard 7
90211 % 13 = 4     → after growing to 13 shards, the formula says shard 4
```

The document is still on shard 7, so a `GET` asks shard 4 and finds nothing. **It is not gone, it is unfindable**, and changing the divisor does this to nearly every document at once: §1.3's modulus problem again.

**Step 5**: replication is **synchronous**. Success means the document is on the primary *and* its in-sync replicas, so one slow replica raises indexing latency.

### Three meanings of "written to disk"

| Operation | Default schedule | What it guarantees |
|---|---|---|
| **Refresh** | every **1 second** | buffer becomes a new segment → **visible to search** (may still be only in filesystem cache) |
| **Flush** | ~every 30 min, or translog > 512 MB | segments `fsync`ed, fresh translog → **durable in segment form** |
| **Translog fsync** | **every request** | the acknowledged write is **crash-safe** |

The translog is §1.6's write-ahead log, so a power cut after the acknowledgement loses nothing. (`index.translog.durability: async` trades a five-second loss window for throughput.) Hence the most surprising property:

> **A document that has been successfully indexed (acknowledged, durable, crash-safe) is not searchable for up to one second.**

OpenSearch is **near real-time**, deliberately: a segment per second rather than per document is the difference between thousands of writes a second and dozens. Watch it on a throwaway index:

```bash
curl -s -X PUT localhost:9200/nrt_demo -H 'Content-Type: application/json' -d '
{ "settings": { "number_of_shards": 1, "number_of_replicas": 0 } }' > /dev/null
curl -s -X POST 'localhost:9200/nrt_demo/_doc/d99' -H 'Content-Type: application/json' -d '
{ "title": "Watermarks in Flink" }'
curl -s 'localhost:9200/nrt_demo/_search?filter_path=hits.total' -H 'Content-Type: application/json' -d '
{ "query": { "match": { "title": "watermarks" } } }'
```

```json
{"_index":"nrt_demo","_id":"d99","_version":1,"result":"created",...}
{"hits":{"total":{"value":0,"relation":"eq"}}}
```

Created, acknowledged, zero hits. Two seconds later the same search finds it. Nothing was fixed; a timer fired.

| Setting | Effect | Use it |
|---|---|---|
| `?refresh=wait_for` | request blocks until the next scheduled refresh | in tests (and the §2.0 bulk load) |
| `?refresh=true` | forces a tiny segment per write | **never in application code**: kills indexing, then search latency |
| `refresh_interval: -1`, `number_of_replicas: 0` | no refreshes, no replication | bulk-loading an unsearched corpus, then restore; three- to fivefold speedups are routine |

### Merging, and the sawtooth

Every refresh makes a segment and **every query consults every segment**, so background **merges** combine them and purge deleted documents. Merges compete with indexing for I/O, hence the sawtooth in indexing throughput, and SSDs for write-heavy clusters.

`_forcemerge` to one segment speeds up a **finished**, read-only index and reclaims deleted space. On an **actively written** index it hurts: future merges must keep rewriting one enormous segment.

---

## 2.7 What happens when you search

A search runs in two phases across every relevant shard:

```
PHASE 1 — QUERY
  Coordinator sends the query to one copy of every shard.
  Each shard returns its top `size` (id, score) pairs, NOT documents.
  Coordinator merges them into a global top `size`.

PHASE 2 — FETCH
  Coordinator fetches _source for just those documents.
```

With 20 shards and `size=10`, that is 200 tiny pairs and 10 documents, instead of 200 full documents with 190 thrown away.

### Consequence one: deep pagination is a trap

To return global ranks 100 001–100 010 (`from=100000&size=10`), the coordinator needs each shard's top **100 010**, since they could all come from one shard:

```
20 shards × 100 010 scored hits = 2 000 200 entries built, shipped, sorted
returns 10
```

Cost grows with depth times shard count, which is why OpenSearch refuses past ten thousand results by default. Use **`search_after`**: pass the last hit's sort values (e.g. `["2026-03-01T00:00:00Z", "d1"]`) and each shard seeks straight past them, at constant cost per page.

For full exports, the **point-in-time** API (or older scroll API) holds a consistent snapshot. And nobody has ever gone to page ten thousand: a product that "needs" it needs better filters.

### Consequence two: relevance is slightly approximate

Each shard computes term rarity from *its own* documents: if "kafka" is in 2% of shard 3 but 6% of shard 7, identical documents score differently on each. With many documents per shard the rates converge; with few (a test fixture split many ways) the effect is baffling, hence the lab's single shard. `search_type=dfs_query_then_fetch` collects global statistics first, for an extra round trip; production rarely needs it.

---

## 2.8 Asking good questions

### Query context vs filter context

The most important distinction in the query language:

| | Query context | Filter context |
|---|---|---|
| Asks | *how well* does it match? | *does* it match, yes or no? |
| Scores | yes | no |
| Cacheable | no (depends on this query) | yes, as a **bitset**: one bit per document |

`status = published` over ten million documents is about 1.2 MB of bits, computed once and reused:

```
docs:    d1 d2 d3 d4 d5 d6 d7 d8 d9 d10
bitset:   1  1  1  1  0  1  1  1  1   1     ← d5 is archived
```

The `bool` query combines both:

```bash
curl -s 'localhost:9200/documents_v2/_search?filter_path=hits.hits._id,hits.hits._score' -H 'Content-Type: application/json' -d '
{
  "query": {
    "bool": {
      "must":     [ { "match": { "body": "partition" } } ],
      "filter":   [ { "term":  { "status": "published" } },
                    { "range": { "updated_at": { "gte": "2025-01-01" } } } ],
      "should":   [ { "match": { "title": "kafka" } } ],
      "must_not": [ { "term":  { "team": "misc" } } ]
    }
  }
}'
```

```json
{"hits":{"hits":[
  {"_id":"d2","_score":1.70151},
  {"_id":"d1","_score":1.6782764},
  {"_id":"d4","_score":1.3972867},
  {"_id":"d6","_score":0.7113347}
]}}
```

- `must`: must match **and** scores. Every hit mentions a partition.
- `filter`: must match, scores **nothing**, cached. d5 (archived) and d3 (2024) are gone.
- `should`: optional, boosts when it matches. That is why the Kafka titles sit above d6.
- `must_not`: excludes. d9 is on team `misc`.

> **Every yes-or-no constraint belongs in `filter`, never in `must`.**

Being published makes a document eligible, not more relevant, and moving such clauses into `filter` often halves latency.

### The families of query

**Full-text queries** analyse their input like the document:

- **`match`**: the workhorse. `"kafka ordering"` becomes `[kafka, order]` and finds documents with any of them (all, with `"operator": "and"`).
- **`match_phrase`**: terms adjacent and in order. `"slop": 2` lets `"kafka ordering"` match "ordering in Kafka".
- **`multi_match`**: one text, several fields, and its `type` matters:

| `type` | Scores by | Right for |
|---|---|---|
| `best_fields` (default) | the single best field | "whichever field this matches" |
| `most_fields` | sum across fields | the same text indexed several ways |
| `cross_fields` | fields treated as one | a name spread across `first_name`, `last_name`, `city` |

**Term-level queries** (`term`, `terms`, `range`, `exists`, `prefix`, `wildcard`, `regexp`, `fuzzy`, `ids`) do **not** analyse input, so they belong on `keyword`, numeric and date fields. Two warnings: `term` on a `text` field quietly finds nothing (§2.4), and a leading-`*` `wildcard` or a `regexp` may scan the whole term dictionary, `LIKE '%...%'` again. Pay for substring matching at index time with ngrams (§2.5).

**Compound queries** shape relevance: `function_score` (typically a **date decay**), `boosting` (**demote** rather than exclude, usually what you want), `dis_max`, `rank_feature`, and the slow `script_score`. And **field boosting**, `"fields": ["title^3", "body"]`, is the cheapest relevance win in existence.

### Sorting

Sorting reads **doc values**, so sort on `title.keyword`, not `title`; in `documents_v2`, `{"sort": [{"title.keyword": "asc"}]}` works where §2.2's failed (d7, d4, d1).

**Always end with a unique tie-breaker such as `_id`**, as in `"sort": [{"updated_at": "desc"}, {"_id": "asc"}]`. Otherwise equal values come back in varying order, and pagination repeats some results and skips others.

### Aggregations

Aggregations answer "how many, grouped by what" for facets and dashboards. They read doc values and nest freely: **bucket** aggregations group (`terms`, `date_histogram`), **metric** ones compute a number per bucket (`max`, `cardinality`, `percentiles`), and **pipeline** ones work on other aggregations' output (`bucket_selector` is SQL's `HAVING`). Documents per team, with each team's latest edit:

```bash
curl -s 'localhost:9200/documents_v2/_search?filter_path=aggregations' -H 'Content-Type: application/json' -d '
{ "size": 0,
  "aggs": { "by_team": {
      "terms": { "field": "team", "size": 10 },
      "aggs": { "latest": { "max": { "field": "updated_at" } } } } } }'
```

```json
{"aggregations":{"by_team":{
  "doc_count_error_upper_bound":0, ...
  "buckets":[
    {"key":"platform","doc_count":4,"latest":{..."value_as_string":"2026-03-01T00:00:00.000Z"}},
    {"key":"data","doc_count":3,"latest":{..."value_as_string":"2026-02-14T00:00:00.000Z"}},
    ...
```

`"size": 0` skips the fetch phase. Note `doc_count_error_upper_bound`: zero here, and the reason for the next heading.

### Three aggregations that lie to you (usefully)

- **`terms`**: each shard ships only its own top N. A value ranked **eleventh** on each of twenty shards (900 documents each) appears in no response, though with 18 000 documents it may be the true #1. `shard_size` asks each shard for more candidates.
- **`cardinality`** (distinct count) uses HyperLogLog++: constant memory, roughly one to two percent error. Exact, over fifty million values, is an outage.
- **`percentiles`** uses t-digest, most accurate at extremes like p99, where you care.

> **At scale, exactness is often unaffordable, and approximation is a legitimate engineering choice**, as long as it is explicit, bounded and understood.

Just don't build billing on a HyperLogLog estimate. Chapter 3 makes the same trade with approximate nearest-neighbour search. And beware nesting: 10 000 users × 1 000 URLs × 100 days is a billion buckets in heap. When you hit `search.max_buckets`, rethink the query, not the limit.

---

## 2.9 Relevance: what "best" means

```bash
curl -s 'localhost:9200/documents_v2/_search?filter_path=hits.hits._id,hits.hits._score' -H 'Content-Type: application/json' -d '
{ "query": { "match": { "title": "kafka" } } }'
```

```json
{"hits":{"hits":[
  {"_id":"d1","_score":0.6859519},
  {"_id":"d2","_score":0.6859519},
  {"_id":"d9","_score":0.6859519},
  {"_id":"d4","_score":0.6859519},
  {"_id":"d5","_score":0.56802315}
]}}
```

**d9, the novelist, is tied for first**, and d5 scored lower for a reason you cannot see yet. This section explains both.

### A score is relative

`_score` means something only within one query's results, so "show results scoring above 5" cannot work: `kafka` tops out at 0.69 and `kafka consumer group lag` at 4.84, because four terms' scores are summed. The corpus moves it too: in the lab "the" is in only two titles, so it is *rare* and outscores "kafka". A score is a statement about a corpus, not a document. For "good enough", normalise within the result set, train a model, or use a cross-encoder (§3.7).

### Three signals

1. **Term frequency: more is better.**
2. **Inverse document frequency (IDF): rarer is more valuable.** Lucene computes `IDF = ln(1 + (N − n + 0.5) / (n + 0.5))`, with `N` total documents and `n` containing the term:

   ```
   "kafka"  in  5 of 10 docs → ln(2.00) = 0.693
   "spark"  in  2 of 10 docs → ln(4.40) = 1.482
   a term   in 10 of 10 docs → ln(1.05) = 0.047
   ```

   "spark" is worth twice "kafka" and thirty times a term in every document, which is why stopword lists are unnecessary.
3. **Field length: shorter is stronger evidence.**

Combined, that is TF-IDF.

### BM25, and the two things it fixes

OpenSearch uses **BM25**, the same signals handled more carefully. Shown once, not to be memorised:

```
                          f(t,d) · (k₁ + 1)
score(q,d) = Σ  IDF(t) · ─────────────────────────────────
            t∈q           f(t,d) + k₁ · (1 − b + b·|d|/avgdl)
```

`f(t,d)` is the term's count in the document, `|d|` the field length, `avgdl` the average field length, and `k₁ = 1.2`, `b = 0.75` by default.

**Fix one: term frequency saturates.** Under TF-IDF, a hundred mentions score a hundred times one, so keyword stuffing wins. BM25's fraction (at average length) climbs toward `k₁ + 1`: 1.000 at one mention, 1.375 at two, 2.075 at twenty, 2.081 at twenty-one, never past 2.2. `k₁` sets how fast it saturates.

**Fix two: length normalisation has a dial.** With `avgdl = 100` and one occurrence, a 10-word title scores 1.583, an average field 1.000, and a 1000-word page 0.214: **seven times** less than the title. `b = 0` ignores length, `b = 1` fully normalises, and 0.75 is a tuned compromise; lower it for fields like tag lists where length means nothing.

**Now the lab numbers make sense.** Titles average 3.9 terms; d1, d2, d4 and d9 have four, and d5 has six:

```
IDF("kafka") = 0.693,  k₁ = 1.2,  b = 0.75,  avgdl = 3.9

d1  |d|=4:  0.693 × 2.2 / (1 + 1.2×(0.25 + 0.75×4/3.9))  = 0.686
d5  |d|=6:  0.693 × 2.2 / (1 + 1.2×(0.25 + 0.75×6/3.9))  = 0.568
```

d5 is not less about Kafka; its longer title reads as dilution. d9 ties for first because BM25 knows words and nothing else. Tuning `k₁` and `b` is a **small** effect; exhaust boosting, analysers and query structure first.

**Finding out why.** Add `"explain": true` to a search and each hit returns exactly this arithmetic, labelled (`n = 5`, `N = 10`, `dl = 4`, `avgdl = 3.9`). `GET /documents_v2/_explain/{id}` answers why *one* document scored as it did, or **why it did not match at all**; `"profile": true` gives per-shard timings.

### Getting better relevance, by effort-to-reward

1. **Boost fields**: `"title^3"`.
2. **Index the same text several ways** (exact, stemmed, ngrammed) and combine with `multi_match` `most_fields`; exact matches then outrank stemmed ones for free.
3. **Add a phrase boost**: a `match_phrase` in `should`.
4. **Decay by recency or popularity**: `function_score` with `gauss`, or cheaper `rank_feature`.
5. **Learn to rank**: a plugin that reranks the top N with a trained model over features like BM25 score and click-through.
6. **Add semantic search**: Chapter 3, where the largest modern gains are.

The first three together. Watch d9 fall:

```bash
curl -s 'localhost:9200/documents_v2/_search?filter_path=hits.hits._id,hits.hits._score' -H 'Content-Type: application/json' -d '
{
  "query": {
    "bool": {
      "must":   [ { "multi_match": { "query": "kafka ordering",
                                     "fields": ["title^3", "body"] } } ],
      "should": [ { "match_phrase": { "title": { "query": "kafka ordering", "slop": 2 } } } ]
    }
  }
}'
```

```json
{"hits":{"hits":[
  {"_id":"d1","_score":7.481207},
  {"_id":"d2","_score":6.4565296},
  {"_id":"d9","_score":2.0578558}, ...
```

"ordering" stems to `order`, which d9 lacks, and the title boost widens the gap to a factor of three. The novelist did not stop matching; he stopped *winning*, which is all that matters in a ranked list.

### Measuring it, which you must do first

> **Without measurement, you cannot tell whether any of those changes helped.**

A title boost helps some queries and hurts others. Build a **judgment list**: queries with graded relevant documents (`"kafka ordering"` → d1 perfect, d2 good, d5 acceptable), from click logs, human ratings, or a language model. Then measure:

| Metric | Meaning | Example |
|---|---|---|
| **Precision@k** | fraction of the top k that is relevant | 6 relevant in top 10 → 0.6 |
| **Recall@k** | fraction of all relevant documents in the top k | 20 exist, 6 in top 10 → 0.3 |
| **MRR** | 1 / rank of first relevant result, averaged | first at rank 3 → 0.333 |
| **nDCG@k** | graded relevance, discounted by position | **the standard headline metric; report this one** |

The **Rank Evaluation API** computes these for you:

```bash
curl -s 'localhost:9200/documents_v2/_rank_eval' -H 'Content-Type: application/json' -d '
{ "requests": [ { "id": "kafka_ordering",
    "request": { "query": { "multi_match": { "query": "kafka ordering", "fields": ["title^3","body"] } } },
    "ratings": [ { "_index": "documents_v2", "_id": "d1", "rating": 3 },
                 { "_index": "documents_v2", "_id": "d2", "rating": 2 },
                 { "_index": "documents_v2", "_id": "d9", "rating": 0 } ] } ],
  "metric": { "dcg": { "k": 10, "normalize": true } } }'
```

```json
{"metric_score":1.0,"details":{"kafka_ordering":{"metric_score":1.0,
  "unrated_docs":[{"_id":"d4"},{"_id":"d5"}], ...}}}
```

1.0: the ranking is already ideal. `unrated_docs` is the honest part: the metric is only as good as your list. Change `title^3` to `title^10`, rerun across two hundred queries, and you *know*. Wire it into CI so a relevance regression **fails a build**: the most valuable infrastructure in a search project, and routinely the last built.

---

## 2.10 Living with an index

### Bulk indexing

One request per document is slow (§1.9). The `_bulk` API you used in §2.0 batches `index`, `update` and `delete` actions as newline-delimited JSON (an action line, then a source line, and a required trailing newline). Size requests by bytes (five to fifteen megabytes), run two to four threads per data node, and retry `429 Too Many Requests` with backoff: it is backpressure (§1.6), not an error.

> **A `200 OK` on a bulk request does not mean the documents were indexed.**

Items fail independently. Send one good document and one with a bad date:

```bash
curl -s -X POST 'localhost:9200/_bulk?refresh=wait_for' -H 'Content-Type: application/x-ndjson' --data-binary '
{"index":{"_index":"documents_v2","_id":"ok"}}
{"title":"A fine document","updated_at":"2026-01-01"}
{"index":{"_index":"documents_v2","_id":"bad"}}
{"title":"A broken document","updated_at":"last tuesday"}
'
```

```json
HTTP/1.1 200 OK
{ "errors": true,                          ← the flag you must check
  "items": [
    { "index": { "_id": "ok",  "status": 201 } },
    { "index": { "_id": "bad", "status": 400,
                 "error": { "type": "mapper_parsing_exception", ... } } } ] }
```

Two hundred OK, and a document is gone. **Check `errors` and the per-item results**, or the pipeline loses documents silently while every dashboard looks healthy. And, per §1.7, **use a deterministic `_id`** from the source system: a retry then overwrites instead of creating a second copy, so the pipeline is **idempotent for free**.

### Aliases, and one rule

An **alias** is a name that points at one or more indexes. Swapping one is a single request:

```json
POST /_aliases
{ "actions": [
    { "remove": { "index": "documents_v1", "alias": "documents" } },
    { "add":    { "index": "documents_v2", "alias": "documents" } }
]}
```

Both actions apply **atomically**: there is no instant when the alias points at both or neither.

> **Your application should never reference a concrete index name. Only ever an alias.**

Then every schema change is a zero-downtime swap with instant rollback, for the cost of typing `documents` instead of `documents_v1` on day one. Aliases can also carry a filter (a per-team view), pin routing, or mark an `is_write_index`, which makes rollover work.

### Reindexing

Changing anything immutable (a field type, an analyser, the primary count) means a new index and a copy, the `_reindex` you ran in §2.5. In production add `?slices=auto` (parallel by shard) and `wait_for_completion=false` (returns a task ID to poll with `GET _tasks/<id>`); a `script` can transform documents in flight. The zero-downtime pattern:

1. Create `documents_v2` with the new mapping.
2. Write new changes to **both**, or better, make the pipeline **replayable** from its source (Chapter 5).
3. Reindex history from v1 to v2.
4. **Verify**: counts, spot checks, and your `_rank_eval` set against both.
5. Swap the alias atomically, and keep v1 for a rollback window.

**Step 4 is the one people skip**, and rollback is trivial only if you notice the problem. Relatives: **`_update_by_query`** rewrites documents in place (a new analyser without a new index), and **`_delete_by_query`** writes tombstones (§2.3).

### Templates, lifecycles and snapshots

For time-series data, an **index template** configures each new index matching a pattern, **rollover** starts the next index when the current one is too large or old, and **ISM** (Index State Management) runs the lifecycle: force-merge at seven days, warm nodes at thirty, snapshot and delete at ninety. **Snapshots** are incremental backups to S3, and, as §4.6 insists, **replication is not backup**: it copies your accidental `_delete_by_query` everywhere, instantly.

---

## 2.11 When it goes wrong

### Reading cluster health

`GET _cluster/health` returns a status colour:

| Status | Meaning | Severity |
|---|---|---|
| **Green** | every primary and replica assigned | fine |
| **Yellow** | a replica unassigned: data serving, **redundancy gone** | fix today (harmless on one node, hence the lab's zero replicas) |
| **Red** | some **primary** unassigned: data unavailable, writes to it fail | outage |

Red does not error: searches quietly return **partial results**, so check `_shards.failed`. Then start with `GET _cluster/allocation/explain`, which says in prose why a shard is unassigned. The usual culprit is a **disk watermark**: allocation stops at **85%**, shards stop moving in at **90%**, and indexes go **read-only at 95%** (the **flood stage**), so writes fail while nobody watched disk. Others: a node that left, filtering rules excluding every node, too many shards per node, or a corrupt shard needing `_cluster/reroute?retry_failed`.

### Sizing, with the reasoning

- **Ten to fifty gigabytes per shard.** Smaller wastes per-shard overhead; larger makes recovery slow, because moving a shard moves all of it.
- **About twenty shards per gigabyte of heap per node**, and fewer is better.
- **Heap at most 31 GB**, a real cliff: above roughly 32 GB the JVM loses *compressed ordinary object pointers*, every pointer doubles, and a 32 GB heap holds less than a 31 GB one.
- **Heap at most half of RAM**, because Lucene's speed comes from the **filesystem cache** holding memory-mapped segments. (Kafka wants the same, §5.3.)

The most common mistake is **oversharding**: every shard has fixed overhead and every query pays §1.3's straggler tax on each. Use `primaries = ceil(expected_total_GB / 30)`, rounded to a multiple of your data node count, and **not up "just in case"**. Replicas are the easy half: at least one in production, more for read throughput.

### Troubleshooting playbook

| Symptom | Check, in order |
|---|---|
| **Searches slow** | `"profile": true` first. Then: yes/no clauses not in `filter`; deep pagination; `wildcard`/`regexp`/`script` to precompute; too many shards; **working set bigger than the filesystem cache** (often the real answer: more RAM). Turn on the slow log. |
| **Indexing slow** | Bulk size and concurrency; `429`s; a stray `?refresh=true`; merges; replicas not zeroed for a backfill; **dynamic mapping updates on every document**. |
| **`CircuitBreakingException`** | The breaker is protecting you: fix the query, not the limit. |
| **GC pauses, nodes flapping** | A node in a two-second pause looks dead (§1.9), so shards reallocate away and back, for hours. Usually too many shards, fielddata on a text field, or an unbounded aggregation. |

---

## 2.12 Where Lantern stands

Lantern's search, where every phrase should now decode:

- 200 million documents in `documents_v1`, **behind the alias** `documents` (§2.10).
- **Twelve primaries of ~25 GB**, one replica each, on six data nodes, plus **three dedicated cluster managers** (§2.3, §2.11).
- A **custom analyser plus a `keyword` multi-field** (§2.4, §2.5); status, team and date only in **`filter`** (§2.8).
- **`title^3`, a phrase clause, a recency decay**, and two hundred judged queries in CI failing the build if nDCG@10 drops (§2.9).
- **`_bulk`** in ten-megabyte batches with source IDs and per-item checks (§2.10); edits searchable about a second later (§2.6).

It works: forty milliseconds at the median, a hundred and eighty at p99, and a node can die without anyone noticing.

### And the wall it hits

A user types *"why does my deployment keep restarting"*. The answer is d7, **"Diagnosing CrashLoopBackOff"**. Run the intersection from §2.2:

```
query terms:     [why, doe, my, deploy, keep, restart]
document terms:  [diagnos, crashloopbackoff]
intersection:    ∅
```

```bash
curl -s 'localhost:9200/documents_v2/_search?filter_path=hits.total' -H 'Content-Type: application/json' -d '
{ "query": { "multi_match": { "query": "why does my deployment keep restarting",
                              "fields": ["title^3","body"] } } }'
# → {"hits":{"total":{"value":0,"relation":"eq"}}}
```

Not ranked low: *not a candidate*. "CrashLoopBackOff" *means* "keeps restarting", and the inverted index has no representation of meaning. Synonyms (`k8s → kubernetes`, `pod → container`, and a thousand more) patch it one word at a time, and phrasings are not enumerable.

---

## Key takeaways

- An **inverted index** maps terms to sorted document lists; a query is an intersection, so nothing is scanned. **Doc values** map documents to values for sorting and aggregation, so sort on `.keyword`, never `text`.
- A **shard is a complete search engine**: searches scatter-gather, scores vary per shard, `terms` counts are approximate.
- Routing is `hash(_id) % primaries`, so **the primary count is fixed at creation**; replicas can change any time.
- **Refresh** (visibility, 1 s) ≠ **flush** (segment durability) ≠ **translog fsync** (crash safety).
- Something should match and doesn't? **`_analyze` both sides**, and look for `term` on a `text` field.
- **Yes-or-no constraints go in `filter`**: no scoring, cached bitsets.
- **BM25** scores are relative; field boosting is the cheapest win, and nothing counts as better until a judgment set says so.
- **Query through an alias, use deterministic IDs, check per-item bulk errors.**
- Shards of 10–50 GB, heap ≤ 31 GB and ≤ half of RAM, and no oversharding.

## Where we are

Everything in this chapter matches words, and users ask about meaning. To close that gap you have to stop indexing words and start indexing something else, which is Chapter 3, and it begins with a very strange idea.
