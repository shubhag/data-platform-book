# Chapter 2 — Finding Things

## 2.1 Why the database can't do this

Lantern has two hundred million documents in PostgreSQL, and a search box. The obvious implementation is the one we wrote in §1.1:

```sql
SELECT * FROM documents WHERE body LIKE '%kafka%';
```

Let us be precise about why this is hopeless, because the reasons are instructive.

It is **slow**, and unfixably so. A `LIKE '%...%'` pattern cannot use a B-tree index, because a B-tree is sorted by the *beginning* of the value and the pattern doesn't anchor there. So the database reads every row of a forty-terabyte table and scans every body text for the substring. This takes hours, and adding an index does not help, because there is no index that answers this question.

It is **wrong**, in at least four ways. A search for "kafka" misses a document that says "Kafka" — unless you lowercase everything, which you now have to remember to do consistently in two places. A search for "streaming" misses a document that only says "streams", because the database has no notion that those are the same word. A search for "kafka streaming" finds nothing at all, because no document contains that exact string, even though thousands are about precisely that. And a search for "kafka" happily matches a document about the novelist, or one that mentions Kafka once in a footnote about something else entirely.

Which brings us to the deepest problem. Even if you fixed all of the above, the query returns **eleven thousand rows in no particular order**. But the user wants *ten* rows, and specifically they want the *best* ten. The database has no concept of "best". It can tell you whether a row matches. It cannot tell you that one match is better than another, because relevance is not a predicate.

Search is a fundamentally different problem from retrieval, and it needs a fundamentally different data structure. That structure has a name, it was invented long before computers — concordances of the Bible were built by hand on the same principle in the thirteenth century — and it is called the **inverted index**.

## 2.2 The inverted index

Start with the thing you already know. A normal index — a database's B-tree, or the index at the back of a textbook's *table of contents* — maps a document to its contents. Row 5 contains these values. Page 5 contains this text.

Invert it. Instead of mapping documents to their words, map **words to the documents that contain them**.

```
Documents:
  1: "Kafka partitions are ordered"
  2: "Ordered delivery in Kafka"
  3: "Spark partitions and tasks"

Inverted index:
  are         → [1]
  and         → [3]
  delivery    → [2]
  in          → [2]
  kafka       → [1, 2]
  ordered     → [1, 2]
  partitions  → [1, 3]
  spark       → [3]
  tasks       → [3]
```

Now look at what queries become. "Which documents contain *kafka*?" is a single lookup in a sorted structure: documents 1 and 2. Instant, regardless of whether there are three documents or three hundred million, because you never touch the documents themselves — only the list.

"Which documents contain *kafka* **and** *partitions*?" is the intersection of two sorted lists: `[1, 2] ∩ [1, 3] = [1]`. And intersecting two sorted lists is one of the cheapest operations in computing — you walk both with two pointers, advancing whichever is behind, and it costs time proportional to the shorter list. This is why a search engine can answer a five-term query across a billion documents in single-digit milliseconds. It is not scanning anything. It is intersecting a handful of precomputed lists.

Those lists are called **postings lists**, and in a real engine each entry carries more than just a document number. It carries the **term frequency** — how many times the word appears in that document, which we will need for ranking in §2.8 — and the **positions** at which it appears:

```
kafka → [ doc1 (tf=1, positions=[0]),  doc2 (tf=1, positions=[3]) ]
```

The positions are what make *phrase* search possible. To find the phrase "ordered delivery", you fetch the postings for both words, intersect to find documents containing both, and then check whether in any of those documents the position of "delivery" is exactly one greater than the position of "ordered". This is why `match_phrase` is more expensive than `match` — it does real work after the intersection — and why it is nonetheless fast.

### The other half: doc values

The inverted index answers one shape of question superbly: *given a term, which documents?* But there is an opposite shape, and it matters just as much: *given a document, what is the value of field X?*

You need that shape for sorting (order these thousand results by date), for aggregating (what is the average salary across these results), and for scripting. And the inverted index is exactly the wrong structure for it — you would have to walk every term in the dictionary asking "does this document appear in your list?"

So search engines maintain a second structure, built at the same time, called **doc values**. It is columnar: for each field, the values for all documents laid out contiguously in document order, on disk, memory-mapped so the operating system pages in what's needed.

```
inverted index:  term → [documents]        "who has this value?"
doc values:      document → value          "what is this document's value?"
```

Two structures, opposite orientations, both built at index time. Almost every confusing behaviour in the next few sections turns out to be a consequence of which of the two a given operation needs. In particular: you cannot sort or aggregate on a full-text field, and §2.4 will explain why that is not an arbitrary restriction but a direct consequence of this split.

## 2.3 The shape of the system

We are going to use **OpenSearch**, which is a distributed search engine. A word on what it actually is, since the lineage confuses people: OpenSearch is an open-source fork of Elasticsearch, branched from version 7.10 in 2021 after Elastic changed its licence. The two remain very similar, which is useful — the overwhelming majority of Elasticsearch documentation, blog posts, and Stack Overflow answers apply directly. And both are, underneath, a distributed system wrapped around **Apache Lucene**, a Java library that implements everything in §2.2. Lucene does the indexing and the scoring; OpenSearch does the sharding, replication, cluster management, and the HTTP API. When you understand that division, certain things stop being mysterious — for example, why a shard behaves like a completely self-contained search engine, which is the subject of the next few paragraphs.

### Documents, indexes, shards, segments

The vocabulary nests, and it is worth laying out in one place:

```
Cluster                     — the whole deployment
 └── Node                   — one OpenSearch process on one machine
      └── Index             — a named collection of documents ("documents")
           └── Shard        — a slice of the index; a complete Lucene index
                └── Segment — an immutable file of indexed data
                     └── Document  — one JSON object
                          └── Field — one named value inside it
```

A **document** is a JSON object. It is the unit you index and the unit you get back. It has an `_id`, either supplied by you or generated, and the original JSON you sent is stored verbatim in a field called `_source` so that it can be returned to you later. (You can disable `_source` to save space, and you should almost never do so: without it you cannot reindex, you cannot use the update API, and you cannot see what you actually stored.)

An **index** is a named collection of documents that share a schema. It is tempting to map this onto "table", and the analogy is roughly serviceable, but a better mental model is "a searchable corpus". The distinction matters because indexes are not as cheap as tables. Each one consumes heap, file handles, and cluster metadata, which is why a design with thousands of tiny indexes — one per customer, say — is a recognised anti-pattern with a recognised outcome.

A **shard** is where Chapter 1 arrives. Lantern's two hundred million documents do not fit on one machine, so the index is divided into shards. And the important thing about an OpenSearch shard is that it is not a fragment of something — **it is a complete, independent, fully functional Lucene index in its own right.** It has its own term dictionary, its own postings lists, its own doc values, its own document numbering. Nothing about it knows that nineteen siblings exist.

This single fact explains a great deal. It explains why searching an index means searching every shard and merging the results — scatter-gather, exactly as described in §1.3, with exactly the straggler problem described there. It explains why relevance scores can differ slightly between shards (§2.8), because each shard computes term statistics from its own local corpus. And it explains why shards are the unit of everything: of scale, of parallelism, of recovery, of rebalancing.

Shards come in two kinds. A **primary shard** accepts writes. A **replica shard** is an exact copy of a primary, living on a different node, which serves reads and stands ready to be promoted if the primary's node dies. This is leader–follower replication from §1.4, with OpenSearch's vocabulary over the top.

```
Index with 3 primaries and 1 replica = 6 shards on 3 nodes

    Node A              Node B              Node C
  ┌──────────┐       ┌──────────┐       ┌──────────┐
  │ P0       │       │ P1       │       │ P2       │
  │ R2       │       │ R0       │       │ R1       │
  └──────────┘       └──────────┘       └──────────┘
```

Look at the placement: no shard sits on the same node as its own copy. That is not a coincidence — the cluster's allocator enforces it, because a primary and its replica on the same machine would provide exactly no protection. Lose node A and P0 is gone, but R0 on node B is promoted to primary, and the cluster continues. It then notices it is short a copy of shard 0 and builds a new replica somewhere.

There is one asymmetry between the two numbers that you must know, and I will flag it here and explain it properly in §2.6 once we've seen the write path:

> **The number of primary shards is fixed when the index is created. The number of replicas can be changed at any time.**

This is the most consequential irreversible decision in OpenSearch, and §2.10 is largely about how to live with it.

### Segments, and the fact that nothing is ever modified

Inside a shard, Lucene stores data in files called **segments**. New documents are written into new segments. And segments are **immutable**: once written, a segment file is never modified.

This raises an obvious question. What happens when you update a document?

The answer is that you don't, not really. OpenSearch writes a *new* version of the document into a new segment, and records in a small auxiliary file that the old version is deleted. Both copies are physically present on disk. Searches consult the deletion list and skip the old one. Later, a background process called a **merge** combines several small segments into one larger segment, and it is at that moment — not at delete time — that the deleted document is actually dropped and its disk space reclaimed.

The same is true of deletes. A `DELETE` marks a tombstone; the bytes persist until a merge sweeps them up.

Immutability seems like a strange design until you notice what it buys. Immutable files need no locking, so many threads can read a segment concurrently with no coordination at all. They can be cached aggressively, because they will never change and so a cached copy can never be stale. They can be copied to another node for recovery without pausing anything. And writing them is purely sequential, which is the fastest thing a disk does.

What it costs is that space is not reclaimed promptly, that the number of segments must be managed, and that merging consumes real I/O and CPU in the background. Nearly every performance characteristic in §2.11 traces back to segments, and now you know why.

### Who does what: node roles

In a small deployment every node does everything. As you grow, you separate the jobs.

The **cluster manager** — called the *master* in older documentation and in the Elasticsearch lineage — maintains the cluster state: which indexes exist, what their mappings are, where each shard lives. It holds no data and serves no queries. It is elected by a quorum protocol of the kind described in §1.6, which is why the recommendation is three dedicated cluster-manager nodes: three tolerate one failure, and a fourth would buy nothing. The cluster manager's job sounds administrative and unglamorous right up until it becomes unstable, at which point nothing works, which is why you give it its own machines rather than letting it compete with search traffic.

**Data nodes** hold shards and do the actual indexing and searching. They are the expensive ones, and the ones you add when you need more capacity.

Any node that receives a client request acts as a **coordinating node** for that request: it fans out to the relevant shards, gathers their responses, merges them, and replies. In a large cluster you may want dedicated coordinating-only nodes, because merging results from fifty shards is real work and you would rather it not steal CPU from the data nodes doing the search.

There are also **ingest nodes** (running lightweight transformation pipelines before indexing) and specialised **warm** and **cold** tiers for cheaper storage of older data.

### The allocator, quietly running

One background process is worth naming because you will see its effects constantly. The cluster manager runs a **shard allocator** which continuously tries to satisfy a set of constraints: every primary assigned somewhere, every replica assigned on a different node from its primary, disk usage balanced, shard counts balanced, and any *awareness* rules respected — for instance, "spread the copies of each shard across availability zones so that losing a zone doesn't lose data."

When a node vanishes, the allocator promotes replicas and then rebuilds the missing copies. When you add a node, it moves shards onto it to rebalance. Both operations move real bytes across the network and take real time, which is one of the reasons §2.11 recommends keeping shards in the tens of gigabytes rather than the hundreds.

## 2.4 The schema: mappings

A **mapping** is OpenSearch's schema. It declares, for each field, what type it is, how it should be analysed, and whether it should be searchable, sortable, aggregatable.

OpenSearch will guess if you don't tell it. Index a document with an unknown field and **dynamic mapping** infers a type and adds it to the mapping permanently. This is delightful in development and treacherous in production, for two reasons that are worth spelling out because both have caused real outages.

First, the guess is made from the *first* document, and it is binding. A field whose first observed value is the string `"12345"` becomes a text field. Every subsequent numeric value gets coerced or rejected, range queries behave strangely, and the fix requires a full reindex.

Second, and more dangerous: if you index a document containing a JSON object with arbitrary user-supplied keys — event properties, say, or a bag of metadata — dynamic mapping adds a field for **every distinct key it ever sees.** Ten thousand keys become ten thousand fields. The mapping becomes enormous, the cluster state that must be replicated to every node becomes enormous, the cluster manager starts struggling, and the whole cluster becomes unstable. This has a name, **mapping explosion**, and the fact that it has a name should tell you how often it happens.

The defence is to define mappings explicitly through an **index template** and then to set `"dynamic": "strict"`, which rejects documents containing unknown fields, or `"dynamic": false`, which stores them in `_source` but does not index them. For anything user-supplied, choose one of those.

### The field types that matter

There are many types; these are the ones you will use.

For numbers: `long`, `integer`, `short`, `byte`, `double`, `float`, `half_float`, and `scaled_float` (which stores a fixed-point value as an integer with a scaling factor — the right choice for prices). Pick the smallest that fits; it directly reduces index size.

For time: `date`, which accepts many input formats and stores epoch milliseconds internally.

For structure: `object` for nested JSON, which gets flattened into dotted paths (`author.name`), and `nested`, which we will come to in a moment because it exists to solve a specific and surprising problem.

For geography: `geo_point` and `geo_shape`, supporting distance queries and geographic aggregations.

For vectors: `knn_vector`, which is the entry point to everything in Chapter 3.

And then the two that matter most, and that everyone gets wrong at least once.

### text and keyword, or: the mistake everybody makes

A `text` field is **analysed**. Its content is broken into terms, lowercased, stemmed, and each term is added to the inverted index separately. A `keyword` field is **not analysed**. Its entire content becomes exactly one term, byte for byte.

Consider the title `"Senior Java Engineer"`.

As `text`, the inverted index receives three entries: `senior`, `java`, `engineer`. A search for "java engineer" matches, because both terms are present. A search for "Java" matches, because the query is lowercased the same way the document was. But an exact lookup for the string `"Senior Java Engineer"` **finds nothing**, because there is no single term equal to that string — the string was taken apart and never stored whole.

As `keyword`, the index receives one entry: `Senior Java Engineer`. An exact lookup matches. You can sort by it. You can build an aggregation counting the ten most common job titles. But a search for "java" **finds nothing**, because the only term is the full string and "java" is not equal to it.

Here is the trap: both behaviours are correct and useful, and which one you want depends on the query, not on the field. So the standard answer is to have both, and OpenSearch supports this directly through a **multi-field**:

```json
"title": {
  "type": "text",
  "fields": {
    "keyword": { "type": "keyword", "ignore_above": 256 }
  }
}
```

One field in your JSON, two fields in the index. You search `title` and you sort or aggregate on `title.keyword`. This is exactly what dynamic mapping produces by default for a string, which is why things often work before you write an explicit mapping and break after — you removed the multi-field without realising it was there.

> The diagnostic shortcut, which will save you many hours: **if a search returns nothing and you expected it to match, check whether you are running a `term` query against a `text` field.** That single mistake accounts for a remarkable share of "OpenSearch is broken" reports.

### The nested-object trap

This one is genuinely surprising, and worth the space because it produces silently wrong results rather than errors.

Suppose a Lantern document has a list of contributors:

```json
{ "contributors": [ { "name": "asha",  "edits": 120 },
                    { "name": "priya", "edits": 3   } ] }
```

Mapped as a plain `object`, OpenSearch flattens this. The index does not contain two contributor objects; it contains two multi-valued fields:

```
contributors.name  → [asha, priya]
contributors.edits → [120, 3]
```

**The pairing is gone.** So a query for "a contributor named priya with more than 100 edits" *matches this document*, because the document does contain the name `priya` and does contain an `edits` value above 100 — just not in the same object. Nothing errors. You simply get wrong answers, and you may not notice for a year.

The fix is to map the field as `nested`, which tells Lucene to store each sub-object as its own hidden document, preserving the correlation, and to query it with a `nested` query. The cost is real — more documents under the hood, slower queries, and a cap on how many you can have — so use it when you need the correlation and not otherwise. But *know which one you need*, because the failure is silent.

### A few other mapping controls

`index: false` keeps a field in `_source` but doesn't make it searchable, which saves space on fields you only ever display. `doc_values: false` disables the columnar structure, saving space on fields you never sort or aggregate on. `copy_to` duplicates several fields into one catch-all field, which gives you a cheap "search everything" target. And `ignore_above` on a keyword field silently skips indexing values longer than N characters, which protects you from a thousand-character string becoming a term.

One rule governs all of this: **you cannot change the type of an existing field.** The inverted index and doc values were built according to the old type and there is no way to reinterpret them. Changing a mapping means creating a new index and copying the data across, which is the subject of §2.10 and the reason aliases exist.

## 2.5 Analysis: how text becomes terms

We have said several times that a `text` field is "analysed". It is time to look at what that means, because analysis is where most of a search engine's quality comes from, and because the single most useful debugging tool in OpenSearch lives here.

**Analysis** is the pipeline that turns a string into the terms stored in the inverted index. It runs twice: at **index time**, on the document, and again at **query time**, on the query string. And the critical thing to understand is that **both must produce compatible terms**, because matching happens on terms. If the document was lowercased and the query was not, the query term `Kafka` will not equal the indexed term `kafka`, and you will get zero results while being entirely certain the document is there.

### Three stages

```
  "The Café's WiFi-router is <b>broken</b>!"
        │
        ▼   1. CHARACTER FILTERS   (zero or more, on raw text)
            html_strip removes the tags
        │
        ▼   2. TOKENIZER           (exactly one; splits into tokens)
            standard → [The, Café's, WiFi, router, is, broken]
        │
        ▼   3. TOKEN FILTERS       (zero or more, in order)
            lowercase    → [the, café's, wifi, router, is, broken]
            asciifolding → [the, cafe's, wifi, router, is, broken]
            stemmer      → [the, cafe, wifi, router, is, broken]
        │
        ▼
  terms written to the inverted index
```

A **character filter** operates on the raw string before tokenisation: stripping HTML, replacing characters, applying a regex.

The **tokenizer** does the actual splitting, and there is exactly one. The `standard` tokenizer applies Unicode word-boundary rules and is the right default for nearly all prose. `whitespace` splits only on spaces. `keyword` emits the entire input as a single token, which is precisely how a `keyword` field behaves. And then there are two that deserve individual attention.

The **ngram** tokenizer emits every substring within a length range: `java` becomes `ja`, `av`, `va`, `jav`, `ava`, `java`. This lets you match on fragments — useful for substring search and for tolerating typos — at the cost of an index that is many times larger, because every word explodes into a dozen terms. Use it deliberately and on small fields.

The **edge_ngram** tokenizer emits only prefixes: `j`, `ja`, `jav`, `java`. This is the correct machinery for **autocomplete**. Index the field with `edge_ngram` so that all prefixes are searchable, and then — this part is essential — **search it with a normal analyzer.** If you also analysed the query with edge_ngram, then the query "java" would become `j, ja, jav, java` and would match any word beginning with "j". Which is why OpenSearch lets you specify a different `search_analyzer` from the index-time `analyzer`, and this is the canonical reason to do so.

Then the **token filters**, applied in order, each transforming the token stream.

`lowercase` you will always want. `asciifolding` folds accented characters to their ASCII equivalents (`café → cafe`, `Zürich → Zurich`), which matters enormously for names and for any international corpus.

`stop` removes very common words — "the", "a", "of". This was important when disks were small, and it is mostly a mistake now: modern relevance scoring already gives near-zero weight to words that appear everywhere (§2.8), and removing them **breaks phrase queries**. Search for "to be or not to be" in an index with stopwords removed and you are searching for nothing at all. Similarly "The Who", "The The", and a surprising number of product names. Leave stopwords in unless you have a specific reason.

`stemmer` reduces words to a root form, so that `running`, `runs`, and `ran` all become `run`. This increases **recall** — you find more of the relevant documents — at some cost to **precision**, because distinct words sometimes collapse together (`universal` and `university` both stem to `univers` under an aggressive stemmer). There is a family of them at different aggressiveness: `porter_stem` is enthusiastic, `minimal_english` and `kstem` are gentler. If users complain that search returns obviously wrong words, an over-eager stemmer is a good suspect.

`synonym` and `synonym_graph` map groups of terms together — `js`, `javascript`, and `ecmascript` becoming interchangeable. There is a placement decision here worth understanding. Applied at **index time**, synonyms cost nothing at query time, but changing the synonym list requires reindexing the entire corpus, and expanding synonyms into the index distorts the term statistics that ranking depends on. Applied at **query time**, via `synonym_graph` in a `search_analyzer`, they cost a little per query but can be edited whenever you like. Prefer query time; the operational flexibility is worth far more than the microseconds.

### Putting one together, and then testing it

```json
PUT /documents
{
  "settings": {
    "analysis": {
      "filter": {
        "en_stem": { "type": "stemmer", "language": "minimal_english" }
      },
      "analyzer": {
        "lantern_text": {
          "type": "custom",
          "char_filter": ["html_strip"],
          "tokenizer": "standard",
          "filter": ["lowercase", "asciifolding", "en_stem"]
        }
      }
    }
  },
  "mappings": {
    "properties": {
      "body": { "type": "text", "analyzer": "lantern_text" }
    }
  }
}
```

And now the most useful endpoint in the entire product:

```json
POST /documents/_analyze
{ "analyzer": "lantern_text", "text": "Running the Café's servers" }
```

It returns the exact list of terms that would be written to the index. That is all it does, and it is invaluable, because it converts an invisible process into something you can look at.

The debugging recipe that follows from it is mechanical and works nearly every time. A document *should* match a query and doesn't. Run `_analyze` on the document's text. Run `_analyze` on the query's text. Put the two lists of terms side by side. In the overwhelming majority of cases you will immediately see the mismatch — a stemmer applied on one side only, a stopword removed, a case difference, an accent, a hyphen split differently than you assumed. The invisible becomes obvious, and you stop guessing.

## 2.6 What actually happens when you write

We can now trace a write through the system, and in doing so resolve two mysteries: why the primary shard count is immutable, and why a document you just successfully indexed cannot immediately be found.

### Routing, and the immutable shard count

```
1. The client sends the document to any node. That node becomes the coordinator.

2. The coordinator computes which shard owns the document:

        shard = hash(routing) % number_of_primary_shards

   where `routing` defaults to the document's _id.

3. It forwards the document to the node holding that primary shard.

4. The primary writes the document into an in-memory buffer,
   and appends the operation to the TRANSLOG.

5. The primary forwards the document to all in-sync replicas, in parallel.

6. When the replicas acknowledge, the primary acknowledges to the client.
```

Step 2 is the answer to the first mystery. The shard is chosen by a modulus over the primary count, exactly as in §1.3, with exactly the consequence described there: **change the number of primaries and nearly every document's routing changes.** The document is no longer where the formula says it should be, so it can no longer be found by ID. Hence the count is fixed at creation, and hence §2.10's whole apparatus of reindexing and aliases.

Step 6 is worth noticing too. **OpenSearch replicates synchronously.** When you get a success response, the document is on the primary and on its in-sync replicas, not just queued somewhere. This is the durable side of the §1.4 trade-off, and it is why indexing latency rises if a replica is slow. There is a related setting, `wait_for_active_shards`, which controls how many copies must be *available* before the write is even attempted.

### The three different meanings of "written to disk"

Here is where most people's mental model of OpenSearch is wrong, and it is worth getting exactly right because it determines both correctness and performance.

There are three distinct operations, they happen on three different schedules, and they guarantee three different things.

**Refresh**, by default every **one second**. The in-memory buffer is turned into a new segment, and that segment is opened for searching. Note what this does *not* necessarily do: it does not guarantee the segment is on physical disk — it may live only in the filesystem cache. What it does is make the documents **visible to search**.

**Flush**, by default roughly every thirty minutes or when the translog reaches half a gigabyte. Segments are `fsync`ed to actual disk and a fresh translog is started. This makes the data **durable in segment form**.

**Translog fsync**, by default on **every request** (the setting is `index.translog.durability: request`). The operation log is forced to disk before the write is acknowledged. This is what makes an acknowledged write **crash-safe** — it is the write-ahead log of §1.6, doing exactly the job described there. You can relax it to `async` (every five seconds) for a meaningful throughput gain and a five-second window of potential data loss on a hard crash.

From which follows the single most surprising property of OpenSearch:

> **A document that has been successfully indexed — acknowledged, durable, crash-safe — is not searchable for up to one second.**

It is on disk. It will survive a power failure. And a search will not find it, because the segment it lives in hasn't been created yet. OpenSearch is **near real-time**, not real-time, and this is a deliberate design decision: creating a segment is not free, and doing it once a second instead of once per document is the difference between thousands of writes per second and dozens.

This catches everybody, usually in a test that indexes a document and immediately searches for it. The right fix in a test is `?refresh=wait_for`, which blocks your request until the next scheduled refresh has happened. The wrong fix — and I mention it because people reach for it — is `?refresh=true`, which forces an immediate refresh and therefore creates a tiny segment for every single write. In production this will destroy your indexing throughput and then destroy your search latency as the segment count climbs. Do not put it in application code.

The other direction is more useful. When you are bulk-loading a large corpus, nobody is searching it yet, so a one-second refresh interval is pure waste. Set `index.refresh_interval: -1` to disable refresh entirely and `number_of_replicas: 0` to avoid replicating every document while you load, then restore both afterwards. Three- to fivefold speedups are routine.

### Merging, and the sawtooth

Every refresh creates a segment. A thousand seconds of indexing creates a thousand segments, and every query must consult every segment, so search latency climbs steadily. Background **merges** fix this by combining small segments into larger ones — and, as we saw in §2.3, this is also when deleted documents are finally purged.

Merging is I/O and CPU intensive, and it runs concurrently with your indexing. This is the usual explanation for a question that puzzles people: *why does our indexing throughput rise and fall in a sawtooth pattern rather than staying flat?* It is competing with merges. On spinning disks this is severe enough to be a design constraint; solid-state storage is not really optional for a write-heavy search cluster.

There is a manual override, `_forcemerge`, which merges a shard down to a specified number of segments. On an index that is finished — a completed time-series index, the output of a reindex, anything read-only — merging to a single segment gives a genuine search speedup and reclaims all deleted space. On an actively-written index it is harmful: you create one enormous segment which future merges must repeatedly rewrite.

## 2.7 What actually happens when you search

A search fans out to every relevant shard, and it does so in two phases. Understanding the two phases explains two important behaviours.

```
PHASE 1 — QUERY
  The coordinator sends the query to one copy of every shard
  (primary or replica, whichever is less busy).
  Each shard runs the query locally and returns the top `size`
  document IDs with their scores — not the documents.
  The coordinator merges these lists into one global top `size`.

PHASE 2 — FETCH
  The coordinator asks the relevant shards for the _source of
  just those documents, and returns them to the client.
```

The separation exists so that phase 1 moves only IDs and scores across the network, which is small, and phase 2 moves only the ten documents you asked for, rather than ten from each of twenty shards.

### Consequence one: deep pagination is a trap

Ask for `from=0&size=10` and each of twenty shards returns its local top ten, the coordinator merges two hundred candidates, and everything is cheap.

Now ask for `from=100000&size=10` — page ten thousand. To find the global hits ranked 100 001 to 100 010, the coordinator needs each shard's top 100 010, because in principle they could all come from one shard. So twenty shards each build a list of 100 010 scored hits, ship them, and the coordinator sorts two million entries to throw away all but ten. The cost grows linearly with the page number and multiplicatively with the shard count.

This is why OpenSearch refuses by default past ten thousand results. It is not an arbitrary limit; it is a guardrail in front of a cliff.

The correct mechanism for deep paging is **`search_after`**, which is a cursor: you pass the sort values of the last hit on the previous page, and each shard can seek straight past them. Cost is constant per page regardless of depth. For exporting an entire result set, the **point-in-time** API (or the older scroll API) holds a consistent view of the index while you page through it.

If you are building a UI, the deeper lesson is that users do not go to page ten thousand. Nobody has ever gone to page ten thousand. If your product needs deep pagination, what it probably needs is better filters.

### Consequence two: relevance is slightly approximate

Recall from §2.3 that each shard is an independent Lucene index with its own statistics. Ranking (§2.8) depends on knowing how rare a term is across the corpus — and each shard computes that from *its own* documents.

So if "kafka" appears in 2% of shard 3's documents and 6% of shard 7's, the two shards will score the same document differently. With plenty of documents per shard this averages out and is invisible. With few documents per shard — a small index split many ways, or a test fixture — the effect can be large enough to be confusing.

The fix, if you need it, is `search_type=dfs_query_then_fetch`, which adds a preliminary round to collect global term statistics before scoring. It costs an extra round trip and is rarely necessary in production. It is, however, worth knowing about when someone shows you two nearly identical documents with wildly different scores in a small index.

## 2.8 Asking good questions

Now the query language. There is a great deal of it; I want to give you the small number of ideas that carry most of the weight.

### Query context and filter context

This is the most important distinction in the query DSL, and the easiest performance win available.

A clause in **query context** asks *how well does this document match?* It computes a relevance score. Scores cannot be cached, because they depend on the specific query.

A clause in **filter context** asks *does this document match, yes or no?* It computes no score. And because the answer is a plain boolean, OpenSearch can cache it — as a bitset, one bit per document — and reuse it across queries.

The `bool` query is where you assemble both:

```json
{
  "query": {
    "bool": {
      "must":     [ { "match": { "body": "distributed systems" } } ],
      "filter":   [ { "term":  { "status": "published" } },
                    { "range": { "updated_at": { "gte": "now-1y" } } } ],
      "should":   [ { "match": { "tags": "kafka" } } ],
      "must_not": [ { "term":  { "archived": true } } ]
    }
  }
}
```

`must` clauses must match and contribute to the score. `filter` clauses must match and do not. `should` clauses are optional and boost the score when present — the "and if it also mentions Kafka, rank it higher" clause. `must_not` excludes.

The rule that follows is simple and worth applying mechanically: **every yes-or-no constraint belongs in `filter`, never in `must`.** Status, ownership, date ranges, tenancy, booleans, enumerations. None of them should influence ranking, all of them are highly cacheable, and moving them is a one-line change that frequently halves query latency.

### The families of query

**Full-text queries** analyse their input, so the query string goes through the same pipeline the document did.

`match` is the workhorse: it analyses your input into terms and finds documents containing any of them (or all, with `"operator": "and"`). `match_phrase` requires the terms to be adjacent and in order, using the positions from §2.2, with a `slop` parameter to allow some reordering. `multi_match` runs the same text against several fields, and its `type` parameter is worth knowing because the default is not always what you want: `best_fields` scores by the single best-matching field (right for "find whichever field this matches"), `most_fields` sums across fields (right when the same text is indexed several different ways), and `cross_fields` treats several fields as one merged field (right for names and addresses spread across `first_name`, `last_name`, `city`).

**Term-level queries** do *not* analyse their input. They look for the exact bytes you gave them, which is why they belong on `keyword` fields, numbers, and dates. `term` and `terms` for exact values, `range` for numeric and date ranges, `exists`, `prefix`, `wildcard`, `regexp`, `fuzzy`, `ids`.

Two warnings about this family. First, the one from §2.4: a `term` query on a `text` field will quietly find nothing. Second, `wildcard` with a leading asterisk and `regexp` in general may have to scan the entire term dictionary, which on a large index is catastrophically slow. If you need substring matching, pay for it at index time with ngrams (§2.5) rather than at query time.

**Compound queries** shape relevance. `function_score` multiplies the score by a function of the document's own fields — the standard use being a decay function on a date, so that fresher documents rank higher without excluding older ones. `boosting` demotes rather than excludes, which is often what you actually want. `dis_max` takes the maximum of its clauses' scores rather than the sum. `rank_feature` provides cheap boosting by a numeric field such as popularity. `script_score` lets you write arbitrary scoring logic, which is powerful and slow.

And field boosting, which is the cheapest relevance improvement in existence: `"fields": ["title^3", "body"]` makes a match in the title worth three times a match in the body. Almost every search application should do this, and many don't.

### Sorting

```json
"sort": [ { "updated_at": "desc" }, "_score", { "_id": "asc" } ]
```

Sorting reads doc values (§2.2), not the inverted index, which is why it is fast and why you cannot sort on a `text` field — a `text` field has no doc values, because its analysed terms are not a single sortable value. Sort on the `.keyword` sub-field instead.

Always include a final tie-breaking sort on something unique, such as `_id`. Without it, documents with equal sort values may appear in a different order on each request, and pagination will duplicate and skip results in ways that are very confusing to debug.

### Aggregations

Aggregations are OpenSearch's analytics half: the machinery behind faceted navigation, dashboards, and any question of the form "how many, grouped by what". They read doc values, they nest arbitrarily, and they come in three families.

**Bucket aggregations** group documents. `terms` gives the top N values of a field — the "top twenty tags" facet. `date_histogram` gives time buckets and is the backbone of every time-series dashboard. There are also `range`, `histogram`, `filters`, `nested`, and `composite`.

**Metric aggregations** compute numbers over a bucket: `avg`, `min`, `max`, `sum`, `stats`, `value_count`, `cardinality`, `percentiles`.

**Pipeline aggregations** operate on the *output* of other aggregations: `derivative`, `moving_avg`, `cumulative_sum`, and `bucket_selector`, which acts like a SQL `HAVING` clause.

They compose:

```json
{
  "size": 0,
  "aggs": {
    "by_team": {
      "terms": { "field": "team", "size": 10 },
      "aggs": {
        "avg_length": { "avg": { "field": "word_count" } },
        "over_time":  { "date_histogram": { "field": "updated_at",
                                            "calendar_interval": "1M" } }
      }
    }
  }
}
```

Note `"size": 0`, which says "I want no documents, only aggregations", and skips the fetch phase entirely.

### Three aggregations that lie to you (usefully)

This deserves its own heading because it surprises people and because the surprise, once understood, generalises into a principle.

A `terms` aggregation asking for the top ten values is **approximate**. Here is why: each shard computes *its own* top ten and sends them to the coordinator, which merges. Now imagine a value that ranks eleventh on every one of twenty shards. It appears in no shard's response, so the coordinator never sees it — even though summed across all shards it might be the most common value overall. The response includes a field called `doc_count_error_upper_bound` that quantifies how wrong the counts might be, and `shard_size` lets you ask each shard for more candidates to reduce the error at the cost of memory.

A `cardinality` aggregation — distinct count — is **approximate**. It uses an algorithm called HyperLogLog++, which estimates the number of distinct values in constant memory with an error of roughly one to two percent. The alternative, an exact distinct count of a high-cardinality field across twenty shards, would require shipping every distinct value to the coordinator, and for a field with fifty million distinct values that is not a query, it is an outage.

A `percentiles` aggregation is **approximate**, using a structure called t-digest, which is cleverly designed to be most accurate at the extremes — the p99 — which is exactly where you care.

And here is the principle: **at scale, exactness is often unaffordable, and approximation is a legitimate engineering choice rather than a failure.** Nobody needs to know that there were exactly 8 431 947 distinct users rather than approximately 8.4 million. What matters is that the approximation is *explicit*, *bounded*, and *understood* — so that nobody builds a billing system on top of a HyperLogLog estimate. You will meet this principle again in Chapter 3, where approximate nearest-neighbour search makes the same trade for the same reason.

A final warning on cost: aggregations build data structures per bucket, per shard, in heap. A deeply nested `terms` aggregation over high-cardinality fields is the single most reliable way to trigger a circuit-breaker exception or an out-of-memory error in OpenSearch. There is a `search.max_buckets` limit for exactly this reason, and when you hit it, the right response is usually to rethink the query rather than raise the limit.

## 2.9 Relevance: what "best" means

We have built the machinery to find matching documents. Now: which of eleven thousand matches goes first?

### What a score is, and is not

Every hit comes back with a `_score`, a positive float. It is a **relative** number, meaningful only within the result set of one query. A score of 14.2 does not mean "very relevant"; it means "more relevant than the document scoring 9.8 for this same query".

This has a practical consequence that trips up product requirements. You cannot set an absolute threshold — "only show results scoring above 5" — and expect it to behave sensibly, because the scale shifts with query length, term rarity, and corpus statistics. A one-word query and a six-word query produce entirely different score ranges. If you need a notion of "good enough", normalise within the result set, or train a model, or use a cross-encoder (§3.7). Do not use a magic constant.

### The intuition: three signals

Long before the formula, there were three observations about what makes a document relevant to a term.

**Term frequency.** A document that mentions "kubernetes" eight times is more likely to be *about* Kubernetes than one that mentions it once in passing. More is better.

**Inverse document frequency.** A term that appears in very few documents is far more informative than a term that appears everywhere. Matching "kubernetes" tells you something; matching "the" tells you nothing at all. Rarer is more valuable. (Notice that this is why aggressive stopword removal is unnecessary — IDF already assigns "the" a weight near zero. The scoring function solved the problem that stopword lists were invented for.)

**Field length.** Matching "java" in a three-word title is stronger evidence than matching it once in a five-thousand-word document, where it might be incidental. Shorter fields make matches more significant.

Combine those three and you have TF-IDF, which served the field for decades.

### BM25, and the two things it fixes

Modern OpenSearch uses **BM25**, which takes the same three signals and handles each more carefully. Here is the formula, which I include for completeness and then immediately tell you not to memorise:

```
                          f(t,d) · (k₁ + 1)
score(q,d) = Σ  IDF(t) · ─────────────────────────────────
            t∈q           f(t,d) + k₁ · (1 − b + b·|d|/avgdl)
```

where `f(t,d)` is how often term t appears in document d, `|d|` is the field's length, `avgdl` is the average field length across the corpus, and `k₁` and `b` are tunable constants defaulting to 1.2 and 0.75.

What matters is the two problems this solves.

**Problem one: term frequency should saturate.** Under plain TF-IDF, a document containing "kubernetes" a hundred times scores a hundred times higher than one containing it once. Which means keyword stuffing wins, and a page that is nothing but the word "kubernetes" repeated outranks the actual documentation. Look at the fraction in BM25: as `f(t,d)` grows, the numerator and denominator both grow, and the whole expression approaches a ceiling of `k₁ + 1`. Going from one occurrence to two makes a large difference. Going from twenty to twenty-one makes almost none. That curve matches human judgement — after a few mentions, you are convinced, and further repetition carries no information. The parameter `k₁` controls how quickly it saturates.

**Problem two: length normalisation needs a dial.** Penalising long documents is right in general, but too much penalty means a genuinely comprehensive forty-page guide loses to a one-line stub. The parameter `b` interpolates: at `b=0` length is ignored completely, at `b=1` scores are fully normalised by length, and the default 0.75 is a tuned compromise. Lowering `b` is occasionally the right move for fields where length carries no meaning, such as a list of tags.

In practice, tuning `k₁` and `b` is a small effect. Field boosting, good analysers, and query structure matter far more, and you should exhaust those before touching the similarity parameters.

### Finding out why

Three endpoints answer relevance questions, and between them they eliminate guesswork.

Adding `"explain": true` to a search returns, for every hit, a full recursive breakdown of how its score was computed — every term, its IDF, its term frequency, its length normalisation, and how they combined. It is extremely verbose and completely definitive.

`GET /documents/_explain/{id}` with a query body answers the more focused question: *why did this specific document score what it did* — or, crucially, *why did it not match at all*. When someone says "this document should be the top result and it isn't even in the list", this is the endpoint that tells you.

And `"profile": true` gives a per-shard timing breakdown of query execution, which is for performance rather than relevance but lives in the same toolbox.

### Getting better relevance

Roughly in order of effort-to-reward:

**Boost your fields.** A title match is worth more than a body match. One line of configuration.

**Index the same text several ways** — exact, stemmed, ngrammed — as multi-fields, and combine them with `multi_match` using `most_fields`. Exact matches then naturally outrank stemmed ones because they match more of your sub-fields, which is a nice property to get for free.

**Add a phrase boost.** Put a `match_phrase` clause in `should`. Documents where the query words actually appear adjacent get lifted above documents that merely contain them all somewhere.

**Decay by recency or popularity** using `function_score` with a `gauss` decay, or the cheaper `rank_feature`. For Lantern, a wiki page edited last week is probably more useful than an equally-relevant one last touched in 2017.

**Learn to rank.** OpenSearch has a Learning to Rank plugin that takes your top N results and reranks them with a trained gradient-boosted model over features you define — BM25 score, recency, click-through rate, author authority, whatever you have. This is a real step up in quality and a real step up in machinery.

**Add semantic search.** Which is Chapter 3, and which is where the largest modern gains are.

### Measuring it, which you must do first

I have listed six ways to improve relevance, and I want to be emphatic about something: **without measurement, you cannot tell whether any of them helped.** Relevance work without evaluation is not engineering; it is a sequence of plausible-sounding changes whose net effect is unknown and quite possibly negative.

So: build a **judgment list**. A set of queries, and for each, which documents are relevant. Mine it from click logs, or have humans rate a few hundred query-document pairs, or generate it synthetically with a language model. Then measure:

**Precision@k** — of the top k results, what fraction were relevant? **Recall@k** — of all the relevant documents that exist, what fraction appeared in the top k? **MRR**, mean reciprocal rank — one divided by the position of the first relevant result, averaged over queries, which is the right metric when there is essentially one correct answer. And **nDCG@k**, normalised discounted cumulative gain, which handles graded relevance (perfect, good, acceptable, bad) and discounts by position so that putting the best result first is rewarded. nDCG is the standard headline metric for ranked retrieval and the one to report.

OpenSearch has a **Rank Evaluation API**, `_rank_eval`, that computes these against a judgment set for you. Wire it into your continuous integration, so that a relevance regression fails a build rather than being discovered three weeks later from a support ticket. This is the single most valuable piece of infrastructure in a search project and it is routinely the last thing teams build.

## 2.10 Living with an index

Three operational patterns carry most of the day-to-day work.

### Bulk indexing

Indexing documents one HTTP request at a time is slow, for all the reasons §1.9 gave. The `_bulk` API batches them:

```
POST /_bulk
{ "index":  { "_index": "documents", "_id": "d-1001" } }
{ "title": "Kafka partitions", "body": "..." }
{ "update": { "_index": "documents", "_id": "d-1002" } }
{ "doc": { "status": "published" } }
{ "delete": { "_index": "documents", "_id": "d-1003" } }
```

The format is newline-delimited JSON: one action line, one source line, repeated, with a trailing newline that the API genuinely requires.

A handful of rules govern using it well. **Size by bytes, not by document count** — aim for five to fifteen megabytes per request, because that is what makes the network and the write pipeline efficient, and a thousand tiny documents and a thousand huge ones are very different requests. **Use a few concurrent bulk threads** rather than one giant serial stream; two to four per data node is a reasonable starting point. And **retry `429 Too Many Requests` with backoff**, because that response is not an error — it is OpenSearch's write queue applying backpressure exactly as §1.6 recommends, and the correct response is to slow down, not to alert.

Then the rule that I want to state as strongly as I can, because ignoring it is the most common data-loss bug in indexing pipelines:

> **A `200 OK` on a bulk request does not mean the documents were indexed.**

The bulk API returns HTTP 200 as long as the *request* was processed. Individual items within it can fail — a mapping conflict, a version conflict, a rejected write — and each item has its own status in the response. There is an `"errors": true` flag at the top level and a per-item result array. **You must check them.** A pipeline that fires bulk requests and checks only the HTTP status will lose documents silently and indefinitely, and will look completely healthy while doing it.

Finally, and connecting back to §1.7: **use a deterministic `_id`.** Set it to the document's own identifier from the source system. Then a bulk request that gets retried after a timeout overwrites rather than duplicates, and your entire indexing pipeline becomes idempotent for free. If you let OpenSearch generate random IDs, every retry creates a new document, and you will slowly accumulate duplicates that are very hard to find and remove.

### Aliases, and one rule

An **alias** is a name that points to one or more indexes. It is a small feature and it is load-bearing for everything else in this section.

```json
POST /_aliases
{ "actions": [
    { "remove": { "index": "documents_v1", "alias": "documents" } },
    { "add":    { "index": "documents_v2", "alias": "documents" } }
]}
```

Both actions in one request, applied **atomically**. There is no instant at which the alias points at both or neither. Every query in flight resolves to one index or the other, and the switch is invisible.

Which leads to the rule:

> **Your application should never reference a concrete index name. Only ever an alias.**

Follow it and every schema change, every reindex, every model upgrade becomes a zero-downtime alias swap with an instant rollback. Ignore it and each of those becomes a coordinated deployment with a maintenance window. The cost of following it is that you type `documents` instead of `documents_v1` on day one.

Aliases do other useful things too: a **filtered alias** carries a built-in filter, so you can expose a per-team view of a shared index; a **routing alias** pins queries to a shard; and `is_write_index` designates which index of a group receives writes, which is what makes automatic rollover work.

### Reindexing

You will need to change something immutable — a field's type, an analyser, the number of primary shards. All of these mean building a new index and copying the data.

```json
POST /_reindex?wait_for_completion=false&slices=auto
{
  "source": { "index": "documents_v1" },
  "dest":   { "index": "documents_v2" }
}
```

`slices=auto` parallelises the copy by shard, which is a large speedup. `wait_for_completion=false` returns a task ID immediately so that a six-hour reindex doesn't sit on an HTTP connection; you poll `GET _tasks/<id>`. A `script` can transform documents in flight, and a remote `source` can pull from an entirely different cluster.

The zero-downtime pattern, which is worth learning as a unit:

1. Create `documents_v2` with the new mapping.
2. Begin writing new changes to both indexes — or, better, make sure your indexing pipeline can be replayed from its source (which Chapter 5 will make easy).
3. Reindex the historical data from v1 to v2.
4. Verify: compare document counts, spot-check a sample, and run your `_rank_eval` judgment set against both to make sure relevance did not regress.
5. Swap the alias atomically.
6. Keep v1 for a rollback window, then delete it.

Step 4 is the one people skip and the one that matters. An alias swap makes rollback trivial, but only if you notice the problem.

Two relatives are worth knowing. `_update_by_query` re-indexes documents in place, which is how you apply a *new analyser* to existing data without a new index (the mapping didn't change, only how the text is processed). And `_delete_by_query` removes matching documents — remembering from §2.3 that this marks tombstones and the space comes back at merge time, not immediately.

### Templates and lifecycles

For time-series data — logs, metrics, events — three features combine into a standard pattern. An **index template** automatically applies settings, mappings, and aliases to any new index matching a name pattern, so `logs-2026-09-17` is born correctly configured. **Rollover** creates the next index and moves the write alias when the current one gets too large or too old. And **ISM**, Index State Management, is OpenSearch's lifecycle engine: after seven days reduce replicas and force-merge, after thirty days move to cheaper warm nodes, after ninety days snapshot and delete. Together they let you keep a rolling window of data without unbounded cost and without a human doing anything.

**Snapshots** round this out: incremental backups to S3 at the segment level, restorable into the same or a different cluster. Note, as §4.6 will insist, that replication is not backup — replication faithfully replicates your accidental `_delete_by_query` too.

## 2.11 When it goes wrong

### Reading cluster health

```
GET _cluster/health
```

The response has a colour, and the colours mean something precise.

**Green** means every primary shard and every replica shard is assigned and active. Everything is fine.

**Yellow** means every primary is assigned, but at least one replica is not. Your data is all there and serving normally, but you have lost redundancy — if the wrong node dies now, you lose data. On a single-node development cluster this is permanent and harmless, because a replica cannot be placed on the same node as its primary and there is nowhere else. In production it means *fix this today*.

**Red** means at least one primary shard is unassigned. Some of your data is unavailable right now. Searches return partial results — possibly without your application noticing — and writes to the affected shard fail. This is an outage.

A handful of endpoints diagnose it:

```
GET _cluster/allocation/explain      ← why is this shard not assigned? Start here.
GET _cat/indices?v&health=red
GET _cat/shards?v&s=state
GET _cat/nodes?v&h=name,heap.percent,disk.used_percent,cpu,load_1m
GET _nodes/stats/thread_pool         ← look for non-zero "rejected"
GET _tasks?detailed                  ← what is running right now
```

`allocation/explain` is the one to remember. It answers the question directly and in prose, and the answer is usually one of: a disk watermark was crossed (OpenSearch stops allocating at 85% disk, stops moving shards in at 90%, and makes indexes read-only at 95% — that last one is the **flood stage** and it surprises people badly); a node left the cluster and hasn't returned; allocation filtering rules exclude every eligible node; there are too many shards per node; or a shard is corrupt and needs `_cluster/reroute?retry_failed`.

### Sizing, and the most common mistake

A few numbers, each with its reasoning, because the reasoning outlives the number.

**Aim for ten to fifty gigabytes per shard.** Below ten, you are paying per-shard overhead — heap, file handles, a separate query execution — for very little data. Above fifty, recovery and rebalancing become painfully slow, because moving a shard means moving all of it, and a 200 GB shard takes a long time to travel across a network while your cluster is degraded.

**Keep the total number of shards per node to roughly twenty per gigabyte of heap.** A node with a 30 GB heap can manage about six hundred shards, and fewer is better.

**Set the JVM heap to at most 31 gigabytes, and to at most half the machine's RAM.** Both halves of that need explaining. The 31 GB ceiling exists because above roughly 32 GB the JVM can no longer use *compressed ordinary object pointers* — it switches to full 64-bit references, every pointer doubles in size, and you end up with **less** usable heap from more memory. It is a genuine cliff, not a guideline. And the half-of-RAM rule exists because Lucene's speed comes from the **filesystem cache**: segments are memory-mapped files, and the operating system keeping them in free RAM is what makes search fast. Giving all your memory to the JVM starves the thing that actually matters.

And then the mistake I have seen more than any other, which is **oversharding.** It is so tempting: shards are how you scale, so more shards must mean more scalable. But every shard is a complete Lucene index with fixed overhead, and every query fans out to all of them, paying the straggler tax from §1.3 on each. A query against five hundred tiny shards is dramatically slower and more expensive than the same query against ten correctly-sized ones. Start with `primaries = ceil(expected_total_GB / 30)`, round to a multiple of your data node count so the shards distribute evenly, and resist the urge to round up "just in case".

Replicas are the easy one: at least one in production, always, and since the replica count *can* be changed live, you can add replicas to increase read throughput whenever you need to.

### A troubleshooting playbook

**Searches are slow.** Run the query with `"profile": true` and find which clause dominates — this usually ends the investigation immediately. Then check the usual suspects in order: are your yes-or-no conditions in `filter` where they can be cached? Is something paginating deeply? Are there `wildcard`, `regexp`, or `script` queries that should be precomputed at index time? Are you fanning out to more shards than necessary — and could routing make a common query hit just one? Is the working set larger than the filesystem cache, so that every query goes to disk? That last one is very often the real answer, and the fix is more RAM or faster storage rather than anything clever. Turn on the **slow log** (`index.search.slowlog.threshold.query.warn: 5s`) so that the actual offenders identify themselves rather than being guessed at.

**Indexing is slow.** Check bulk request size and concurrency, and look for `429` rejections that indicate you are already at capacity. Check whether something is forcing refreshes — a stray `?refresh=true` in application code is a classic. Look at merge activity, because merges compete with indexing for I/O. Consider dropping replicas to zero during a large backfill. And look for dynamic mapping updates firing on every document, which serialises through the cluster manager and throttles everything.

**Memory problems.** A `CircuitBreakingException` means a request wanted more heap than it was allowed. The circuit breaker is *protecting you*; the correct response is to fix the query, not to raise the limit. Long garbage-collection pauses in the logs are worth taking seriously for the reason §1.9 gave: a node in a two-second GC pause is indistinguishable from a dead node, so it gets removed from the cluster, its shards get reallocated, and then it comes back and everything reallocates again. A cluster can thrash like this for hours, and the root cause is usually too many shards, or fielddata on a text field, or an unbounded aggregation.

## 2.12 Where Lantern stands

Let us take stock, because we have built something real.

Lantern now has an OpenSearch cluster. Two hundred million documents live in an index called `documents_v1`, behind an alias called `documents`. There are twelve primary shards of roughly twenty-five gigabytes each and one replica apiece, spread over six data nodes, with three small dedicated cluster-manager nodes holding the election quorum.

Each document's text is indexed as `text` with a custom analyser — HTML stripped, lowercased, accent-folded, gently stemmed — and simultaneously as `keyword` through a multi-field, so that the same field can be searched, sorted, and aggregated. Status, team, and timestamp are keyword and date fields used exclusively in `filter` clauses, where they are cached. Queries are `multi_match` across title and body with the title boosted threefold, plus a `should` phrase clause for adjacency and a `function_score` recency decay. A judgment set of two hundred queries runs in CI and fails the build if nDCG@10 drops.

Documents are written through the `_bulk` API in ten-megabyte batches, keyed by the source system's document ID so that retries overwrite rather than duplicate, with per-item error checking. A document edited in PostgreSQL becomes searchable about a second later, because of the refresh interval, and everybody involved knows that number and monitors it.

It works. Searches return in forty milliseconds at the median and a hundred and eighty at the ninety-ninth percentile. A node can die without anyone noticing.

And yet there is a category of query it handles badly, and it is not a small category. A user types *"why does my deployment keep restarting"* and gets nothing useful, because the document that answers the question is titled "Diagnosing CrashLoopBackOff" and shares not one word with the query. Another types *"kubernetes pod memory limits"* and gets a page about "k8s container resource constraints" ranked fourteenth, below several documents that merely mention Kubernetes a lot.

The inverted index is a machine for matching *words*. Our users are asking about *meaning*. Every tool in this chapter — stemming, synonyms, boosting — is an attempt to paper over that gap one word at a time, and it does not scale, because you cannot enumerate the ways people phrase things.

To close the gap we need to stop indexing words and start indexing something else. Which is Chapter 3, and it begins with a very strange idea.
