# Chapter 2 — Finding Things

## How to read this chapter

This chapter has one job: to get you from "I have used a database" to "I can reason about a search engine". It is long, so here is the shape of it before you start.

**There is one example, and it runs.** Ten documents from Lantern's wiki, indexed into a real OpenSearch you start in §2.0 with one command. Every rule in this chapter is demonstrated by a request you can paste into a terminal and a response you can look at. Ten documents instead of two hundred million, but every mechanism is visible at ten.

**Every section starts with something breaking.** A search returns nothing. A sort throws an error. A document is indexed and then cannot be found. The mechanism is introduced to explain the failure, not before it. If you ever feel a term arrive before you needed it, that is a bug in the chapter and not in you.

**The spine is: build it on one machine, then make it survive many machines, then operate it.** §2.1 to §2.2 build the data structure. §2.3 splits it across machines. §2.4 to §2.9 make it correct and fast. §2.10 to §2.11 keep it alive. §2.12 shows you the wall it hits, which is what Chapter 3 is for.

**The last two sections are reference, not reading.** The one-page summary and the glossary at the end are for later.

---

## §2.0 The lab

Start a single-node OpenSearch. One command, about thirty seconds:

```bash
docker run -d --name lantern -p 9200:9200 \
  -e "discovery.type=single-node" \
  -e "DISABLE_SECURITY_PLUGIN=true" \
  -e "OPENSEARCH_JAVA_OPTS=-Xms1g -Xmx1g" \
  opensearchproject/opensearch:2
```

Check it is up:

```bash
curl -s localhost:9200 | head -5
```

```json
{
  "name" : "a1b2c3d4e5f6",
  "cluster_name" : "docker-cluster",
  "cluster_uuid" : "...",
```

Now create the index. One shard, no replicas — on a laptop that is the honest configuration, and one shard also makes every relevance score in this chapter reproducible for reasons that become clear in §2.7.

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

And load the corpus. Ten documents, one bulk request. Note `?refresh=wait_for`, which we will come back to and explain in §2.6 — without it the documents are indexed but not yet findable, which is the single most confusing thing about this system.

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
```

Confirm all ten landed:

```bash
curl -s 'localhost:9200/documents/_count'
```

```json
{"count":10,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0}}
```

That is the whole lab. Keep the terminal open; everything from here on runs against it, and `docker rm -f lantern` tears it down when you are finished.

Every request in this chapter was run against exactly this setup, and every response printed is the real one. If yours differs, that is worth a moment of your attention rather than none.

> **A note on the corpus.** d9 is about the novelist, and it is there on purpose: it will match "kafka" perfectly and be useless every time, which is the entire problem of relevance in one document. d7 is titled "Diagnosing CrashLoopBackOff" and will turn out to be unfindable by the people who need it most, which is the entire problem of Chapter 3 in one document.

---

## §2.1 Why the database can't do this

Lantern has two hundred million documents in PostgreSQL and a search box. The obvious implementation:

```sql
SELECT * FROM documents WHERE body LIKE '%kafka%';
```

Let us be precise about why this is hopeless, because each reason tells you something about what has to replace it.

### It is slow, and an index cannot save it

You know an index makes queries fast. Here is why *this* query cannot use one.

A B-tree index is a sorted structure — a phone book, sorted by the **beginning** of the value:

```
"Diagnosing CrashLoopBackOff"
"Kafka consumer groups rebalance"
"Kafka partitions are ordered"
"Ordered delivery in Kafka"
"Postgres replication basics"
"The Kafka log is append only"
"The novelist Franz Kafka"
```

Ask for `LIKE 'Kafka%'` — *starts with* — and the index is beautiful. Binary-search to the K's, read forward until you leave them, stop. You skipped almost everything because sorting told you where to look.

Now ask for `LIKE '%kafka%'` — *contains, anywhere*. Look at the list above. `"Ordered delivery in Kafka"` matches and is filed under O. `"The novelist Franz Kafka"` matches and is filed under T. The matches are scattered uniformly through the sort order, so **sorting tells you nothing about where they are, and there is nothing to skip**. The database reads all forty terabytes. That is a full table scan, and no index of this kind can prevent it, because the index is sorted by the wrong thing.

Hold on to that phrase — *sorted by the wrong thing*. §2.2 is precisely the structure sorted by the right thing.

### It is wrong, four times

Run the SQL mentally against the ten documents.

1. **Case.** `'%kafka%'` misses d1, d2, d5, d9 — all four capitalise it. You can write `LOWER(body) LIKE '%kafka%'`, but now you must remember to lowercase in two places forever, and one day someone won't.
2. **Word forms.** Searching `streaming` misses a document that says `streams`. Two unrelated strings to the database; one idea to a human.
3. **Multiple words.** `'%kafka ordering%'` matches **nothing** — no document contains that character sequence. But d1 and d2 are exactly what the user wanted. The user typed two concepts; SQL heard one literal string.
4. **Spurious matches.** `'%kafka%'` cheerfully returns d9, about the novelist. It matches. It is useless.

### And the deepest problem: there is no "best"

Even if you fixed all of the above, the query returns eleven thousand rows **in no particular order**. The user wants ten rows, and specifically the *best* ten.

The database has no concept of "best". It can tell you whether a row matches. It cannot tell you that one match is better than another, because **relevance is not a predicate**. There is no `WHERE` clause for "about".

That is the real split, and it is worth stating as a table because the rest of the chapter falls out of it:

| | Database retrieval | Search |
|---|---|---|
| The question | a precise predicate | a fuzzy intention |
| The answer | the set of matching rows | an *ordered* list, best first |
| "Correct" means | exactly the matching rows | the user found what they wanted |

That last row is the uncomfortable one. **Search has no provably correct answer.** Which is why §2.9 spends so long on *measuring* relevance: when there is no proof, you need evidence.

---

## §2.2 The inverted index

### Turn the arrow around

A database stores, in effect, a forward map — *document → its contents*:

```
d1 → "Kafka partitions are ordered"
d2 → "Ordered delivery in Kafka"
d3 → "Spark partitions and tasks"
```

Perfect for "show me d2". Useless for "who mentions Kafka?", which requires reading everything.

So invert it. For each word, write down which documents contain it. Here is the real thing, built from the ten titles in your lab:

```
and         → [d3]
append      → [d5]
are         → [d1]
basics      → [d10]
consumer    → [d4]
crashloopbackoff → [d7]
delivery    → [d2]
diagnosing  → [d7]
franz       → [d9]
groups      → [d4]
in          → [d2]
is          → [d5]
kafka       → [d1, d2, d4, d5, d9]
kubernetes  → [d8]
limits      → [d8]
log         → [d5]
memory      → [d8]
novelist    → [d9]
only        → [d5]
ordered     → [d1, d2]
partitions  → [d1, d3, d6]
pod         → [d8]
postgres    → [d10]
rebalance   → [d4]
replication → [d10]
shuffle     → [d6]
spark       → [d3, d6]
tasks       → [d3]
the         → [d5, d9]
tuning      → [d6]
```

That is an inverted index. The left column, sorted so you can binary-search it, is the **term dictionary**. Each right-hand list is a **postings list**.

You have met this structure before without noticing: it is the index at the back of a textbook. *Kafka … 12, 47, 88.* Nobody reads a book cover to cover to find the Kafka pages. Thirteenth-century monks built exactly this by hand for the Bible and called it a concordance.

### Why this makes queries fast

**One term.** "Who mentions kafka?" → binary-search the dictionary to `kafka`, read the list: `[d1, d2, d4, d5, d9]`. You never opened a single document. The cost depends on the length of that list, not on the size of the corpus. Ten documents or ten billion, the lookup is the same shape.

**Two terms, AND.** "kafka AND partitions" is the **intersection** of two sorted lists:

```
kafka       → [d1, d2, d4, d5, d9]
partitions  → [d1, d3, d6]
```

Because both lists are sorted, you walk them with two fingers, always advancing whichever finger is behind:

```
A on d1,  B on d1   → equal! emit d1, advance both
A on d2,  B on d3   → 2 < 3, advance A
A on d4,  B on d3   → 3 < 4, advance B
A on d4,  B on d6   → 4 < 6, advance A
A on d5,  B on d6   → 5 < 6, advance A
A on d9,  B on d6   → 6 < 9, advance B
B runs out          → done

result: [d1]
```

The work is bounded by the total length of the lists — and real engines skip ahead rather than stepping one at a time, so it is closer to the length of the *shorter* list. This is why a five-word query over a billion documents returns in single-digit milliseconds. **Nothing is being scanned.** Five precomputed lists are being intersected.

This is the single most important mechanical idea in the chapter. Everything else is refinement.

### What is actually in a postings entry

Real postings carry more than a document number. Two extras matter.

**Term frequency (tf)** — how many times the term appears in that document. Needed for ranking in §2.9: a document saying "kafka" eight times is more likely to be *about* Kafka than one that says it once in a footnote.

**Positions** — where in the field, as word offsets.

```
kafka → [ d1(tf=1, pos=[0]), d2(tf=1, pos=[3]), d4(tf=1, pos=[0]),
          d5(tf=1, pos=[1]), d9(tf=1, pos=[3]) ]
```

Positions are what make **phrase search** possible. Take the phrase `"ordered delivery"` against d2, `"Ordered delivery in Kafka"`:

```
ordered  in d2 at position [0]
delivery in d2 at position [1]
```

Intersect first to find documents containing both words (only d2 qualifies), then check positions: is there a position of `delivery` exactly one greater than a position of `ordered`? `1 = 0 + 1`. Yes — d2 contains the phrase.

Now d1, `"Kafka partitions are ordered"`: it has `ordered` at position 3 but no `delivery` at all, so it never survives the intersection. And a hypothetical document saying *"delivery was ordered"* would survive the intersection but fail the position check — `ordered` at 2, `delivery` at 0, and `0 ≠ 3`. Correctly rejected.

Try both against your lab:

```bash
curl -s 'localhost:9200/documents/_search?filter_path=hits.hits._id' -H 'Content-Type: application/json' -d '
{ "query": { "match": { "title": "ordered delivery" } } }'
```

```json
{"hits":{"hits":[{"_id":"d2"},{"_id":"d1"}]}}
```

```bash
curl -s 'localhost:9200/documents/_search?filter_path=hits.hits._id' -H 'Content-Type: application/json' -d '
{ "query": { "match_phrase": { "title": "ordered delivery" } } }'
```

```json
{"hits":{"hits":[{"_id":"d2"}]}}
```

`match` found both documents containing *either* word; `match_phrase` used the positions and found only the one where they are adjacent. That extra position check is why `match_phrase` costs more than `match` — and why it is still fast, because the expensive filtering already happened.

### Doc values: the other direction

The inverted index answers **"given a term, which documents?"** superbly.

Now ask the opposite: **"given d4, what is its `updated_at`?"** You need this to *sort* results by date, to *average* a numeric field, to count documents per team.

Try it with an inverted index. You would have to walk every date in the dictionary asking "is d4 in your list?" until one said yes. Hopeless — the structure is oriented the wrong way.

So the engine builds a **second** structure at the same time, called **doc values**: for each field, every document's value laid out contiguously in document order.

```
doc values for `team`:
  doc:    d1        d2        d3     d4        d5        d6     ...
  value:  platform  platform  data   platform  platform  data   ...
```

To sort ten thousand results by team you read this array at ten thousand offsets. Contiguous, columnar, memory-mapped so the operating system pages in what is needed — very fast.

```
inverted index:   term → [documents]     "who has this value?"
doc values:       document → value       "what is this document's value?"
```

**Two structures, opposite orientations, both built when you index.** Keep this pair in your head, because the chapter's most confusing rules are consequences of it:

- You *search* with the inverted index.
- You *sort and aggregate* with doc values.
- A `text` field has no useful doc values — it was shredded into terms, so there is no single value to store — which is why **you cannot sort or aggregate on a `text` field**. That rule in §2.4 is not an arbitrary restriction. There is literally no data there to read.

Watch it fail, right now:

```bash
curl -s 'localhost:9200/documents/_search' -H 'Content-Type: application/json' -d '
{ "sort": [ { "title": "asc" } ] }' | head -c 400
```

```json
{"error":{"root_cause":[{"type":"illegal_argument_exception","reason":
"Text fields are not optimised for operations that require per-document
field data like aggregations and sorting, so these operations are disabled
by default. Please use a keyword field instead..."}]}}
```

The error message is telling you exactly what this section just said. §2.4 gives you the fix.

---

## §2.3 The shape of the system

So far everything has been a data structure on one machine. That machine is real, and it has a name.

**Lucene** is a Java *library* that implements everything in §2.2 — term dictionary, postings lists, positions, doc values, scoring. It runs in one process, over one set of files on one disk. It has no notion of a network.

**OpenSearch** wraps Lucene in a distributed system: an HTTP API, splitting data across machines, replication, failure handling, cluster management.

> *Lucene does the searching. OpenSearch does the distributing.*

When something confuses you, ask which layer owns it. Scoring is Lucene. Shard placement is OpenSearch. (OpenSearch is a 2021 fork of Elasticsearch, branched at 7.10 over a licence change, which is why almost all Elasticsearch documentation and Stack Overflow answers apply verbatim.)

### The wall: two hundred million documents do not fit

Your lab holds ten documents in one Lucene index. Lantern holds two hundred million, which is roughly six terabytes. That does not fit on one machine, and even if it did, one machine searching six terabytes is one machine's worth of CPU.

So the index is **split**, exactly as §1.3 described. The vocabulary nests:

```
Cluster          the whole deployment
 └── Node        one OpenSearch process on one machine
   └── Index     a named collection of documents ("documents")
     └── Shard   a slice of the index — itself a complete Lucene index
       └── Segment    an immutable file of indexed data
         └── Document one JSON object
           └── Field  one named value inside it
```

**Document** — one JSON object, the unit you put in and get back:

```json
{ "_id": "d1",
  "title": "Kafka partitions are ordered",
  "body": "Within a partition Kafka guarantees order...",
  "team": "platform",
  "updated_at": "2026-03-01" }
```

The original JSON is kept verbatim in a field called `_source` so it can be returned to you. You *can* disable `_source` to save space, and you almost never should: without it you cannot reindex, cannot use the update API, and cannot see what you actually stored.

**Index** — a named collection of documents sharing a schema. "Like a table" is roughly right and slightly misleading, because an index is much more expensive than a table: each one costs heap, file handles, and cluster metadata. "One index per customer" across ten thousand customers is a recognised way to kill a cluster.

**Shard** — here is the idea that takes longest to internalise, so let me be blunt about it:

> **A shard is not a piece of a search engine. A shard IS a search engine.**

Split the ten lab documents across three shards and you would get something like:

```
shard 0: d1, d4, d7, d10
shard 1: d2, d5, d8
shard 2: d3, d6, d9
```

Shard 2 has its *own* term dictionary, its *own* postings lists, its own doc values, its own internal document numbering starting at 0. It does not know shards 0 and 1 exist. Hand it a query and it will answer completely and correctly — about its own three documents.

Three consequences drop straight out of that one fact, and the chapter returns to all three:

1. **Searching an index means searching every shard and merging the results.** That is §1.3's scatter-gather, with its straggler problem: the query is as slow as the slowest shard.
2. **Relevance scores differ slightly between shards** (§2.7), because "how rare is this term?" is computed per shard, from local documents only.
3. **`terms` aggregations are approximate** (§2.8), for the same reason — each shard only knows its own counts.

This is also why your lab uses `"number_of_shards": 1`. With ten documents across three shards, "how rare is kafka?" would be computed from three or four documents at a time and the scores would be nonsense. One shard makes every number in this chapter reproducible.

**Primaries and replicas.** A **primary** shard accepts writes. A **replica** is an exact copy on a *different* node; it serves reads and gets promoted if the primary's node dies. This is leader–follower replication from §1.4 with OpenSearch vocabulary on top.

```
3 primaries, 1 replica each = 6 shards on 3 nodes

    Node A            Node B            Node C
  ┌──────────┐     ┌──────────┐     ┌──────────┐
  │ P0       │     │ P1       │     │ P2       │
  │ R2       │     │ R0       │     │ R1       │
  └──────────┘     └──────────┘     └──────────┘
```

No shard shares a node with its own copy — the allocator enforces this, because a copy on the same machine protects against nothing. Kill node A: P0 is gone, but R0 on node B is promoted, and the cluster keeps serving. It then quietly builds a fresh replica of shard 0 somewhere.

And the rule that shapes the rest of the chapter:

> **The primary count is fixed when the index is created. The replica count can change at any time.**

§2.6 shows you *why* (it is a modulus, and the arithmetic is worth doing yourself). §2.10 is mostly about living with it.

### Segments, and the fact that nothing is ever modified

Inside a shard, Lucene writes **segments** — files that, once written, are **never modified**.

Which raises an obvious question: what happens when you update a document? Update d4 in your lab:

```bash
curl -s -X POST 'localhost:9200/documents/_update/d4?refresh=wait_for' -H 'Content-Type: application/json' -d '
{ "doc": { "status": "archived" } }' > /dev/null
curl -s 'localhost:9200/documents/_stats/docs?filter_path=_all.primaries.docs'
```

```json
{"_all":{"primaries":{"docs":{"count":10,"deleted":1}}}}
```

Ten live documents and **one deleted** — from a request you would have called an update. Here is what physically happened:

```
segment_1:  d4 (version 1)                 ← still on disk, untouched
segment_7:  d4 (version 2, status archived) ← new segment
deletes:    "segment_1 doc 3 is deleted"    ← tiny auxiliary file
```

Both copies are on disk. Every search consults the deletion list and skips the old one. The old bytes are reclaimed only later, when a background **merge** combines small segments into a bigger one and simply declines to copy the dead documents forward. Deletes work identically: a tombstone now, real removal at merge.

Put d4 back the way it was, and count again:

```bash
curl -s -X POST 'localhost:9200/documents/_update/d4?refresh=wait_for' -H 'Content-Type: application/json' -d '
{ "doc": { "status": "published" } }' > /dev/null
curl -s 'localhost:9200/documents/_stats/docs?filter_path=_all.primaries.docs'
```

```json
{"_all":{"primaries":{"docs":{"count":10,"deleted":2}}}}
```

Still ten live documents, and now **two** dead ones. Undoing a change does not undo the writes; it adds another. Nothing shrinks until a merge.

And a tombstone is not inert while it waits. **A deleted document still counts in the corpus statistics** — `N`, the number of documents, and `avgdl`, the average field length, both of which BM25 divides by in §2.9. Delete a million documents from a ten-million-document index and, until the merges catch up, every relevance score is computed as though they were still there. It is a real effect on clusters with high delete rates, and it is invisible unless you go looking at `_explain`.

Why accept this weirdness? Immutable files:

- need **no locking** — any number of threads read concurrently, coordinating about nothing;
- can be **cached without invalidation**, because a cached copy can never go stale;
- can be **copied to another node** for recovery without pausing anything;
- are written **purely sequentially**, the fastest thing a disk does.

The price: space comes back late, segment count must be managed, and merges burn real I/O and CPU in the background. Almost every performance oddity in §2.11 traces back to segments.

You will meet this exact trade again in §5.1, where Kafka's log is fast for precisely the same reasons.

### Node roles

In a small deployment every node does everything. As you grow, you separate the jobs.

- **Cluster manager** (called *master* in older documentation) — keeps the cluster state: which indexes exist, their mappings, where every shard lives. Holds no data, answers no queries. Elected by a quorum protocol of the kind in §1.6, which is why the recommendation is **three** dedicated nodes: three survive one failure, and a fourth adds nothing. It sounds like paperwork right up until it wobbles, at which point nothing works — which is why you give it its own machines rather than letting it compete with search traffic.
- **Data nodes** — hold shards, do the indexing and searching. The expensive ones, and the ones you add for capacity.
- **Coordinating node** — not a configuration but a *role in a request*. Whichever node receives your query fans it out, merges the responses, and replies. In a large cluster you dedicate nodes to this, because merging results from fifty shards is real work and you would rather it not steal CPU from searching.
- **Ingest nodes** run lightweight transformation pipelines before indexing; **warm** and **cold** tiers hold older data on cheaper storage.

### The allocator, quietly running

A background loop on the cluster manager continuously tries to satisfy a set of constraints: every primary assigned somewhere; every replica on a different node from its primary; disk usage balanced; shard counts balanced; and any *awareness* rules respected — "spread the copies of each shard across availability zones, so losing a zone does not lose data."

Node vanishes → promote replicas, rebuild the missing copies. Node added → move shards onto it. Both move real bytes over the network and take real time, which is one reason §2.11 wants shards in the tens of gigabytes rather than the hundreds.

---

## §2.4 The schema: mappings

A **mapping** is OpenSearch's schema. It declares, per field: what type it is, how it should be analysed, and whether it is searchable, sortable, aggregatable.

Look at the one your lab is using:

```bash
curl -s 'localhost:9200/documents/_mapping?pretty'
```

```json
{ "documents": { "mappings": { "properties": {
  "body":       { "type": "text" },
  "status":     { "type": "keyword" },
  "team":       { "type": "keyword" },
  "title":      { "type": "text" },
  "updated_at": { "type": "date" }
}}}}
```

### Dynamic mapping, and two ways it bites

Index a document with a field OpenSearch has never seen and it **guesses** a type and permanently adds it to the mapping. Watch:

```bash
curl -s -X POST 'localhost:9200/documents/_doc/d11?refresh=wait_for' -H 'Content-Type: application/json' -d '
{ "title": "Sharding strategies", "views": "417" }' > /dev/null
curl -s 'localhost:9200/documents/_mapping?filter_path=documents.mappings.properties.views'
```

```json
{"documents":{"mappings":{"properties":{"views":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}}}}}}
```

`views` was `"417"` — a string, because whoever wrote the producer forgot to strip the quotes — so `views` is now a **text** field, permanently. Try a numeric range on it and you will get coercion and confusion; the real fix is a full reindex. That is failure mode one: **the first document decides, forever.**

Failure mode two is worse. Index a bag of user-supplied keys:

```json
{ "properties": { "utm_campaign_spring_2026": "x", "ab_test_4471": "b" } }
```

Dynamic mapping adds a field for **every distinct key it ever sees**. Ten thousand campaigns become ten thousand fields. The mapping lives in the cluster state; the cluster state is replicated to every node on every change; the cluster manager starts drowning; the cluster becomes unstable. This is called **mapping explosion**, and the fact that it has a name should tell you how often it happens.

The defence is to define mappings explicitly through an **index template** and then set one of:

- `"dynamic": "strict"` — reject documents containing unknown fields;
- `"dynamic": false` — keep them in `_source` but do not index them.

For anything user-supplied, pick one. Now take d11 back out, so that the corpus is ten documents again and every score later in the chapter is reproducible:

```bash
curl -s -X DELETE 'localhost:9200/documents/_doc/d11?refresh=wait_for' > /dev/null
curl -s 'localhost:9200/documents/_count?filter_path=count'
```

```json
{"count":10}
```

The mapping keeps the `views` field, by the way. Deleting the document does not un-guess the type — which is the whole point of this section.

### The types worth knowing

- **Numbers**: `long`, `integer`, `short`, `byte`, `double`, `float`, `half_float`, and `scaled_float` (fixed-point stored as an integer with a scaling factor — the right choice for prices). Pick the smallest that fits; it directly shrinks the index.
- **Time**: `date`, which accepts many input formats and stores epoch milliseconds internally.
- **Structure**: `object` (flattened to dotted paths like `author.name`) and `nested` (below, and it exists to solve one specific surprising problem).
- **Geo**: `geo_point`, `geo_shape` — distance queries and geographic aggregations.
- **Vectors**: `knn_vector`, the entry point to everything in Chapter 3.
- And the two everyone gets wrong at least once: `text` and `keyword`.

### `text` vs `keyword`, or: the mistake everybody makes

- **`text`** is **analysed**: chopped into terms, lowercased, stemmed; each term indexed separately.
- **`keyword`** is **not analysed**: the whole value becomes exactly one term, byte for byte.

Take a title like `"Senior Java Engineer"`.

**As `text`**, the index receives three terms:

```
senior → [d]     java → [d]     engineer → [d]
```

- Search `java engineer` → **matches** (both terms present).
- Search `Java` → **matches** (the query is lowercased the same way the document was).
- Exact lookup for the string `"Senior Java Engineer"` → **no match**. There is no term equal to that string. It was taken apart, and the whole was never stored.

**As `keyword`**, the index receives one term:

```
Senior Java Engineer → [d]
```

- Exact lookup → **matches**.
- Sort by it → works (it has doc values: one value per document).
- Aggregate "top 10 job titles" → works.
- Search `java` → **no match**. The only term is the full string, and `java ≠ Senior Java Engineer`.

See both in the lab. `title` is `text`, `team` is `keyword`:

```bash
# term query on a TEXT field — the exact string was never stored as a term
curl -s 'localhost:9200/documents/_search?filter_path=hits.total' -H 'Content-Type: application/json' -d '
{ "query": { "term": { "title": "Kafka partitions are ordered" } } }'
```

```json
{"hits":{"total":{"value":0,"relation":"eq"}}}
```

```bash
# term query on a KEYWORD field — exact bytes, exact match
curl -s 'localhost:9200/documents/_search?filter_path=hits.total' -H 'Content-Type: application/json' -d '
{ "query": { "term": { "team": "platform" } } }'
```

```json
{"hits":{"total":{"value":4,"relation":"eq"}}}
```

Zero hits, no error, no hint. That first response is what "OpenSearch is broken" usually looks like.

Neither type is right in general. **Which you want depends on the query, not on the field.** So have both, through a **multi-field**:

```json
"title": {
  "type": "text",
  "fields": {
    "keyword": { "type": "keyword", "ignore_above": 256 }
  }
}
```

One field in your JSON, two fields in the index. You search `title`; you sort and aggregate on `title.keyword`. This is exactly what dynamic mapping produces for a string by default — which is why things often work *before* you write an explicit mapping and break *after*: you removed a multi-field you did not know was there. (It is also why the `views` field above came back with a `.keyword` sub-field you never asked for.)

> **The diagnostic shortcut worth memorising: a search returns nothing and you are certain it should match → check whether you ran a `term` query against a `text` field.** That one mistake accounts for a remarkable share of search bug reports.

### The nested-object trap

This one produces **silently wrong answers** rather than errors, which is why it earns the space.

Suppose a Lantern document lists its contributors:

```json
{ "contributors": [ { "name": "asha",  "edits": 120 },
                    { "name": "priya", "edits": 3   } ] }
```

Mapped as a plain `object`, OpenSearch flattens it. The index does not hold two contributor objects. It holds two multi-valued fields:

```
contributors.name  → [asha, priya]
contributors.edits → [120, 3]
```

**The pairing is gone.** Now query "a contributor named priya with more than 100 edits":

- does this document contain the name `priya`? Yes.
- does it contain an `edits` value above 100? Yes — 120.
- → **match.**

Which is wrong. Priya made three edits. The two facts came from different objects and the index could not tell. Nothing errors. You may not notice for a year.

The fix is `"type": "nested"`, which tells Lucene to store each sub-object as its own hidden document, preserving the correlation, and to query it with a `nested` query. The cost is real — more documents under the hood, slower queries, a cap on how many you can have — so use it when you need the correlation and not otherwise. But *know which one you need*, because the failure mode is silence.

### A few other mapping controls

- `index: false` — keep it in `_source`, do not make it searchable. Saves space on display-only fields.
- `doc_values: false` — no columnar structure. Saves space on fields you never sort or aggregate on.
- `copy_to` — duplicate several fields into one catch-all, giving a cheap "search everything" target.
- `ignore_above` — on a keyword, silently skip indexing values longer than N characters, so a thousand-character string never becomes a term.

And the rule that the whole of §2.10 exists to work around:

> **You cannot change the type of an existing field.** The inverted index and doc values were built to the old type; there is no reinterpreting them. Changing a mapping means creating a new index and copying the data across. Which is why aliases exist.

---

## §2.5 Analysis: how text becomes terms

### The wall

Search your lab for "ordering". Two documents are obviously about ordered delivery in Kafka.

```bash
curl -s 'localhost:9200/documents/_search?filter_path=hits.total' -H 'Content-Type: application/json' -d '
{ "query": { "match": { "title": "ordering" } } }'
```

```json
{"hits":{"total":{"value":0,"relation":"eq"}}}
```

Nothing. d1 is titled "Kafka partitions are **ordered**" and it did not match "ordering". This is the most common bug in search, and the section exists to make it a five-second diagnosis instead of an afternoon.

### What analysis is

**Analysis** is the pipeline that turns a string into the terms stored in the inverted index. The crucial fact:

> It runs **twice** — at index time on the document, and at query time on the query string — and both must produce **compatible terms**, because matching happens on terms and on nothing else.

Index `"Kafka"`, lowercase it to `kafka`. Query `"Kafka"` without lowercasing and you look up the term `Kafka`. `Kafka ≠ kafka`. Zero results, while you stare at the document and swear it is right there. Nearly every "why doesn't this match" bug is this, in some costume.

### Three stages

```
  "The Café's WiFi-router is <b>broken</b>!"
        │
        ▼   1. CHARACTER FILTERS  (zero or more, on the raw string)
            html_strip → "The Café's WiFi-router is broken!"
        │
        ▼   2. TOKENIZER  (exactly one; splits into tokens)
            standard → [The, Café's, WiFi, router, is, broken]
        │
        ▼   3. TOKEN FILTERS  (zero or more, applied in order)
            lowercase    → [the, café's, wifi, router, is, broken]
            asciifolding → [the, cafe's, wifi, router, is, broken]
            stemmer      → [the, cafe, wifi, router, is, broken]
        │
        ▼
  these terms go into the inverted index
```

**Character filters** work on the raw string before anything is split: strip HTML, replace characters, apply a regex.

**The tokenizer** does the splitting, and there is exactly one:

- `standard` — Unicode word-boundary rules. Right for essentially all prose.
- `whitespace` — split on spaces only, so `WiFi-router` stays one token.
- `keyword` — emit the whole input as one token. This is literally how a `keyword` field behaves.

Two more deserve attention.

**`ngram`** emits every substring within a length range. `java` with min=2, max=4 becomes:

```
ja, av, va, jav, ava, java
```

Six terms for one word. Now a search for `av` finds it — substring matching, and some typo tolerance — at the cost of an index several times larger, because every word explodes. Use it deliberately, on small fields.

**`edge_ngram`** emits only **prefixes**. `java` becomes:

```
j, ja, jav, java
```

This is the machinery for **autocomplete**: index a title this way and, when the user has typed `jav`, that prefix is a real term in the index and the lookup is instant.

But here is the part people get wrong, and it is worth slowing down for. Analysis runs at query time too. If you use `edge_ngram` on *both* sides, the query `java` becomes `[j, ja, jav, java]`, and the term `j` matches **every word beginning with j** in the corpus — javascript, jenkins, jira, jupyter. Your autocomplete returns nonsense.

The fix is to analyse the two sides differently:

```json
"title_ac": {
  "type": "text",
  "analyzer":        "edge_ngram_analyzer",   ← index time: all prefixes
  "search_analyzer": "standard"               ← query time: the word as typed
}
```

Index side produces `j, ja, jav, java`; query side produces just `jav`; `jav` matches. **This is the canonical reason `search_analyzer` exists.**

### Token filters

- **`lowercase`** — you will always want it.
- **`asciifolding`** — `café → cafe`, `Zürich → Zurich`. Enormous for names and any international corpus, because users do not type accents.
- **`stop`** — removes "the", "a", "of". This mattered when disks were small and is mostly a mistake now, for two reasons. BM25 (§2.9) already gives near-zero weight to words that appear everywhere, so it solves nothing; and it **breaks phrase queries**. Search for `"to be or not to be"` in a stopword-stripped index and you are searching for the empty set. Same for "The Who", "The The", and a surprising number of product names. Leave stopwords in unless you have a specific reason.
- **`stemmer`** — reduce words to a root: `running`, `runs`, `ran` → `run`. This raises **recall** (you find more of the relevant documents) at some cost to **precision** (you also find some irrelevant ones), because distinct words sometimes collapse: an aggressive stemmer turns both `universal` and `university` into `univers`. They come in strengths — `porter_stem` is enthusiastic, `minimal_english` only strips plurals, `kstem` and `light_english` sit in between. If users report obviously wrong words in their results, suspect the stemmer.

  *(Precision and recall, since the chapter uses them freely: **precision** = of what I returned, how much was relevant. **Recall** = of everything relevant, how much did I return. Push one and the other usually sags.)*

- **`synonym` / `synonym_graph`** — make `js`, `javascript`, and `ecmascript` interchangeable. There is a placement decision worth understanding:
  - **Index time**: free at query time, but changing the list requires reindexing the whole corpus, *and* expanding synonyms into the index distorts the term statistics that ranking depends on — suddenly `javascript` appears in far more documents than it really does, so IDF discounts it.
  - **Query time**, via `synonym_graph` in a `search_analyzer`: costs a little per query, editable whenever you like.

  Prefer query time. The operational flexibility is worth far more than the microseconds.

### `_analyze`, and the five-second diagnosis

This is the most useful endpoint in the product, and it does exactly one thing: it shows you the terms.

```bash
curl -s 'localhost:9200/documents/_analyze?filter_path=tokens.token' -H 'Content-Type: application/json' -d '
{ "field": "title", "text": "Kafka partitions are ordered" }'
```

```json
{"tokens":[{"token":"kafka"},{"token":"partitions"},{"token":"are"},{"token":"ordered"}]}
```

```bash
curl -s 'localhost:9200/documents/_analyze?filter_path=tokens.token' -H 'Content-Type: application/json' -d '
{ "field": "title", "text": "ordering" }'
```

```json
{"tokens":[{"token":"ordering"}]}
```

Put them side by side and the bug from the top of this section is *visible*:

```
document terms:  [kafka, partitions, are, ordered]
query terms:     [ordering]
                            ↑ no term in common. Not a ranking problem — not a candidate.
```

The default analyzer lowercases and splits, and that is all. It does not stem. `ordered` and `ordering` are two unrelated strings to it, exactly as they were to PostgreSQL in §2.1.

**The debugging recipe** — mechanical, works nearly every time. A document should match and does not:

1. `_analyze` the document's text.
2. `_analyze` the query's text.
3. Put the two lists side by side.

In the overwhelming majority of cases the mismatch is immediately visible: a stemmer applied on one side only, a stopword removed, a case difference, an accent, a hyphen split differently than you assumed. The invisible becomes something you can look at, and you stop guessing.

### Fixing it: a custom analyzer

Build one. Strip HTML, split on word boundaries, lowercase, fold accents, and stem with Porter:

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

Note the `title` multi-field: `title` for searching, `title.keyword` for sorting and aggregating — the §2.4 fix for the error you saw at the end of §2.2.

Check the analyzer before you trust it, which is the whole point of `_analyze`:

```bash
curl -s 'localhost:9200/documents_v2/_analyze?filter_path=tokens.token' -H 'Content-Type: application/json' -d '
{ "analyzer": "lantern_text", "text": "Ordered delivery, ordering, and the Café'"'"'s <b>servers</b>" }'
```

```json
{"tokens":[{"token":"order"},{"token":"deliveri"},{"token":"order"},
           {"token":"and"},{"token":"the"},{"token":"cafe'"},{"token":"server"}]}
```

`Ordered` and `ordering` both became `order`, which is the fix we wanted. The `<b>` tags are gone. `delivery` became `deliveri`, which is not a word — stems do not have to be words, they only have to be *consistent*, because both sides of the match go through the same pipeline.

And then there is `cafe'`. The accent folded as expected, but the possessive did not: the `standard` tokenizer keeps `Café's` as one token, the stemmer strips the `s`, and you are left with a term ending in an apostrophe. Nobody will ever type that. A user searching for `cafe` will not match it.

That is a small bug, sitting in an analyzer that looked obviously correct when you read it, found in four seconds by looking at the output. It is exactly why this endpoint is worth more than the rest of the tooling combined — and the fix is a `char_filter` mapping `'` to nothing, or an `apostrophe` token filter before the stemmer.

Copy the corpus across and try the failing query again:

```bash
curl -s -X POST 'localhost:9200/_reindex?refresh=true' -H 'Content-Type: application/json' -d '
{ "source": { "index": "documents" }, "dest": { "index": "documents_v2" } }' > /dev/null

curl -s 'localhost:9200/documents_v2/_search?filter_path=hits.hits._id' -H 'Content-Type: application/json' -d '
{ "query": { "match": { "title": "ordering" } } }'
```

```json
{"hits":{"hits":[{"_id":"d1"},{"_id":"d2"}]}}
```

Both documents, from a word neither of them contains. That is stemming earning its keep — and §2.10 will show you how to make that swap from `documents` to `documents_v2` without your application noticing.

**Use `documents_v2` from here on.** It is what Lantern actually runs.

---

## §2.6 What happens when you write

We can now trace a write, and resolve two mysteries: why the primary shard count is immutable, and why a document you just successfully indexed cannot be found.

### Routing, and the immutable shard count

```
1. The client sends the document to any node → that node becomes the coordinator.

2. The coordinator computes which shard owns it:

       shard = hash(routing) % number_of_primary_shards

   where `routing` defaults to the document's _id.

3. It forwards the document to the node holding that primary shard.

4. The primary writes it into an in-memory buffer,
   and appends the operation to the TRANSLOG.

5. The primary forwards it to all in-sync replicas, in parallel.

6. The replicas acknowledge → the primary acknowledges to the client.
```

**Step 2 is the answer to the first mystery.** Work it through with real numbers. Lantern has 12 primaries, and suppose `hash("d-1001") = 90211`:

```
90211 % 12 = 7     → document d-1001 lives on shard 7
```

Now grow the index to 13 shards:

```
90211 % 13 = 4     → the formula now says shard 4
```

The document is physically still on shard 7. A `GET /documents/d-1001` computes 4, asks shard 4, and shard 4 has never heard of it. **The document is not gone — it is unfindable.** And this happens to *nearly every document at once*, because changing the divisor reshuffles almost everything.

That is §1.3's modulus problem arriving in a new costume, and it is why the primary count is frozen at creation, and why §2.10 has a whole apparatus of reindexing and aliases.

**Step 6 is worth noticing too.** OpenSearch replicates **synchronously**: when you get a success response, the document is on the primary *and* on its in-sync replicas, not queued somewhere hopeful. That is the durable side of the §1.4 trade-off, and it is why indexing latency rises when one replica gets slow. (A related setting, `wait_for_active_shards`, controls how many copies must be *available* before the write is even attempted.)

### The three different meanings of "written to disk"

This is where most people's mental model is wrong, and getting it exactly right determines both correctness and performance. Three operations, three schedules, three different guarantees. Do not blur them.

| Operation | Default schedule | What it guarantees |
|---|---|---|
| **Refresh** | every **1 second** | documents become **visible to search** |
| **Flush** | ~every 30 min, or translog > 512 MB | segments `fsync`ed → **durable in segment form** |
| **Translog fsync** | **every request** | the acknowledged write is **crash-safe** |

**Refresh** turns the in-memory buffer into a new segment and opens that segment for searching. Note what it does *not* promise: the segment may still live only in the filesystem cache. Refresh is about **visibility**, not durability.

**Flush** `fsync`s segments to physical disk and starts a fresh translog. Durability, in segment form.

**Translog fsync** (`index.translog.durability: request`) forces the operation log to disk *before* your write is acknowledged. This is the write-ahead log of §1.6 doing exactly its usual job: if the machine loses power one millisecond after the acknowledgement, the document is recoverable from the log even though no segment exists yet. You can relax it to `async` (every five seconds) for a real throughput gain and a five-second window of possible loss on a hard crash.

From which follows the single most surprising property of OpenSearch:

> **A document that has been successfully indexed — acknowledged, durable, crash-safe — is not searchable for up to one second.**

It is on disk. It will survive a power cut. And a search will not find it, because the *segment* it lives in has not been created yet. OpenSearch is **near real-time**, not real-time, and this is deliberate: creating a segment is not free, and doing it once a second instead of once per document is the difference between thousands of writes per second and dozens.

Watch it happen. Index a document and immediately search for it:

```bash
curl -s -X PUT localhost:9200/nrt_demo -H 'Content-Type: application/json' -d '
{ "settings": { "number_of_shards": 1, "number_of_replicas": 0 } }' > /dev/null

curl -s -X POST 'localhost:9200/nrt_demo/_doc/d99' -H 'Content-Type: application/json' -d '
{ "title": "Watermarks in Flink" }'
curl -s 'localhost:9200/nrt_demo/_search?filter_path=hits.total' -H 'Content-Type: application/json' -d '
{ "query": { "match": { "title": "watermarks" } } }'
```

```json
{"_index":"nrt_demo","_id":"d99","_version":1,"result":"created",
 "_shards":{"total":1,"successful":1,"failed":0},"_seq_no":0,"_primary_term":1}
{"hits":{"total":{"value":0,"relation":"eq"}}}
```

Created, acknowledged, zero hits. Wait a second, run the search again, and it is there:

```bash
sleep 2
curl -s 'localhost:9200/nrt_demo/_search?filter_path=hits.total' -H 'Content-Type: application/json' -d '
{ "query": { "match": { "title": "watermarks" } } }'
curl -s -X DELETE localhost:9200/nrt_demo > /dev/null
```

```json
{"hits":{"total":{"value":1,"relation":"eq"}}}
```

Nothing was fixed in between. A timer fired. This catches everybody, always in the same test.

(The demo used a throwaway index on purpose. Adding d99 to `documents_v2` and deleting it again would leave a tombstone behind, and §2.3 just showed you that a tombstone is not nothing — see the note there about what it does to corpus statistics.)

- **The right fix in a test**: `?refresh=wait_for` — your request blocks until the next scheduled refresh happens. (It is why the bulk load in §2.0 used it.)
- **The wrong fix**: `?refresh=true` — forces an immediate refresh, creating a tiny segment for *every single write*. In production this destroys indexing throughput and then destroys search latency as the segment count climbs. Do not put it in application code.

The reverse move is genuinely useful. Bulk-loading a large corpus nobody is searching yet? A one-second refresh is pure waste:

```json
{ "index.refresh_interval": -1, "index.number_of_replicas": 0 }
```

Load, then restore both. Three- to fivefold speedups are routine.

### Merging, and the sawtooth

Every refresh makes a segment. A thousand seconds of indexing makes a thousand segments, and **every query must consult every segment**, so latency climbs steadily. Background **merges** combine small segments into larger ones — and, as §2.3 showed, this is also when deleted documents are finally purged and their space returned.

Merging is I/O- and CPU-hungry and it runs *concurrently with your indexing*. That is the usual answer to a question that puzzles people: *why does our indexing throughput rise and fall in a sawtooth rather than staying flat?* It is competing with merges. On spinning disks this is severe enough to be a design constraint; solid-state storage is not really optional for a write-heavy search cluster.

`_forcemerge` is the manual override, merging a shard down to a chosen number of segments.

- On a **finished** index — a completed time-series index, the output of a reindex, anything read-only — merging to one segment is a genuine search speedup and reclaims all deleted space.
- On an **actively written** index it is harmful: you create one enormous segment that future merges must repeatedly rewrite.

---

## §2.7 What happens when you search

A search fans out to every relevant shard, in two phases. Understanding the two phases explains two important behaviours.

```
PHASE 1 — QUERY
  The coordinator sends the query to one copy of every shard
  (primary or replica, whichever is less busy).
  Each shard runs it locally and returns the top `size`
  document IDs with their scores — NOT the documents.
  The coordinator merges those lists into one global top `size`.

PHASE 2 — FETCH
  The coordinator asks the relevant shards for the _source of
  just those documents, and returns them to the client.
```

Why split it? With 20 shards and `size=10`, phase 1 moves 200 tiny `(id, score)` pairs, and phase 2 moves exactly 10 full documents. The naive alternative ships 200 *full documents* across the network in order to throw 190 away.

### Consequence one: deep pagination is a trap

`from=0&size=10` — each of 20 shards returns its local top 10, the coordinator merges 200 candidates and keeps 10. Cheap.

`from=100000&size=10` — page ten thousand. To know the global ranks 100 001 to 100 010, the coordinator needs each shard's top **100 010**, because in principle all of them could come from one shard. So:

```
20 shards × 100 010 scored hits  = 2 000 200 entries built and shipped
coordinator sorts 2 000 200 entries
returns 10
```

The cost grows **linearly with page depth** and **multiplicatively with shard count**. This is why OpenSearch refuses past ten thousand results by default. It is not an arbitrary limit; it is a guardrail at the edge of a cliff.

The correct mechanism is **`search_after`**, a cursor: you pass the sort values of the last hit on the previous page and each shard seeks straight past them.

```json
{ "size": 10,
  "sort": [ { "updated_at": "desc" }, { "_id": "asc" } ],
  "search_after": [ "2026-03-01T00:00:00Z", "d1" ] }
```

Each shard is now asked "give me the first 10 after this point" — constant cost per page, no matter how deep. For exporting an entire result set, the **point-in-time** API (or the older scroll API) holds a consistent snapshot of the index while you page through it.

And the product lesson underneath: nobody goes to page ten thousand. Nobody has ever gone to page ten thousand. If your product needs deep pagination, what it probably needs is better filters.

### Consequence two: relevance is slightly approximate

Each shard is an independent Lucene index with its own statistics (§2.3). Ranking depends on **how rare a term is** — and each shard computes that from *its own* documents.

```
shard 3: "kafka" in 2% of its documents  → treats it as rare    → scores it high
shard 7: "kafka" in 6% of its documents  → treats it as common  → scores it lower
```

Two identical documents on different shards get different scores. With plenty of documents per shard, random assignment makes the local percentages converge on the global one and this is invisible. With *few* documents per shard — a small index split many ways, or a ten-document test fixture — the effect can be large enough to be baffling. It is precisely why your lab index has one shard.

The fix, when you need it, is `search_type=dfs_query_then_fetch`, which adds a preliminary round to collect global term statistics before scoring. It costs an extra round trip and is rarely necessary in production. It is worth knowing about when someone shows you two nearly identical documents with wildly different scores in a small index.

---

## §2.8 Asking good questions

Now the query language. There is a great deal of it; this section gives you the small number of ideas that carry most of the weight.

### Query context vs filter context

The most important distinction in the query DSL, and the easiest performance win available.

- **Query context** asks *how well does this document match?* → it computes a score. **Not cacheable**, because the score depends on this exact query.
- **Filter context** asks *does this document match, yes or no?* → no score. **Cacheable**, as a bitset — one bit per document — reusable by every future query.

The bitset is the point. `status = published` over ten million documents becomes ten million bits, about 1.2 MB, computed once and then reused:

```
docs:    d1 d2 d3 d4 d5 d6 d7 d8 d9 d10
bitset:   1  1  1  1  0  1  1  1  1   1     ← "is this published?"  (d5 is archived)
```

The next query that filters on `published` recomputes nothing; it ANDs against this.

The `bool` query is where you assemble everything. Run this against the lab:

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

- `must` — must match **and** contributes to the score. (Every hit mentions a partition.)
- `filter` — must match, contributes **nothing** to the score, and is cached. (d5 is archived, so it is gone; d3 is from 2024, so it is gone.)
- `should` — optional; boosts the score when it matches. It is the "…and if it also mentions Kafka, rank it higher" clause, and it is why d2, d1, and d4 all sit above d6, whose title is about Spark.
- `must_not` — excludes. (d9, the novelist, is on team `misc`.)

> **The rule, applied mechanically: every yes-or-no constraint belongs in `filter`, never in `must`.**

Status, ownership, date ranges, tenancy, booleans, enumerations. None of them should influence *ranking* — a document is not more relevant for being published, it is merely eligible — and all of them are highly cacheable. Moving a clause from `must` to `filter` is a one-line change that frequently halves query latency.

### The families of query

**Full-text queries** analyse their input, so the query string goes through the same pipeline the document did.

- **`match`** — the workhorse. Analyses your input into terms and finds documents containing *any* of them, or all with `"operator": "and"`.
  ```json
  { "match": { "body": "kafka ordering" } }
  ```
  → terms `[kafka, order]` → documents containing either, ranked by how well (§2.9).
- **`match_phrase`** — the terms must be adjacent and in order, using the positions from §2.2. A `slop` parameter allows some reordering: `"slop": 2` lets the phrase `"kafka ordering"` match *"ordering in Kafka"*.
- **`multi_match`** — the same text against several fields. Its `type` matters more than people expect:
  - `best_fields` (default) — score by the single best-matching field. Right for "find whichever field this matches".
  - `most_fields` — sum across fields. Right when the *same* text is indexed several ways (exact + stemmed + ngrammed).
  - `cross_fields` — treat several fields as one merged field. Right for a name or address spread across `first_name`, `last_name`, `city`, because nobody's query lives entirely in one of them.

**Term-level queries** do **not** analyse their input. They look for the exact bytes you gave them, which is why they belong on `keyword` fields, numbers, and dates: `term`, `terms`, `range`, `exists`, `prefix`, `wildcard`, `regexp`, `fuzzy`, `ids`.

Two warnings about this family.

1. The §2.4 one again: **a `term` query on a `text` field quietly finds nothing.** No error, no hint, zero hits.
2. **`wildcard` with a leading `*`, and `regexp` in general, may have to scan the entire term dictionary.** `*ordering*` cannot binary-search anything — the same problem as `LIKE '%...%'` in §2.1, arriving through a different door. If you need substring matching, pay for it at index time with ngrams (§2.5) rather than at query time.

**Compound queries** shape relevance:

- `function_score` — multiply the score by a function of the document's own fields. The standard use is a **decay on a date**, so fresher documents rank higher without being the only thing that matters.
- `boosting` — **demote** rather than exclude, which is usually what you actually want. ("Rank archived documents lower" beats "hide them", because sometimes the archived one is the answer.)
- `dis_max` — take the *maximum* of the clause scores rather than the sum.
- `rank_feature` — cheap boosting by a numeric field such as popularity.
- `script_score` — arbitrary scoring logic. Powerful, and slow.

And **field boosting**, the cheapest relevance improvement in existence:

```json
"fields": ["title^3", "body"]
```

A title match counts triple. Almost every search application should do this, and a startling number do not.

### Sorting

```json
"sort": [ { "updated_at": "desc" }, "_score", { "_id": "asc" } ]
```

Sorting reads **doc values** (§2.2), not the inverted index — which is why it is fast, and why **you cannot sort on a `text` field**: there are no doc values, because analysed terms are not a single sortable value. Sort on the `.keyword` sub-field instead. Your `documents_v2` mapping has one, so this now works where §2.2's did not:

```bash
curl -s 'localhost:9200/documents_v2/_search?filter_path=hits.hits._id&size=3' -H 'Content-Type: application/json' -d '
{ "sort": [ { "title.keyword": "asc" } ] }'
```

```json
{"hits":{"hits":[{"_id":"d7"},{"_id":"d4"},{"_id":"d1"}]}}
```

**Always end with a unique tie-breaker such as `_id`.** Without one, documents with equal sort values can come back in a different order on each request, and pagination will duplicate some results and skip others — a bug that looks like a ghost:

```
page 1 (sorted by date only):  [A, B, C]   ← B and C have the same date
page 2:                        [C, D, E]   ← C repeated, and something was lost
```

### Aggregations

Aggregations are OpenSearch's analytics half: faceted navigation, dashboards, anything shaped like "how many, grouped by what". They read doc values, they nest arbitrarily, and they come in three families.

- **Bucket** — group documents. `terms` gives the top N values of a field (your "top twenty tags" facet); `date_histogram` gives time buckets and is the backbone of every time-series dashboard; plus `range`, `histogram`, `filters`, `nested`, `composite`.
- **Metric** — compute a number over a bucket: `avg`, `min`, `max`, `sum`, `stats`, `value_count`, `cardinality`, `percentiles`.
- **Pipeline** — operate on the *output* of other aggregations: `derivative`, `moving_avg`, `cumulative_sum`, and `bucket_selector`, which acts like SQL's `HAVING`.

They compose. Count documents per team, and get each team's most recent edit:

```bash
curl -s 'localhost:9200/documents_v2/_search?filter_path=aggregations' -H 'Content-Type: application/json' -d '
{
  "size": 0,
  "aggs": {
    "by_team": {
      "terms": { "field": "team", "size": 10 },
      "aggs": { "latest": { "max": { "field": "updated_at" } } }
    }
  }
}'
```

```json
{"aggregations":{"by_team":{
  "doc_count_error_upper_bound":0,"sum_other_doc_count":0,
  "buckets":[
    {"key":"platform","doc_count":4,"latest":{"value":1.7723232E12,"value_as_string":"2026-03-01T00:00:00.000Z"}},
    {"key":"data","doc_count":3,"latest":{"value":1.7710272E12,"value_as_string":"2026-02-14T00:00:00.000Z"}},
    {"key":"infra","doc_count":2,"latest":{"value":1.7731008E12,"value_as_string":"2026-03-10T00:00:00.000Z"}},
    {"key":"misc","doc_count":1,"latest":{"value":1.5463008E12,"value_as_string":"2019-01-01T00:00:00.000Z"}}
  ]}}}
```

Read it as: bucket by team, top 10, and inside each bucket compute the maximum `updated_at`. `"size": 0` says "no documents, only aggregations" and skips the fetch phase entirely, which is a meaningful saving.

Note the two extra fields in that response — `doc_count_error_upper_bound` and `sum_other_doc_count`. They are zero here, and they are why the next heading exists.

### Three aggregations that lie to you (usefully)

**`terms` is approximate.** Each shard computes *its own* top N and ships it; the coordinator merges. So picture a value that ranks **eleventh** on every one of twenty shards:

```
shard 1  top-10: ... (k8s is #11, with 900 docs)
shard 2  top-10: ... (k8s is #11, with 900 docs)
...
shard 20 top-10: ... (k8s is #11, with 900 docs)

k8s appears in NO shard's response
→ the coordinator never sees it
→ yet globally it has 18 000 documents, and is probably the true #1
```

`doc_count_error_upper_bound` quantifies how wrong the counts could be, and `shard_size` lets you ask each shard for more candidates — say each shard's top 100 to return a global top 10 — trading memory for accuracy. On your one-shard lab the error is exactly zero, which is worth seeing once so that you know what the field is *for*.

**`cardinality`, a distinct count, is approximate.** It uses HyperLogLog++, which estimates the number of distinct values in *constant* memory with roughly one to two percent error. The exact alternative means shipping every distinct value from twenty shards to one coordinator; for a field with fifty million distinct values that is not a query, it is an outage.

**`percentiles` is approximate**, using a structure called t-digest that is cleverly built to be most accurate at the extremes — the p99 — which is exactly where you care.

And here is the principle, which generalises far beyond OpenSearch:

> **At scale, exactness is often unaffordable, and approximation is a legitimate engineering choice rather than a failure.**

Nobody needs to know there were exactly 8 431 947 distinct users rather than "about 8.4 million". What matters is that the approximation is **explicit**, **bounded**, and **understood** — so that nobody builds a billing system on top of a HyperLogLog estimate. Chapter 3 makes the same trade for the same reason with approximate nearest-neighbour search.

**A cost warning.** Aggregations build structures per bucket, per shard, in heap. A deeply nested `terms` aggregation over high-cardinality fields is the single most reliable way to trigger a circuit breaker or an out-of-memory error: 10 000 users × 1 000 URLs × 100 days is a billion buckets, all in memory, all at once. There is a `search.max_buckets` limit for exactly this reason, and when you hit it the right response is to rethink the query, not to raise the limit.

---

## §2.9 Relevance: what "best" means

We can now find every matching document. Which one goes first?

### The wall

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

Two things to notice. **d9 — the novelist — is tied for first.** And d5 scored lower than the rest for a reason you cannot see. The rest of this section is about where those numbers came from, and what you can do about them.

### What a score is, and is not

Every hit returns a `_score`, a positive float. It is **relative**: meaningful only within the result set of one query. A score of 14.2 does not mean "very relevant". It means "more relevant than the document scoring 9.8, *for this same query*".

This trips up product requirements constantly. You **cannot** write "only show results scoring above 5" and expect sane behaviour, because the scale shifts with query length, term rarity, and corpus statistics:

```
query "kafka"                      → top score  0.69   (one term)
query "kafka consumer group lag"   → top score  4.84   (four terms, scores summed)
```

A threshold of 2 would return everything for the second query and nothing for the first — and the query did not get seven times better, it got three words longer.

The corpus moves the scale too. In a real corpus "the" is in every document, so IDF drives its score to nearly zero. In your ten-document lab "the" appears in exactly two titles, so it is *rare*, and it scores higher than "kafka" does. Same word, same engine, opposite verdict — because a score is a statement about a corpus, not about a document. If you need a notion of "good enough", normalise within the result set, train a model, or use a cross-encoder (§3.7). Not a magic constant.

### The intuition: three signals

Long before any formula, three observations about what makes a document relevant to a term.

**1. Term frequency — more is better.** A document mentioning "kubernetes" eight times is more likely to be *about* Kubernetes than one mentioning it once in a footnote.

**2. Inverse document frequency — rarer is more valuable.** A term appearing in few documents is enormously more informative than one appearing everywhere. Matching "kubernetes" tells you something; matching "the" tells you nothing.

Lucene's BM25 computes it as `IDF = ln(1 + (N − n + 0.5) / (n + 0.5))`, where `N` is the total number of documents and `n` is the number containing the term. With your ten-document corpus:

```
"kafka"  in  5 docs → ln(1 + (10−5+0.5)/(5+0.5))    = ln(2.00) = 0.693
"spark"  in  2 docs → ln(1 + (10−2+0.5)/(2+0.5))    = ln(4.40) = 1.482
a term   in 10 docs → ln(1 + (10−10+0.5)/(10+0.5))  = ln(1.05) = 0.047
```

Matching "spark" is worth **twice** as much as matching "kafka" and **thirty times** as much as matching a term that is in every document. Notice what this settles: aggressive stopword removal is unnecessary, because **IDF already weights "the" to nearly zero.** The scoring function solved the problem that stopword lists were invented for.

**3. Field length — shorter is stronger evidence.** Matching "java" in a three-word title is far stronger evidence than matching it once in a five-thousand-word document, where it may be incidental.

Combine the three and you have TF-IDF, which served the field for decades.

### BM25, and the two things it fixes

Modern OpenSearch uses **BM25**: the same three signals, each handled more carefully. Here is the formula, which I show once and then tell you not to memorise:

```
                          f(t,d) · (k₁ + 1)
score(q,d) = Σ  IDF(t) · ─────────────────────────────────
            t∈q           f(t,d) + k₁ · (1 − b + b·|d|/avgdl)
```

`f(t,d)` is how often term t appears in document d; `|d|` is the field's length in terms; `avgdl` is the average field length across the corpus; `k₁` and `b` are constants defaulting to 1.2 and 0.75.

What matters is the two problems it solves, and the arithmetic is where the insight lives.

**Problem one: term frequency must saturate.**

Under plain TF-IDF, a document containing "kubernetes" a hundred times scores a hundred times higher than one containing it once. Which means keyword stuffing wins, and a page that is nothing but the word "kubernetes" repeated outranks the actual documentation.

Watch BM25's fraction as `f` grows (taking `|d| = avgdl`, so the length term is 1, and `k₁ = 1.2`):

```
f = 1:    1 × 2.2 / (1 + 1.2)   = 1.000
f = 2:    2 × 2.2 / (2 + 1.2)   = 1.375      ← +0.375
f = 3:    3 × 2.2 / (3 + 1.2)   = 1.571      ← +0.196
f = 5:    5 × 2.2 / (5 + 1.2)   = 1.774
f = 20:  20 × 2.2 / (20 + 1.2)  = 2.075
f = 21:  21 × 2.2 / (21 + 1.2)  = 2.081      ← +0.006
f = ∞:                          → 2.200      ← hard ceiling = k₁ + 1
```

Numerator and denominator both grow with `f`, so the ratio climbs toward a ceiling of `k₁ + 1` and never passes it. Going from **1 to 2** occurrences gains 0.375. Going from **20 to 21** gains 0.006 — sixty times less. Which matches human judgement exactly: after a few mentions you are convinced, and further repetition carries no information. `k₁` controls how fast it saturates.

**Problem two: length normalisation needs a dial.**

Penalising long documents is right in general, but overdo it and a genuinely comprehensive forty-page guide loses to a one-line stub. `b` interpolates. With `avgdl = 100` words and one occurrence:

```
title,     |d| = 10:    norm = 1 − 0.75 + 0.75×(10/100)  = 0.325
                        component = 2.2/(1 + 1.2×0.325)  = 1.583

average,   |d| = 100:   norm = 1.0
                        component = 2.2/(1 + 1.2×1.0)    = 1.000

long page, |d| = 1000:  norm = 1 − 0.75 + 0.75×10        = 7.75
                        component = 2.2/(1 + 1.2×7.75)   = 0.214
```

One mention in a short title is worth **seven times** one mention in a long page. At `b = 0` length is ignored entirely; at `b = 1` scores are fully normalised by length; 0.75 is a tuned compromise. Lowering `b` is occasionally right for fields where length carries no meaning — a list of tags, say, where having more tags should not dilute each one.

**And now the lab numbers make sense.** Titles in your corpus average 3.9 terms. d1, d2, d4, and d9 each have four-term titles containing "kafka" once; d5 ("The Kafka log is append only") has six:

```
IDF("kafka") = 0.693,  k₁ = 1.2,  b = 0.75,  avgdl = 3.9

d1  |d|=4:  0.693 × 2.2 / (1 + 1.2×(0.25 + 0.75×4/3.9))  = 0.686
d5  |d|=6:  0.693 × 2.2 / (1 + 1.2×(0.25 + 0.75×6/3.9))  = 0.568
```

d5 is not less about Kafka. It just has a longer title, and BM25 reads length as dilution. Every number in that result set is now accounted for — including the fact that d9, about a novelist, is tied for first, because BM25 knows about words and nothing else.

In practice, tuning `k₁` and `b` is a **small** effect. Field boosting, good analysers, and query structure matter far more, and you should exhaust those before touching the similarity parameters.

### Finding out why

Three endpoints eliminate guesswork entirely.

**`"explain": true`** on a search returns, for every hit, a full recursive breakdown of how its score was computed:

```bash
curl -s 'localhost:9200/documents_v2/_search?filter_path=hits.hits._explanation' -H 'Content-Type: application/json' -d '
{ "query": { "match": { "title": "kafka" } }, "size": 1, "explain": true }'
```

```json
{"hits":{"hits":[{"_explanation":{
  "value":0.6859519,"description":"weight(title:kafka in 0) [PerFieldSimilarity], result of:",
  "details":[{"value":0.6859519,"description":"score(freq=1.0), computed as boost * idf * tf from:",
    "details":[
      {"value":2.2,"description":"boost"},
      {"value":0.6931472,"description":"idf, computed as log(1 + (N - n + 0.5) / (n + 0.5)) from:",
        "details":[{"value":5,"description":"n, number of documents containing term"},
                   {"value":10,"description":"N, total number of documents with field"}]},
      {"value":0.44982696,"description":"tf, computed as freq / (freq + k1 * (1 - b + b * dl / avgdl)) from:",
        "details":[{"value":1.0,"description":"freq, occurrences of term within document"},
                   {"value":1.2,"description":"k1, term saturation parameter"},
                   {"value":0.75,"description":"b, length normalization parameter"},
                   {"value":4.0,"description":"dl, length of field"},
                   {"value":3.9,"description":"avgdl, average length of field"}]}]}]}}]}}
```

Every term of the formula, labelled, with the actual numbers. It is extremely verbose and completely definitive — and it is the same arithmetic you just did by hand.

**`GET /documents_v2/_explain/{id}`** with a query body answers the more focused question: why did *this specific document* score what it did — or, crucially, **why did it not match at all**. When someone says "this document should be the top result and it isn't even in the list", this is the endpoint that answers.

**`"profile": true`** gives a per-shard timing breakdown of query execution. That one is for performance rather than relevance, but it lives in the same toolbox.

### Getting better relevance, roughly by effort-to-reward

1. **Boost your fields.** `"title^3"`. One line of configuration.
2. **Index the same text several ways** — exact, stemmed, ngrammed — as multi-fields, and combine them with `multi_match` and `most_fields`. Exact matches then naturally outrank stemmed ones, because they match *more sub-fields*, which is a lovely property to get for free.
3. **Add a phrase boost.** Put a `match_phrase` clause in `should`. Documents where the query words actually appear adjacent get lifted above documents that merely contain them all somewhere.
4. **Decay by recency or popularity** — `function_score` with a `gauss` decay, or the cheaper `rank_feature`. For Lantern, a wiki page edited last week is probably more useful than an equally relevant one last touched in 2017.
5. **Learn to rank.** OpenSearch has a Learning to Rank plugin that takes your top N results and reranks them with a trained gradient-boosted model over features you define — BM25 score, recency, click-through rate, author authority. A real step up in quality, and a real step up in machinery.
6. **Add semantic search.** That is Chapter 3, and it is where the largest modern gains are.

Here are the first three, together, on the lab. Watch d9 fall:

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
  {"_id":"d9","_score":2.0578558},
  {"_id":"d4","_score":2.0578558},
  {"_id":"d5","_score":1.7040696}
]}}
```

d1 and d2 now sit far above d9 — a factor of three, where a moment ago it was a four-way tie for first. "ordering" stems to `order`, which d9 does not contain at all, and the title boost multiplies the gap. The novelist did not stop matching. He stopped *winning*, which is the only thing that matters in a ranked list.

### Measuring it, which you must do first

I have listed six ways to improve relevance, and here is the part to be emphatic about:

> **Without measurement, you cannot tell whether any of them helped.**

Relevance work without evaluation is not engineering. It is a sequence of plausible-sounding changes whose net effect is unknown and quite possibly negative. And it is genuinely unknowable by intuition: boosting titles threefold helps some queries and hurts others, and no amount of staring at one example tells you the balance.

So build a **judgment list**: a set of queries and, for each, which documents are relevant.

```
query "kafka ordering"     → relevant: d1 (perfect), d2 (good), d5 (acceptable)
query "spark partitions"   → relevant: d6 (perfect), d3 (good)
```

Mine it from click logs, have humans rate a few hundred query-document pairs, or generate it synthetically with a language model. Then measure:

- **Precision@k** — of the top k results, what fraction were relevant? (Top 10, six relevant → 0.6.)
- **Recall@k** — of all the relevant documents that exist, what fraction appeared in the top k? (Twenty exist, six in the top 10 → 0.3.)
- **MRR**, mean reciprocal rank — one divided by the position of the *first* relevant result, averaged over queries. First relevant at position 3 → 0.333. The right metric when there is essentially one correct answer.
- **nDCG@k**, normalised discounted cumulative gain — handles *graded* relevance (perfect / good / acceptable / bad) and discounts by position, so putting the best result first is rewarded and burying it at rank 9 is penalised. **The standard headline metric for ranked retrieval, and the one to report.**

OpenSearch's **Rank Evaluation API**, `_rank_eval`, computes these against a judgment set for you:

```bash
curl -s 'localhost:9200/documents_v2/_rank_eval' -H 'Content-Type: application/json' -d '
{
  "requests": [
    { "id": "kafka_ordering",
      "request": { "query": { "multi_match": { "query": "kafka ordering",
                                               "fields": ["title^3","body"] } } },
      "ratings": [ { "_index": "documents_v2", "_id": "d1", "rating": 3 },
                   { "_index": "documents_v2", "_id": "d2", "rating": 2 },
                   { "_index": "documents_v2", "_id": "d9", "rating": 0 } ] }
  ],
  "metric": { "dcg": { "k": 10, "normalize": true } }
}'
```

```json
{"metric_score":1.0,"details":{"kafka_ordering":{"metric_score":1.0,
  "unrated_docs":[{"_id":"d4"},{"_id":"d5"}],
  "metric_details":{"dcg":{"dcg":8.892789,"ideal_dcg":8.892789,
                           "normalized_dcg":1.0,"unrated_docs":2}}}}}
```

One number: 1.0, because for this query the ranking is already the ideal one — d1 (rating 3) above d2 (rating 2) above d9 (rating 0). Note `unrated_docs`, which is the honest part of the report: two returned documents were never judged, so the metric is only as good as your judgment list is complete.

Now change the boost from `title^3` to `title^10`, run it again, and you find out whether you helped — across two hundred queries rather than the one you happened to be looking at. Wire it into continuous integration so that a relevance regression **fails a build** rather than surfacing three weeks later in a support ticket. This is the single most valuable piece of infrastructure in a search project and routinely the last thing teams build.

---

## §2.10 Living with an index

Three operational patterns carry most of the day-to-day work.

### Bulk indexing

One HTTP request per document is slow, for all the reasons §1.9 gave. The `_bulk` API batches them:

```
POST /_bulk
{ "index":  { "_index": "documents", "_id": "d-1001" } }
{ "title": "Kafka partitions", "body": "..." }
{ "update": { "_index": "documents", "_id": "d-1002" } }
{ "doc": { "status": "published" } }
{ "delete": { "_index": "documents", "_id": "d-1003" } }
```

Newline-delimited JSON: one action line, one source line, repeated — with a trailing newline the API genuinely requires. (The odd format exists so the server can split the payload without parsing all of it first.)

Rules for using it well:

- **Size by bytes, not by document count** — aim for five to fifteen megabytes per request. A thousand tiny documents and a thousand huge ones are completely different requests; only the byte size predicts behaviour.
- **A few concurrent bulk threads** rather than one giant serial stream. Two to four per data node is a reasonable starting point.
- **Retry `429 Too Many Requests` with backoff.** That response is not an error — it is OpenSearch's write queue applying backpressure exactly as §1.6 recommends. The correct response is to slow down, not to page someone.

Then the rule I want to state as strongly as I can, because ignoring it is the most common data-loss bug in indexing pipelines:

> **A `200 OK` on a bulk request does not mean the documents were indexed.**

The bulk API returns 200 as long as the *request* was processed. Individual items inside it can fail — a mapping conflict, a version conflict, a rejected write — each with its own status. Prove it to yourself: send one good document and one with a date where a date cannot go.

```bash
curl -s -X POST 'localhost:9200/_bulk?refresh=wait_for' -H 'Content-Type: application/x-ndjson' --data-binary '
{"index":{"_index":"documents_v2","_id":"ok"}}
{"title":"A fine document","updated_at":"2026-01-01"}
{"index":{"_index":"documents_v2","_id":"bad"}}
{"title":"A broken document","updated_at":"last tuesday"}
'
```

```
HTTP/1.1 200 OK
```
```json
{ "took": 8,
  "errors": true,                          ← the flag you must check
  "items": [
    { "index": { "_id": "ok",  "status": 201 } },
    { "index": { "_id": "bad", "status": 400,
                 "error": { "type": "mapper_parsing_exception",
                            "reason": "failed to parse field [updated_at] of type [date] in
                                       document with id 'bad'. Preview of field's value:
                                       'last tuesday'" } } }
  ] }
```

Two hundred OK, and one of your documents is gone. **You must check `errors` and the per-item results.** A pipeline that checks only the HTTP status will lose documents silently and indefinitely, while looking completely healthy on every dashboard you own.

Finally, connecting to §1.7: **use a deterministic `_id`** — the document's own identifier from the source system.

```
with _id = "d-1001":   a retry after a timeout → overwrites. Same document. Fine.
with a generated _id:  a retry after a timeout → a second copy. Forever.
```

Deterministic IDs make your entire indexing pipeline **idempotent for free**. Random IDs accumulate duplicates that are very hard to find and remove later.

### Aliases, and one rule

An **alias** is a name that points at one or more indexes. It is a small feature, and it is load-bearing for everything else in this section.

You have two indexes in the lab right now: the original `documents` and the better-analysed `documents_v2`. Point an alias at the new one:

```bash
curl -s -X POST localhost:9200/_aliases -H 'Content-Type: application/json' -d '
{ "actions": [ { "add": { "index": "documents_v2", "alias": "search" } } ] }'

curl -s 'localhost:9200/search/_search?filter_path=hits.total' -H 'Content-Type: application/json' -d '
{ "query": { "match": { "title": "ordering" } } }'
```

```json
{"hits":{"total":{"value":2,"relation":"eq"}}}
```

The client asked `search`; the request was served by `documents_v2`. Now do the swap that matters, in one request:

```json
POST /_aliases
{ "actions": [
    { "remove": { "index": "documents_v1", "alias": "documents" } },
    { "add":    { "index": "documents_v2", "alias": "documents" } }
]}
```

Both actions applied **atomically**. There is no instant at which the alias points at both or at neither. Every in-flight query resolves to one index or the other, and the switch is invisible to users.

> **Your application should never reference a concrete index name. Only ever an alias.**

Follow that and every schema change, every reindex, every model upgrade becomes a zero-downtime alias swap with an instant rollback. Ignore it and each of them becomes a coordinated deployment with a maintenance window. The cost of compliance is typing `documents` instead of `documents_v1` on day one.

Aliases do more: a **filtered alias** carries a built-in filter, so you can expose a per-team view of a shared index; a **routing alias** pins queries to a shard; and `is_write_index` designates which index of a group receives writes, which is what makes automatic rollover work.

### Reindexing

Sooner or later you must change something immutable — a field's type, an analyser, the primary shard count. All of these mean building a new index and copying the data. You already did one in §2.5; here is the production form:

```json
POST /_reindex?wait_for_completion=false&slices=auto
{
  "source": { "index": "documents_v1" },
  "dest":   { "index": "documents_v2" }
}
```

`slices=auto` parallelises the copy by shard, which is a large speedup. `wait_for_completion=false` returns a task ID immediately so that a six-hour reindex does not sit on an HTTP connection; you poll `GET _tasks/<id>`. A `script` can transform documents in flight, and a remote `source` can pull from an entirely different cluster.

The zero-downtime pattern, worth learning as one unit:

1. Create `documents_v2` with the new mapping.
2. Write new changes to **both** indexes — or, better, make sure your indexing pipeline can be **replayed** from its source, which Chapter 5 will make easy.
3. Reindex the historical data from v1 to v2.
4. **Verify**: compare document counts, spot-check a sample, and run your `_rank_eval` judgment set against *both* to confirm relevance did not regress.
5. Swap the alias atomically.
6. Keep v1 for a rollback window, then delete it.

**Step 4 is the one people skip and the one that matters.** An alias swap makes rollback trivial — but only if you notice there is a problem.

Two relatives are worth knowing. **`_update_by_query`** reindexes documents in place, which is how you apply a *new analyser* to existing data without a new index (the mapping did not change, only how the text is processed). **`_delete_by_query`** removes matching documents — remembering from §2.3 that this writes tombstones, and the space comes back at merge time, not immediately.

### Templates and lifecycles

For time-series data — logs, metrics, events — three features combine into the standard pattern.

- An **index template** automatically applies settings, mappings, and aliases to any new index matching a name pattern, so `logs-2026-09-17` is born correctly configured without anyone doing anything.
- **Rollover** creates the next index and moves the write alias when the current one gets too large or too old.
- **ISM**, Index State Management, is the lifecycle engine: after seven days reduce replicas and force-merge; after thirty days move to cheaper warm nodes; after ninety days snapshot and delete.

Together they maintain a rolling window of data at bounded cost with no human in the loop.

**Snapshots** round this out: incremental backups to S3 at the segment level, restorable into the same or a different cluster. And, as §4.6 will insist: **replication is not backup.** Replication faithfully replicates your accidental `_delete_by_query` to every copy, instantly.

---

## §2.11 When it goes wrong

### Reading cluster health

```bash
curl -s 'localhost:9200/_cluster/health?pretty'
```

```json
{ "cluster_name" : "docker-cluster",
  "status" : "green",
  "number_of_nodes" : 1,
  "number_of_data_nodes" : 1,
  "active_primary_shards" : 4,
  "active_shards" : 4,
  "relocating_shards" : 0,
  "initializing_shards" : 0,
  "unassigned_shards" : 0,
  "active_shards_percent_as_number" : 100.0 }
```

The colours mean something precise.

**Green** — every primary and every replica is assigned and active. Everything is fine.

**Yellow** — every primary is assigned, at least one replica is not. All your data is present and serving normally, but **redundancy is gone**: if the wrong node dies now, you lose data. On a single-node development cluster this is permanent and harmless — a replica cannot sit on its primary's node and there is nowhere else — which is why the lab set `number_of_replicas: 0`. In production it means *fix this today*.

**Red** — at least one **primary** is unassigned. Some of your data is unavailable right now. Searches return **partial results**, possibly without your application noticing, and writes to the affected shard fail. This is an outage.

That "possibly without noticing" deserves a beat: a red cluster does not error, it quietly returns fewer results. Check `_shards.failed` in your search responses, which is present in every response you have run in this chapter.

Diagnostic endpoints:

```
GET _cluster/allocation/explain      ← why is this shard unassigned? START HERE.
GET _cat/indices?v&health=red
GET _cat/shards?v&s=state
GET _cat/nodes?v&h=name,heap.percent,disk.used_percent,cpu,load_1m
GET _nodes/stats/thread_pool         ← look for non-zero "rejected"
GET _tasks?detailed                  ← what is running right now
```

`allocation/explain` is the one to remember — it answers in prose, and the answer is usually one of:

- a **disk watermark** was crossed. OpenSearch stops allocating at **85%**, stops moving shards in at **90%**, and makes indexes **read-only at 95%** — the **flood stage**, which surprises people badly, because writes start failing and disk usage is not where they were looking;
- a node left the cluster and has not returned;
- allocation filtering rules exclude every eligible node;
- there are too many shards per node;
- a shard is corrupt and needs `_cluster/reroute?retry_failed`.

### Sizing, with the reasoning (which outlives the numbers)

**Ten to fifty gigabytes per shard.** Below ten you pay per-shard overhead — heap, file handles, a separate query execution — for very little data. Above fifty, recovery and rebalancing get painful, because moving a shard means moving *all of it*, and a 200 GB shard takes a long time to cross a network while the cluster sits degraded.

**Roughly twenty shards per gigabyte of heap, per node.** A node with 30 GB of heap manages about six hundred shards. Fewer is better.

**JVM heap at most 31 GB, and at most half the machine's RAM.** Both halves need explaining:

- The **31 GB ceiling** is a genuine cliff, not a guideline. Above roughly 32 GB the JVM can no longer use *compressed ordinary object pointers*; it switches to full 64-bit references, every pointer doubles in size, and you end up with **less usable heap from more memory**. A 32 GB heap holds less than a 31 GB one.
- The **half-of-RAM rule** exists because Lucene's speed comes from the **filesystem cache**. Segments are memory-mapped files, and the operating system keeping them in free RAM is what makes search fast. Give all your memory to the JVM and you starve the thing that actually matters. (Kafka wants the same thing for the same reason — §5.3.)

And then the mistake I have seen more than any other: **oversharding.**

It is so tempting. Shards are how you scale, so more shards must be more scalable. But every shard is a complete Lucene index with fixed overhead, and every query fans out to **all** of them, paying §1.3's straggler tax on each. A query against five hundred tiny shards is dramatically slower and more expensive than the same query against ten correctly sized ones.

```
primaries = ceil(expected_total_GB / 30)
```

then round to a multiple of your data node count so the shards distribute evenly — and **resist the urge to round up "just in case".**

Replicas are the easy half: at least one in production, always. And since the replica count *can* be changed live, you can add replicas to increase read throughput whenever you need to.

### A troubleshooting playbook

**Searches are slow.**
Run the query with `"profile": true` and find which clause dominates — this usually ends the investigation immediately. Then, in order:

- Are the yes-or-no conditions in `filter`, where they can be cached?
- Is something paginating deeply?
- Are there `wildcard`, `regexp`, or `script` queries that should be precomputed at index time?
- Are you fanning out to more shards than necessary — could routing make a common query hit just one?
- **Is the working set larger than the filesystem cache**, so that every query goes to disk? That last one is very often the real answer, and the fix is more RAM or faster storage rather than anything clever.

Turn on the **slow log** (`index.search.slowlog.threshold.query.warn: 5s`) so that the actual offenders identify themselves instead of being guessed at.

**Indexing is slow.**
Check bulk request size and concurrency, and look for `429` rejections indicating you are already at capacity. Check whether something is forcing refreshes — a stray `?refresh=true` in application code is a classic. Look at merge activity, because merges compete with indexing for I/O. Consider dropping replicas to zero during a large backfill. And look for **dynamic mapping updates firing on every document**, which serialise through the cluster manager and throttle everything.

**Memory problems.**
A `CircuitBreakingException` means a request wanted more heap than it was allowed. **The circuit breaker is protecting you**; the correct response is to fix the query, not to raise the limit.

Long garbage-collection pauses in the logs matter for the reason §1.9 gave: a node in a two-second GC pause is **indistinguishable from a dead node**. So it is removed from the cluster, its shards reallocate, and then it comes back and everything reallocates again. A cluster can thrash like this for hours. The root cause is usually too many shards, fielddata on a text field, or an unbounded aggregation.

---

## §2.12 Where Lantern stands

We have built something real. Here it is, and every phrase should now decode.

Lantern has an OpenSearch cluster. Two hundred million documents live in an index called `documents_v1`, behind an alias called `documents`. There are twelve primary shards of roughly twenty-five gigabytes each with one replica apiece, spread over six data nodes, with three small dedicated cluster-manager nodes holding the election quorum.

Each document's text is indexed as `text` with a custom analyser — HTML stripped, lowercased, accent-folded, stemmed — and simultaneously as `keyword` through a multi-field, so that the same field can be searched, sorted, and aggregated. Status, team, and timestamp are keyword and date fields used exclusively in `filter` clauses, where they are cached as bitsets. Queries are `multi_match` across title and body with the title boosted threefold, plus a `should` phrase clause for adjacency and a `function_score` recency decay. A judgment set of two hundred queries runs in CI and fails the build if nDCG@10 drops.

Documents are written through the `_bulk` API in ten-megabyte batches, keyed by the source system's document ID so that retries overwrite rather than duplicate, with per-item error checking. A document edited in PostgreSQL becomes searchable about a second later, because of the refresh interval, and everybody involved knows that number and monitors it.

Annotated, so that nothing is decoration:

- *twelve primary shards of ~25 GB* — inside the 10–50 GB window (§2.11), and 12 divides evenly across 6 data nodes.
- *behind an alias* — the §2.10 rule, so every future migration is an atomic swap.
- *three dedicated cluster-manager nodes* — a quorum of three tolerating one failure, isolated from search traffic (§2.3).
- *`text` plus `keyword` multi-field* — so the same field can be searched *and* sorted or aggregated (§2.4, and the error at the end of §2.2).
- *filters used exclusively in `filter` clauses* — no scoring, cached bitsets (§2.8).
- *`title^3`, a `should` phrase clause, a recency decay* — improvements 1, 3, and 4 from §2.9.
- *a judgment set in CI* — the thing teams build last, built first (§2.9).
- *`_bulk`, deterministic IDs, per-item error checks* — every rule from §2.10.
- *searchable about a second later* — the refresh interval (§2.6), understood and monitored rather than discovered in a bug report.

It works. Searches return in forty milliseconds at the median and a hundred and eighty at the ninety-ninth percentile. A node can die without anyone noticing.

### And the wall it hits

A user types *"why does my deployment keep restarting"*. The document that answers it is d7, **"Diagnosing CrashLoopBackOff"**.

Run the intersection from §2.2 by hand:

```
query terms:     [why, doe, my, deploy, keep, restart]
document terms:  [diagnos, crashloopbackoff]

intersection:    ∅
```

**Empty.** Not ranked low — *not a candidate at all*. The postings lists do not touch. Confirm it in the lab:

```bash
curl -s 'localhost:9200/documents_v2/_search?filter_path=hits.total' -H 'Content-Type: application/json' -d '
{ "query": { "multi_match": { "query": "why does my deployment keep restarting",
                              "fields": ["title^3","body"] } } }'
```

```json
{"hits":{"total":{"value":0,"relation":"eq"}}}
```

Zero. And no amount of stemming, synonyms, or boosting fixes this, because those operate on words and this is not a word problem. "CrashLoopBackOff" *means* "keeps restarting", and the inverted index has no representation of meaning whatsoever. It is a machine for matching strings that happen to be words.

The same happens for *"kubernetes pod memory limits"* against a document titled *"k8s container resource constraints"*: zero shared terms, so a document that fully answers the question ranks below several that merely say "Kubernetes" a lot.

You could add a synonym: `k8s → kubernetes`. And another: `pod → container`. And `limits → constraints`. And then a thousand more, and you still will not have covered how people phrase things, because the space of phrasings is not enumerable. Every tool in this chapter papers over the gap one word at a time, and one word at a time does not scale.

To close it, you have to stop indexing words and start indexing something else. Which is Chapter 3, and it begins with a very strange idea.

---

## The one-page summary

**The core mechanism.** An inverted index maps terms to documents. Queries become intersections of sorted lists, which is why search is fast without scanning anything. Doc values are the mirror structure — documents to values — and they are why sorting and aggregating work, and why they do *not* work on `text` fields.

**The distributed layer.** An index is split into shards, and each shard is a complete, independent Lucene index. Searching means scatter-gather over all of them. That single fact explains score variation between shards, approximate aggregations, and the straggler tax.

**The write path.** Routing is `hash(id) % primaries`, which is why the primary count is frozen at creation. Refresh (1 s, visibility) ≠ flush (durability of segments) ≠ translog fsync (crash safety of the acknowledgement). Segments are immutable; an update is a new copy plus a tombstone; merges do the real cleanup.

**The query language.** Score-bearing clauses go in `must` and `should`; yes-or-no constraints go in `filter`, where they are cached. Full-text queries analyse their input, term-level queries do not. Analysis must produce compatible terms on both sides, and `_analyze` is how you see it.

**Relevance.** BM25 = saturating term frequency × inverse document frequency × length normalisation. Scores are relative, never absolute. Field boosting is the cheapest win available. And nothing counts as an improvement until a judgment set says it is.

**Operations.** Always query through an alias. Always use deterministic IDs and check per-item bulk errors. Size shards at 10–50 GB and resist oversharding. Heap at most 31 GB and at most half of RAM.

**The limit.** All of it matches words. Users ask about meaning. That gap is Chapter 3.

---

## Glossary of terms this chapter assumes

| Term | Meaning |
|---|---|
| **term** | one indexed token — the atomic unit of matching |
| **term dictionary** | the sorted list of all terms in a shard |
| **postings list** | the sorted list of documents containing a term |
| **tf** | term frequency — occurrences of a term in one document |
| **IDF** | inverse document frequency — how rare a term is; rarer scores higher |
| **analysis** | the pipeline turning a string into terms; runs at index *and* query time |
| **tokenizer** | the stage that splits text into tokens (exactly one per analyzer) |
| **token filter** | a stage that transforms the token stream (lowercase, stem, fold…) |
| **stemming** | reducing words to a root form (`running` → `run`) |
| **precision** | of what I returned, how much was relevant |
| **recall** | of everything relevant, how much did I return |
| **doc values** | the columnar document-to-value structure used for sorting and aggregating |
| **segment** | an immutable file of indexed data inside a shard |
| **merge** | background combination of segments; also when deletes are really applied |
| **refresh** | making recent writes *visible* to search (default 1 s) |
| **flush** | fsyncing segments to disk |
| **translog** | the write-ahead log that makes an acknowledged write crash-safe |
| **shard** | a slice of an index, and a complete Lucene index in its own right |
| **primary / replica** | the write-accepting copy / an exact copy elsewhere for reads and failover |
| **routing** | `hash(id) % primaries`, deciding which shard owns a document |
| **coordinating node** | whichever node received your request and fans it out |
| **mapping** | the schema: field types and how each is analysed and stored |
| **multi-field** | one JSON field indexed several ways (e.g. `title` and `title.keyword`) |
| **alias** | a name pointing at indexes; the thing your application should always query |
| **nDCG** | the standard position-discounted, graded-relevance ranking metric |
