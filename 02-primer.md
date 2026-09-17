# Chapter 2, Explained From Zero

*A companion to `02-finding-things.md` for a reader who has never worked with a search engine. Read a section here, then the matching section of the chapter.*

Throughout I use one tiny corpus so that every idea lands on something concrete. Ten documents in Lantern's wiki:

```
d1:  "Kafka partitions are ordered"
d2:  "Ordered delivery in Kafka"
d3:  "Spark partitions and tasks"
d4:  "Kafka consumer groups rebalance"
d5:  "The Kafka log is append only"
d6:  "Tuning Spark shuffle partitions"
d7:  "Diagnosing CrashLoopBackOff"
d8:  "Kubernetes pod memory limits"
d9:  "The novelist Franz Kafka"
d10: "Postgres replication basics"
```

Ten documents instead of two hundred million, but every mechanism in the chapter is visible at this size.

---

## Part 0 — What problem is this chapter even solving?

You have used a database. You ask it a question, it gives you rows. That is **retrieval**: you state a precise condition, and every row either satisfies it or doesn't. `WHERE user_id = 42` has exactly one right answer, and the database's job is to produce it quickly.

**Search** is a different job wearing similar clothes. A user types `kafka ordering` into a box. There is no precise condition here. There is a vague intention, expressed in words that may not appear in the document that best answers it, and there are probably thousands of documents that are *sort of* about it. The system's job is not to decide who matches. It is to **rank**: to put the single most useful document first.

Three differences follow, and the whole chapter falls out of them:

| | Database retrieval | Search |
|---|---|---|
| The question | a precise predicate | a fuzzy intention |
| The answer | the set of matching rows | an *ordered* list, best first |
| "Correct" means | exactly the matching rows | the user found what they wanted |

That last row is the uncomfortable one. Search has no provably correct answer. That is why §2.9 spends so long on *measuring* relevance — when there is no proof, you need evidence.

---

## §2.1 — Why the database can't do this

The chapter opens with:

```sql
SELECT * FROM documents WHERE body LIKE '%kafka%';
```

Four things are wrong. Let me make each one physical.

### It is slow, and an index won't save you

You know an index makes queries fast. Here is why *this* query cannot use one.

A database B-tree index is a sorted structure, like a phone book sorted by surname. Sorted by the **beginning** of the value:

```
"Diagnosing CrashLoopBackOff"
"Kafka consumer groups rebalance"
"Kafka partitions are ordered"
"Ordered delivery in Kafka"
"Postgres replication basics"
```

Ask `LIKE 'Kafka%'` — starts with "Kafka" — and the index works beautifully. Binary-search to the K's, read forward, stop. You skipped 99% of the data because sorting told you where to look.

Now ask `LIKE '%kafka%'` — contains "kafka" *anywhere*. Look at the list: `"Ordered delivery in Kafka"` matches, and it is sorted under O. `"The novelist Franz Kafka"` matches, and it is sorted under T. The matches are scattered uniformly through the sort order. Sorting tells you **nothing** about where they are, so there is nothing to skip. The database reads all forty terabytes. That is a full table scan, and no index of this kind can prevent it, because the index is sorted by the wrong thing.

Hold on to that phrase — *sorted by the wrong thing*. The inverted index in §2.2 is precisely the structure sorted by the right thing.

### It is wrong, four times

Run the SQL mentally against the corpus:

1. **Case.** `'%kafka%'` misses d1, d2, d5, d9 — every one of them capitalises it. You can fix this with `LOWER(body) LIKE '%kafka%'`, but now you must remember to lowercase in two places forever, and one day someone won't.
2. **Word forms.** Searching `streaming` misses a document that says `streams`. To the database these are two unrelated strings. To a human they are the same idea.
3. **Multiple words.** `'%kafka ordering%'` matches **nothing** in the corpus — no document contains that exact character sequence. But d1 and d2 are exactly what the user wanted. The user typed two concepts; SQL heard one literal string.
4. **Spurious matches.** `'%kafka%'` cheerfully returns d9, about the novelist. It matches. It is useless.

### And the deepest problem: no notion of "best"

Suppose you fixed all of that and got back d1, d2, d4, d5, d9 — five matches (eleven thousand, at real scale). In what order?

SQL has no answer. `WHERE` is a **predicate**: it returns true or false. There is no expression you can write that says "d1 is more about Kafka ordering than d5 is". Relevance is not a property of a row; it is a property of the *relationship* between a row and a query, and it is a number, not a boolean.

> This is the sentence to take from §2.1: **search needs a data structure that ranks, and a database index is built to filter.** Different jobs, different structures.

---

## §2.2 — The inverted index

### The inversion, concretely

A database stores, in effect, a forward map:

```
d1 → "Kafka partitions are ordered"
d2 → "Ordered delivery in Kafka"
d3 → "Spark partitions and tasks"
```

*Document → its words.* Perfect for "show me document 2". Useless for "who mentions Kafka?" — you'd read everything.

Turn the arrow around. For each word, write down which documents contain it:

```
and         → [3]
append      → [5]
are         → [1]
consumer    → [4]
delivery    → [2]
groups      → [4]
in          → [2]
is          → [5]
kafka       → [1, 2, 4, 5, 9]
log         → [5]
only        → [5]
ordered     → [1, 2]
partitions  → [1, 3, 6]
rebalance   → [4]
shuffle     → [6]
spark       → [3, 6]
the         → [5, 9]
tuning      → [6]
```

That's it. That's an inverted index. The left column is the **term dictionary** (sorted, so you can binary-search it); each right-hand list is a **postings list**.

You have met this structure before without noticing: it is the index at the back of a textbook. "Kafka ... 12, 47, 88." Nobody reads a book cover to cover to find the Kafka pages. Thirteenth-century monks built exactly this by hand for the Bible and called it a concordance.

### Why this makes queries fast

**One term.** "Who mentions kafka?" → binary-search the dictionary to `kafka`, read the list: `[1, 2, 4, 5, 9]`. You never opened a single document. The cost depends on the length of that list, not on the size of the corpus. Three documents or three billion, the lookup is the same shape.

**Two terms, AND.** "kafka AND partitions" is the **intersection** of two sorted lists:

```
kafka       → [1, 2, 4, 5, 9]
partitions  → [1, 3, 6]
```

Because both lists are *sorted*, you walk them with two fingers, always advancing whichever finger is behind:

```
finger A on 1, finger B on 1   → equal! emit 1, advance both
finger A on 2, finger B on 3   → 2 < 3, advance A
finger A on 4, finger B on 3   → 3 < 4, advance B
finger A on 4, finger B on 6   → 4 < 6, advance A
finger A on 5, finger B on 6   → 5 < 6, advance A
finger A on 9, finger B on 6   → 6 < 9, advance B
finger B runs out              → done

result: [1]
```

Each step advances a finger, so the work is bounded by the total length of the lists — and real engines skip ahead rather than stepping one at a time, so it is closer to the length of the *shorter* list. This is why a five-word query over a billion documents returns in milliseconds. **Nothing is being scanned.** Five precomputed lists are being intersected.

This is the single most important mechanical idea in the chapter. Everything else is refinement.

### What's actually in a postings entry

Real postings carry more than a document number. Two extras matter:

**Term frequency (tf)** — how many times the term appears in that document. Needed for ranking (§2.9): a document saying "kafka" eight times is more likely to be *about* Kafka.

**Positions** — where in the document, as word offsets.

```
kafka → [ d1 (tf=1, pos=[0]), d2 (tf=1, pos=[3]), d4 (tf=1, pos=[0]), ... ]
```

Positions are what make **phrase search** possible. Take `"ordered delivery"` against d2, `"Ordered delivery in Kafka"`:

```
ordered  in d2 at position [0]
delivery in d2 at position [1]
```

Intersect to find documents with both words (d2 qualifies), then check the positions: is there a position of `delivery` exactly one more than a position of `ordered`? `1 = 0 + 1`. Yes — d2 contains the phrase.

Now d1, `"Kafka partitions are ordered"`: it has `ordered` at position 3 but no `delivery` at all, so it drops out at the intersection step. And a document saying *"delivery was ordered"* would survive the intersection but fail the position check — `ordered` at 2, `delivery` at 0, and 0 ≠ 3. Correctly rejected.

That extra position check is why `match_phrase` costs more than `match`, and why it is still fast: the expensive filtering already happened.

### Doc values: the other direction

The inverted index answers **"given a term, which documents?"** superbly.

Now ask the opposite: **"given document 4, what is its `updated_at`?"** You need this to *sort* results by date, or to *average* a numeric field, or to count documents per team.

Try it with an inverted index. You'd have to walk every date in the dictionary asking "is doc 4 in your list?" until one says yes. Hopeless — the structure is oriented the wrong way.

So engines build a **second** structure at the same time, called **doc values**: for each field, every document's value laid out contiguously in document order.

```
doc values for `team`:
  doc:    1        2       3        4        5
  value:  platform search  platform  data    platform
```

To sort ten thousand results by team, you read this array at ten thousand offsets. Contiguous, columnar, memory-mapped — very fast.

```
inverted index:   term → [documents]     "who has this value?"
doc values:       document → value       "what is this document's value?"
```

**Two structures, opposite orientations, both built when you index.** Keep this pair in your head, because the chapter's most confusing rules are just consequences of it:

- You *search* with the inverted index.
- You *sort and aggregate* with doc values.
- A `text` field has no useful doc values (it was shredded into terms — there is no single value to store), which is why **you cannot sort or aggregate on a `text` field**. That rule in §2.4 is not arbitrary. There is literally no data there to read.

---

## §2.3 — The shape of the system

### What OpenSearch is

- **Lucene** is a Java *library* that implements everything in §2.2 — term dictionary, postings, doc values, scoring. It runs in one process, on one machine, over one set of files. It has no notion of a network.
- **OpenSearch** wraps Lucene in a distributed system: an HTTP API, splitting data across machines, replication, failure handling.

So: *Lucene does the searching; OpenSearch does the distributing.* When something confuses you, ask which layer owns it. Scoring is Lucene. Shard placement is OpenSearch.

(OpenSearch is a 2021 fork of Elasticsearch over a licence change, which is why Elasticsearch documentation applies almost verbatim.)

### The nesting, with our corpus

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
{ "_id": "d-1001",
  "title": "Kafka partitions are ordered",
  "body": "Within a partition, Kafka guarantees...",
  "team": "platform",
  "updated_at": "2026-03-01" }
```

The original JSON is kept verbatim in `_source` so it can be returned to you. Don't disable it — without it you can't reindex, can't use the update API, and can't see what you stored.

**Index** — a named collection sharing a schema. "Like a table" is roughly right and slightly misleading: an index is much more expensive than a table. Each one costs heap, file handles, and cluster metadata, so "one index per customer" across ten thousand customers is a known way to kill a cluster.

**Shard** — here's the idea that took me longest to internalise, so let me be blunt about it:

> **A shard is not a piece of a search engine. A shard IS a search engine.**

Split the ten documents across three shards:

```
shard 0: d1, d4, d7, d10
shard 1: d2, d5, d8
shard 2: d3, d6, d9
```

Shard 2 has its *own* term dictionary, its *own* postings lists, its own doc values, its own internal document numbering starting at 0. It does not know shards 0 and 1 exist. Hand it a query and it will answer completely and correctly — about its own four documents.

Three consequences drop straight out of this one fact, and the chapter returns to all three:

1. **Searching an index means searching every shard and merging.** That's the scatter-gather of §1.3, with its straggler problem: the query is as slow as the slowest shard.
2. **Scores differ slightly between shards** (§2.7), because "how rare is this term?" is computed per shard, from local documents only.
3. **`terms` aggregations are approximate** (§2.8), for the same reason — each shard only knows its own counts.

**Primaries and replicas.** A **primary** shard accepts writes. A **replica** is an exact copy on a *different* node; it serves reads and gets promoted if the primary's node dies.

```
3 primaries, 1 replica each = 6 shards on 3 nodes

    Node A            Node B            Node C
  ┌──────────┐     ┌──────────┐     ┌──────────┐
  │ P0       │     │ P1       │     │ P2       │
  │ R2       │     │ R0       │     │ R1       │
  └──────────┘     └──────────┘     └──────────┘
```

Note that no shard shares a node with its own copy — the allocator enforces this, because a copy on the same machine protects against nothing. Kill node A: P0 is gone, but R0 on node B is promoted, and the cluster keeps serving. It then quietly builds a fresh replica of shard 0 somewhere.

And the rule that shapes the rest of the chapter:

> **Primary count is fixed at index creation. Replica count can change any time.**

§2.6 explains why (it's a modulus). §2.10 is mostly about living with it.

### Segments and immutability

Inside a shard, Lucene writes **segments** — files that, once written, are **never modified**.

So what happens on an update? You index a new version of d4:

```
segment_1:  d4 (version 1)   ← still physically there
segment_7:  d4 (version 2)   ← new
deletes:    "segment_1 doc 3 is deleted"   ← tiny auxiliary file
```

Both copies are on disk. Every search consults the deletion list and skips the old one. The old bytes are reclaimed only later, when a background **merge** combines small segments into a bigger one and simply declines to copy the dead documents forward. Deletes work identically — a tombstone now, real removal at merge.

Why accept this weirdness? Immutable files:

- need **no locking** — any number of threads can read concurrently, coordinating about nothing;
- can be **cached without invalidation**, because a cached copy can never go stale;
- can be **copied to another node** for recovery without pausing anything;
- are written **purely sequentially**, the fastest thing a disk does.

The price: space comes back late, segment count must be managed, and merges burn real I/O and CPU in the background. Almost every performance oddity in §2.11 traces back to segments.

### Node roles

- **Cluster manager** (called *master* in older docs): keeps the cluster state — which indexes exist, their mappings, where every shard lives. Holds no data, answers no queries. Elected by quorum, hence *three* dedicated nodes: three survive one failure, and a fourth adds nothing. It sounds like paperwork right up until it wobbles, at which point nothing works.
- **Data nodes**: hold shards, do the indexing and searching. The expensive ones.
- **Coordinating node**: not a configuration, a *role in a request*. Whichever node receives your query fans it out, merges the responses, and replies. In big clusters you dedicate nodes to this so merging fifty shards' results doesn't steal CPU from searching.
- **Ingest nodes**, and **warm**/**cold** tiers for cheap storage of old data.

### The allocator

A background loop on the cluster manager, continuously trying to satisfy: every primary assigned; every replica on a different node from its primary; disk balanced; shard counts balanced; awareness rules respected ("spread copies across availability zones"). Node vanishes → promote replicas, rebuild missing copies. Node added → move shards onto it. Both move real bytes over the network and take real time, which is one reason §2.11 wants shards in the tens of gigabytes, not hundreds.

---

## §2.4 — Mappings (the schema)

A **mapping** declares, per field: what type, how to analyse it, whether it's searchable, sortable, aggregatable.

### Dynamic mapping, and two ways it bites

Index a document with a field OpenSearch has never seen, and it guesses a type and **permanently** adds it. Lovely in development. Two production failure modes:

**The first document decides, forever.** A field whose first value is `"12345"` — a string — becomes `text`. Now every later numeric value is coerced or rejected, `range` queries misbehave, and the fix is a full reindex.

**Mapping explosion.** Index a bag of user-supplied keys:

```json
{ "properties": { "utm_campaign_spring_2026": "x", "ab_test_4471": "b" } }
```

Dynamic mapping adds a field for **every distinct key it ever sees**. Ten thousand campaigns → ten thousand fields. The mapping lives in the cluster state, the cluster state is replicated to every node on every change, the cluster manager starts drowning, and the cluster becomes unstable. It has a name because it happens constantly.

The defence: define mappings explicitly in an index template, then set

- `"dynamic": "strict"` — reject documents with unknown fields, or
- `"dynamic": false` — keep them in `_source`, don't index them.

For anything user-supplied, pick one.

### The types worth knowing

- **Numbers**: `long`, `integer`, `short`, `byte`, `double`, `float`, `half_float`, `scaled_float` (fixed-point — the right pick for prices). Smallest that fits; it directly shrinks the index.
- **Time**: `date`, stored internally as epoch milliseconds.
- **Structure**: `object` (flattened to dotted paths like `author.name`) and `nested` (below).
- **Geo**: `geo_point`, `geo_shape`.
- **Vectors**: `knn_vector` — the door to Chapter 3.
- And the two everyone gets wrong: `text` and `keyword`.

### `text` vs `keyword` — the mistake everybody makes

- **`text`** is **analysed**: chopped into terms, lowercased, stemmed; each term indexed separately.
- **`keyword`** is **not analysed**: the whole value becomes exactly one term, byte for byte.

Take the title `"Senior Java Engineer"`.

**As `text`**, the index receives three terms:

```
senior → [d]     java → [d]     engineer → [d]
```

- Search `java engineer` → **matches** (both terms present).
- Search `Java` → **matches** (the query is lowercased the same way the document was).
- Exact lookup for `"Senior Java Engineer"` → **no match**. There is no term equal to that string. It was taken apart and the whole was never stored.

**As `keyword`**, the index receives one term:

```
Senior Java Engineer → [d]
```

- Exact lookup → **matches**.
- Sort by it → works (it has doc values — one value per document).
- Aggregate "top 10 job titles" → works.
- Search `java` → **no match**. The only term is the full string, and `java ≠ Senior Java Engineer`.

Neither is right. *Which you want depends on the query, not on the field.* So have both, via a **multi-field**:

```json
"title": {
  "type": "text",
  "fields": {
    "keyword": { "type": "keyword", "ignore_above": 256 }
  }
}
```

One field in your JSON, two in the index. Search `title`; sort and aggregate on `title.keyword`. This is exactly what dynamic mapping gives you for free — which is why things often work *before* you write an explicit mapping and break *after*: you removed a multi-field you didn't know was there.

> **Diagnostic shortcut, worth memorising: a search returns nothing and you're sure it should match → check whether you ran a `term` query against a `text` field.** That one mistake explains a remarkable share of "OpenSearch is broken" reports.

### The nested-object trap

This one produces **silently wrong answers**, not errors, which is why it deserves the space.

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
- does it contain an `edits` value > 100? Yes (120).
- → **match.**

But it's wrong. Priya made 3 edits. The two facts came from different objects and the index couldn't tell. Nothing errors; you may not notice for a year.

The fix is `"type": "nested"`, which stores each sub-object as its own hidden document, preserving the correlation, queried with a `nested` query. It costs real money — more hidden documents, slower queries, a cap on how many — so use it when you need the correlation and not otherwise. But *know which one you need*, because the failure mode is silence.

### Other controls

- `index: false` — keep it in `_source`, don't make it searchable. Saves space on display-only fields.
- `doc_values: false` — no columnar structure. Saves space on fields you never sort or aggregate.
- `copy_to` — duplicate several fields into one catch-all, giving a cheap "search everything" target.
- `ignore_above` — on a keyword, silently skip indexing values longer than N characters, so a thousand-character string never becomes a term.

And the rule behind all of §2.10:

> **You cannot change an existing field's type.** The inverted index and doc values were built to the old type; there's no reinterpreting them. Changing a mapping means a new index and a copy. Which is why aliases exist.

---

## §2.5 — Analysis: how text becomes terms

We've said `text` fields are "analysed". Here is what that actually means, and it's where most of a search engine's quality lives.

**Analysis** is the pipeline that turns a string into terms. The crucial fact:

> It runs **twice** — at index time on the document, and at query time on the query string — and both must produce **compatible terms**, because matching happens on terms and nothing else.

Index `"Kafka"`, lowercase it to `kafka`. Query `"Kafka"` without lowercasing, and you look up the term `Kafka`. `Kafka ≠ kafka`. Zero results, while you stare at the document and swear it's there. Nearly every "why doesn't this match" bug is this, in some costume.

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

**The tokenizer** does the splitting, and there is exactly one.

- `standard` — Unicode word-boundary rules. Right for essentially all prose.
- `whitespace` — split on spaces only, so `WiFi-router` stays one token.
- `keyword` — emit the whole input as one token. This is literally how a `keyword` field behaves.

Two more deserve attention:

**`ngram`** emits every substring in a length range. `java` with min=2, max=4 becomes:

```
ja, av, va, jav, ava, java
```

Six terms for one word. Now a search for `av` finds it — substring matching, and some typo tolerance. The cost is an index several times larger, because every word explodes. Use deliberately, on small fields.

**`edge_ngram`** emits only **prefixes**. `java` becomes:

```
j, ja, jav, java
```

This is the machinery for **autocomplete**. Index `"java"` this way, and when the user has typed `jav`, that prefix is a real term in the index and the lookup is instant.

But here's the part people get wrong, and it's worth slowing down for. Analysis runs at query time too — so if you use `edge_ngram` on *both* sides, the query `java` becomes `[j, ja, jav, java]`, and the term `j` matches **every word starting with j** in the corpus: javascript, jenkins, jira, jupyter. Your autocomplete returns nonsense.

The fix is to analyse the two sides *differently*:

```json
"title_ac": {
  "type": "text",
  "analyzer":        "edge_ngram_analyzer",   ← index time: all prefixes
  "search_analyzer": "standard"               ← query time: the word as typed
}
```

Index side produces `j, ja, jav, java`; query side produces just `jav`; `jav` matches. This is *the* canonical reason `search_analyzer` exists.

### Token filters

- **`lowercase`** — always want it.
- **`asciifolding`** — `café → cafe`, `Zürich → Zurich`. Enormous for names and international corpora, because users don't type accents.
- **`stop`** — removes "the", "a", "of". This mattered when disks were small and is now mostly a mistake, for two reasons. BM25 (§2.9) already gives near-zero weight to words that appear everywhere, so it solves nothing; and it **breaks phrase queries**. Search `"to be or not to be"` in a stopword-stripped index and you are searching for the empty set. Same for "The Who", "The The", and a surprising number of product names. Leave stopwords in unless you have a specific reason.
- **`stemmer`** — reduce to a root: `running`, `runs`, `ran` → `run`. This raises **recall** (you find more of the relevant documents) at some cost to **precision** (you also find some irrelevant ones) — an aggressive stemmer collapses `universal` and `university` both to `univers`. They come in strengths: `porter_stem` is enthusiastic, `minimal_english` and `kstem` are gentler. If users report obviously wrong words in results, suspect the stemmer.

  *(Recall and precision, since the chapter uses them freely: **precision** = of what I returned, how much was relevant. **Recall** = of everything relevant, how much did I return. Pushing one usually drags the other down.)*

- **`synonym` / `synonym_graph`** — make `js`, `javascript`, `ecmascript` interchangeable. There's a placement decision worth understanding:
  - **Index time**: free at query time, but changing the list requires reindexing the whole corpus, *and* expanding synonyms into the index distorts the term statistics that ranking depends on (suddenly `javascript` appears in far more documents than it really does, so IDF drops it).
  - **Query time** (`synonym_graph` in a `search_analyzer`): costs a little per query, editable whenever you like.

  Prefer query time. The operational flexibility is worth far more than the microseconds.

### Building one, and then testing it

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

And now the most useful endpoint in the product:

```json
POST /documents/_analyze
{ "analyzer": "lantern_text", "text": "Running the Café's servers" }

→ [running→run, the, cafe's→cafe, servers→server]
```

That's all it does: shows you the exact terms that would be written. It converts an invisible process into something you can look at.

**The debugging recipe** — mechanical, works nearly every time. A document *should* match and doesn't:

1. `_analyze` the document's text.
2. `_analyze` the query's text.
3. Put the two lists side by side.

```
document terms:  [kubernet, pod, memori, limit]
query terms:     [Kubernetes, pods, memory, limits]
                  ^^^^ not lowercased, not stemmed — no overlap at all
```

There's your bug: the query used a different analyzer. In the overwhelming majority of cases the mismatch is visible instantly — a stemmer on one side only, a stopword removed, a case difference, an accent, a hyphen split differently than you assumed. You stop guessing.

---

## §2.6 — What happens when you write

### Routing

```
1. Client sends the document to any node → that node is the coordinator.

2. The coordinator computes which shard owns it:

       shard = hash(routing) % number_of_primary_shards

   where `routing` defaults to the document's _id.

3. It forwards the document to the node holding that primary shard.

4. The primary writes it into an in-memory buffer,
   and appends the operation to the TRANSLOG.

5. The primary forwards it to all in-sync replicas, in parallel.

6. Replicas acknowledge → the primary acknowledges to the client.
```

**Step 2 explains why primaries are immutable.** Work it through. With 12 primaries, suppose `hash("d-1001") = 90211`:

```
90211 % 12 = 7    → document d-1001 lives on shard 7
```

Now grow to 13 shards:

```
90211 % 13 = 4    → the formula now says shard 4
```

The document is physically still on shard 7. A `GET /documents/d-1001` computes 4, asks shard 4, and shard 4 has never heard of it. **The document is not gone, it is unfindable** — and this happens to nearly every document at once, because changing the divisor reshuffles almost everything. That's §1.3's modulus problem, and it's why the primary count is frozen at creation and why §2.10 has a whole apparatus of reindexing and aliases.

**Step 6 is worth noticing too.** OpenSearch replicates **synchronously**: when you get a success, the document is on the primary *and* its in-sync replicas, not queued somewhere hopeful. That's the durable side of the §1.4 trade-off, and it's why indexing latency rises when one replica gets slow. (`wait_for_active_shards` controls how many copies must be *available* before the write is even attempted.)

### Three different meanings of "written to disk"

This is where most people's mental model is wrong. Three operations, three schedules, three different guarantees. Do not blur them.

| Operation | Default schedule | What it guarantees |
|---|---|---|
| **Refresh** | every **1 second** | documents become **visible to search** |
| **Flush** | ~every 30 min, or translog > 512 MB | segments `fsync`ed → **durable in segment form** |
| **Translog fsync** | **every request** | the acknowledged write is **crash-safe** |

**Refresh** turns the in-memory buffer into a new segment and opens it for searching. Note what it does *not* promise: the segment may still live only in the filesystem cache. Refresh is about **visibility**, not durability.

**Flush** `fsync`s segments to physical disk and starts a fresh translog. Durability, in segment form.

**Translog fsync** (`index.translog.durability: request`) forces the operation log to disk *before* acknowledging your write. This is the write-ahead log of §1.6 doing exactly its usual job: if the machine loses power one millisecond after the ack, the document is recoverable from the log even though no segment exists yet. You can relax it to `async` (every 5s) for real throughput gains and a 5-second window of possible loss on a hard crash.

From which follows the most surprising property in OpenSearch:

> **A document that has been successfully indexed — acknowledged, durable, crash-safe — is not searchable for up to one second.**

It's on disk. It will survive a power cut. And a search won't find it, because the *segment* it lives in hasn't been created yet. OpenSearch is **near real-time**, not real-time. Deliberately: creating a segment isn't free, and doing it once a second instead of once per document is the difference between thousands of writes per second and dozens.

This catches everybody, always in the same test:

```
index d-1001    → 200 OK
search "kafka"  → 0 hits          ← "the database is broken!"
```

- **Right fix in a test**: `?refresh=wait_for` — your request blocks until the next scheduled refresh happens.
- **Wrong fix**: `?refresh=true` — forces an immediate refresh, creating a tiny segment for *every single write*. In production this destroys indexing throughput, and then destroys search latency as the segment count climbs. Never put it in application code.

The reverse move is genuinely useful. Bulk-loading a large corpus that nobody is searching yet? A one-second refresh is pure waste:

```json
{ "index.refresh_interval": -1, "index.number_of_replicas": 0 }
```

Load, then restore both. Three- to fivefold speedups are routine.

### Merging and the sawtooth

Every refresh makes a segment. A thousand seconds of indexing makes a thousand segments, and **every query must consult every segment**, so latency climbs steadily. Background **merges** combine small segments into larger ones — and, per §2.3, this is also when deleted documents are finally purged and space returned.

Merging is I/O- and CPU-hungry and runs *concurrently with your indexing*. That's the usual answer to "why does our indexing throughput rise and fall in a sawtooth instead of staying flat?" — it's competing with merges. On spinning disks this is severe enough to be a design constraint; SSDs are not really optional for a write-heavy cluster.

`_forcemerge` is the manual override, merging down to a chosen segment count.

- On a **finished** index — a completed time-series index, the output of a reindex, anything read-only — merging to one segment is a genuine speedup and reclaims all deleted space.
- On an **actively written** index it's harmful: you create one enormous segment that future merges must repeatedly rewrite.

---

## §2.7 — What happens when you search

Two phases, and each explains an important behaviour.

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

Why split it? With 20 shards and `size=10`: phase 1 moves 200 tiny (id, score) pairs; phase 2 moves exactly 10 full documents. The naive alternative ships 200 *full documents* across the network to throw 190 away.

### Consequence one: deep pagination is a trap

`from=0&size=10` — each of 20 shards returns its local top 10, the coordinator merges 200 candidates and keeps 10. Cheap.

`from=100000&size=10` — page ten thousand. To know the global ranks 100 001–100 010, the coordinator needs each shard's top **100 010**, because in principle all of them could come from one shard. So:

```
20 shards × 100 010 scored hits  = 2 000 200 entries built and shipped
coordinator sorts 2 000 200 entries
returns 10
```

Cost grows **linearly with page depth** and **multiplicatively with shard count**. This is why OpenSearch refuses past 10 000 results by default. Not an arbitrary limit — a guardrail at the edge of a cliff.

The right mechanism is **`search_after`**, a cursor: you pass the sort values of the last hit on the previous page, and each shard seeks straight past them.

```json
{ "size": 10,
  "sort": [ { "updated_at": "desc" }, { "_id": "asc" } ],
  "search_after": [ "2026-03-01T10:00:00Z", "d-4471" ] }
```

Each shard says "give me the first 10 after this point" — constant cost per page, no matter how deep. For exporting an entire result set, the **point-in-time** API (or the older scroll API) holds a consistent snapshot while you page.

And the product lesson: nobody goes to page ten thousand. Nobody has ever gone to page ten thousand. If your product needs deep pagination, what it probably needs is better filters.

### Consequence two: relevance is slightly approximate

Each shard is an independent Lucene index with its own statistics (§2.3). Ranking depends on **how rare a term is** — and each shard computes that from *its own* documents.

```
shard 3: "kafka" in 2% of its documents  → treats it as rare     → scores high
shard 7: "kafka" in 6% of its documents  → treats it as common   → scores lower
```

Two identical documents on different shards get different scores. With plenty of documents per shard, random assignment makes the local percentages converge on the global one and this is invisible. With *few* documents per shard — a small index split many ways, or a test fixture with twelve documents — it can be large enough to be baffling.

The fix, if you need it, is `search_type=dfs_query_then_fetch`, which adds a preliminary round to collect global term statistics before scoring. An extra round trip; rarely necessary in production. Worth knowing when someone shows you two near-identical documents with wildly different scores in a small index.

---

## §2.8 — Asking good questions

### Query context vs filter context

The most important distinction in the query language, and the easiest performance win available.

- **Query context** asks *how well does this document match?* → produces a score. **Not cacheable**, because the score depends on this exact query.
- **Filter context** asks *does this match, yes or no?* → no score. **Cacheable**, as a bitset — one bit per document — and reusable across every future query.

The bitset is the point. `status = published` over ten million documents becomes ten million bits ≈ 1.2 MB, computed once and then reused:

```
docs:    1 2 3 4 5 6 7 8 9 ...
bitset:  1 0 1 1 0 1 0 1 1      ← "is this published?"
```

Next query that filters on `published` doesn't recompute anything; it ANDs against this.

The `bool` query is where you assemble everything:

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

- `must` — must match **and** contributes to the score.
- `filter` — must match, contributes **nothing** to the score, cached.
- `should` — optional; boosts the score when it matches. The "…and if it also mentions Kafka, rank it higher" clause.
- `must_not` — excludes.

> **The rule, applied mechanically: every yes-or-no constraint belongs in `filter`, never in `must`.**

Status, ownership, date ranges, tenancy, booleans, enumerations. None of them should influence *ranking* — a document isn't more relevant for being published, it's just eligible — and all of them are highly cacheable. Moving them is a one-line change that frequently halves query latency.

### The families of query

**Full-text queries** analyse their input, so the query goes through the same pipeline the document did.

- **`match`** — the workhorse. Analyses your input into terms and finds documents containing *any* of them (or all, with `"operator": "and"`).
  ```json
  { "match": { "body": "kafka ordering" } }
  ```
  → terms `[kafka, order]` → documents with either, ranked by how well (§2.9).
- **`match_phrase`** — the terms must be adjacent and in order, using the positions from §2.2. `slop` allows some reordering: `"slop": 2` lets `"kafka ordering"` match *"ordering in Kafka"*.
- **`multi_match`** — same text against several fields. Its `type` matters more than people expect:
  - `best_fields` (default) — score by the single best-matching field. Right for "find whichever field this matches".
  - `most_fields` — sum across fields. Right when the *same* text is indexed several ways (exact + stemmed + ngram).
  - `cross_fields` — treat several fields as one merged field. Right for a name or address spread across `first_name`, `last_name`, `city` — nobody's query lives entirely in one of them.

**Term-level queries** do **not** analyse their input. They look for the exact bytes you gave them, which is why they belong on `keyword` fields, numbers, and dates: `term`, `terms`, `range`, `exists`, `prefix`, `wildcard`, `regexp`, `fuzzy`, `ids`.

Two warnings:

1. The §2.4 one again: **a `term` query on a `text` field quietly finds nothing.** `{"term": {"title": "Senior Java Engineer"}}` against an analysed title returns zero, no error, no hint.
2. **`wildcard` with a leading `*`, and `regexp` generally, may scan the entire term dictionary.** `*ordering*` can't binary-search anything — same problem as `LIKE '%...%'` in §2.1, arriving by a different door. If you need substring matching, pay for it at index time with ngrams (§2.5), not at query time.

**Compound queries** shape relevance:

- `function_score` — multiply the score by a function of the document's own fields. The standard use is a **decay on a date**, so fresher documents rank higher without excluding older ones.
- `boosting` — **demote** rather than exclude, which is usually what you actually want. ("Rank archived docs lower" beats "hide them", because sometimes the archived one is the answer.)
- `dis_max` — take the *maximum* of clause scores rather than the sum.
- `rank_feature` — cheap boosting by a numeric field like popularity.
- `script_score` — arbitrary scoring logic. Powerful; slow.

And **field boosting**, the cheapest relevance improvement that exists:

```json
"fields": ["title^3", "body"]
```

A title match counts triple. Almost every search application should do this, and a startling number don't.

### Sorting

```json
"sort": [ { "updated_at": "desc" }, "_score", { "_id": "asc" } ]
```

Sorting reads **doc values** (§2.2), not the inverted index — which is why it's fast, and why **you cannot sort on a `text` field**: there are no doc values, because the analysed terms aren't a single sortable value. Sort on `.keyword` instead.

**Always end with a unique tie-breaker like `_id`.** Without it, documents with equal sort values can come back in a different order on each request, and pagination will duplicate some results and skip others — a bug that looks like a ghost:

```
page 1 (sorted by date only):  [A, B, C]   ← B and C tie
page 2:                        [C, D, E]   ← C repeated, something lost
```

### Aggregations

The analytics half of OpenSearch: faceted navigation, dashboards, anything shaped like "how many, grouped by what". They read doc values, they nest arbitrarily, three families:

- **Bucket** — group documents. `terms` (top N values of a field — your "top twenty tags" facet), `date_histogram` (time buckets — the backbone of every time-series dashboard), plus `range`, `histogram`, `filters`, `nested`, `composite`.
- **Metric** — compute a number over a bucket: `avg`, `min`, `max`, `sum`, `stats`, `value_count`, `cardinality`, `percentiles`.
- **Pipeline** — operate on the *output* of other aggregations: `derivative`, `moving_avg`, `cumulative_sum`, and `bucket_selector` (a SQL `HAVING`).

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

Read it as: bucket by team (top 10), and inside each bucket compute the average word count and a monthly histogram. `"size": 0` says "no documents, only aggregations" — it skips the fetch phase entirely and is a meaningful saving.

### Three aggregations that lie to you (usefully)

**`terms` is approximate.** Each shard computes *its own* top N and ships it; the coordinator merges. So picture a value ranking **eleventh** on every one of 20 shards:

```
shard 1  top-10: ... (k8s is #11, 900 docs)
shard 2  top-10: ... (k8s is #11, 900 docs)
...
shard 20 top-10: ... (k8s is #11, 900 docs)

k8s appears in NO shard's response
→ the coordinator never sees it
→ yet globally it has 18 000 documents, likely the true #1
```

The response carries `doc_count_error_upper_bound` quantifying how wrong the counts could be, and `shard_size` lets you ask each shard for more candidates (say top 100 to return top 10) — less error, more memory.

**`cardinality` (distinct count) is approximate.** It uses HyperLogLog++, which estimates distinct values in *constant* memory with roughly 1–2% error. The exact alternative means shipping every distinct value from 20 shards to one coordinator; for a field with 50 million distinct values that isn't a query, it's an outage.

**`percentiles` is approximate**, via t-digest, cleverly built to be most accurate at the extremes — the p99 — which is exactly where you care.

And the principle, which generalises well beyond OpenSearch:

> **At scale, exactness is often unaffordable, and approximation is a legitimate engineering choice rather than a failure.**

Nobody needs 8 431 947 distinct users rather than "about 8.4 million". What matters is that the approximation is **explicit**, **bounded**, and **understood** — so nobody builds a billing system on a HyperLogLog estimate. Chapter 3 makes the same trade for the same reason with approximate nearest-neighbour search.

**A cost warning.** Aggregations build structures per bucket, per shard, in heap. A deeply nested `terms` aggregation over high-cardinality fields is the most reliable way to trigger a circuit breaker or an OOM: 10 000 users × 1 000 URLs × 100 days is a billion buckets, all in memory, all at once. There's a `search.max_buckets` limit for exactly this, and when you hit it the right response is to rethink the query, not raise the limit.

---

## §2.9 — Relevance: what "best" means

We can find eleven thousand matching documents. Which one goes first?

### What a score is, and is not

Every hit returns a `_score`, a positive float. It is **relative**: meaningful only *within the result set of one query*. A score of 14.2 doesn't mean "very relevant". It means "more relevant than the document scoring 9.8, for this same query".

The practical consequence trips up product requirements constantly. You **cannot** write "only show results scoring above 5" and expect sane behaviour, because the scale shifts with query length, term rarity, and corpus statistics:

```
query "kafka"                       → top score  8.1
query "kafka consumer group lag"    → top score 31.4   (four terms, summed)
query "the"                         → top score  0.04  (matches everything)
```

A threshold of 5 would return everything for the second query and nothing for the third. If you need "good enough", normalise within the result set, train a model, or use a cross-encoder (§3.7). Not a magic constant.

### The intuition: three signals

Long before any formula, three observations about what makes a document relevant to a term:

**1. Term frequency (TF) — more is better.** A document mentioning "kubernetes" eight times is more likely to be *about* Kubernetes than one mentioning it once in a footnote.

**2. Inverse document frequency (IDF) — rarer is more valuable.** A term appearing in few documents is enormously more informative than one appearing everywhere. Matching "kubernetes" tells you something. Matching "the" tells you nothing.

Let's compute it. Lucene's BM25 uses `IDF = ln(1 + (N − n + 0.5) / (n + 0.5))`, where N is total documents and n is documents containing the term. With N = 10:

```
"kafka"  in 5 docs  → ln(1 + (10−5+0.5)/(5+0.5))  = ln(2.00) = 0.69
"spark"  in 2 docs  → ln(1 + (10−2+0.5)/(2+0.5))  = ln(4.40) = 1.48
"the"    in 10 docs → ln(1 + (10−10+0.5)/(10+0.5))= ln(1.05) = 0.05
```

Matching "spark" is worth **twice** as much as matching "kafka", and **thirty times** as much as matching "the". Notice what this settles: it's why aggressive stopword removal is unnecessary. IDF already weights "the" to nearly zero. **The scoring function solved the problem stopword lists were invented for.**

**3. Field length — shorter is stronger evidence.** Matching "java" in a three-word title is far stronger evidence than matching it once in a five-thousand-word document, where it may be incidental.

Combine the three and you have TF-IDF, which served the field for decades.

### BM25, and the two things it fixes

Modern OpenSearch uses **BM25**: same three signals, each handled more carefully. Here's the formula, which I'll show once and then tell you not to memorise:

```
                          f(t,d) · (k₁ + 1)
score(q,d) = Σ  IDF(t) · ─────────────────────────────────
            t∈q           f(t,d) + k₁ · (1 − b + b·|d|/avgdl)
```

`f(t,d)` = times term t appears in document d; `|d|` = the field's length; `avgdl` = average field length across the corpus; `k₁` and `b` are constants defaulting to 1.2 and 0.75.

What matters is the two problems it solves. Let's do the arithmetic, because that's where the insight lives.

**Problem one: term frequency must saturate.**

Under plain TF-IDF, a document with "kubernetes" 100 times scores 100× one with it once. So keyword stuffing wins, and a page that is nothing but the word "kubernetes" repeated outranks the actual documentation.

Watch BM25's fraction as `f` grows (taking `|d| = avgdl`, so the length term is 1, and `k₁ = 1.2`):

```
f = 1:    1 × 2.2 / (1 + 1.2)   = 1.000
f = 2:    2 × 2.2 / (2 + 1.2)   = 1.375      ← +0.375
f = 3:    3 × 2.2 / (3 + 1.2)   = 1.571      ← +0.196
f = 5:    5 × 2.2 / (5 + 1.2)   = 1.774      ← +0.10 per step
f = 20:  20 × 2.2 / (20 + 1.2)  = 2.075
f = 21:  21 × 2.2 / (21 + 1.2)  = 2.081      ← +0.006
f = ∞:                          → 2.200      ← hard ceiling = k₁ + 1
```

Both numerator and denominator grow with `f`, so the ratio climbs toward a ceiling of `k₁ + 1` and never passes it. Going from **1 to 2** occurrences gains 0.375. Going from **20 to 21** gains 0.006 — sixty times less. Which matches human judgement exactly: after a few mentions you're convinced, and further repetition carries no information. `k₁` controls how fast it saturates.

**Problem two: length normalisation needs a dial.**

Penalising long documents is right in general, but overdo it and a genuinely comprehensive forty-page guide loses to a one-line stub. `b` interpolates. With `avgdl = 100` words and one occurrence:

```
title,     |d| = 10:    norm = 1 − 0.75 + 0.75×(10/100)   = 0.325
                        score component = 2.2/(1 + 1.2×0.325) = 1.583

average,   |d| = 100:   norm = 1.0
                        score component = 2.2/(1 + 1.2×1.0)   = 1.000

long page, |d| = 1000:  norm = 1 − 0.75 + 0.75×10          = 7.75
                        score component = 2.2/(1 + 1.2×7.75) = 0.214
```

One mention in a short title is worth **7×** one mention in a long page. At `b = 0` length is ignored entirely; at `b = 1` scores are fully normalised by length; 0.75 is a tuned compromise. Lowering `b` is occasionally right for fields where length carries no meaning — a list of tags, say, where having more tags shouldn't dilute each one.

In practice, tuning `k₁` and `b` is a **small** effect. Field boosting, good analysers, and query structure matter far more. Exhaust those before touching similarity parameters.

### Finding out why

Three endpoints eliminate guesswork:

- **`"explain": true`** in a search — for every hit, a full recursive breakdown: every term, its IDF, its TF, its length normalisation, and how they combined. Extremely verbose, completely definitive.
- **`GET /documents/_explain/{id}`** with a query body — the focused question: why did *this specific document* score what it did, or, crucially, **why did it not match at all**. When someone says "this document should be the top result and it isn't even in the list", this is the endpoint that answers.
- **`"profile": true`** — per-shard timing breakdown of query execution. For performance rather than relevance, but the same toolbox.

### Getting better relevance, roughly by effort-to-reward

1. **Boost your fields.** `"title^3"`. One line.
2. **Index the same text several ways** — exact, stemmed, ngrammed — as multi-fields, and combine with `multi_match` + `most_fields`. Exact matches then naturally outrank stemmed ones because they match *more sub-fields*, which is a lovely property to get for free.
3. **Add a phrase boost.** Put a `match_phrase` in `should`. Documents where the words actually appear adjacent get lifted above documents that merely contain them all somewhere.
4. **Decay by recency or popularity** — `function_score` with a `gauss` decay, or the cheaper `rank_feature`. A wiki page edited last week beats an equally relevant one last touched in 2017.
5. **Learn to rank.** A plugin that takes your top N and reranks them with a trained gradient-boosted model over features you define — BM25 score, recency, click-through rate, author authority. A real step up in quality, and a real step up in machinery.
6. **Add semantic search** — Chapter 3, where the largest modern gains are.

### Measuring it, which you must do first

Six ways to improve relevance, and here is the part to be emphatic about:

> **Without measurement, you cannot tell whether any of them helped.**

Relevance work without evaluation is not engineering. It's a sequence of plausible-sounding changes whose net effect is unknown and quite possibly negative. And it is genuinely unknowable by intuition: boosting titles 3× helps some queries and hurts others, and no amount of staring at one example tells you the balance.

So build a **judgment list**: a set of queries, and for each, which documents are relevant.

```
query "kafka ordering"     → relevant: d1 (perfect), d2 (good), d5 (ok)
query "spark partitions"   → relevant: d6 (perfect), d3 (good)
```

Mine it from click logs, have humans rate a few hundred query-document pairs, or generate it synthetically with a language model. Then measure:

- **Precision@k** — of the top k results, what fraction were relevant? (Top 10, 6 relevant → 0.6.)
- **Recall@k** — of all relevant documents that exist, what fraction appeared in the top k? (20 relevant exist, 6 in top 10 → 0.3.)
- **MRR** (mean reciprocal rank) — 1 divided by the position of the *first* relevant result, averaged over queries. First relevant at position 3 → 0.333. The right metric when there is essentially one correct answer.
- **nDCG@k** (normalised discounted cumulative gain) — handles *graded* relevance (perfect / good / acceptable / bad) and discounts by position, so putting the best result first is rewarded and burying it at rank 9 is penalised. **The standard headline metric for ranked retrieval, and the one to report.**

OpenSearch's **Rank Evaluation API** (`_rank_eval`) computes these against a judgment set for you. Wire it into CI so a relevance regression **fails a build** rather than surfacing three weeks later in a support ticket. This is the single most valuable piece of infrastructure in a search project and routinely the last thing teams build.

---

## §2.10 — Living with an index

### Bulk indexing

One HTTP request per document is slow. The `_bulk` API batches:

```
POST /_bulk
{ "index":  { "_index": "documents", "_id": "d-1001" } }
{ "title": "Kafka partitions", "body": "..." }
{ "update": { "_index": "documents", "_id": "d-1002" } }
{ "doc": { "status": "published" } }
{ "delete": { "_index": "documents", "_id": "d-1003" } }
```

Newline-delimited JSON: one action line, one source line, repeated — and a trailing newline the API genuinely requires. (The odd format exists so the server can split the payload without parsing all of it.)

Rules for using it well:

- **Size by bytes, not document count** — aim for 5–15 MB per request. A thousand tiny documents and a thousand huge ones are completely different requests; only the byte size predicts behaviour.
- **A few concurrent bulk threads** rather than one giant serial stream. Two to four per data node is a reasonable start.
- **Retry `429 Too Many Requests` with backoff.** That response is not an error — it is OpenSearch's write queue applying backpressure exactly as §1.6 recommends. The correct response is to slow down, not to page someone.

Then the rule I want to state as strongly as possible:

> **A `200 OK` on a bulk request does not mean the documents were indexed.**

The bulk API returns 200 as long as the *request* was processed. Individual items inside it can fail — mapping conflict, version conflict, rejected write — each with its own status:

```json
{ "took": 42,
  "errors": true,                         ← check this
  "items": [
    { "index": { "_id": "d-1001", "status": 201 } },
    { "index": { "_id": "d-1002", "status": 400,
                 "error": { "type": "mapper_parsing_exception", ... } } }
  ] }
```

**You must check `errors` and the per-item results.** A pipeline that checks only the HTTP status will lose documents silently and indefinitely, while looking completely healthy on every dashboard you own.

Finally, connecting to §1.7: **use a deterministic `_id`** — the document's own identifier from the source system.

```
with _id = "d-1001":     retry after a timeout → overwrites. Same document. Fine.
with a generated _id:    retry after a timeout → a second copy. Forever.
```

Deterministic IDs make your entire indexing pipeline **idempotent for free**. Random IDs accumulate duplicates that are very hard to find and remove later.

### Aliases, and one rule

An **alias** is a name pointing at one or more indexes. Small feature; load-bearing for everything else here.

```json
POST /_aliases
{ "actions": [
    { "remove": { "index": "documents_v1", "alias": "documents" } },
    { "add":    { "index": "documents_v2", "alias": "documents" } }
]}
```

Both actions in one request, applied **atomically**. There is no instant at which the alias points at both or at neither. Every in-flight query resolves to one index or the other, and the switch is invisible to users.

> **Your application should never reference a concrete index name. Only ever an alias.**

Follow it and every schema change, reindex, and model upgrade becomes a zero-downtime alias swap with instant rollback. Ignore it and each becomes a coordinated deployment with a maintenance window. The cost of compliance is typing `documents` instead of `documents_v1` on day one.

Aliases do more: a **filtered alias** carries a built-in filter (a per-team view of a shared index); a **routing alias** pins queries to a shard; `is_write_index` designates which index of a group takes writes, which is what makes automatic rollover work.

### Reindexing

Sooner or later you must change something immutable — a field's type, an analyser, the primary shard count. All of these mean a new index and a copy.

```json
POST /_reindex?wait_for_completion=false&slices=auto
{
  "source": { "index": "documents_v1" },
  "dest":   { "index": "documents_v2" }
}
```

`slices=auto` parallelises by shard — a large speedup. `wait_for_completion=false` returns a task ID immediately, so a six-hour reindex doesn't sit on an HTTP connection; you poll `GET _tasks/<id>`. A `script` can transform documents in flight; a remote `source` can pull from another cluster entirely.

The zero-downtime pattern, worth learning as one unit:

1. Create `documents_v2` with the new mapping.
2. Write new changes to **both** indexes — or, better, make sure the indexing pipeline can be **replayed** from its source (Chapter 5 makes this easy).
3. Reindex the historical data v1 → v2.
4. **Verify**: compare document counts, spot-check a sample, and run your `_rank_eval` judgment set against *both* to confirm relevance didn't regress.
5. Swap the alias atomically.
6. Keep v1 for a rollback window, then delete.

**Step 4 is the one people skip and the one that matters.** An alias swap makes rollback trivial — but only if you notice there's a problem.

Two relatives: **`_update_by_query`** reindexes documents in place, which is how you apply a *new analyser* to existing data without a new index (the mapping didn't change, only how text is processed). **`_delete_by_query`** removes matching documents — remembering from §2.3 that this writes tombstones, and the space returns at merge time, not immediately.

### Templates and lifecycles

For time-series data — logs, metrics, events — three features combine into the standard pattern:

- An **index template** auto-applies settings, mappings, and aliases to any new index matching a name pattern, so `logs-2026-09-17` is born correctly configured without anyone doing anything.
- **Rollover** creates the next index and moves the write alias when the current one gets too large or too old.
- **ISM** (Index State Management) is the lifecycle engine: after 7 days reduce replicas and force-merge; after 30 days move to cheaper warm nodes; after 90 days snapshot and delete.

Together they maintain a rolling window of data at bounded cost with no human in the loop.

**Snapshots** round it out: incremental backups to S3 at the segment level, restorable into the same or a different cluster. And, as §4.6 will insist: **replication is not backup.** Replication faithfully replicates your accidental `_delete_by_query` to every copy, instantly.

---

## §2.11 — When it goes wrong

### Cluster health colours

```
GET _cluster/health
```

**Green** — every primary and every replica assigned and active. Fine.

**Yellow** — every primary assigned, at least one replica isn't. All your data is present and serving normally, but **redundancy is gone**: if the wrong node dies now, you lose data. On a single-node development cluster this is permanent and harmless (a replica can't sit on its primary's node, and there is nowhere else). In production it means *fix this today*.

**Red** — at least one **primary** is unassigned. Some of your data is unavailable right now. Searches return **partial results**, possibly without your application noticing, and writes to that shard fail. This is an outage.

That "possibly without noticing" is worth a beat: a red cluster doesn't error, it quietly returns fewer results. Check `_shards.failed` in search responses.

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

- a **disk watermark** was crossed. OpenSearch stops allocating at **85%**, stops moving shards in at **90%**, and makes indexes **read-only at 95%** — the **flood stage**, which surprises people badly because writes start failing and disk usage is not where they were looking;
- a node left and hasn't returned;
- allocation filtering rules exclude every eligible node;
- too many shards per node;
- a shard is corrupt and needs `_cluster/reroute?retry_failed`.

### Sizing, with the reasoning (which outlives the numbers)

**10–50 GB per shard.** Below 10 GB you pay per-shard overhead — heap, file handles, a separate query execution — for very little data. Above 50 GB, recovery and rebalancing get painful: moving a shard means moving *all of it*, and a 200 GB shard takes a long time to cross a network while the cluster sits degraded.

**Roughly 20 shards per GB of heap, per node.** A node with 30 GB heap manages about 600 shards. Fewer is better.

**JVM heap ≤ 31 GB, and ≤ half the machine's RAM.** Both halves need explaining:

- The **31 GB ceiling** is a genuine cliff, not a guideline. Above ~32 GB the JVM can no longer use *compressed ordinary object pointers*; it switches to full 64-bit references, every pointer doubles, and you end up with **less usable heap from more memory**. A 32 GB heap can hold less than a 31 GB one.
- The **half-of-RAM rule** exists because Lucene's speed comes from the **filesystem cache**. Segments are memory-mapped files; the OS keeping them in free RAM is what makes search fast. Give all your memory to the JVM and you starve the thing that actually matters.

And the most common mistake of all: **oversharding.**

It's so tempting — shards are how you scale, so more shards must be more scalable. But every shard is a complete Lucene index with fixed overhead, and every query fans out to **all** of them, paying §1.3's straggler tax on each. A query against 500 tiny shards is dramatically slower and more expensive than the same query against 10 correctly sized ones.

```
primaries = ceil(expected_total_GB / 30)
```

then round to a multiple of your data node count so shards distribute evenly — and **resist the urge to round up "just in case"**.

Replicas are the easy half: at least one in production, always. And since the replica count *can* change live, you can add replicas to increase read throughput whenever you need to.

### Troubleshooting playbook

**Searches are slow.**
Run it with `"profile": true` and find the dominant clause — this usually ends the investigation immediately. Then, in order:
- Are the yes-or-no conditions in `filter` where they can be cached?
- Is something paginating deeply?
- Are there `wildcard` / `regexp` / `script` queries that should be precomputed at index time?
- Are you fanning out to more shards than necessary — could routing make a common query hit just one?
- **Is the working set larger than the filesystem cache**, so every query goes to disk? That last one is very often the real answer, and the fix is more RAM or faster storage rather than anything clever.

Turn on the **slow log** (`index.search.slowlog.threshold.query.warn: 5s`) so the actual offenders identify themselves instead of being guessed at.

**Indexing is slow.**
Check bulk size and concurrency; look for `429`s indicating you're already at capacity. Check whether something forces refreshes — a stray `?refresh=true` in application code is a classic. Look at merge activity (merges compete for I/O). Consider dropping replicas to zero during a large backfill. And look for **dynamic mapping updates firing on every document**, which serialise through the cluster manager and throttle everything.

**Memory problems.**
A `CircuitBreakingException` means a request wanted more heap than allowed. **The circuit breaker is protecting you**; fix the query, don't raise the limit.

Long GC pauses in the logs matter for the reason §1.9 gave: a node in a two-second GC pause is **indistinguishable from a dead node**. So it's removed from the cluster, its shards reallocate, then it comes back and everything reallocates again. A cluster can thrash like this for hours. Root cause is usually too many shards, fielddata on a text field, or an unbounded aggregation.

---

## §2.12 — Where Lantern stands

Reread the chapter's summary now; every phrase should decode. Annotated:

- *"Twelve primary shards of ~25 GB each"* — inside the 10–50 GB window, and 12 divides evenly across 6 data nodes.
- *"behind an alias called `documents`"* — the §2.10 rule, so every future migration is an atomic swap.
- *"three small dedicated cluster-manager nodes"* — quorum of three, tolerating one failure, isolated from search traffic.
- *"indexed as `text` … and simultaneously as `keyword` through a multi-field"* — so the same field can be searched *and* sorted/aggregated (§2.4).
- *"status, team, timestamp … used exclusively in `filter` clauses"* — no scoring, cached bitsets (§2.8).
- *"`multi_match` across title and body with title boosted threefold, plus a `should` phrase clause, plus a `function_score` recency decay"* — improvements 1, 3, and 4 from §2.9.
- *"a judgment set of 200 queries runs in CI and fails the build if nDCG@10 drops"* — the thing teams build last, built first.
- *"`_bulk` in 10 MB batches, keyed by the source document ID, with per-item error checking"* — every rule from §2.10.
- *"searchable about a second later"* — the refresh interval, understood and monitored rather than discovered in a bug report.

### And the wall it hits

A user types *"why does my deployment keep restarting"*. The document that answers it is titled **"Diagnosing CrashLoopBackOff"**.

Run the intersection from §2.2:

```
query terms:     [why, doe, my, deploy, keep, restart]
document terms:  [diagnos, crashloopbackoff]

intersection:    ∅
```

**Empty.** Not ranked low — *not a candidate at all*. The postings lists don't touch. And no amount of stemming, synonyms, or boosting fixes this, because those operate on words and the problem isn't a word problem. "CrashLoopBackOff" *means* "keeps restarting", and the inverted index has no representation of meaning whatsoever. It is a machine for matching *strings that happen to be words*.

Same for *"kubernetes pod memory limits"* against *"k8s container resource constraints"*: zero shared terms, so a document that fully answers the question ranks below several that merely say "Kubernetes" a lot.

You could add a synonym: `k8s → kubernetes`. And another: `pod → container`. And `limits → constraints`. And then a thousand more, and you still won't have covered how people phrase things, because the space of phrasings is not enumerable. Every tool in this chapter papers over the gap one word at a time, and one word at a time does not scale.

To close it you must stop indexing words and start indexing something else — which is Chapter 3.

---

## The one-page summary

**The core mechanism.** An inverted index maps terms → documents. Queries become intersections of sorted lists, which is why search is fast without scanning. Doc values are the mirror structure (documents → values), and they're why sorting and aggregating work, and why they *don't* work on `text` fields.

**The distributed layer.** An index is split into shards; each shard is a complete, independent Lucene index. Searching means scatter-gather over all of them. That single fact explains score variation, approximate aggregations, and the straggler tax.

**The write path.** Routing is `hash(id) % primaries`, which is why the primary count is frozen. Refresh (1s, visibility) ≠ flush (durability of segments) ≠ translog fsync (crash safety of the ack). Segments are immutable; updates are new-copy-plus-tombstone; merges do the real cleanup.

**The query language.** Score-bearing clauses go in `must`/`should`; yes-or-no constraints go in `filter` where they're cached. Full-text queries analyse their input, term-level queries don't. Analysis must produce matching terms on both sides, and `_analyze` is how you see it.

**Relevance.** BM25 = saturating term frequency × inverse document frequency × length normalisation. Scores are relative, never absolute. Field boosting is the cheapest win. And nothing counts as an improvement until a judgment set says it is.

**Operations.** Always use aliases. Always use deterministic IDs and check per-item bulk errors. Size shards at 10–50 GB and resist oversharding. Heap ≤ 31 GB and ≤ half of RAM.

**The limit.** All of it matches words. Users ask about meaning. That gap is Chapter 3.

---

## Glossary of terms this chapter assumes

| Term | Meaning |
|---|---|
| **term** | one indexed token — the atomic unit of matching |
| **postings list** | the sorted list of documents containing a term |
| **term dictionary** | the sorted list of all terms in a shard |
| **tf** | term frequency — occurrences of a term in one document |
| **IDF** | inverse document frequency — how rare a term is; rarer scores higher |
| **analysis** | the pipeline turning a string into terms; runs at index *and* query time |
| **tokenizer** | the stage that splits text into tokens (exactly one per analyzer) |
| **token filter** | a stage that transforms the token stream (lowercase, stem, fold…) |
| **stemming** | reducing words to a root form (`running` → `run`) |
| **doc values** | the columnar document → value structure used for sort and aggregate |
| **segment** | an immutable file of indexed data inside a shard |
| **merge** | background combining of segments; also when deletes are really applied |
| **refresh** | making recent writes *visible* to search (~1s) |
| **flush** | fsyncing segments to disk |
| **translog** | the write-ahead log making an acknowledged write crash-safe |
| **shard** | a slice of an index; itself a complete Lucene index |
| **primary / replica** | the write-accepting copy / an exact copy elsewhere for reads and failover |
| **routing** | `hash(id) % primaries`, deciding which shard owns a document |
| **coordinating node** | whichever node received your request and fans it out |
| **mapping** | the schema: field types and how they're analysed |
| **multi-field** | one JSON field indexed several ways (e.g. `title` and `title.keyword`) |
| **alias** | a name pointing at indexes; the thing your app should always query |
| **precision** | of what I returned, how much was relevant |
| **recall** | of everything relevant, how much did I return |
| **nDCG** | the standard position-discounted, graded-relevance ranking metric |
