# Chapter 1 — The Machine That Isn't One

A program on one machine behaves predictably. Add machines and that's gone: data must be split and copied, machines fail invisibly, and messages get lost or arrive twice. This chapter covers the core problems behind every distributed system, so Kafka, Flink, Spark and OpenSearch later read as answers to questions you already understand.

**By the end you'll be able to:**

- Choose a partitioning scheme and spot skew and scatter-gather costs.
- Explain leader–follower replication, the sync/async trade-off, and quorum arithmetic (R + W > N).
- Describe why "dead or slow?" can't be answered, and how majority elections and fencing cope with it.
- Make retried operations safe with idempotency, and read delivery guarantees correctly.
- Reason about consistency (CAP, PACELC) and latency (percentiles, Little's Law, utilisation).

## 1.1 A program that used to work

Picture the simplest possible Lantern. One server, PostgreSQL on the same machine, and a small web app that runs `SELECT * FROM documents WHERE body LIKE '%kafka%'` and returns the rows.

It has a property you never noticed: **everything either happens or doesn't.** A write lands; a variable reads back what you set. There is one truth, in one process, and you can attach a debugger to it.

Then the company grows to two hundred million documents and four hundred searches a second. The server falls over, you add machines, and the lovely property vanishes at once.

Every later system is an opinionated answer to this chapter's problems. Understand them and you'll read the Kafka docs thinking *of course they did it that way*.

## 1.2 Why anyone does this

Nobody distributes for fun; one machine is easier to reason about, debug, deploy, and pay for. There are exactly three reasons to give that up:

| Pressure | Example (Lantern) | Technique |
|---|---|---|
| **The data doesn't fit** | ~40 TB of text and indexes; search wants its working set in memory, and no machine has 40 TB of RAM | **Partitioning** (§1.3) |
| **The traffic doesn't fit** | 400 searches/s, each scanning a large index, saturate any one CPU | **Horizontal scaling**, usually on top of partitioning |
| **You can't afford to be down** | A disk, power supply, kernel panic or careless `kill -9` will eventually kill one machine | **Replication** (§1.4) |

If none applies, don't distribute. Plenty of teams do it because serious people seem to, then spend two years paying this chapter's costs for benefits they never needed.

## 1.3 Cutting the data into pieces

**Partitioning**, or **sharding** (same thing), splits one logical dataset into physical pieces on different machines. Lantern's 200 million documents become twenty **shards** of ten million each, on ten servers. A search asks all twenty in parallel and merges; a write goes to the one shard that owns the document.

This buys **capacity** (no machine holds everything) and **parallelism** (twenty machines work at once). Every system in this book partitions; only the nouns change: Kafka and Spark say *partitions*, OpenSearch *shards*, a data lake *partition directories*, Flink *keyed subtasks*.

### How does a record know where it lives?

That is the whole design problem, with four common answers.

**Hash partitioning** hashes a field (the **key**) and takes the remainder:

```
partition = hash(document_id) % 20
```

Any client can compute it with no lookup, and varied keys spread evenly. But **range queries become impossible**, since hashing scatters neighbouring keys. Worse, **changing the partition count reshuffles almost everything**: going from `% 20` to `% 21` moves roughly 95% of keys, a multi-day, network-saturating job on 40 TB.

**Consistent hashing** fixes the reshuffle. Hash nodes and keys onto a ring (0 to 2³²); a key belongs to the first node clockwise from it. A new node takes over only the arc just behind it, so you move about `1/N` of the data. Each machine sits at 100–200 points on the ring (**virtual nodes**), which evens out load and spreads a dead node's data across many survivors. Cassandra, DynamoDB, and most memcached clients work this way.

**Range partitioning** assigns key ranges (`a`–`f`, `g`–`m`, …). Range scans work, and it suits time series partitioned by day. The danger is the **hotspot**: with a timestamp key, all of today's writes hit one partition, and twenty machines deliver the throughput of one. Fix it by putting variety at the front of the key: a hash prefix, shard number, or tenant ID.

**A directory** is a lookup table ("tenant 42 lives on shard 7"). It's the most flexible: a customer generating a third of your traffic can get its own shard. But the directory becomes critical infrastructure, highly available and cached everywhere.

### The two ways partitioning goes wrong

**Skew** is the broken promise that work divides evenly. Causes are mundane: a celebrity with ten million followers, a tenant forty times bigger than the next, a bug that sets a field to `null` so every affected record hashes to one place. The symptom: one machine at 100% CPU while nine idle, or the hundredth task running two hours after the rest finished in a minute. When a job is slow, ask "is it skewed?" early.

**Scatter-gather** is the cost of queries that visit every partition. "Fetch document 12345" hits one shard; "find documents about Kafka" hits all twenty, and **your latency is the slowest shard's**. If each shard has a 1% chance of a brief stall (GC pause, noisy neighbour, cold cache), the chance at least one of twenty stalls is about 18%. So **more shards is not automatically faster**: each adds parallelism but also another roll of the dice.

The rule: **partition so your most common query touches as few partitions as possible.** If Lantern searches are almost always scoped to one department, partition by department: single-shard searches, no stragglers, no merge. The partition key is the highest-leverage decision in the system, and very hard to change later.

## 1.4 Making copies

Partitioning divides data; **replication** duplicates it on more than one machine. A **replication factor of three** means three copies in total, not an original plus three backups. It buys **durability** (a dead disk destroys nothing), **availability** (a dead machine doesn't hide data), and **read throughput** (three copies can serve reads).

### The leader and its followers

The dominant arrangement is **leader–follower** (primary–replica; older docs say master–slave). One copy of each partition, the **leader**, takes all writes and forwards each change to its **followers**, which apply them in the same order. Reads can go to any copy.

```
   client ──write──► ┌──────────┐
                     │  LEADER  │
                     └────┬─────┘
                          │ stream of changes
                ┌─────────┴─────────┐
                ▼                   ▼
          ┌──────────┐        ┌──────────┐
          │ FOLLOWER │        │ FOLLOWER │ ◄── reads may come here
          └──────────┘        └──────────┘
```

It dominates because ordering becomes trivial: writes are sequenced in one place and followers replay that order. OpenSearch, Kafka, PostgreSQL, MySQL, and MongoDB all use it.

### How long does the leader wait?

The leader can wait for followers to confirm before telling the client "done", or reply at once and replicate in the background. This is the most consequential decision in a replicated system, and it returns in Chapter 5 as a Kafka config flag.

| Mode | Promise to client | Cost |
|---|---|---|
| **Synchronous** (wait for all) | Data is in at least two places; a follower can be promoted with nothing lost | Write latency = slowest follower. A 3-second GC pause on one stalls every write; a dead one stops writes entirely |
| **Asynchronous** (don't wait) | Hollow. If the leader dies first, an acknowledged write **is simply gone** | None; a sick follower is harmless |
| **Wait for some** | Survives loss of any one machine without data loss | Never waits on the slowest copy |

The real answer is the last row: with three copies, require at least two to have the write. Kafka spells this `acks=all` plus `min.insync.replicas=2` at replication factor three, and it is the right default for nearly every durable system (§5.3).

### Quorums: one piece of arithmetic worth memorising

With **N** replicas, let a write need acknowledgement from **W** of them and a read consult **R** of them. Then:

```
if   R + W > N   then every read is guaranteed to see the latest write
```

It's counting: if W + R > N, the write set and read set **cannot be disjoint**, so at least one node you read has the latest write. With N = 3:

| W | R | Result |
|---|---|---|
| 3 | 1 | Fastest reads; any single failure blocks writes |
| 1 | 1 | Everything fast, nothing guaranteed: this is **eventual consistency** |
| 2 | 2 | Reads and writes both tolerate one node down, and stay correct. The popular choice |

### Multiple leaders, and no leaders at all

**Multi-leader** replication accepts writes at several nodes, typically one per datacenter, so Europeans write to Frankfurt and Americans to Virginia at local latency. The catch: two users can edit the same record in two places at once, with no single sequencer to say which came first. You must **resolve the conflict**, and every strategy is unsatisfying somehow.

The popular one is *last write wins*, and bluntly: **last-write-wins is a data-loss strategy with a reassuring name.** It silently discards one write: fine for a cache, a shipped bug for anything a human typed. Alternatives: keep both versions for the application (or user) to merge, or use **CRDTs** (conflict-free replicated data types) that merge deterministically, like increment-only counters or grow-only sets.

**Leaderless** (Dynamo-style) replication has no leader. Any node accepts any write, quorums provide the guarantees, and **read repair** (a read that spots disagreement fixes it) and **anti-entropy** (periodic replica comparison) heal divergence. Cassandra and DynamoDB work this way. It is very available and forgiving, and you pay in consistency complexity that leaks into application code.

### Lag, and the bugs it produces

With async or semi-sync replication, followers trail the leader by milliseconds, occasionally minutes. This **replication lag** produces three bugs that users report confusingly and you can diagnose instantly by name.

| Broken guarantee | What the user sees | Fix |
|---|---|---|
| **Read-your-own-writes** | Saves an edit, reload hits a lagging follower, edit is missing. Reported as "the save button doesn't work" | Briefly route *that user's* reads to the leader or a caught-up replica |
| **Monotonic reads** | Refreshes twice: new version, then old. Time runs backwards | **Sticky routing**: each user always reads the same replica |
| **Consistent prefix reads** | Across partitions replicating at different speeds, sees an answer before its question | Keep causally related data in one partition, or use logical clocks (§1.7) |

## 1.5 The thing that makes all of this hard

Splitting and copying data is mechanical. This next part is irreducibly hard, and it is the most important idea in the chapter:

> **When a machine does not respond, you cannot tell whether it has crashed, whether it is merely slow, or whether it is perfectly healthy but unreachable from where you are standing.**

Five seconds, no reply. Maybe the machine caught fire, or is in a four-second GC pause, or a switch between you failed while it serves others. Maybe only the *response* was lost.

These demand opposite reactions. If it's dead, you must promote a follower. If it's alive and you promote anyway, you get **split brain**: two leaders, both accepting writes, diverging every second, and recovery means choosing whose data to throw away.

No algorithm can tell these cases apart. This is the essence of the **FLP impossibility**: with asynchronous message delivery you cannot reliably distinguish a crashed process from a slow one, so consensus can't be guaranteed in bounded time.

So real systems guess, with **timeouts**. Nodes send **heartbeats** every few hundred milliseconds; miss several and a peer is declared dead. The threshold trades two failures:

- **Too short: false positives.** A healthy leader is declared dead during a GC pause, a failover happens, and you caused an outage trying to prevent one.
- **Too long: slow detection.** The system really is down, and stays down for sixty seconds before anything reacts.

There is no correct value. Not for the last time, "what's the right setting?" really means "what would you rather have go wrong?"

### The additional lie: clocks

Every clock drifts, and NTP corrects drift by **jumping**, sometimes backwards. Servers routinely disagree by milliseconds, and far more when misconfigured; I have seen production machines an hour apart.

So **you cannot order events by comparing wall-clock timestamps from different machines.** If "the later timestamp wins", NTP configuration partly decides whose write survives, which is no basis for a business rule. If correctness depends on order, use §1.7's tools.

## 1.6 Agreement, and how a group picks a leader

If a leader can die, something must choose its replacement, and **everyone must agree** on the choice, or you have split brain. This is **consensus**, solved (within §1.5's limits) by Paxos (first, famously hard), Raft (built to be understandable; it has largely won), and ZAB (behind ZooKeeper). You won't implement one, but learn Raft's intuition.

Raft divides time into numbered **terms**; each node is a *follower*, *candidate*, or *leader*. A follower that stops hearing the leader's heartbeats increments the term, becomes a **candidate**, and asks for votes. Each node votes at most once per term, and a candidate with a **majority** becomes leader.

Everything important is in *majority*. Two majorities of one set **must overlap**, so two candidates can't both win term 5 unless some node voted twice, and nodes don't. **Two leaders in the same term is impossible**, not just unlikely.

Split five nodes into three and two. The three form a majority, elect a leader, and carry on. The two **cannot**, so they go read-only or refuse service, but never diverge. The minority gives up availability to protect correctness, automatically.

That's why cluster sizes are odd:

| Nodes | Majority | Failures tolerated |
|---|---|---|
| 3 | 2 | 1 |
| 4 | 3 | 1 (the fourth node buys nothing) |
| 5 | 3 | 2 |

Hence the three or five dedicated coordinator nodes recommended for OpenSearch, Kafka, ZooKeeper, and etcd.

One more safeguard handles zombies: an old leader, only paused, wakes after a new election and tries to write. **Fencing** stops it. Each leader attaches its monotonically increasing term to every write, and storage **rejects any write with a lower number than the highest it has seen.** Without fencing, a long GC pause can corrupt data even when elections work.

### Where recovery actually comes from

Elections decide *who* leads. Recovering data rests on three mechanisms:

- **Write-ahead log** (commit log; OpenSearch's *translog*). Before changing state, append the change to a durable, append-only file on disk; then apply it. After a crash, replay what you can't confirm. Appending is the fastest thing a disk does, so it's cheap. In Chapter 5 Kafka makes the log *the entire product*.
- **Checkpointing.** Periodically write down the full state, so recovery only replays the log since then. Flink's checkpoints (§6.3) are the most sophisticated version you'll meet.
- **Catch-up.** A node down for ten minutes fetches the delta from the leader instead of rebuilding. OpenSearch calls this peer recovery; a Kafka follower does it continuously.

### Keeping failures small

Each stops one component's failure from becoming everyone's:

- **Timeout on every remote call.** Otherwise requests to a hung dependency hold threads until none are left, and someone else's small outage becomes your total one.
- **Exponential backoff with jitter.** Wait longer between attempts, with a random **jitter**. Otherwise clients that failed together retry together: the **thundering herd**, flattening a recovering service in a loop that can last hours.
- **Circuit breaker.** After repeated failures, stop calling the dependency for a while and fail fast, freeing your threads and giving it room to recover.
- **Bulkheads.** Give each dependency its own thread or connection pool, like compartments in a hull, so a slow one drains only its own pool.
- **Backpressure.** When work arrives faster than you can finish it, buffering lasts only until memory runs out. Instead, **tell the producer to slow down**. How well a streaming system does this is a good proxy for its quality: Flink does it automatically from sink to source (§6.1); Spark makes you set a rate limit by hand (§7.3).
- **Degrade gracefully.** If Lantern's embedding service is down, keyword-only results are a worse product; an error page is a broken one. Decide what can be shed in advance, not at 3 a.m.

## 1.7 Retries, duplicates, and the idea that saves you

### The problem, concretely

You send a request to index a document and the connection times out. Did it get indexed? You can't know: a lost request and a lost reply look identical. Don't retry, and you may lose data. Retry, and you may index it twice.

So **in any system with retries, duplicates are inevitable.** Anyone claiming otherwise built something very expensive or hasn't looked closely.

### The solution: make duplicates not matter

§1.5 gave the fundamental problem; this is the fundamental solution. You can't prevent the second delivery, so make it harmless. An operation is **idempotent** if doing it many times has the same effect as doing it once.

| Operation | Idempotent? |
|---|---|
| Set the balance to 100 | Yes: five times, still 100 |
| Add 10 to the balance | No: five times, you gave away 50 |
| Delete user 42 | Yes: the second delete finds nothing |
| Send a confirmation email | Emphatically no, and your users will tell you |

Four reliable ways to get idempotency, roughly in order of preference:

1. **Upsert on a natural key.** Write the record *at* a key derived from the data. In OpenSearch, set `_id` to the document's own ID rather than a server-generated one; a repeat just overwrites identical content. The most elegant answer, and it recurs throughout the book.
2. **Overwrite a whole partition.** In batch jobs, delete and rewrite today's partition instead of appending. Chapter 4 calls this the single most useful idempotency pattern in batch data engineering.
3. **Carry an idempotency key.** The client sends a unique ID per logical *operation* (not per attempt) with every retry; the server returns the stored result for keys it has seen. Payment APIs avoid double charges this way, and Kafka's producer removes duplicates with it (§5.4).
4. **Keep a dedup window.** Remember recently seen IDs (in Redis, RocksDB, or stream-processor state) and drop repeats. It needs a time-to-live or grows forever, and a duplicate arriving after the window slips through. The fallback when the others aren't available.

The sentence to take away from this chapter:

> **Deliver at least once, and make the receiver idempotent. You now have exactly-once *effects*, for a fraction of the cost of exactly-once *delivery*.**

Chapter 9 traces this through every hop of a full architecture.

### Retrying well

- **Retry only errors that might succeed next time**: timeouts, connection resets, HTTP 503, rate limits. Never retry a 400; you've just built a log-noise machine.
- **Cap attempts and total elapsed time**, or a transient failure becomes a permanent resource leak.
- **Beware retry amplification.** Three retries each at gateway, service, and data client = 27 requests to a struggling backend from one click. Retry at one layer (usually the outermost) and pass failures through elsewhere.
- **Consider a retry budget**: retries capped at 10% of traffic. In a broad outage, it separates degraded service from a self-inflicted DoS.
- **Decide what happens when you give up.** Send unprocessable messages to a **dead-letter queue** with context, or one bad row from 2019 blocks a partition and stops the pipeline.

### Ordering, without clocks

If wall clocks can't be trusted (§1.5), how do you order events?

Mostly, **you don't need global ordering**, which would funnel everything through one sequencer and kill parallelism. You need order among events *about the same thing*.

**Per-key ordering is cheap and almost always sufficient.** Kafka gives strict order within a partition and routes same-key events to the same partition. Key by document ID and you get per-document order while processing a thousand documents concurrently, practically the defining pattern of Chapter 5.

For causality across the system, use a **logical clock**, which replaces "what time is it?" with "what have I seen?"

- **Lamport timestamps.** Each node increments a counter on every local event and attaches it to every message; on receipt, it sets the counter to `max(own, received) + 1`. If A caused B, A's timestamp is lower. But a lower timestamp doesn't prove causation, so you can't tell "A caused B" from "concurrent", which conflict detection needs.
- **Vector clocks** keep a counter *per node*. If one vector dominates, the events are causally ordered; if each is ahead somewhere, they're concurrent: a conflict. The vector grows with node count, so they suit modest, stable clusters, not thousands of ephemeral workers.

### Event time versus processing time

Chapter 6 is built on this. **Event time** is when something happened, stamped by the device that saw it. **Processing time** is when your system got around to it.

They can differ wildly: a phone without signal uploads forty minutes of buffered events at once, or a lagging consumer chews through an hour of backlog in five minutes. Compute "orders in 10:00–10:05" by processing time and re-running on the same data gives a different number. That isn't a metric; it's a rumour. Correct analytics use event time, and deciding how long to wait for stragglers is the job of the **watermark** (§6.4).

### The delivery guarantee vocabulary

These three phrases just describe where the acknowledgement sits relative to the work:

```
receive → acknowledge → process                  at-most-once
receive → process → acknowledge                  at-least-once
receive → (process and acknowledge atomically)   exactly-once-ish
```

- **At-most-once**: crash between ack and process and the message is gone. No duplicates, occasional loss. Fine for metrics and debug logs.
- **At-least-once**: crash between process and ack and the message is redelivered. No loss, occasional duplicates. **The right default for almost everything**, because idempotency makes the duplicates harmless.
- **Exactly-once**: the message affects state precisely once. But **exactly-once *delivery* over a network is impossible** (the two-generals problem: no finite exchange of messages guarantees both sides know the outcome). Real systems give exactly-once *processing semantics*, by committing output and input position in one atomic transaction (Kafka transactions, §5.4; Flink two-phase-commit sinks, §6.3) or by making output idempotent so replays overwrite.

When someone says their system is exactly-once, ask: *where is the acknowledgement, and what is it atomic with?*

## 1.8 Consistency, and the theorem everyone misquotes

*Consistency* names three unrelated things, and the collision causes real confusion:

- The **C in ACID**: the database enforces your declared constraints (foreign keys, uniqueness, checks). Nothing to do with distribution.
- **Replica consistency**: all copies hold the same bytes. Useful operationally, not our topic.
- A **consistency model**: a contract about what a reader may observe. This is the one this section is about.

### A spectrum, from expensive to cheap

| Model | Guarantee |
|---|---|
| **Linearizability** | Behaves as if there were one copy; each operation takes effect at one instant between call and return. Once a write returns, every later read from anywhere sees it |
| **Sequential consistency** | Everyone sees the same order, but it needn't match real time |
| **Causal consistency** | Causally related operations appear in that order everywhere; concurrent ones may not. Cheap, and close to what humans expect |
| Per-session (read-your-writes, monotonic reads, §1.4) | Usually routing tricks rather than storage properties |
| **Eventual consistency** | If writes stop, replicas converge; no promise of when, or what you read meanwhile. The weakest real guarantee, and fine for many applications |

Linearizability is what §1.1's single machine gave you free. Buying it back costs a quorum round trip per operation and total unavailability on the minority side of a partition.

### CAP, stated properly

The usual "consistency, availability, partition tolerance: pick two" is so misleading you'd be better off not knowing it. The accurate version:

> **When a network partition occurs, a distributed system must choose between consistency (linearizability) and availability (every non-failing node answers requests).**

**Partition tolerance is not a choice**: cables get unplugged and switches reboot. The real question is what your system does *during* a partition, decided in advance:

| Choice | Behaviour | Examples |
|---|---|---|
| **CP** | A node that can't reach a quorum stops accepting writes (maybe reads) rather than diverge | ZooKeeper, etcd, Kafka with `acks=all`, the minority side of a Raft partition (§1.6) |
| **AP** | Every node answers with what it knows; divergence is reconciled later (last-write-wins, app merge, CRDT) | Cassandra with low quorums, DNS, probably a shopping cart (a blocked "add item" is a lost sale; a briefly stale basket is mild confusion) |

CAP says **nothing** about the healthy case, when you can have both C and A.

### PACELC, which you will use more often

> **If Partitioned**, choose **A**vailability or **C**onsistency. **Else**, in normal operation, choose **L**atency or **C**onsistency.

The second half matters more: partitions are rare, normal operation is constant. Even on a perfect network, stronger consistency costs latency, because you must talk to other machines before answering. Linearizability is expensive on Tuesday afternoon, on every request; most real choices live in the "else" branch.

### What Lantern chooses

Lantern's search index is **deliberately not strongly consistent**. An edit is durable in PostgreSQL immediately and searchable a couple of seconds later. In exchange, Lantern batches writes, indexes in parallel across twenty shards, serves reads from replicas without coordination, and survives a dead machine.

It's a good trade only because it was made on purpose, with a written number ("edits are searchable within five seconds, p99") that is monitored. The failure is choosing eventual consistency by accident and learning from a user complaint that staleness hit forty minutes during a backlog. In §2.5 you'll see that OpenSearch's one-second *refresh interval* is the largest single term in Lantern's end-to-end latency budget.

## 1.9 Speed: what it means and where it goes

**Latency** is how long one operation takes; **throughput** is how many complete per second. They aren't reciprocals, and improving one often hurts the other, because of **batching**, which appears in every chapter.

Each send carries fixed overhead: a round trip, a system call, a handshake, a fresh compression context. Wait 50 ms and send a hundred documents together, and you pay it once and compress far better. Throughput might rise tenfold, but each document waited up to 50 ms longer.

It's one knob under many names: Kafka's `linger.ms`, OpenSearch `_bulk` size, Spark's micro-batch trigger, Flink's checkpoint interval, a data lake's target file size. See one curve and much configuration stops feeling arbitrary.

### Stop reporting averages

If you take one operational habit from this book, take this: **report percentiles, not averages.** A 50 ms average fits every request taking 50 ms, and equally fits 95% at 10 ms and 5% at a full second, where users are furious. Report p50 (the median), p95, p99, p99.9, and the max. The p99 is where the users who complain live.

The tail matters more than intuition suggests. If a Lantern page makes twenty parallel calls, each 1% likely to be slow, all twenty are fast only 0.99²⁰ ≈ 82% of the time: **18% of page loads are slow**. This is **tail amplification**, and scatter-gather search does it on every query.

Two defences: **hedged requests** (past the expected p95, send a duplicate to another replica and take the first answer, trading a little load for a much shorter tail), and **fewer shards per query**, §1.3's conclusion again.

### One equation worth knowing

**Little's Law**:

```
L = λ × W

concurrency = arrival rate × time in system
```

At 1,000 searches/s and 200 ms each, about 200 are in flight at once, so you need at least 200 threads, connections, or executor slots. With 100, requests queue and latency is no longer 200 ms.

Queueing is also what ruins more systems than anything else. As utilisation ρ approaches 1, queueing time grows roughly as `1/(1−ρ)`:

| Utilisation | Queueing vs. baseline |
|---|---|
| 50% | Negligible |
| 90% | ~10× |
| 95% | ~20× |
| 99% | ~100× |

Systems degrade *hyperbolically* as they fill. **Never run a latency-sensitive system near full utilisation**; aim for 60–70%. The idle 30% isn't waste; it's the headroom that keeps p99 sane, and the cheapest latency improvement you'll ever buy.

### Scaling up and scaling out

- **Vertical scaling** (a bigger machine): simple, no code changes, and almost always your first move, since a larger instance is far cheaper than three months of engineering. It has a ceiling and leaves one big failure domain.
- **Horizontal scaling** (more machines): effectively unbounded, but it demands everything in this chapter.

The key variable is state. **Stateless** services scale out almost free: ten copies behind a load balancer. **Stateful** services are hard, because adding a machine means moving data, rebalancing, and ambiguous ownership for a while. That's why databases scale harder than web servers, and why architectures push state to the edges.

Two limits to know. **Amdahl's law**: speedup is capped by the serial fraction; if 5% is serial, 20× is your ceiling. The **Universal Scalability Law** is gloomier: past some point, adding machines makes the system *slower*, because coordination grows faster than the work. There is an optimal cluster size, and you can be past it.

### Finding the bottleneck

Don't guess; measure. The **USE method** checks every resource for **U**tilisation (fraction of time busy), **S**aturation (work queued for it), and **E**rrors. Saturation is the one people forget and the one that usually gives it away.

The usual culprits, most frequent first:

| Resource | What to look for |
|---|---|
| **Disk I/O** (first, by far, for databases and search) | Random reads, IOPS limits, `fsync` latency. If the working set doesn't fit in the filesystem cache, every query hits disk and no CPU will save you |
| **Network** | Rarely bandwidth; usually **round trips**. One remote call per item (distributed N+1) pays latency serially, a thousand times |
| **Memory** | GC pauses. A 2-second stop-the-world pause makes a healthy node **indistinguishable from a dead one** (§1.5): its shards are reallocated, it rejoins, they move again. Clusters can thrash for hours over a heap setting |
| **CPU** | Compression, serialisation, JSON parsing, relevance scoring, embedding inference |
| **Locks and coordination** | The hidden one: *low* CPU plus *bad* latency. Dashboards say headroom, users say slow; look for contention |

Then measure end to end, find the largest span, drill in, repeat. Tracing and flame graphs beat intuition every time.

## 1.10 Ten questions for any system

Systems differ enormously in vocabulary and hardly at all in structure. Answer these ten and you can use a system, argue about it, and form a first hypothesis when it breaks:

1. **What is the unit of partitioning, and how is a record's partition chosen?**
2. **What is the unit of replication, and is replication synchronous or asynchronous?**
3. **Who is the leader, and what happens when it dies?**
4. **What consistency model do reads get?**
5. **What happens during a network partition — does it choose C or A?**
6. **Where is the durable log, and how does recovery use it?**
7. **What delivery guarantee does it provide, and where exactly is the acknowledgement?**
8. **How does it apply backpressure when overloaded?**
9. **What is the scatter-gather cost of a typical read?**
10. **What is its characteristic skew or hotspot failure?**

Twenty minutes with the docs and these questions beats a week of tutorials. Try them on Kafka in Chapter 5.

---

## Key takeaways

- Distribute only when data size, traffic, or uptime leaves no choice.
- The partition key is the highest-leverage decision: minimise partitions per common query, and watch for skew.
- Scatter-gather latency is the slowest shard's, so more shards isn't automatically faster.
- Replication factor 3 with writes acknowledged by 2 (`acks=all`, `min.insync.replicas=2`) is the sane durable default; R + W > N guarantees fresh reads.
- You can't tell dead from slow; timeouts are guesses, and majority quorums plus fencing make wrong guesses safe.
- Retries make duplicates inevitable: deliver at least once and make the receiver idempotent.
- Order per key, not globally, and never order events by wall clocks from different machines.
- CAP only covers partitions; PACELC's latency-vs-consistency trade governs everyday operation.
- Report percentiles, not averages, and keep latency-sensitive systems at 60–70% utilisation.

## Where we are

We broke a working program by adding machines and collected the tools for coping: partitioning, replication, quorums, write-ahead logs, and idempotency. In Chapter 2 we build Lantern's search index and watch OpenSearch make its own choices about shards, replicas, durability, and staleness. You already know what to ask of it.
