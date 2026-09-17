# Chapter 1 — The Machine That Isn't One

## 1.1 A program that used to work

Imagine the simplest possible version of Lantern. One server. A PostgreSQL database on the same machine. A small web application that takes a search query, runs `SELECT * FROM documents WHERE body LIKE '%kafka%'`, and returns the rows.

This program has a lovely property that you probably never noticed while writing programs like it: **everything either happens or doesn't.** When you call a function, it runs. When you write to the database, the write lands. When you read a variable you just set, you get the value you set. Time moves forward at one rate, in one place. There is exactly one version of the truth, and it lives in one process on one machine, and you can attach a debugger to it and look at it.

Now the company grows. Two hundred million documents. Four hundred searches a second. The single server falls over, and you are asked to make it work.

You will reach for more machines, because there is nothing else to reach for. And the moment you do, you lose the lovely property. Not gradually — immediately and completely. You have crossed a line, and on the other side of that line is a different discipline with different rules, and the rest of this book is about those rules.

The purpose of this chapter is to make you *feel* the shape of that new territory, because every specific system in later chapters — Kafka, Flink, Spark, OpenSearch — is a particular, opinionated answer to the problems described here. If you understand the problems, the answers become predictable. You will find yourself reading the Kafka documentation and thinking *ah, of course, they had to do it that way*, which is a far more durable kind of knowledge than memorising configuration flags.

## 1.2 Why anyone does this

Nobody distributes a system for fun. One machine is simpler in every respect: easier to reason about, easier to debug, easier to deploy, cheaper to run. You give that up only under duress, and the duress comes in exactly three flavours.

**The data doesn't fit.** Two hundred million documents with their full text and indexes might be forty terabytes. You can buy a machine with forty terabytes of disk, but you cannot buy one that holds forty terabytes in memory, and search engines want their working set in memory. So the data has to be cut into pieces that live on different machines. That is *partitioning*, and it is the subject of §1.3.

**The traffic doesn't fit.** Four hundred searches a second, each needing to scan a large index, will saturate any single machine's CPU. So you need several machines doing the work concurrently. That is *horizontal scaling*, and it usually rides on top of partitioning: if the data is already in twenty pieces, twenty CPUs can search those pieces at once.

**You can't afford to be down.** A single machine will eventually fail — a disk, a power supply, a kernel panic, a careless `kill -9`. If Lantern going down for four hours is unacceptable, you need more than one copy of everything, so that the loss of any single component is survivable. That is *replication*, §1.4.

Those three pressures are the only legitimate reasons. If none of them applies to your system, do not distribute it. A great deal of unnecessary suffering in our industry comes from engineers reaching for a distributed architecture because it is what serious people are seen to use, and then spending two years paying the costs described in this chapter for benefits they did not need.

## 1.3 Cutting the data into pieces

Partitioning — also called **sharding**; the words are interchangeable and which one you hear depends on which vendor's documentation you last read — means taking one logical dataset and splitting it into many physical pieces, each living on a different machine.

Lantern's two hundred million documents become twenty **shards** of ten million documents each, spread across ten servers. A search now works by asking all twenty shards in parallel, then merging what comes back. A write goes to whichever single shard owns that document.

This buys two things at once, which is why it is so fundamental. It buys **capacity**, because no single machine has to hold the whole dataset. And it buys **parallelism**, because twenty machines can work on twenty pieces simultaneously. Every system in this book partitions: Kafka calls the pieces *partitions*, OpenSearch calls them *shards*, Spark calls them *partitions*, a data lake calls them *partition directories*, and Flink calls them *keyed subtasks*. Same idea, different nouns.

### How does a record know where it lives?

This is the whole design problem. There are four common answers, and choosing badly among them causes a specific, recognisable kind of pain later.

The most common answer is **hash partitioning**. Take some field of the record — call it the *key* — hash it, and take the remainder modulo the number of partitions:

```
partition = hash(document_id) % 20
```

This is beautifully simple. Any client can compute it independently with no lookup. Assuming your hash function is decent and your keys are varied, the records spread out evenly, which is exactly what you want.

It has two weaknesses, and both of them bite in practice. The first is that **range queries become impossible**. If you want all documents whose IDs start with "A", they are scattered across all twenty shards, because hashing deliberately destroys any relationship between nearby keys. The second is subtler and more painful: **changing the number of partitions reshuffles almost everything.** That innocuous `% 20` means that if you grow to twenty-one partitions, `hash(x) % 21` gives a different answer than `hash(x) % 20` for roughly 95% of your keys. Every one of those records has to physically move. On a forty-terabyte dataset, this is a multi-day operation that saturates your network.

This weakness is important enough that there is a whole technique for avoiding it, called **consistent hashing**. Instead of taking a modulus, you imagine a circle — the numbers from zero up to 2³², arranged in a ring. You hash each *node* onto a point on that ring, and you hash each *key* onto a point too. A key belongs to whichever node is the first one you encounter walking clockwise from the key's position.

The magic is what happens when you add a node. The new node lands somewhere on the ring and takes over only the arc between itself and the previous node counter-clockwise. Every other key stays exactly where it was. Instead of moving 95% of your data, you move roughly `1/N` of it. In practice each physical machine is hashed onto the ring at a hundred or two hundred different points — these are called **virtual nodes** — for two reasons: it evens out the load (one unlucky arc doesn't dump a huge share onto one machine), and when a node dies its data disperses across many survivors rather than landing entirely on its neighbour. Cassandra, DynamoDB, and most memcached clients work this way.

The third approach is **range partitioning**: shard one holds keys `a` through `f`, shard two holds `g` through `m`, and so on. This preserves the ordering that hashing destroys, so range scans work, and it is the natural fit for time-series data, where you partition by day. But it introduces a new danger with a name worth learning: the **hotspot**. If your partition key is a timestamp, then *all of today's writes* go to exactly one partition, while the other nineteen sit idle. You have twenty machines and the throughput of one. The fix is to give the key more variety at the front — prefix it with a hash, or a shard number, or a tenant ID — so that concurrent writes spread out.

The fourth approach is to simply **keep a directory**: a lookup table that says "tenant 42 lives on shard 7." This is the most flexible option by a wide margin, because it lets you make individual decisions — that one enormous customer who generates a third of your traffic can be given a shard of their own. The cost is that the directory itself becomes a piece of critical infrastructure that must be highly available and aggressively cached everywhere.

### The two ways partitioning goes wrong

You should carry two failure modes in your head permanently, because between them they explain an enormous share of "why is our system slow" investigations.

The first is **skew**, which we have already met under the name hotspot. Partitioning promises that work divides evenly, and skew is the broken promise. One partition ends up with far more data, or far more traffic, than its siblings. The causes are mundane: a celebrity user with ten million followers; a single tenant who is forty times bigger than the next; a bug that sets a field to `null` so that every affected record hashes to the same place. The symptom is unmistakable once you know to look for it — one machine at 100% CPU while nine others idle, and a job where ninety-nine tasks finish in a minute and the hundredth runs for two hours. Whenever someone tells you a distributed job is slow, "is it skewed?" should be among your first three questions.

The second is the cost of **scatter-gather**. Some queries can be answered by a single partition: "fetch document 12345" hashes straight to one shard. But many cannot. "Find documents about Kafka" has to be sent to *every* shard, because any of them might hold a match. And here is the part that surprises people: when you fan out to twenty shards and wait for all of them, your latency is not the average shard's latency. **It is the slowest shard's latency.** If each shard independently has a one-in-a-hundred chance of being briefly slow — a garbage collection pause, a noisy neighbour, a cold cache — then the chance that *at least one* of your twenty is slow is about 18%. Nearly one query in five is dragged down by a straggler you can't predict.

This has a direct and slightly counter-intuitive consequence: **more shards is not automatically faster.** Splitting into more pieces increases parallelism, but it also increases the number of dice you roll on every query, and therefore your exposure to the slowest one. There is an optimum, and it is usually lower than people's instinct.

The practical rule that falls out of all this is: **partition so that your most common query touches as few partitions as possible.** If Lantern searches are almost always scoped to one department, partition by department, and a search becomes a single-shard operation with no stragglers and no merge. Designing the partition key is the highest-leverage decision in the whole system, and it is very hard to change later.

## 1.4 Making copies

Partitioning divides the data; **replication** duplicates it. Each partition is stored on more than one machine, so that the failure of any one machine doesn't take data with it.

The vocabulary is worth nailing down because it trips people up: a **replication factor of three** means three total copies, not one original and three backups. Lose two machines and you still have your data.

Replication buys three distinct things, and it is worth separating them because they pull in different directions. It buys **durability**, because a disk failure no longer destroys anything. It buys **availability**, because a machine failure no longer makes data unreachable. And it buys **read throughput**, because three copies means three machines that can answer a read.

### The leader and its followers

The dominant arrangement, by a very large margin, is **leader–follower** replication, which you will also see called primary–replica or (in older documentation) master–slave.

One copy of each partition is designated the **leader**. All writes go to the leader, and only to the leader. The leader records the change and then forwards it to its **followers**, who apply the same changes in the same order. Reads may be served by the leader or by any follower.

```
                  write
   client  ──────────────────►  ┌──────────┐
                                │  LEADER  │
                                └────┬─────┘
                                     │ stream of changes
                          ┌──────────┴──────────┐
                          ▼                     ▼
                    ┌──────────┐          ┌──────────┐
                    │ FOLLOWER │          │ FOLLOWER │  ◄─── reads may come here
                    └──────────┘          └──────────┘
```

The reason this arrangement dominates is that it makes ordering trivial. There is exactly one place where writes are sequenced, so there is exactly one authoritative order of events, and every follower can simply replay that order. OpenSearch uses it (primary shard and replica shards), Kafka uses it (partition leader and followers), PostgreSQL, MySQL, and MongoDB all use it.

### The question that defines everything: how long does the leader wait?

When a write arrives at the leader, the leader has a choice. It can forward the write to the followers and **wait** for them to confirm receipt before telling the client "done". Or it can tell the client "done" immediately and forward the write in the background.

This sounds like a minor implementation detail. It is in fact the most consequential decision in the design of a replicated system, and you will meet it again in Chapter 5 disguised as a Kafka configuration flag.

If the leader waits — **synchronous replication** — then when the client is told the write succeeded, that is a promise with real weight behind it. The data exists in at least two places. If the leader is struck by lightning one microsecond later, a follower has the write and can be promoted without losing anything. The cost is that **your write latency is now the slowest follower's latency.** Worse, if a follower is merely slow — not dead, just paused for a three-second garbage collection — then every write in the system stalls for three seconds. And if a follower dies entirely, writes stop completely, because the leader is waiting for an acknowledgement that will never come.

If the leader doesn't wait — **asynchronous replication** — writes are fast and a sick follower is harmless. The cost is that the promise you made to the client was hollow. If the leader dies before the write reaches any follower, then a write you acknowledged, that the user saw succeed, that the UI confirmed, **is simply gone**. Not delayed. Gone, permanently, with no record that it ever existed.

Neither option is acceptable for a serious system, which is why the real answer is the compromise in between. Wait for *some* of the followers, but not all of them. With three copies, insist that at least two have the write. Now you survive the loss of any single machine without losing data, and you never wait on the slowest of the three, because you only ever needed the faster two. Kafka expresses this exact configuration as `acks=all` combined with `min.insync.replicas=2` at a replication factor of three, and it is the correct default for essentially every durable system. We will spend proper time on it in §5.3.

### Quorums, and one piece of arithmetic worth memorising

That compromise generalises into a small, elegant rule. Suppose you have **N** replicas. You require a write to be acknowledged by **W** of them before you call it successful, and you require a read to consult **R** of them before you trust the answer. Then:

```
if   R + W > N   then every read is guaranteed to see the latest write
```

The reason is pure counting. If the set of nodes that received the write has W members, and the set of nodes you asked during the read has R members, and W + R exceeds the total N, then those two sets **cannot be disjoint** — they must share at least one node. That shared node has the latest write, so your read sees it.

Work through the configurations and you can feel the trade-off in your hands. With N=3 and W=3, R=1: reads are as fast as possible (ask one node, trust it) but writes must reach every single replica, so any failure blocks writing. With W=1, R=1: everything is fast and nothing is guaranteed — this is what people mean by *eventual consistency*. With W=2, R=2: both reads and writes tolerate one node being down, and correctness is preserved. That middle configuration is the popular one for good reason.

### Multiple leaders, and no leaders at all

Two other arrangements are worth recognising, mostly so you can identify them when someone proposes one.

**Multi-leader** replication accepts writes at several nodes — typically one leader per datacenter, so that European users write to Frankfurt and American users write to Virginia, each at local latency. The appeal is obvious. The problem is equally obvious once stated: two users can now modify the same record in two datacenters at the same instant, and there is no single sequencer to say which happened first. You must **resolve the conflict**, and every resolution strategy is unsatisfying in some way.

The most popular strategy is *last write wins*, and I want to be blunt about it: **last-write-wins is a data-loss strategy with a reassuring name.** It resolves the conflict by silently discarding one of the two writes. For a cache or a metrics counter, fine. For anything a human typed into a form and expects to still be there, it is a bug you have decided to ship. The alternatives are to keep both versions and ask the application (or the user) to merge them, or to use data structures that are designed to merge deterministically — **CRDTs**, conflict-free replicated data types, such as counters that only ever increment or sets that only ever grow.

**Leaderless** replication, sometimes called Dynamo-style after the Amazon paper that popularised it, removes the leader entirely. Any node accepts any write, quorums provide the guarantees, and background processes — *read repair*, where a read that notices disagreement fixes it, and *anti-entropy*, which periodically compares replicas — heal divergence over time. Cassandra and DynamoDB work this way. It is very available and very operationally forgiving, and you pay for it in consistency complexity that leaks upward into your application code.

### Lag, and the bugs it produces

With asynchronous or semi-synchronous replication, followers are always a little behind the leader. Usually milliseconds; occasionally, when something is wrong, minutes. This gap is called **replication lag**, and it produces a family of user-visible bugs that are worth knowing by name, because they are reported to you in confusing ways and diagnosed instantly if you recognise the pattern.

The most common is the failure of **read-your-own-writes**. A user edits a Lantern document, the write goes to the leader, the page reloads, the read is served by a follower that hasn't caught up, and the user's own edit is missing. They will report this as "the save button doesn't work", which is not what is happening at all. The fix is routing: for a short window after a user writes something, send *their* reads to the leader, or to a replica known to be caught up.

A related bug breaks **monotonic reads**. The user refreshes twice. The first read hits a fresh replica and shows the new version; the second hits a stale replica and shows the old one. Time appears to run backwards, which is deeply unsettling to look at. The fix is *sticky routing*: a given user always reads from the same replica, so they may be behind but they are never inconsistent with themselves.

The third in the family breaks **consistent prefix reads**, and it only appears once you have multiple partitions. Two causally related events — a question and its answer — live in different partitions replicating at different speeds, and a reader sees the answer arrive before the question. The fix is either to keep causally related data in the same partition, or to track causality explicitly with the logical clocks we'll meet in §1.7.

## 1.5 The thing that makes all of this hard

Everything so far has been mechanical. Split the data, copy the data — you could have worked it out yourself with a whiteboard and an afternoon.

Now comes the part that is genuinely, irreducibly difficult, and I want to state it as plainly as I can, because it is the single most important idea in this chapter:

> **When a machine does not respond, you cannot tell whether it has crashed, whether it is merely slow, or whether it is perfectly healthy but unreachable from where you are standing.**

Consider what you actually observe. You sent a request to a follower. Five seconds have passed. No reply. What do you know?

Almost nothing. Perhaps the machine caught fire. Perhaps it is in the middle of a four-second stop-the-world garbage collection pause and will wake up shortly with no idea anything was wrong. Perhaps the network switch between you and it failed, while the switch between *it* and the storage system is fine, so it is happily continuing to serve other clients and write data. Perhaps your request arrived, was processed correctly, and only the *response* was lost.

These situations demand completely different reactions. If the machine is dead, you must promote a follower immediately or the system stays down. If the machine is alive and you promote a follower anyway, you now have **two leaders**, both accepting writes, both convinced they are in charge, and their data diverging with every second that passes. This is called **split brain**, and recovering from it means choosing which user's data to throw away.

You cannot distinguish these cases. Not with better code, not with a smarter algorithm. This is a theoretical result, not an engineering failure — it is the essence of what is called the **FLP impossibility**, which says that in a system with asynchronous message delivery you cannot reliably distinguish a crashed process from a slow one, and therefore cannot guarantee consensus in bounded time.

So what do real systems do? They guess, using **timeouts**, and they accept the consequences of guessing wrong.

Every node sends a **heartbeat** to its peers every few hundred milliseconds. If you miss several in a row, you declare the peer dead and act accordingly. Where you set that threshold is a choice between two kinds of pain. Set it short and you get **false positives**: a healthy leader is declared dead during a GC pause, a failover happens, the old leader wakes up confused, and you have caused an outage while trying to prevent one. Set it long and you get **slow detection**: the system really is down, and it stays down for sixty seconds before anything reacts.

There is no correct value. There is only the value that trades the failure modes in the way your business prefers. This is the first of many places in this book where the honest answer to "what's the right setting?" is "what would you rather have go wrong?"

### The additional lie: clocks

There is a second unreliable thing you were probably relying on without noticing, and that is time itself.

Every machine has a clock, and every clock drifts. NTP corrects them, but correction means **jumping** — sometimes backwards. Two servers, asked for the current time at the same instant, will give answers that differ by milliseconds routinely and by much more when something is misconfigured. I have seen production machines an hour apart.

The practical consequence: **you cannot order events by comparing wall-clock timestamps taken on different machines.** If your logic says "the write with the later timestamp wins", and the two writes were timestamped by two different servers, then whose write survives is determined partly by NTP configuration, which is not a property anyone intended to build a business rule on. If correctness depends on ordering, you need something better, and §1.7 describes what.

## 1.6 Agreement, and how a group picks a leader

If a leader can die, something must choose the replacement. And whatever chooses must ensure that **everyone agrees** on the choice, because two leaders is the split-brain disaster we just described.

This is the problem of **consensus**, and it is solved — genuinely, provably solved, subject to the caveat below — by a family of algorithms: Paxos, which came first and is famously hard to understand; Raft, which was explicitly designed to be understandable and has largely won; and ZAB, which powers ZooKeeper.

You do not need to implement one. You need the intuition, and for that, Raft is the one to learn.

Raft divides time into numbered **terms**, and each node is in one of three states: *follower*, *candidate*, or *leader*. Normally there is one leader, sending heartbeats, and everyone else is a follower. If a follower stops hearing heartbeats, it suspects the leader is dead, increments the term number, declares itself a **candidate**, and asks every other node to vote for it. Each node votes for at most one candidate per term. A candidate that collects votes from a **majority** of the cluster becomes leader for that term.

Everything important is in that word *majority*.

Two majorities of the same set **must overlap** — it is arithmetically impossible for two disjoint groups to each contain more than half of the members. So if node A collected a majority of votes in term 5, then node B cannot also have collected a majority in term 5, because at least one node would have had to vote twice, and nodes don't. **Two leaders in the same term is impossible.** Not unlikely: impossible.

This also tells you what happens during a network partition, and the answer is elegant. Suppose a five-node cluster splits into a group of three and a group of two. The group of three can reach a majority, so it elects a leader and carries on serving. The group of two **cannot** reach a majority no matter what it does, so it cannot elect a leader, so it cannot accept writes. It goes read-only, or refuses service entirely. It does not diverge. The minority half of a partition sacrifices its own availability to protect the cluster's correctness, automatically, by construction.

This is also why you will notice that cluster sizes are always odd. Three nodes tolerate one failure (two remain, which is a majority of three). Five tolerate two. A *four*-node cluster also tolerates only one failure, because two out of four is not a majority — so the fourth node costs you money and buys you nothing. When you see a recommendation for three or five dedicated coordinator nodes in OpenSearch, Kafka, ZooKeeper, or etcd, this arithmetic is the reason.

One more safety mechanism deserves a mention, because it handles the zombie case. Suppose the old leader was only paused, not dead, and it wakes up after a new leader has been elected. It still believes it is in charge and tries to write. The defence is **fencing**: every leader is issued a monotonically increasing number (the term), it must attach that number to every write, and the storage layer **rejects any write carrying a number lower than the highest it has seen.** The zombie's writes bounce, and it discovers it has been deposed. Without fencing, a long GC pause can corrupt data even in a system that elects leaders correctly.

### Where recovery actually comes from

Elections decide *who*. Recovery of the data itself rests on three mechanisms which recur constantly in later chapters.

The first and most important is the **write-ahead log**, sometimes called a commit log or, in OpenSearch, the *translog*. The principle is: before you modify any state, append a description of the modification to a durable, append-only file, and make sure it hits the disk. Only then apply the change. If you crash at any point, you restart, read the log, and replay every operation whose effects you can't confirm. Since appending to the end of a file is the fastest thing a disk can do, this costs surprisingly little. It is the foundation of durability in nearly every storage system ever built, and in Chapter 5 you'll see Kafka take the idea to its logical conclusion by making the log *the entire product*.

The second is **checkpointing**. Replaying the log from the beginning of time gets slow once there is a lot of time. So periodically you write down your complete current state, and after that, recovery only needs to replay the log from the last checkpoint. Flink's checkpoints (§6.3) are the most sophisticated version of this idea you are likely to meet.

The third is **catch-up**. A node that was down for ten minutes comes back with data that is ten minutes stale. Rather than rebuilding it from scratch, it asks the leader for the delta and applies it. OpenSearch calls this peer recovery; a Kafka follower does it continuously as a matter of normal operation.

### Keeping failures small

There is a body of practical technique for stopping one component's failure from becoming everyone's failure. These are simple ideas, each of which prevents a specific catastrophe I promise you will otherwise eventually witness.

**Set a timeout on every remote call.** A call with no timeout is a thread that may never come back, and threads are finite. Ten thousand requests to a hung dependency will consume every thread you have, at which point your healthy endpoints stop responding too, and a small outage somewhere else has become a total outage in your service.

**Retry with exponential backoff, and add jitter.** Backoff is obvious — wait longer between each attempt rather than hammering. Jitter, meaning a random component in the delay, is less obvious and just as important. Without it, ten thousand clients that all failed at the same moment will all retry at the same moment, and then again in unison two seconds later. This is the **thundering herd**: the service comes up, is immediately flattened by a synchronised wave of retries, and goes down again, in a loop that can last hours. Randomising the delay smears the wave out.

**Use a circuit breaker.** After some number of consecutive failures to a dependency, stop calling it for a while and fail immediately instead. This protects you (no threads blocked waiting for something you know is broken) and protects it (a struggling service gets a chance to recover instead of being pinned under load).

**Build bulkheads.** Give each dependency its own thread pool or connection pool, named after the compartments in a ship's hull. Then one flooded compartment doesn't sink the vessel: a slow dependency exhausts only its own pool, and the rest of your service keeps working.

**Apply backpressure.** When you are receiving work faster than you can complete it, you have exactly two options, and one of them is bad. You can buffer the excess, which works until memory runs out and the process dies, taking the buffer with it. Or you can **tell the producer to slow down**. The second is correct, it is called backpressure, and the sophistication of a streaming system's backpressure mechanism is one of the better proxies for its overall quality. Flink propagates backpressure automatically from sink to source (§6.1); Spark makes you configure a rate limit by hand (§7.3).

**Degrade gracefully.** If Lantern's embedding service is down, returning keyword-only results is a mildly worse product. Returning an error page is a broken one. Decide in advance which features are load-bearing and which can be shed, because at three in the morning you will not want to be deciding it then.

## 1.7 Retries, duplicates, and the idea that saves you

We now arrive at the most practically useful section of this chapter. If §1.5 described the fundamental problem, this section describes the fundamental solution — and it is a solution that is simple enough to state in a sentence and deep enough that people spend careers getting it wrong.

### The problem, restated concretely

You send a request to index a document. The connection times out. You have no response.

Did the document get indexed?

You cannot know, and by now you know why you cannot know. The request may have been lost on the way. It may have arrived and been processed, with only the reply lost on the way back. Both look identical from where you stand.

You have two choices, and both are wrong. If you don't retry, and the request never arrived, you have silently lost data. If you do retry, and the request *did* arrive, you have now indexed the document twice.

So: **in any system with retries, duplicates are inevitable.** Not likely. Not an edge case to be handled someday. Guaranteed, by the structure of the situation. Anyone who tells you their pipeline cannot produce duplicates has either built something very expensive or not looked closely.

### The solution: make duplicates not matter

Since you cannot prevent the second delivery, make the second delivery *harmless*. An operation is **idempotent** if performing it many times has the same effect as performing it once.

The distinction is easiest to feel through examples. "Set the balance to 100" is idempotent: do it five times, the balance is 100. "Add 10 to the balance" is not: do it five times and you have given away fifty. "Delete user 42" is idempotent, since the second delete finds nothing to do. "Send a confirmation email" is emphatically not idempotent, and your users will tell you so.

There are four reliable ways to make an operation idempotent, roughly in order of how much I recommend them.

**Upsert on a natural key.** Instead of appending a new record, write the record *at* a key derived from the data itself. In OpenSearch, index the document with `_id` set to the document's own identifier rather than letting the server generate a random one. Now the first write creates it, and the second write overwrites it with identical content. The final state is the same either way, and the whole question of whether you delivered once or twice becomes uninteresting. This is by far the most elegant answer, and the reason it appears again and again in later chapters.

**Overwrite a whole partition.** In batch processing, don't append today's results to a table. Delete and rewrite the entire partition for today. Re-running the job for the same day produces exactly the same partition, so running it twice is indistinguishable from running it once. Chapter 4 calls this the single most useful idempotency pattern in batch data engineering, and means it.

**Carry an idempotency key.** The client generates a unique identifier for the logical operation — not for each attempt, for the *operation* — and attaches it to every retry. The server keeps a record of the keys it has processed, and when it sees a repeat, it returns the stored result of the first attempt without doing the work again. This is how payment APIs avoid charging your card twice, and it is how Kafka's producer eliminates duplicates internally (§5.4).

**Keep a dedup window.** Remember the identifiers you have recently seen, in Redis or RocksDB or a stream processor's state, and drop repeats. This works, but note the word *recently*: the set must be bounded by a time-to-live or it grows without limit, and a duplicate that arrives after the window expires slips through. It is the fallback when the first three options aren't available.

The payoff is the sentence I would most like you to take away from this chapter:

> **Deliver at least once, and make the receiver idempotent. You now have exactly-once *effects*, for a fraction of the cost of exactly-once *delivery*.**

This is the pattern that real pipelines are built on. Chapter 8 traces it through every hop of a complete architecture.

### Retrying well

A few rules, each of which encodes a scar.

Retry only errors that could plausibly succeed on a second attempt: timeouts, connection resets, HTTP 503, rate-limit responses. Never retry a 400. The request is malformed; it will be malformed again in two seconds, and again in four, and you have built a machine for generating log noise.

**Cap the number of attempts and the total elapsed time.** Unbounded retrying turns a transient failure into a permanent resource leak.

Watch for **retry amplification** in layered systems. Three retries at the API gateway, times three at the service, times three at the data client, is twenty-seven requests to a struggling backend from one user action. Layers multiply. Retry at one layer — usually the outermost one that can make a sensible decision — and pass failures through elsewhere.

Consider a **retry budget**. Instead of a per-request retry count, enforce a global rule: retries may be at most ten percent of total traffic. In normal conditions this is invisible. During a broad outage, it is the difference between a degraded service and a self-inflicted denial-of-service attack on your own infrastructure.

Finally, decide what happens when you give up. A message that cannot be processed must go somewhere — a **dead-letter queue** — with enough context to diagnose it later. The alternative is that one malformed record blocks a partition forever, and the whole pipeline stops because of one bad row from 2019.

### Ordering, without clocks

We established in §1.5 that you cannot trust wall-clock timestamps from different machines. So how do you order events?

The first realisation is that **you usually don't need global ordering.** A total order across every event in a distributed system requires funnelling everything through a single sequencer, which destroys the parallelism you built the system for. But look at what you actually need: you need the events *about the same thing* to be ordered. Two edits to the same document must be applied in the right order. An edit to document A and an edit to document B have no meaningful relationship at all.

So: **per-key ordering is achievable, cheap, and almost always sufficient.** Kafka gives you exactly this — strict ordering within a partition, and the ability to route all events with the same key to the same partition. Key your events by document ID and you get perfect per-document ordering while still processing a thousand documents concurrently. This is such a good deal that it is essentially the defining design pattern of Chapter 5.

When you do need to reason about causality across the system, the tool is a **logical clock**, which replaces "what time is it" with "what have I seen".

**Lamport timestamps** are the simplest version. Each node keeps a counter. It increments on every local event, and attaches the counter to every message it sends. On receiving a message, a node sets its counter to `max(own, received) + 1`. The result: if event A caused event B, then A's timestamp is definitely lower than B's. The limitation is the converse — a lower timestamp does *not* prove causation, so you cannot distinguish "A caused B" from "A and B were concurrent", which is exactly what you need to know to detect a conflict.

**Vector clocks** fix that by keeping a counter *per node* rather than one counter. Comparing two vectors tells you whether one strictly dominates the other (causally ordered) or whether each is ahead in some dimension (genuinely concurrent, therefore a conflict requiring resolution). The cost is that the vector grows with the number of nodes, which is why vector clocks appear in systems with modest, stable node counts and not in systems with thousands of ephemeral workers.

### Event time versus processing time

One more distinction, which Chapter 6 is built entirely around.

**Event time** is when something happened in the world: the moment the user clicked, stamped by the device that observed it. **Processing time** is when your system got around to looking at it.

These differ, sometimes wildly. A phone with no signal buffers events for forty minutes and uploads them all at once. A Kafka consumer group falls behind and processes an hour of backlog in five minutes. A retry redelivers something from ten minutes ago.

If you compute "orders in the 10:00–10:05 window" by processing time, your answer depends on network conditions and consumer lag, and re-running the computation over the same data gives a different result. That is not a metric; it is a rumour. Correct analytics use event time, which raises the difficult question of how long to wait for stragglers before declaring a window complete. The answer is a mechanism called a **watermark**, and it is beautiful, and it is in §6.4.

### The delivery guarantee vocabulary

Three phrases you will hear constantly, and which are much simpler than they sound. They describe where you put the acknowledgement relative to the work.

**At-most-once**: acknowledge first, then process. If you crash in between, the message is gone and nobody will send it again. You never see duplicates; you sometimes lose data. Acceptable for metrics and debug logs, where a missing sample is invisible.

**At-least-once**: process first, then acknowledge. If you crash in between, nobody acknowledged, so the message is redelivered. You never lose data; you sometimes see duplicates. **This is the right default for almost everything**, because §1.7's idempotency makes the duplicates harmless.

**Exactly-once**: the message affects the system's state precisely once. And here the important clarification: **exactly-once *delivery* over a network is impossible.** It is the two-generals problem — you cannot have a finite exchange of messages that guarantees both parties know the outcome. What real systems provide is exactly-once *processing semantics*, achieved either by committing the output and the input position in a single atomic transaction (Kafka's transactions, §5.4; Flink's two-phase commit sinks, §6.3) or by making the output idempotent so replays overwrite rather than accumulate.

```
receive → acknowledge → process              at-most-once
receive → process → acknowledge              at-least-once
receive → (process and acknowledge atomically)   exactly-once-ish
```

That diagram is worth more than the three paragraphs above it. When someone tells you their system is exactly-once, the useful question is: *where exactly is the acknowledgement, and what is it atomic with?*

## 1.8 Consistency, and the theorem everyone misquotes

We need to talk about what a reader is guaranteed to see. But first, a warning about vocabulary, because the word *consistency* is used for three unrelated things and the collision causes real confusion.

The **C in ACID**, in database transactions, means "the database enforces your declared constraints" — foreign keys, uniqueness, check constraints. It has essentially nothing to do with distributed systems.

**Replica consistency** means "all the copies hold the same bytes". Useful operationally, not what we mean here.

A **consistency model** is a contract about what a reader may observe. That is the one that matters, and it is what the rest of this section is about.

### A spectrum, from expensive to cheap

The strongest useful guarantee is **linearizability**. It says: the system behaves *as if* there were only one copy of the data, and every operation takes effect at a single instant somewhere between when you called it and when it returned. Once a write returns successfully, every subsequent read — from any client, against any replica — sees it. There is a single, real, global order of events and everyone agrees on it.

This is what your single-machine program gave you for free in §1.1, and it is what you have been missing ever since. You can buy it back, but the price is a round trip to a quorum on every operation, and complete unavailability in the minority side of a network partition.

Slightly weaker is **sequential consistency**: everyone sees operations in the same order, but that order need not match real time. Then **causal consistency**: operations that are causally related are seen in that order everywhere, while genuinely concurrent operations may be seen in different orders by different observers. This turns out to be remarkably cheap and remarkably close to what humans actually expect, which is why it gets a lot of research attention.

Weaker still are the per-session guarantees we met in §1.4 — read-your-writes, monotonic reads — which are usually implemented as routing tricks rather than as properties of the storage layer.

And at the bottom, **eventual consistency**: if writes stop, the replicas will converge. Note everything this does not say. It does not say when. It does not say what you might read in the meantime. It is the weakest guarantee that is still a guarantee, and it is also, for a very large class of applications, completely fine.

### CAP, stated properly

Here is the theorem as it is usually recited: "in a distributed system you can have consistency, availability, and partition tolerance — pick two." This framing is so misleading that it would be better not to know it.

The accurate statement is:

> **When a network partition occurs, a distributed system must choose between consistency (linearizability) and availability (every non-failing node answers requests).**

The difference is that **partition tolerance is not a choice.** Networks fail. Cables get unplugged, switches reboot, cloud availability zones lose connectivity to each other. You do not get to select "no partitions" from a menu. So the theorem is not about picking two of three; it is about deciding, *in advance*, what your system should do during a partition. And there are only two honest answers.

You can refuse to serve. A node that cannot reach a quorum stops accepting writes, and possibly reads, rather than risk diverging from its peers. This is the **CP** choice: correctness preserved, availability sacrificed. ZooKeeper does this, etcd does this, Kafka with `acks=all` does this, and the minority side of a Raft partition does it automatically, as we saw in §1.6.

Or you can keep serving. Every node answers with whatever it knows, divergence is accepted, and reconciliation happens later — by last-write-wins, by application merge, by CRDT. This is the **AP** choice: availability preserved, correctness deferred. Cassandra with low quorums does this. DNS does this. A shopping cart should probably do this, because a customer who cannot add an item to their basket is a lost sale, whereas a customer who briefly sees a stale basket is merely mildly confused.

Note also what CAP says about the non-partitioned case: **nothing at all.** When the network is healthy you can have both C and A. Which brings us to the more useful formulation.

### PACELC, which you will use more often

**PACELC** extends CAP with the clause that actually governs your daily decisions:

> **If Partitioned**, choose **A**vailability or **C**onsistency. **Else** — in normal, healthy operation — choose **L**atency or **C**onsistency.

That second half is the one that matters, because partitions are rare and normal operation is constant. And it captures something CAP hides: even with a perfect network, stronger consistency costs latency, because before you can answer you must talk to other machines. Linearizability isn't expensive only during disasters. It's expensive on Tuesday afternoon, on every single request, forever.

Most of your real engineering choices live in that "else" branch.

### What Lantern chooses

It is worth making this concrete, because abstract discussion of consistency models is exactly the sort of thing that feels clear while reading and evaporates on contact with a design review.

Lantern's search index is **deliberately not strongly consistent**. When someone edits a document, the edit is durable in PostgreSQL immediately, and it becomes findable in search a couple of seconds later. In exchange for that couple of seconds, Lantern gets to batch writes, to index in parallel across twenty shards, to serve reads from replicas without coordination, and to stay up when a machine dies.

That is a *good trade* — but only because someone made it on purpose, wrote down the number ("edits are searchable within five seconds, ninety-ninth percentile"), and monitors it. The failure mode is not choosing eventual consistency; it is choosing it by accident and discovering the staleness window is forty minutes during a backlog, from a user complaint.

You will meet the specific mechanism in §2.5, where it turns out that OpenSearch's one-second *refresh interval* is the largest single term in Lantern's end-to-end latency budget.

## 1.9 Speed: what it means and where it goes

Two words get used interchangeably and mean opposite things.

**Latency** is how long one operation takes. **Throughput** is how many operations complete per second.

They are not reciprocals, and improving one very often damages the other. The mechanism that makes this true is **batching**, and since batching appears in every chapter of this book it is worth understanding once, properly.

Suppose you are sending documents to be indexed. Sending each one immediately gives the lowest possible latency for that document. But each send carries fixed overhead — a network round trip, a system call, a protocol handshake, a fresh compression context. If instead you wait fifty milliseconds and send a hundred documents together, you pay that overhead once instead of a hundred times, and you compress a hundred similar documents together rather than each alone, which compresses far better. Throughput might improve tenfold. And every one of those documents waited up to fifty milliseconds longer than it needed to.

That is the trade, and you will meet it wearing different hats: Kafka's `linger.ms`, the size of an OpenSearch `_bulk` request, Spark's micro-batch trigger interval, Flink's checkpoint interval, the target file size in a data lake. Same curve every time. When you understand that they are all the same knob, a great deal of configuration stops feeling arbitrary.

### Stop reporting averages

If you take one operational habit from this book, take this one: **report percentiles, not averages.**

An average latency of 50 milliseconds is consistent with every request taking 50 milliseconds, and it is equally consistent with 95% of requests taking 10 milliseconds and 5% taking a full second. Those are completely different systems, one of which is fine and one of which has furious users. The average cannot tell them apart, which means the average is nearly useless.

Report p50 (the median — half of requests are faster), p95, p99, p99.9, and the maximum. The p99 is where the users who complain live.

And in a distributed system the tail matters much more than intuition suggests, for a reason we already met in §1.3. Suppose a Lantern search page makes twenty parallel backend calls, and each call independently has a 1% chance of being slow. The probability that *all twenty* are fast is 0.99²⁰, which is about 82%. So **18% of page loads are slow**, even though only 1% of individual calls are.

This is **tail amplification**, and it is exactly what scatter-gather search does across shards on every query. Your user-facing latency is governed by your p99, not your median, and the more pieces you fan out to, the more true that becomes.

There are techniques for fighting it. **Hedged requests**: when a request exceeds its expected p95, send a duplicate to a different replica and use whichever answer arrives first — you pay a small amount of extra load to cut the tail dramatically. And simply having **fewer shards per query**, which is the same conclusion §1.3 reached from a different direction.

### One equation worth knowing

**Little's Law** relates the three quantities you care about:

```
L = λ × W

concurrency = arrival rate × time in system
```

If Lantern serves a thousand searches a second and each takes two hundred milliseconds, then on average two hundred searches are in flight at any moment. Which means you need at least two hundred threads, or connections, or executor slots — and if you have a hundred, requests are queueing, and your latency is not two hundred milliseconds any more.

This is extraordinarily useful for sizing thread pools, and it is also how you understand the phenomenon that ruins more systems than any other. As utilisation ρ approaches 1 — as you push a system towards using every bit of capacity you paid for — queueing time scales roughly as `1/(1−ρ)`. At 50% utilisation, waiting is negligible. At 90%, it is about ten times worse. At 95%, twenty times. At 99%, a hundred times.

The system doesn't degrade gracefully as it fills up; it degrades *hyperbolically*. Which means: **never run a latency-sensitive system near full utilisation.** Sixty to seventy percent is a healthy target. The idle thirty percent is not waste; it is the headroom that keeps your p99 from exploding, and it is the cheapest latency improvement you will ever buy.

### Scaling up and scaling out

**Vertical scaling** means a bigger machine. It is simple, immediate, requires no code changes, and should almost always be your first move — a larger instance type is vastly cheaper than three months of engineering. It has a ceiling, and it leaves you with one large failure domain.

**Horizontal scaling** means more machines, and it is effectively unbounded, but it demands everything this chapter has described: partitioning, replication, coordination, failure handling.

The crucial variable is state. **Stateless** services scale horizontally almost for free — put ten copies behind a load balancer and you are done, because no copy needs to know anything the others know. **Stateful** services are hard, because adding a machine means moving data to it, which means rebalancing, which means a period where ownership is ambiguous. This is the entire reason databases are harder to scale than web servers, and it is why so much architecture consists of pushing state to the edges and keeping the middle stateless.

Two limits are worth knowing by name. **Amdahl's law** observes that your speedup is capped by whatever fraction of the work cannot be parallelised: if 5% of a job is inherently serial, then twenty times faster is your ceiling no matter how many machines you rent. The **Universal Scalability Law** goes further and rather more pessimistically: beyond some point, adding machines makes the system *slower*, because coordination and crosstalk grow faster than the work you added. There is an optimal cluster size, and it is possible to be past it.

### Finding the bottleneck

When a distributed system is slow, the instinct is to guess. Resist it; guessing has a poor track record, and measurement is not much harder.

The useful discipline is the **USE method**: for every resource, check three things — **U**tilisation (what fraction of the time is it busy?), **S**aturation (how much work is queued waiting for it?), and **E**rrors. Saturation is the one people forget and the one that usually gives it away, because a queue is the physical evidence that demand exceeded supply.

Here are the resources, roughly in order of how often they turn out to be the culprit.

**Disk I/O** comes first and it isn't close, at least for databases and search engines. Specifically: random reads, IOPS limits, and the latency of `fsync`. If your working set doesn't fit in the filesystem cache, every query goes to disk, and no amount of CPU will save you.

**Network** is second, but usually not in the way people expect. Raw bandwidth is rarely the issue; **round trips** are. A loop that makes one remote call per item — the distributed version of the N+1 query problem — will be slow no matter how fat your pipe is, because you are paying latency serially, a thousand times.

**Memory** is third, and its most interesting failure mode is garbage collection. A two-second stop-the-world GC pause makes a healthy node **indistinguishable from a dead one** — this is precisely the ambiguity of §1.5 arriving in your own JVM. The node stops heartbeating, gets declared dead, its shards get reallocated, and then it wakes up and rejoins, triggering another reallocation. A cluster can spend hours thrashing this way, and the root cause is a heap setting.

**CPU** is fourth: compression, serialisation, JSON parsing, relevance scoring, embedding inference.

And fifth, the invisible one: **locks and coordination**. This is the bottleneck that hides, because it presents as *low* CPU utilisation combined with *bad* latency. Everything looks idle and nothing is fast. If your dashboards say there is plenty of headroom and your users say the system is slow, look for contention.

The method is always the same. Measure end to end. Find the largest span. Drill into it. Repeat. Distributed tracing and flame graphs will beat your intuition every time, including when your intuition is very good.

## 1.10 Ten questions for any system

I want to leave you with something portable.

Every chapter after this one describes a distributed system, and so does every system you will meet at work. They differ enormously in vocabulary and hardly at all in structure. If you can answer these ten questions about a system, you understand it well enough to use it, to argue about it, and to have a decent first hypothesis when it breaks.

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

Twenty minutes with the documentation and those ten questions will get you further than a week of reading tutorials. Try it on Kafka when you reach Chapter 5; I think you'll find the chapter mostly answers them in order.

---

## Where we are

We began with a program that worked, and we broke it by adding machines. In exchange we got capacity, throughput, and survivability, and we paid with an inability to tell dead from slow, a family of consistency problems, an unavoidable population of duplicate messages, and a latency distribution with a long unpleasant tail.

We also collected the tools for coping. Partitioning divides work but introduces skew and scatter-gather. Replication protects data but introduces lag and the synchronous/asynchronous dilemma. Quorums let a group agree despite failures, and majority overlap is why. Write-ahead logs make recovery possible. And idempotency — writing to a deterministic key so that a repeat is harmless — converts the impossible problem of exactly-once delivery into the tractable problem of exactly-once effect.

Every one of those ideas is about to reappear. In Chapter 2 we build Lantern's search index, and you will watch OpenSearch make its own particular choices about shards, replicas, durability, and how stale a read is allowed to be. You already know what questions to ask of it.
