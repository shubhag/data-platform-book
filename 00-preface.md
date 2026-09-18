# Preface

## What this book is

This is a book about how very large amounts of data get stored, moved, processed, indexed, and searched — and, more importantly, how all of that is made to work *reliably* on hardware that fails, over networks that drop packets, with software that gets deployed badly on a Friday afternoon.

It is written for an engineer who is competent but new to this particular territory. You know how to program. You have used a database. You may have run a query against a search engine, or written a job that reads a file and writes another file. What you do not yet have is the *map* — the sense of how these pieces relate, which concepts are fundamental and which are vendor vocabulary, and what the experienced people in the room are actually worried about when they push back on a design.

The goal is not mastery. Mastery of any one of these systems takes years and is mostly acquired through outages. The goal is **fluency**: being able to follow a design discussion without silently losing the thread, read the official documentation without drowning, form an opinion and defend it, and — when something breaks at 2 a.m. — know which of four possible explanations to check first.

## How it is organised

There are nine chapters and two appendices.

**Chapter 1, *The Machine That Isn't One*,** is the foundation. It is about what happens to your assumptions when a program stops running on one computer and starts running on fifty. Everything in the rest of the book is a special case of what this chapter describes. If you read only one chapter, read this one.

**Chapter 2, *Finding Things*,** is about OpenSearch, and through it, about search engines generally: how text is taken apart and rebuilt into a structure that can answer "which documents are about this?" in milliseconds across a billion documents.

**Chapter 3, *Meaning as Geometry*,** is about semantic search: the strange and rather beautiful idea that you can turn meaning into coordinates, and then find related ideas by measuring angles.

**Chapter 4, *Moving Data*,** is about the discipline of data engineering — file formats, batch versus stream, data quality, and the single most important practical skill in the field, which is making things safe to run twice.

**Chapters 5, 6, and 7** are about the three systems that dominate modern data movement and processing: **Kafka** (*The Log*), **Flink** (*Time*), and **Spark** (*The Unbounded Table*). Each chapter is organised around the one central idea that system is built on, because once you have that idea, the rest of the system is a consequence of it.

**Chapter 8, *The Warehouse and the Lake*,** is about analytics: why the questions a business asks need a completely different kind of database, how columnar engines answer them, and how the modern answer — the lakehouse, Delta Lake, Databricks, and the medallion architecture — is assembled. It assumes no prior knowledge of OLAP.

**Chapter 9, *The Whole Machine*,** assembles everything into a single working architecture and traces one piece of data through it from end to end. It also contains a self-test, because reading creates a comfortable illusion of understanding that questions dispel very quickly.

The appendices hold a glossary and a set of reference tables — the things you will want to look up rather than read.

## A note on how to read it

Chapters build on each other, and I cross-reference heavily, in the form *§3.4* (meaning chapter 3, section 4). Follow the references when a concept feels underexplained; it usually means it was introduced properly somewhere else and I am only reminding you of it here.

But the most useful advice I can give is this: **reading is not enough, and the gap is larger than you expect.** These are operational systems, and the understanding that matters is the kind you get from watching them misbehave. Somewhere in the middle of Chapter 2, stop, start a local OpenSearch, index a few thousand documents, and run the `_analyze` endpoint against a sentence. You will learn more in ten minutes of that than in an hour of my prose about tokenizers. In Chapter 5, run a local Kafka, start a consumer, kill it mid-batch, and watch duplicate records appear in your output. Then make the write idempotent and watch them stop mattering. That sequence — break it, understand why, fix it structurally — is how this material actually gets learned.

## A running example

Abstract discussion of distributed systems tends to slide off the mind. So throughout the book I will refer to a single fictional system, which I will call **Lantern**.

Lantern is a search platform inside a mid-sized company. It indexes a few hundred million documents — think internal wiki pages, support tickets, technical specifications, and candidate profiles — and lets employees search them. Documents live in a PostgreSQL database that other applications write to. They change constantly: a few hundred edits a second at peak. Employees expect their searches to be fast (under 200 milliseconds), relevant (the thing they wanted is in the top five), and current (an edit made a minute ago should be findable).

That one sentence contains, hiding inside it, nearly every problem in this book. Fast means partitioning and caching. Relevant means ranking, and eventually embeddings. Current means a streaming pipeline. *Hundreds of millions* means it doesn't fit on one machine. And "expects" means somebody gets paged when it doesn't work.

We will build Lantern, piece by piece, across nine chapters.

## What I have deliberately left out

There is no code you can run directly. The snippets are illustrative — enough to make a concept concrete, not enough to be a tutorial. Tutorials go stale; the concepts in this book will still be true in ten years.

I also avoid benchmark numbers wherever I can, and where I do give a figure ("aim for 10 to 50 gigabytes per shard") you should read it as *the shape of the right answer*, not as a constant. Hardware changes. The reasoning behind the number is the durable part, so I always try to give you the reasoning.

Finally, I am opinionated. Where there is a genuine trade-off I will lay out both sides, but where the industry has largely converged on an answer I will tell you the answer rather than pretending the question is open. You can disagree later, once you have grounds to.

---

*Let's begin with the thing that makes all of this hard.*
