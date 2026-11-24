---
title: "Choosing the FlowFile Size"
---
## Introduction

Apache NiFi has no universal optimal FlowFile size. It depends on the nature of the data, the processors in the flow, the throughput of the external system, and the way the data is delivered.

There is, however, one important principle: **it is usually cheaper to move fewer, relatively large FlowFiles than a huge number of small ones.**

This summary explains why, gives practical size ranges, and shows how to tune batching when the data goes to an external system.

## 1. Why the Size Matters

- Every FlowFile has its own attributes and metadata.
- NiFi tracks every FlowFile in the **FlowFile Repository** and stores its content in the **Content Repository**.
- A large number of small FlowFiles increases the overhead of scheduling, queues, repositories, and processing.
- Very large FlowFiles increase latency, memory requirements of some processors, and the cost of reprocessing after an error.

## 2. Practical Ranges

For regular (not Stateless) NiFi, a reasonable starting point:

| FlowFile Size | Assessment |
|---|---|
| **Less than 1 KB** | Too small for high-load flows |
| **1–100 KB** | Acceptable, but may add unnecessary overhead |
| **100 KB – 1 MB** | A good starting point |
| **1–10 MB** | Good for batch delivery |
| **10–100 MB** | Depends on the processors and the receiver |
| **More than 100 MB** | Needs dedicated testing |

These are practical guidelines, **not** Apache NiFi requirements.

**Important nuance:** some processors read the whole FlowFile into memory, while others process it as a stream (`InputStream`). These are fundamentally different scenarios: a 100 MB FlowFile is fine for a streaming processor and dangerous for one that loads the content into the heap.

## 3. Delivery to an External System

Start with **1–5 MB per FlowFile** and measure:
- throughput (MB/s);
- latency;
- FlowFiles per second;
- processing time per FlowFile;
- the number of retries after errors.

### The Size Depends on the Protocol

| External System | What to Consider |
|---|---|
| **REST API** | HTTP request size limits, latency, rate limits |
| **Kafka** | Batch size, compression, `max.request.size` on the producer and `message.max.bytes` on the broker (both about 1 MB by default) |
| **SFTP** | File size, transfer speed, parallel uploads |
| **JDBC** | The number of records per batch matters more than the FlowFile size |
| **Another NiFi** | Site-to-Site: the number of FlowFiles and the amount of data transferred |

## 4. How to Configure Batching

If the incoming FlowFiles are small, you do not need a separate FlowFile for every record. Merge them with `MergeRecord` or `MergeContent` before sending, based on size and timeout.

Example settings:
- **Minimum size:** 1 MB (`Minimum Bin Size` in `MergeRecord`, `Minimum Group Size` in `MergeContent`).
- **Maximum size:** 5 MB (`Maximum Bin Size` / `Maximum Group Size`).
- **Max Bin Age:** 1–5 seconds, so a half-full bin is still sent at low load.
- **Maximum Number of Bins:** chosen according to parallelism and load.

The Apache NiFi documentation describes exactly this approach: micro-batches from a few KB to a few MB.

## 5. Do Not Forget About Parallelism

Example:
- FlowFile = 5 MB;
- Concurrent Tasks = 4;
- 4 FlowFiles are processed at the same time.

That gives up to **20 MB of data in processing on one node**, not counting extra buffers and internal copies.

Therefore, increasing the FlowFile size does not always increase throughput.

## Brief Recap
- There is no universal optimal size; fewer, larger FlowFiles are usually cheaper than many small ones.
- A good starting range is 100 KB – 1 MB inside the flow and 1–5 MB for delivery to an external system.
- Check whether the processors stream the content or load it into memory.
- Respect the receiver's limits: HTTP request size, Kafka `max.request.size` / `message.max.bytes`, JDBC batch size.
- Merge small FlowFiles with `MergeRecord` / `MergeContent` using a minimum size, a maximum size, and a Max Bin Age.
- Size × Concurrent Tasks is the amount of data in processing at once; measure instead of just increasing the size.
