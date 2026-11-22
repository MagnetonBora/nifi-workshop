---
title: "Lab 3 — A Split flow versus a Record flow"
aliases:
  - exercises/lab-3
---

> From the materials of [[materials/day-1|Day 1 — Apache NiFi Fundamentals]]. The lab stand is described on the [[labs/index|Labs]] page; loading the reference solutions — on the [[solutions/index|Solutions]] page.
> Solution: [[solutions/lab-3|Lab 3 — solution]].

**Goal.** Quantitatively compare two variants of processing the same data set.

**Source data.** `GenerateRecord` (the processor appeared in 1.20.0) creates a FlowFile with N random records according to a given schema. Or `GenerateFlowFile` with NDJSON text.

**Measurement method.**

1. The same input: for example, 100 FlowFiles of 100,000 records each (configured in `GenerateRecord`: *Number of Records*). Mind the scale: variant A turns such an input into ~10 million FlowFiles, which does not fit into the training heap (1–2 GB on the stand). On a laptop, start with 1 FlowFile of 10,000 records (*Run Once* on the generator) and increase the volume while watching the heap and back pressure.
2. Freeze the initial conditions: clean queues, the same `Concurrent Tasks`.
3. Run variant A and wait until all the data is processed; record: the processing time, the *Tasks/Time* values, *Bytes Read/Written*, the number of FlowFiles in the queues, heap and GC (System Diagnostics), and the number of provenance events.
   - **Before *Run Once*:** write down the GC values: ☰ → **Summary** → **System Diagnostics** → **JVM** → *Count* and *Time* for G1 Young and G1 Old.
   - **Start:** start every processor of the variant except the generator, then right-click the generator → **Run Once** (not *Start*: it would keep producing new batches).
   - **Done** when every queue of the variant shows `Queued 0`.
   - **Then record:**

     | # | Metric | Where to click | What to record |
     |---|---|---|---|
     | 1 | Processing time | ☰ → **Data Provenance**, sort by *Date/Time* | the last event minus the `CREATE` event of `GenerateRecord` |
     | 2 | *Tasks/Time* | each processor card → *Tasks/Time* row | the value per processor and the total |
     | 3 | *Bytes Read/Written* | the variant's group card (one level up) → *Read/Write* | Read and Written |
     | 4 | FlowFiles in the queues | right-click the connection after the split → **View Status History** → *Queued Count* | the peak and the final value (should be 0) |
     | 5 | Heap | ☰ → **Node Status History** → *Used Heap* | the peak during the run |
     | 6 | GC | ☰ → **Summary** → **System Diagnostics** → **JVM** | *Count* and *Time* after the run minus before |
     | 7 | Provenance events | ☰ → **Data Provenance** → **Search** limited to the run's time window | *M* from "Displaying N of **M**" |

   - **Keep in mind:** the statistics on the processor cards cover a sliding 5-minute window, so wait 5 minutes between runs of the same variant. The processing time of variant A includes the `MergeContent` wait (*Max Bin Age*).
4. Stop, clear the queues, run variant B with the same input data; repeat the measurements.
5. Repeat 3 times and take the median.

**Expected behavior (qualitatively).** Variant B processes the data noticeably faster and with less load on the heap/GC; the number of provenance events and entries in the FlowFile Repository is orders of magnitude smaller. The absolute values depend on the hardware — **the numbers must be obtained in your environment**, not taken from a textbook.

**Correctness check.** The sum of the records in `valid` and `rejected` = the input count; the `record.count` attributes (`QueryRecord` sets it on the output FlowFiles) match. For example, with 10,000 records on the input: `valid` 4910 + `rejected` 5090 (the `ROUTE` event of `QueryRecord` in provenance).
