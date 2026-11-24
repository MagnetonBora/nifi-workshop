---
title: "Lab 9 — A performance test and tuning"
aliases:
  - exercises/lab-9-solution
  - labs/lab-9-solution
---

> From the materials of [[materials/day-3|Day 3 — Production, Security, and Performance]]. The lab stand is described on the [[labs/index|Labs]] page; loading the reference solutions — on the [[solutions/index|Solutions]] page.
> Task: [[labs/lab-9|Lab 9 — A performance test and tuning]].

**Structure of the reference flow.** One group *Lab 9 - Performance test* with the Parameter Context `lab9-params` (`out.dir`, the input schema `log.schema.in`, the output schema `log.schema.out`) — the pipeline of [[labs/lab-6|Lab 6]] part 1, with a generator that can produce a large, repeatable workload and counters that make every record visible.

## Architecture

```text
GenerateRecord (10,000 records / FlowFile, Run Schedule 0 sec)
   ║ workload connection: Back Pressure Object Threshold 200 = 2,000,000 records, Size 2 GB
   ▼
ValidateRecord ─ invalid ─► count invalid   (lab9.records.invalid += record.count)
   │           └ failure ─► count failed    (lab9.flowfiles.failed += 1)
   │ valid
   ▼
UpdateRecord (/ingest_ts) ► MergeRecord (10,000–50,000 records, Max Bin Age 1 sec)
   ► UpdateCounter (lab9.records.out += record.count) ► UpdateAttribute (filename by second) ► PutFile #{out.dir}
```

## Configuration

**Controller Services.**

| Service | Type | Settings | Used by |
|---|---|---|---|
| `writer-as-is` | `JsonRecordSetWriter` | *Inherit Record Schema*, *One Line Per Object* | `GenerateRecord` |
| `reader-inferred-schema` | `JsonTreeReader` | *Infer Schema* | `ValidateRecord` in scenario A |
| `reader-input-schema` | `JsonTreeReader` | *Use 'Schema Text' Property* = `#{log.schema.in}` | `ValidateRecord` from scenario B |
| `reader-output-schema` | `JsonTreeReader` | *Use 'Schema Text' Property* = `#{log.schema.out}` | `UpdateRecord`, `MergeRecord` |
| `writer-output-schema` | `JsonRecordSetWriter` | *Use 'Schema Text' Property* = `#{log.schema.out}`, *One Line Per Object* | `ValidateRecord`, `UpdateRecord`, `MergeRecord` |

**Processors.**

| Component | Settings |
|---|---|
| `GenerateRecord` | Record Writer `writer-as-is`; Number of Records `10000`; Schema Text — the `GeneratedLogEvent` schema (`ts`, `level` enum, `svc`, `msg`, `user {id, pwd}`); Run Schedule `0 sec` — as fast as back pressure allows |
| the workload connection | Back Pressure Object Threshold `200`, Data Size Threshold `2 GB` (one FlowFile is ~1.9 MB, the 200 are ~0.37 GB) |
| `ValidateRecord` | Record Reader — the subject of scenarios A/B; Record Writer `writer-output-schema`; Schema Access Strategy `Use 'Schema Text' Property` = `#{log.schema.in}`; Strict Type Checking `true`. `invalid` and `failure` go to the counters instead of being auto-terminated |
| `UpdateRecord` | Reader `reader-output-schema`, Writer `writer-output-schema`; `/ingest_ts` = `${now():format("yyyy-MM-dd'T'HH:mm:ss.SSSXXX", "UTC")}`, Literal Value |
| `MergeRecord` | Reader `reader-output-schema`, Writer `writer-output-schema`; Minimum / Maximum Number of Records `10000` / `50000`; Max Bin Age `1 sec` |
| `UpdateCounter` | Counter Name `lab9.records.out`, Delta `${record.count}` |
| `count invalid` / `count failed` | `lab9.records.invalid` / `${record.count}`; `lab9.flowfiles.failed` / `1` |
| `UpdateAttribute` + `PutFile` | `filename` = `lab9-${now():format('ss')}.ndjson`; Directory `#{out.dir}`, Conflict Resolution `replace` — at most 60 files on disk |

Two settings exist only for the measurement:

- **The workload connection.** With a fixed input rate the pipeline either keeps up or not, and tuning shows nothing while it keeps up. Back pressure turns the generator into a "fill exactly 2,000,000 records and stop" device; every run then processes the same data, and *records/s = 2,000,000 ÷ seconds* compares the scenarios directly.
- **Max Bin Age 1 sec.** With `10 sec` the last, incomplete bin of a run waited ten seconds while everything else stood still — in scenario D this was a third of the run. At ~250,000 records/s the bins fill by size in a fraction of a second, so the age only flushes the end of the workload.

## Results on the stand

One run of `make lab09-measure`: NiFi 2.12.0 in Docker Desktop on a 12-core laptop, heap 1 GB / 2 GB, 3 runs per scenario, the median shown.

| Scenario | Change (cumulative) | records/s | vs A | Runs | CPU s / 1M records | GC, ms | Busiest processor |
|---|---|---|---|---|---|---|---|
| A | `ValidateRecord` Reader with *Infer Schema* | 263,000 | 1.00× | 230k / 263k / 282k | 10.0 | 94 | `ValidateRecord` = `UpdateRecord` (0.87 threads) |
| B | + explicit schema (`reader-input-schema`) | 266,000 | 1.01× | 285k / 265k / 266k | 9.0 | 72 | `UpdateRecord` (0.80) |
| C | + Concurrent Tasks `4` on `UpdateRecord` | **309,000** | **1.17×** | 334k / 309k / 286k | 8.8 | 79 | `UpdateRecord` (1.08), `MergeRecord` queue ×2 |
| D | + `MergeRecord` 100,000 / 200,000 | 248,000 | 0.94× | 248k / 248k / 310k | 8.7 | 70 | — (all processors less busy) |

`lab9.records.invalid` and `lab9.flowfiles.failed` were 0 in all twelve runs. Heap stayed under 800 MB, GC under 100 ms per run.

## What the numbers mean

**A → B: cheaper, not faster.** With *Infer Schema* `ValidateRecord` reads every 10,000-record FlowFile twice: once to infer the schema, once to read it. The explicit schema removes the first pass: `ValidateRecord` drops from 0.87 to 0.67 busy threads, the CPU per million records falls by 10% (10.0 → 9.0 s), GC by a quarter. But the throughput does not move: `UpdateRecord` is just as busy (0.87) and becomes the busiest processor in B — the pipeline runs at the speed of its slowest stage, and making another stage faster only frees CPU. This is the central lesson of the lab: **optimize the bottleneck, not the processor that looks expensive.** (In an earlier run with 5,000,000 records the same change gave +9% and cut the CPU per million records almost in half, 15.6 → 8.8 s; the direction is stable, the size varies with the data and the machine.)

**B → C: parallelism at the bottleneck pays.** Four Concurrent Tasks on `UpdateRecord` let it work on several FlowFiles at once: 1.08 busy threads on average instead of 0.80, +17% throughput. The bottleneck moves: the queue in front of `MergeRecord` doubles (16 → 33 FlowFiles), and `ValidateRecord` (one task, 0.77 threads, a long queue of the workload in front of it) is next. The next step would be Concurrent Tasks on `ValidateRecord` — not on everything at once: every task is a thread from the shared pool (*Maximum Timer Driven Thread Count*, 10 by default), and more threads than cores only adds contention.

**C → D: larger batches made it slower.** All processors were *less* busy, yet the run took longer: `MergeRecord` now waits until 100,000 records (10 FlowFiles) have arrived, then writes one 20–40 MB file in a single task, and `PutFile` writes it in one go. The stages overlap less — the pipeline works in larger, rarer steps. Larger batches pay off when the destination has a high cost per request (a database transaction, an HTTP call, Kafka, S3), not when it is a local file. Keep D only if the destination needs it; here it is rolled back.

**The spread of the runs.** Single runs of the same scenario differ by up to 20% (D: 248k–310k). On a laptop other processes, Docker Desktop and the JIT warm-up all play a part; that is why three runs and the median, and why a difference under ~10% is not a result.

**E and F (not measured automatically).**

- *E — less provenance indexing.* The workload is only ~600 FlowFiles per run, a few thousand provenance events: indexing is not what limits this flow. It matters with millions of small FlowFiles (a Split flow, see [[labs/lab-3|Lab 3]]). Measure it anyway — and expect "no effect" as a valid answer.
- *F — more heap and threads.* The heap never went over ~800 MB of 2 GB, GC took < 100 ms per run: more heap will not help. More threads help only together with more Concurrent Tasks at the bottleneck, and only up to the number of cores Docker gives NiFi.

**The final settings.** Explicit schemas everywhere, Concurrent Tasks `4` on `UpdateRecord` (and the next step on `ValidateRecord`), `MergeRecord` batches sized for the destination, the default heap. On this machine: ~300,000 records/s, 8.8 CPU seconds per million records.

## Disk space

Every run writes the 0.37 GB of the workload four times — content in NiFi is immutable, so `GenerateRecord`, `ValidateRecord`, `UpdateRecord` and `MergeRecord` each write a new copy — and NiFi keeps the replaced content in the archive of the content repository (NiFi 2.12: up to 3 hours, up to 90% of the disk). Twelve runs left ~17 GB in the archive on the stand; with Docker Desktop that space is taken from the laptop's disk. `make lab09-measure` checks the free space before and during the measurement; see the task, "Disk space", for limiting the archive.

## Common mistakes

- **Measuring the generator instead of the pipeline.** A fixed rate (10,000 events every second) shows the same number in every scenario as long as the pipeline keeps up. Fixed workload, or at least a generator that is faster than the pipeline.
- **Changing several things at once** — then you do not know which change did what, and one change can cancel another (C and D here).
- **Trusting a single run.** ±10–20% between runs is normal on a laptop; three runs, the median.
- **Silent losses.** With `invalid` / `failure` auto-terminated, a Reader that suddenly rejects records looks like a *faster* pipeline (fewer records to write). Count everything that leaves the pipeline.
- **Counters left over.** NiFi keeps a counter per processor and does not reset it on its own; reset `lab9.*` before every run. Counters of deleted processors stay in the list until NiFi restarts — read the one of your `UpdateCounter`, not one with the same name.
- **Concurrent Tasks on every processor.** It adds threads where there is no work, takes them from the shared pool and adds contention; raise it only at the bottleneck and measure.
- **A full disk.** See "Disk space": the archive of the content repository grows with every run.
- **The 5-minute window.** NiFi reports *In/Out/Tasks/Time* for a rolling 5 minutes; back-to-back runs mix in those numbers. Read the counters and the time of each run, or the active threads during the run.

**Reference solution:** [lab9-performance.json](https://github.com/MagnetonBora/nifi-playground/blob/master/nifi/flows/lab9-performance.json). The measurement: `make lab09-measure` (report in `labs/` of the stand repository); live view: Grafana → *NiFi playground* → **Lab 9: Performance test**.
