---
title: "Lab 9 — A performance test and tuning"
aliases:
  - exercises/lab-9
---

> From the materials of [[materials/day-3|Day 3 — Production, Security, and Performance]]. The lab stand is described on the [[labs/index|Labs]] page; loading the reference solutions — on the [[solutions/index|Solutions]] page.
> Solution: [[solutions/lab-9|Lab 9 — solution]].

**Goal.** Put the pipeline from [[labs/lab-6|Lab 6]] under load, measure how many records per second it can process, find the bottleneck, and improve it one setting at a time. The result is a short report: change → effect → conclusion.

**What is measured.**

```text
GenerateRecord ═══ 200 FlowFiles × 10,000 records ═══► ValidateRecord ─ valid ─► UpdateRecord ► MergeRecord ► UpdateCounter ► UpdateAttribute ► PutFile
 (workload: back pressure stops it at 2,000,000 records)     ├ invalid ► count invalid                          lab9.records.out
                                                             └ failure ► count failed
```

**The method: a fixed workload, not a fixed input rate.** If the generator produced, say, 10,000 events per second, the pipeline would either keep up or not — and as long as it keeps up, every tuning step shows the same 10,000 events/s. Instead, the workload is prepared in advance and the pipeline processes it as fast as it can:

1. Only `GenerateRecord` runs. The connection to `ValidateRecord` holds at most **200 FlowFiles** (*Back Pressure Object Threshold*), so the generator stops by itself at 200 × 10,000 = **2,000,000 records** (~0.37 GB).
2. The generator is stopped, everything else is started. The time until `lab9.records.out` reaches 2,000,000 gives the throughput: **records/s = 2,000,000 ÷ seconds**.

Every run processes exactly the same data, so the scenarios can be compared with each other.

| | On the stand |
|---|---|
| Workload | 200 FlowFiles × 10,000 records waiting in front of `ValidateRecord`; the generated events match the input schema |
| Throughput | the counter `lab9.records.out` (☰ → **Counters**), increased by `${record.count}` after `MergeRecord` |
| Losses | the counters `lab9.records.invalid` (records rejected by `ValidateRecord`) and `lab9.flowfiles.failed` — both must stay **0**, otherwise the run lost data and its throughput means nothing |
| Output | `/opt/nifi/data/lab9-out` (on the host: `nifi/data/lab9-out`), file names by second (`lab9-${now():format('ss')}.ndjson`, overwritten) — never more than 60 files |
| Metrics | Grafana `http://localhost:3000` → *NiFi playground* → **Lab 9: Performance test** (records/s, queues, threads, CPU per processor, heap, GC); `docker stats nifi` |

## Before you start

```bash
make import f=nifi/flows/lab9-performance.json
```

Enable the group's services (right-click → *Enable all controller services*), do **not** start the group. Write down the baseline: the NiFi version, heap (`NIFI_JVM_HEAP_INIT` / `NIFI_JVM_HEAP_MAX` in `nifi/single/docker-compose-nifi-single.yml`), *Maximum Timer Driven Thread Count* (☰ → Controller Settings), *Concurrent Tasks* of every processor, CPUs and memory given to Docker.

## Disk space

Every run writes the 0.37 GB of data **four times**: `GenerateRecord`, `ValidateRecord`, `UpdateRecord` and `MergeRecord` each write a new copy of the content (content in NiFi is immutable) — ~1.5 GB per run. NiFi does not delete content it no longer needs right away: it moves it into the **content repository archive** and keeps it there — in NiFi 2.12 by default for up to 3 hours (`nifi.content.repository.archive.max.retention.period`) and up to **90%** of the disk (`nifi.content.repository.archive.max.usage.percentage`). Twelve runs therefore leave ~18 GB behind (measured on the stand), plus the provenance repository.

On Docker Desktop this is easy to miss: NiFi sees the disk of the Docker virtual machine (often 100+ GB "free"), but the image of that machine lives on your laptop's disk and grows with it — the laptop runs out first.

- Check before you start: `df -h ~` on the host, the *Content Repository* line in ☰ → *Cluster/System Diagnostics* → *System*.
- If the laptop's disk is short, switch the archive off for the lab, then `docker restart nifi`:

  ```bash
  docker exec nifi sed -i 's/^nifi.content.repository.archive.enabled=.*/nifi.content.repository.archive.enabled=false/' conf/nifi.properties
  ```

  The archive is what lets you open the content of old FlowFiles in provenance and replay them; switch it back on (`=true`) after the lab.

  Lowering `max.usage.percentage` instead is less predictable: the percentage is the usage of the **whole** disk the content repository is on (here the disk of the Docker VM, together with everything else stored there), not the size of the archive. If the disk is already fuller than the threshold, the archive is purged all the time — the same as switching it off; if it is much emptier, the archive can still grow beyond what the laptop has left.
- `make lab09-measure` needs, besides a 10 GB reserve (`MIN_FREE_GB`), ~1.5 GB per run (12 runs: ~18 GB) and takes the smaller of the free space NiFi reports and the free space of the host disk.

## Steps

1. **Prepare the workload.** Start only `GenerateRecord`. After ~8 seconds the queue in front of `ValidateRecord` shows 200 FlowFiles and the connection turns red (back pressure); stop the generator.
2. **Measure.** Reset the counters (☰ → Counters → reset `lab9.*`), note the time, and start all the other processors (select them all except `GenerateRecord` → *Start*). Refresh the Counters page until `lab9.records.out` = 2,000,000 and note the time again. Before it ends, look at the canvas: where does the queue stay long, which processor always shows an active thread (the number in its top-right corner)?
3. **Find the bottleneck:** the processor that is busy all the time and has a long queue in front of it. The *Status History* of a processor (right-click) shows its *Tasks/Time* over time.
4. **Change one thing**, prepare the same workload again, and measure the same way. The scenarios are cumulative — each one keeps the previous changes:

   | Scenario | Change | How on the stand |
   |---|---|---|
   | A | baseline | the `ValidateRecord` Reader is `reader-inferred-schema` (*Infer Schema*: the schema is derived from the data of every FlowFile) |
   | B | explicit schema | switch the `ValidateRecord` Reader to `reader-input-schema`. Validation itself always uses the explicit schema `#{log.schema.in}` — only the way of reading changes |
   | C | more parallelism at the bottleneck | *Concurrent Tasks* = `4` on the processor you found in step 3 (measured in B) |
   | D | larger batches | `MergeRecord`: *Minimum / Maximum Number of Records* `100000` / `200000` |
   | E | less provenance indexing | `nifi.provenance.repository.indexed.fields`, `…indexed.attributes`, `…max.storage.time` in `conf/nifi.properties` (volume `nifi_conf`), then `docker restart nifi`. A separate disk cannot be reproduced on Docker Desktop: all volumes share one virtual disk |
   | F | more heap and threads | `NIFI_JVM_HEAP_*` + `make nifi` (mind the Docker Desktop memory limit); *Maximum Timer Driven Thread Count* |

5. **Repeat each measurement 3 times** and take the median: a single run on a laptop easily varies by 10–20%.
6. **Write the report**: the table below, the final settings, and recommendations.

**Results table.**

| Scenario | Change | records/s | vs A | CPU s per 1M records | GC time, ms | Bottleneck | Conclusion |
|---|---|---|---|---|---|---|---|
| A | baseline | | 1.00× | | | | |
| B | explicit schema | | | | | | |
| C | Concurrent Tasks at the bottleneck | | | | | | |
| D | larger `MergeRecord` batches | | | | | | |
| E | less provenance indexing | | | | | | |
| F | more heap and a larger thread pool | | | | | | |

- **CPU s per 1M records** — how much CPU the pipeline burns per million records: `docker stats nifi` shows CPU % (100% = one core); CPU s ≈ average CPU % ÷ 100 × seconds of the run, divided by 2. A change can make the pipeline cheaper without making it faster — when it speeds up a processor that is not the bottleneck.
- **GC time** — the `GCT` column of `jstat -gcutil` (seconds, cumulative): the difference between the end and the start of the run; or the *GC time per minute* panel in Grafana.

## Measuring automatically

Scenarios A–D can be run without clicking:

```bash
make lab09-measure        # ~6 min; the group must be imported, the command enables its services itself
```

The command finds the group *Lab 9 - Performance test*, runs A → B → C → D three times each exactly as described above (prepare the workload, reset the counters, start everything except the generator, wait for 2,000,000 records), picks the bottleneck for C from the measurements of B, and at the end restores the original settings and leaves the group stopped with empty queues. While it runs it samples every 0.5 s the active threads of every processor and the queues; at the start and end of each run it reads the JVM GC time (`/system-diagnostics`) and the CPU time of the `nifi` container. Before the start and before every run it checks the free disk space (see below) and stops if it is too low.

The report goes to `labs/lab9-measure-<date-time>.txt` and `.html` in the root of the stand repository:

| In the report | What to look at |
|---|---|
| tiles | records/s of the baseline and the best scenario, the speed-up, the bottleneck before and after |
| *Throughput by scenario* | the median of three runs and every run as a dot — how stable the numbers are |
| *Where the time goes* | the average busy threads per processor and scenario: the darkest cell of a column is the bottleneck, and it moves after each fix |
| *Where FlowFiles wait* | the average queue in front of every processor |
| *CPU per million records*, *Garbage collection* | the cost of each scenario, independent of its speed |

E and F need a restart of NiFi and are done by hand.

## Useful commands

```bash
docker stats nifi                                                                     # CPU / memory of the container
docker exec nifi sh -c 'jstat -gcutil $(pgrep -f org.apache.nifi.NiFi | head -1) 5000'   # GC every 5 s
docker exec nifi /opt/nifi/nifi-current/bin/nifi.sh dump /tmp/nifi-dump.txt           # thread dump
docker exec nifi /opt/nifi/nifi-current/bin/nifi.sh diagnostics /tmp/diag.txt         # diagnostics
```

Outside the stand the same commands are run directly on the NiFi host (`bin/nifi.sh dump …`, `jstat`, `iostat -x 5`).

## Re-running

Before each measurement: stop the group, empty its queues (right-click the group → *Empty all queues*), reset the counters (☰ → Counters → reset `lab9.*`), then prepare the workload again. Changes of `nifi.properties` need `docker restart nifi`; changes of the heap in the compose file need `make nifi` (it recreates the container).
