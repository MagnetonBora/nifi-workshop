---
title: "Lab 9 — A performance test and tuning"
aliases:
  - exercises/lab-9
---

> From the materials of [[materials/day-3|Day 3 — Production, Security, and Performance]]. The lab stand is described on the [[labs/index|Labs]] page; loading the reference solutions — on the [[solutions/index|Solutions]] page.
> Solution: [[solutions/lab-9|Lab 9 — solution]].

**Goal.** Put the pipeline from [[labs/lab-6|Lab 6]] under load, measure its throughput, find the bottleneck, and improve it one setting at a time. The result is a short report: change → effect → conclusion.

**What is measured.**

```text
GenerateRecord (10,000 events/s) ► ValidateRecord ► UpdateRecord ► MergeRecord ► UpdateCounter ► PutFile
                                                                               lab9.records.out
```

| | On the stand |
|---|---|
| Load | `GenerateRecord`: *Number of Records* `10000` every second ≈ 10,000 events/s; more load — more records or a shorter *Run Schedule* |
| Throughput | the counter `lab9.records.out` (☰ → **Counters**), increased by `${record.count}` after `MergeRecord`: events/s = its growth ÷ seconds |
| Output | `/opt/nifi/data/lab9-out` (on the host: `nifi/data/lab9-out`), file names by second (`lab9-${now():format('ss')}.ndjson`, overwritten) — never more than 60 files |
| Metrics | Prometheus `http://localhost:9090` (job `nifi`), Grafana `http://localhost:3000`, `docker stats nifi` |

## Before you start

Import the prepared pipeline and enable its services:

```bash
make import f=nifi/flows/lab9-performance.json
```

Write down the baseline: the NiFi version, heap (`NIFI_JVM_HEAP_INIT` / `NIFI_JVM_HEAP_MAX` in `nifi/single/docker-compose-nifi-single.yml`), *Maximum Timer Driven Thread Count* (☰ → Controller Settings), *Concurrent Tasks* of every processor, CPU and memory of Docker.

## Steps

1. **Baseline (scenario A).** Start the group, let it run 2–3 minutes, then measure for 1 minute: events/s from the counter, *Tasks/Time* per processor, where queues grow, heap/GC, CPU. While the generator itself is the slowest part, that is your baseline.
2. **Find the bottleneck:** the processor with the largest *Tasks/Time* and a growing queue in front of it.
3. **Change one thing**, measure again the same way. Go through the scenarios:

   | Scenario | Change | How on the stand |
   |---|---|---|
   | A | baseline | the `ValidateRecord` Reader is `r-log-infer` (*Infer Schema*) |
   | B | explicit schemas | switch the `ValidateRecord` Reader to `r-log-explicit` |
   | C | more parallelism at the bottleneck | *Concurrent Tasks* of that processor |
   | D | larger batches | *Minimum / Maximum Number of Records* of `MergeRecord` |
   | E | less provenance indexing | `nifi.provenance.repository.indexed.fields`, `…indexed.attributes`, `…max.storage.time` in `conf/nifi.properties` (volume `nifi_conf`), then `docker restart nifi`. A separate disk cannot be reproduced on Docker Desktop: all volumes share one virtual disk |
   | F | more heap and threads | `NIFI_JVM_HEAP_*` + `make nifi` (mind the Docker Desktop memory limit); *Maximum Timer Driven Thread Count* |

4. **Repeat each measurement 3 times** and take the median.
5. **Write the report**: the table below, the final settings, and recommendations.

**Results table.**

| Scenario | Change | events/s | CPU % | GC pause (p95) | I/O await (ms) | Conclusion |
|---|---|---|---|---|---|---|
| A | baseline | | | | | |
| B | explicit schemas | | | | | |
| C | more Concurrent Tasks at the bottleneck | | | | | |
| D | larger `MergeRecord` batches | | | | | |
| E | less provenance indexing | | | | | |
| F | more heap and a larger thread pool | | | | | |

## Useful commands

```bash
docker stats nifi                                                                     # CPU / memory of the container
docker exec nifi sh -c 'jstat -gcutil $(pgrep -f org.apache.nifi.NiFi | head -1) 5000'   # GC every 5 s
docker exec nifi /opt/nifi/nifi-current/bin/nifi.sh dump /tmp/nifi-dump.txt           # thread dump
docker exec nifi /opt/nifi/nifi-current/bin/nifi.sh diagnostics /tmp/diag.txt         # diagnostics
```

Outside the stand the same commands are run directly on the NiFi host (`bin/nifi.sh dump …`, `jstat`, `iostat -x 5`).

## Re-running

Before each measurement: stop the group, empty its queues (right-click the group → *Empty all queues*), reset the counter (☰ → Counters → reset `lab9.records.out`). Changes of `nifi.properties` need `docker restart nifi`; changes of the heap in the compose file need `make nifi` (it recreates the container).
