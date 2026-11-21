---
title: "Lab 8 — A performance test and tuning"
aliases:
  - exercises/lab-8
---

> From the materials of [[materials/day-3|Day 3 — Production, Security, and Performance]]. The lab stand is described on the [[labs/index|Labs]] page; loading the reference solutions — on the [[solutions/index|Solutions]] page.
> Solution: [[solutions/lab-8|Lab 8 — solution]].

**Goal.** Measure the throughput of the pipeline from Lab 5, find the bottleneck, and improve the indicators.

**Method (step by step).**

1. **The baseline.** Record the version, the configuration (heap, thread pool, concurrent tasks), the hardware. Prepare a load generator (`GenerateRecord`, a Kafka producer perf-test).
2. **Metrics.** Events/s at the output; Tasks/Time per processor; queues (where they grow); heap/GC; CPU; `iostat`; the size/time of provenance events.
3. **Scenarios.**
   - A: the baseline configuration.
   - B: explicit schemas (remove Infer).
   - C: increase `Concurrent Tasks` of the bottleneck.
   - D: increase the batch size (`MergeRecord`).
   - E: move Provenance to a separate volume/reduce indexing.
   - F: increase the heap and thread pool (in line with the growing number of cores).
4. **One parameter at a time;** 3 runs each; the median.
5. **The report:** a "change → effect → conclusion" table, the updated settings, recommendations.

**On the stand ([[materials/Appendix-A|Appendix A]]).**

- **The generator:** `GenerateRecord` (a log event schema with `user.pwd`, Number of Records `10000`, Run Schedule `1 sec`) before the Lab 5 pipeline — about 10,000 events/s; the load is increased through *Number of Records* and *Run Schedule*. While the generator remains the bottleneck, that is the baseline.
- **The output counter:** `UpdateCounter` after `MergeRecord` with Delta `${record.count}` — events/s = the growth of the counter (☰ → Counters) over an interval.
- **Scenario A** — a Reader with `Infer Schema`; **B** — switch the Record Reader of `ValidateRecord` to a `JsonTreeReader` with an explicit schema.
- **Scenario D** — *Minimum/Maximum Number of Records* of `MergeRecord`.
- **Scenario E** — on Docker Desktop all volumes sit on one virtual disk, so the effect of a separate volume cannot be reproduced; reduce the provenance indexing in `conf/nifi.properties` (the volume `nifi_conf`): `nifi.provenance.repository.indexed.fields`, `nifi.provenance.repository.indexed.attributes`, `nifi.provenance.repository.max.storage.time`, then restart with `make nifi` (**[verify]** the effect on your version).
- **Scenario F** — heap: `NIFI_JVM_HEAP_INIT` / `NIFI_JVM_HEAP_MAX` in `nifi/single/docker-compose-nifi-single.yml` and `make nifi` (mind the Docker Desktop memory limit); thread pool: ☰ → Controller Settings → *Maximum Timer Driven Thread Count*.
- **Metrics:** Prometheus already collects NiFi metrics (`http://localhost:9090`, job `nifi`), and the graphs are in Grafana at `http://localhost:3000`.
- **The output** is written to `nifi/data/lab8-out` with names by seconds (`lab8-${now():format('ss')}.ndjson`, Conflict Resolution `replace`) — no more than 60 files are on disk at any load.

**A results table template.**

| Scenario | Change | events/s | CPU % | GC pause (p95) | I/O await (ms) | Conclusion |
|---|---|---|---|---|---|---|
| A | baseline | | | | | |
| B | explicit schemas | | | | | |
| C | more Concurrent Tasks at the bottleneck | | | | | |
| D | larger `MergeRecord` batches | | | | | |
| E | Provenance on a separate volume / less indexing | | | | | |
| F | more heap and a larger thread pool | | | | | |

**Useful commands.**

```bash
bin/nifi.sh dump /tmp/nifi-dump.txt           # thread dump
bin/nifi.sh diagnostics /tmp/diag.txt         # diagnostics
iostat -x 5                                   # disk load
jstat -gcutil <pid> 5000                      # GC
```

On the stand the same commands are run inside the container, and the load is visible through `docker stats`:

```bash
docker exec nifi /opt/nifi/nifi-current/bin/nifi.sh dump /tmp/nifi-dump.txt
docker exec nifi /opt/nifi/nifi-current/bin/nifi.sh diagnostics /tmp/diag.txt
docker exec nifi sh -c 'jstat -gcutil $(pgrep -f org.apache.nifi.NiFi | head -1) 5000'   # [verify]
docker stats nifi                                                                     # the container's CPU / memory
```
