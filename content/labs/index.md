---
title: Labs
aliases:
  - exercises/index
---

The hands-on part of the workshop: nine labs over the three days and a final end-to-end lab. Each lab is a
self-contained task with an architecture, component settings, verification commands, and a review of common mistakes.
All labs are verified on Apache NiFi 2.12.0.

## Labs

| Lab | Day | What you practice | Solution |
|---|---|---|---|
| [[labs/lab-1\|Lab 1 — The NiFi UI and a first flow]] | Day 1 | The UI, running a flow, queues, provenance, Content Viewer | [[solutions/lab-1\|Solution]] |
| [[labs/lab-2\|Lab 2 — Several sources and routing]] | Day 1 | ListFile/FetchFile, ListenHTTP, Process Groups and ports, Parameter Context, RouteOnAttribute | [[solutions/lab-2\|Solution]] |
| [[labs/lab-3\|Lab 3 — A Split flow versus a Record flow]] | Day 1 | GenerateRecord, SplitRecord versus QueryRecord/UpdateRecord, measuring with `make lab03-measure` and Grafana | [[solutions/lab-3\|Solution]] |
| [[labs/lab-4\|Lab 4 — Connecting NiFi to a database]] | Day 2 | DBCPConnectionPool and Verify, ExecuteSQLRecord, QueryDatabaseTableRecord and its state, PutDatabaseRecord INSERT vs UPSERT | [[solutions/lab-4\|Solution]] |
| [[labs/lab-5\|Lab 5 — A database + REST + Kafka with record processing]] | Day 2 | ConsumeKafka, ValidateRecord, LookupRecord + REST, PutDatabaseRecord UPSERT, DLQ | [[solutions/lab-5\|Solution]] |
| [[labs/lab-6\|Lab 6 — An NDJSON transformation pipeline]] | Day 2 | ValidateRecord, removing a secret and field order via the Writer schema, MergeRecord | [[solutions/lab-6\|Solution]] |
| [[labs/lab-7\|Lab 7 — Delivering events to Splunk]] | Day 2 | PutSplunk (TCP), InvokeHTTP → HEC, batch size and throughput | [[solutions/lab-7\|Solution]] |
| [[labs/lab-8\|Lab 8 — Retry and error handling]] | Day 3 | InvokeHTTP, RetryFlowFile, DLQ, back pressure when the destination fails | [[solutions/lab-8\|Solution]] |
| [[labs/lab-9\|Lab 9 — A performance test and tuning]] | Day 3 | A baseline, tuning scenarios, metrics, and a report | [[solutions/lab-9\|Solution]] |
| [[labs/final-lab\|Final lab — an end-to-end pipeline]] | Day 3 | Kafka → validation and transformation → Splunk → DLQ, reconciliation and failure tests | [[solutions/final-lab\|Solution]] |

Try to build each flow yourself first, then compare it with the solution. Reference Flow Definitions and how to
import them are on the [[solutions/index|Solutions]] page.

## Lab stand

The labs run on a ready-made stand, [NiFi playground](https://github.com/MagnetonBora/nifi-playground):
NiFi (a single node or a cluster), NiFi Registry, PostgreSQL, Kafka, Prometheus and Grafana, Splunk, and mocks of
external systems (a customers REST API, a Splunk emulator, an unstable API). You need Docker with at least 8 GB of
memory and `make`.

```bash
git clone https://github.com/MagnetonBora/nifi-playground.git
cd nifi-playground
make up          # NiFi and the whole environment; the first start takes a few minutes
make lab-setup   # the tables and topics for the labs
make urls        # addresses and logins
```

NiFi opens at `https://localhost:8443/nifi`. The addresses used in the labs (Kafka, PostgreSQL, directories, mocks)
are collected in [[materials/Appendix-A#a5-addresses-for-the-labs|Appendix A.5]].

## Before a re-run

The labs with Kafka (Lab 5 and the final lab) remember the messages they have read in a consumer group. To re-run
them from the start of the topic, reset the group and clear the results — the commands are in
[[materials/Appendix-A#a7-test-data-and-reset|Appendix A.7]].
