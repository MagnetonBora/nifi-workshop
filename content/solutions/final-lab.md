---
title: "Final lab — an end-to-end pipeline"
aliases:
  - exercises/final-lab-solution
  - labs/final-lab-solution
---

> From the materials of [[materials/day-3|Day 3 — Production, Security, and Performance]]. The lab stand is described on the [[labs/index|Labs]] page; loading the reference solutions — on the [[solutions/index|Solutions]] page.
> Task: [[labs/final-lab|Final lab — an end-to-end pipeline]].

## Architecture

```text
                         ┌───────────────────────── PG: ingest ─────────────────────────┐
 Kafka topic logs.raw ──►│ ConsumeKafka (Processing=RECORD, Group=nifi-logs, Output=Value)│
                         │   └ parse failure ───────────────────────────► PG: errors     │
                         └───────────────┬───────────────────────────────────────────────┘
                                         ▼
                         ┌───────────────── PG: validate-transform ───────────────────┐
                         │ ValidateRecord (schema LogEvent.in)                         │
                         │   ├ invalid ─────────────────────────────► PG: errors        │
                         │ UpdateRecord (ingest_ts; a writer schema without secrets,    │
                         │               field order: ingest_ts first)                  │
                         │ QueryRecord (WHERE level <> 'DEBUG')                         │
                         │ MergeRecord (NDJSON, up to 10,000 records, Max Bin Age 30 sec)│
                         └───────────────┬───────────────────────────────────────────────┘
                                         ▼
                         ┌───────────────────── PG: deliver ─────────────────────────┐
                         │ PutSplunk (TCP)  or  InvokeHTTP→HEC  or PutFile              │
                         │   ├ failure/retry ► RetryFlowFile ► (exhausted) ► PG: errors │
                         └───────────────┬───────────────────────────────────────────────┘
                                         ▼
                         ┌───────────────────── PG: errors ───────────────────────────┐
                         │ UpdateAttribute(error.*) ► PublishKafka (logs.dlq) / PutFile │
                         └───────────────────────────────────────────────────────────────┘
```

## Controller Services and parameters

| Service/parameter | Value |
|---|---|
| `Kafka3ConnectionService` | `#{kafka.bootstrap}` |
| `AvroSchemaRegistry` | `LogEvent.in`, `LogEvent.out` |
| `JsonTreeReader` | Schema Name Property, the registry; without Infer |
| `JsonRecordSetWriter` | `LogEvent.out`, `One Line Per Object`, without Pretty Print |
| Parameter Context `logs-prod` | `kafka.bootstrap`, `splunk.host`, `splunk.port`, `dlq.topic`, `out.dir` |

Values for the stand ([[materials/Appendix-A#a5-addresses-for-the-labs|Appendix A.5]]): `kafka.bootstrap=kafka1:19092`, `splunk.host=splunk-sim`, `splunk.port=5514`, `dlq.topic=logs.dlq`, `out.dir=/opt/nifi/data/final` (a fallback sink for the DLQ; on the host — `nifi/data/final`).

In `RECORD` mode `ConsumeKafka` has a relationship **`parse failure`** (messages that the Reader could not parse) — route it to `errors`, otherwise the processor is invalid. For reconciliation it is convenient to put an `UpdateCounter` with Delta `${record.count}` at the points "accepted" (after `ConsumeKafka`), "delivered" (before `PutSplunk`), "filtered" (the `debug` output of the second `QueryRecord`: `SELECT * FROM FLOWFILE WHERE level = 'DEBUG'`), and "to the DLQ" (in the `errors` group).

## Common mistakes and diagnostics

| Symptom | Diagnostics |
|---|---|
| Data does not appear | The status of `ConsumeKafka` (Group/Offset Reset), bulletins; `kafka-consumer-groups.sh --describe` (on the stand `docker exec kafka1 kafka-consumer-groups --bootstrap-server kafka1:19092 --describe --group nifi-logs`); the group has already read the topic — see "Re-running" |
| `ConsumeKafka` is invalid | The `parse failure` relationship is not connected |
| Everything goes to the DLQ | The `LogEvent.in` schema does not match the data (types/nested structures) — look at the error attributes |
| Low throughput | Tasks/Time, queues; not enough partitions; small FlowFiles; a single thread on `PutSplunk` |
| Duplicates at the destination | Restarts/rebalances; idempotency is needed |

**Reference solution:** [lab9-final-pipeline.json](https://github.com/MagnetonBora/nifi-playground/blob/master/nifi/flows/lab9-final-pipeline.json). The failure tests (stopping the destination, restarting NiFi under load) were not run on the reference solution.
