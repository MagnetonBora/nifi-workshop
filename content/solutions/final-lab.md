---
title: "Final lab — an end-to-end pipeline"
aliases:
  - exercises/final-lab-solution
  - labs/final-lab-solution
---

> From the materials of [[materials/day-3|Day 3 — Production, Security, and Performance]]. The lab stand is described on the [[labs/index|Labs]] page; loading the reference solutions — on the [[solutions/index|Solutions]] page.
> Task: [[labs/final-lab|Final lab — an end-to-end pipeline]].

**Structure of the reference flow.** One group *Final lab - End-to-end pipeline* with the Parameter Context `logs-prod` and all the Controller Services; inside it four groups connected by ports: `ingest`, `validate-transform`, `deliver`, `errors`. Every stage can be stopped, inspected and changed on its own; the error paths of all stages meet in one place.

## Architecture

```text
                         ┌──────────────────────────── PG: ingest ────────────────────────────┐
 Kafka logs.raw ────────►│ ConsumeKafka (RECORD, Group nifi-logs) ► UpdateCounter final.accepted │
                         │   └ parse failure ► error.stage=ingest-parse ─────────────► to-errors │
                         └──────────────────────────────┬──────────────────────────────────────┘
                                                        ▼
                         ┌──────────────────────── PG: validate-transform ─────────────────────┐
                         │ ValidateRecord (LogEvent.in, strict)                                │
                         │   └ invalid / failure ► error.stage=validate-transform ─► to-errors │
                         │ UpdateRecord (/ingest_ts; written with LogEvent.out: no pwd,        │
                         │               ingest_ts first)                                      │
                         │ QueryRecord ─ delivered: level <> 'DEBUG'                           │
                         │             └ debug:     level = 'DEBUG' ► UpdateCounter            │
                         │                                            final.filtered.debug     │
                         │ MergeRecord (1,000–10,000 records, Max Bin Age 30 sec)              │
                         └──────────────────────────────┬──────────────────────────────────────┘
                                                        ▼
                         ┌───────────────────────────── PG: deliver ───────────────────────────┐
                         │ PublishKafka (#{out.topic} = logs.clean, key = id)                  │
                         │   ├ success ► UpdateCounter final.delivered                         │
                         │   └ failure ► RetryFlowFile (5 × 10 sec) ─ retry ► PublishKafka     │
                         │                  └ retries_exceeded ► error.stage=deliver ► to-errors│
                         └─────────────────────────────────────────────────────────────────────┘
                         ┌────────────────────────────── PG: errors ───────────────────────────┐
 to-errors (all stages) ►│ UpdateAttribute (error.time) ► UpdateCounter final.dlq              │
                         │   ► PublishKafka (logs.dlq, one message per line, headers error.*)  │
                         │        └ failure ► UpdateAttribute (filename) ► PutFile #{out.dir}/dlq│
                         └─────────────────────────────────────────────────────────────────────┘
```

## Configuration

**Parameter Context `logs-prod`.**

| Parameter | Value | Purpose |
|---|---|---|
| `kafka.bootstrap` | `kafka1:19092` | the broker, from inside the Docker network |
| `out.topic` | `logs.clean` | clean events |
| `dlq.topic` | `logs.dlq` | errors of all stages |
| `out.dir` | `/opt/nifi/data/final` | the fallback sink of the DLQ (on the host: `nifi/data/final`) |

**Controller Services** (in the top group, shared by all stages).

| Service | Settings |
|---|---|
| `kafka` (`Kafka3ConnectionService`) | Bootstrap Servers `#{kafka.bootstrap}`, PLAINTEXT |
| `schema-registry` (`AvroSchemaRegistry`) | `LogEvent.in`: `id`, `ts`, `level`, `svc`, `msg` (all required strings), `user` {`id`, `pwd`} (optional). `LogEvent.out`: **`ingest_ts`**, `id`, `ts`, `level`, `svc`, `msg`, `user` {`id`} — no `pwd` |
| `reader-in` / `reader-out` | `JsonTreeReader`, *Use 'Schema Name' Property* with `LogEvent.in` / `LogEvent.out` from the registry — no *Infer Schema* anywhere |
| `writer-in` / `writer-out` | `JsonRecordSetWriter`, the same schemas by name, *One Line Per Object*, no Pretty Print, *Do Not Write Schema* |

**Processors.**

| Group | Component | Settings |
|---|---|---|
| ingest | `ConsumeKafka` | Kafka Connection Service `kafka`; Topics `logs.raw`; Group ID `nifi-logs`; Offset Reset `earliest`; Processing Strategy `RECORD`; Reader `reader-in`, Writer `writer-in`; Output Strategy `Use Content as Value`; `parse failure` → errors |
| | `UpdateCounter` | `final.accepted` += `${record.count}` |
| validate-transform | `ValidateRecord` | Reader `reader-in`, Writer `writer-in`; Schema Access Strategy *Use Reader's Schema*; Strict Type Checking `true`; Validation Details Attribute Name `error.details` |
| | `UpdateRecord` | Reader `reader-in`, Writer **`writer-out`**; `/ingest_ts` = `${now():format("yyyy-MM-dd'T'HH:mm:ss.SSSXXX", "UTC")}` (Literal Value). The writer schema drops `pwd` and puts `ingest_ts` first — the same trick as in [[labs/lab-6|Lab 6]] |
| | `QueryRecord` | Reader/Writer `reader-out` / `writer-out`; `delivered` = `SELECT * FROM FLOWFILE WHERE level <> 'DEBUG'`, `debug` = `SELECT * FROM FLOWFILE WHERE level = 'DEBUG'`; Include Zero Record FlowFiles `false`; `original` auto-terminated |
| | `MergeRecord` | `reader-out` / `writer-out`; Minimum / Maximum Number of Records `1000` / `10000`; Max Bin Age `30 sec` |
| deliver | `PublishKafka (logs.clean)` | Topic `#{out.topic}`; Record Reader `reader-out`, Record Writer `writer-out`; Publish Strategy `Use Content as Record Value`; **Message Key Field `id`**; **Penalty Duration `0 sec`**, no framework retry |
| | `UpdateCounter` | after `success`: `final.delivered` += `${record.count}` |
| | `RetryFlowFile` | Maximum Retries `5`; Penalize Retries `true`; **Penalty Duration `10 sec`** — the only backoff of the delivery |
| errors | `UpdateAttribute` | `error.time` = `${now():format("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")}` |
| | `UpdateCounter` | `final.dlq` += `${record.count:replaceEmpty(1)}` — a `parse failure` FlowFile has no `record.count` |
| | `PublishKafka (logs.dlq)` | Topic `#{dlq.topic}`; no Record Reader — Message Demarcator `\n` (one message per line); FlowFile Attribute Header Pattern `error\..*` |
| | `PutFile (fallback)` | Directory `#{out.dir}/dlq`, `filename` = `${uuid}.ndjson` |

**Back pressure.** The main path: 10,000 FlowFiles / 1 GB per connection; every connection into `errors` and inside it: 100,000 / 10 GB — when the DLQ grows, it must not stop the main flow.

## What happens to the data

1. **ingest.** `ConsumeKafka` reads `logs.raw` in RECORD mode: every poll becomes one FlowFile with many records, parsed with `LogEvent.in`. `{"ts":"bad"}` *is* valid JSON, so it parses (the missing fields are `null`) and counts in `final.accepted`; only a message that is not JSON at all goes to `parse failure`.
2. **validate-transform.** `ValidateRecord` splits each FlowFile into valid and invalid records: `{"ts":"bad"}` lacks the required `id`, `level`, `svc`, `msg`; the reason lands in `error.details` and later in the Kafka header of the DLQ message. `UpdateRecord` adds `ingest_ts` and writes with `LogEvent.out` — the password disappears, `ingest_ts` becomes the first field. `QueryRecord` sends DEBUG events only to a counter; the rest are merged into FlowFiles of up to 10,000 records.
3. **deliver.** `PublishKafka` writes every record as a separate message to `logs.clean`, with the event `id` as the key, inside one Kafka transaction per FlowFile. Only after `success` does `final.delivered` count the records.
4. **errors.** Every failed record ends here with `error.stage` and, depending on the stage, `error.details`; `PublishKafka` writes one message per line, the `error.*` attributes become Kafka headers.

A message in `logs.dlq`:

```text
error.time:2026-11-24T21:36:19.022Z,error.stage:validate-transform,error.details:Records in this FlowFile were invalid for the following reasons: The following 4 fields were missing: [/level, /msg, /svc, /id]	{"ts":"bad"}
```

A message in `logs.clean` (key = `id`):

```json
{"ingest_ts":"2026-11-24T21:31:13.685Z","id":"4e3ac88b5f634445987b342cf879d5df","ts":"2026-11-24T21:31:13Z","level":"ERROR","svc":"api","msg":"event 199","user":{"id":99}}
```

## Results on the stand

NiFi 2.12.0, Kafka in KRaft mode (one broker), `make lab-final-load n=20000`:

| Check | Result |
|---|---|
| Balance | `final.accepted` 20,000 = `final.delivered` 12,760 + `final.filtered.debug` 6,276 + `final.dlq` 964 |
| `logs.clean` | 12,760 messages, all `id`s unique, key = `id`, 0 × `pwd`, 0 × `DEBUG`, `ingest_ts` first in every message |
| `logs.dlq` | 964 messages, all `error.stage=validate-transform` (`{"ts":"bad"}`), with `error.details` |
| Time | the whole load processed within ~15 seconds; the Kafka lag back at 0 |

`logs.dlq` showed a couple more *offsets* than messages: `PublishKafka` writes in transactions, and every committed transaction leaves a commit marker in each partition it wrote to. Count messages by reading the topic.

## The failure tests

**1. The destination refuses writes** (a DENY ACL for `Write` on `logs.clean`, then 20,000 events).

| Time | What was seen |
|---|---|
| +5 sec | the whole load already read from `logs.raw`: lag 0, `final.accepted` +20,000; `final.delivered` unchanged; 2 FlowFiles (12,606 records) in the queue `RetryFlowFile → PublishKafka` |
| +60 sec | the retries exhausted: `final.dlq` +12,606, `logs.dlq` has 12,606 messages with `error.stage=deliver`; the balance holds again |
| ACL removed | the next 2,000 events: `final.delivered` +1,280 at once |

The lag does not grow, because NiFi never stops reading: the connection in front of `deliver` holds 10,000 FlowFiles, and with `MergeRecord` producing FlowFiles of up to 10,000 records, 20,000 events are just two of them. The pipeline absorbs the failure itself — retry, then the DLQ — instead of pushing it back to Kafka. That is a design choice: if the data must stay in Kafka while the destination is down (for example, the DLQ must not fill up during a long outage), the thresholds on the main path have to be low enough that back pressure reaches `ConsumeKafka` — then the lag grows and nothing leaves Kafka until the destination is back.

The records that went to the DLQ stay there after the destination is repaired: getting them back is a separate replay step (a flow that reads `logs.dlq`, filters by `error.stage=deliver` and publishes into `logs.clean`).

**2. A restart of NiFi under load** (20,000 events, `docker restart nifi` one second later). NiFi had read the whole load from Kafka before it stopped; it was back after ~10 seconds, restored the queues from the FlowFile repository and delivered them. In `logs.clean`: all 12,676 expected `id`s, none missing, none twice. The counters, however, started from 0 after the restart (`final.accepted` = 0, `final.delivered` = 12,676): they live in memory. Reconcile within one run of NiFi, and over restarts by the content of the topics.

No duplicates in this run does not mean none ever: the delivery is at-least-once. If NiFi stops after Kafka has accepted a transaction but before NiFi has committed its own session, the FlowFile is published again after the restart. A consumer of `logs.clean` deduplicates by the key — the event `id` — or the topic uses log compaction.

**3. 100% garbage** (1,000 × `{"ts":"bad"}`): `final.accepted` +1,000, `final.dlq` +1,000, the main flow untouched.

## Design decisions

- **Count delivery after `success`.** A counter in front of the destination counts attempts: in an earlier version of this flow the counter stood in front of the sender and grew by 12,722 while nothing could be delivered. Records that end up in the DLQ would then be counted twice — as delivered and as DLQ — and the balance would never add up.
- **One retry mechanism.** `RetryFlowFile` (5 × 10 sec) alone gets a batch to the DLQ after about a minute. With a framework retry on the publisher as well (3 attempts, penalty 30 → 60 → 120 sec) inside every `RetryFlowFile` round, the same batch would stay away from the DLQ for ~21 minutes: one round was measured at 3.5 minutes, × 6 rounds. *Penalty Duration* `0 sec` on `PublishKafka` keeps its own penalty out of it (see [[labs/lab-8|Lab 8]]).
- **High thresholds into `errors`.** If the DLQ topic is slow or unavailable, the errors queue up in `errors` instead of stopping the whole pipeline; `PutFile` is the last resort.
- **The event `id` as the key.** It keeps the events of one id in one partition and lets consumers deduplicate.

## Simplifications of the reference solution

- **No replay.** What goes to `logs.dlq` with `error.stage=deliver` is valid data that only needs another try — a replay flow (read `logs.dlq`, filter by the header, publish to `logs.clean`) is the missing piece.
- **`PutFile (fallback)` auto-terminates `failure`**: if even the disk fails, the error record is lost. In production it would be routed to an alert.
- **The counters are a lab tool**: they reset on restart and are local to one node; in production the reconciliation uses metrics (Prometheus) or the topics themselves.
- **Values with a leading newline.** All messages of a transaction except the first start with `\n` in `logs.clean`: the *One Line Per Object* writer puts its record separator in front of the next record, and `PublishKafka` writes every record as a message. JSON parsers ignore it; a consumer that compares raw bytes would not.

## Common mistakes

| Symptom | Cause | Action |
|---|---|---|
| Nothing arrives | `ConsumeKafka`: another Group ID with `latest`, or the group has already read the topic; bulletins | `earliest` for a new group; delete the group (see "Re-running"); `kafka-consumer-groups --describe --group nifi-logs` |
| `ConsumeKafka` is invalid | `parse failure` is not connected (RECORD mode) | route it to `errors` |
| Everything goes to the DLQ | `LogEvent.in` does not match the data (types, nesting, the new `id`) | read `error.details` in the headers of `logs.dlq` |
| `pwd` or `DEBUG` in `logs.clean` | `UpdateRecord` or `QueryRecord` writes with `writer-in`, or the query is on the wrong relationship | the output schema must be `LogEvent.out` from `UpdateRecord` on |
| `ingest_ts` is `null` | a Reader *after* `UpdateRecord` uses `LogEvent.in`, which has no `ingest_ts`, and drops it | `reader-out` from `QueryRecord` on |
| The balance never adds up during an outage | `final.delivered` counted in front of `PublishKafka` | count after `success` |
| Errors reach the DLQ only after ~20 minutes | framework retry on `PublishKafka` plus `RetryFlowFile` | one mechanism; *Penalty Duration* `0 sec` on the publisher |
| The counters do not match the topics after a restart | the counters were reset | reconcile per run; compare the topics by `id` |
| More offsets in a topic than messages | transaction markers | count by reading the topic |
| Duplicates in `logs.clean` | at-least-once after a restart / rebalance | deduplicate by the key `id` |

**Reference solution:** [lab10-final-pipeline.json](https://github.com/MagnetonBora/nifi-playground/blob/master/nifi/flows/lab10-final-pipeline.json). All three failure tests above were run on the reference solution on the stand.
