---
title: "Final lab — an end-to-end pipeline"
aliases:
  - exercises/final-lab
---

> From the materials of [[materials/day-3|Day 3 — Production, Security, and Performance]]. The lab stand is described on the [[labs/index|Labs]] page; loading the reference solutions — on the [[solutions/index|Solutions]] page.
> Solution: [[solutions/final-lab|Final lab — solution]].

**Goal.** Build a production-style pipeline from everything of the three days: read logs from Kafka, validate and clean them, deliver them to Splunk, send every error to a DLQ — then break it on purpose and check that nothing is lost, and tune it under load.

**Where the data goes.**

| | On the stand |
|---|---|
| Input | Kafka topic `logs.raw` (6 partitions), load from `make lab-final-load` |
| Output | the Splunk emulator, TCP `splunk-sim:5514` — without `DEBUG`, without `user.pwd`, with `ingest_ts` first |
| Errors | Kafka topic `logs.dlq` (headers `error.*`); if Kafka is not available — files in `nifi/data/final/dlq` |
| Reconciliation | counters `final.accepted`, `final.delivered`, `final.filtered.debug`, `final.dlq` (☰ → **Counters**) |

```text
ingest              validate-transform                                   deliver                      errors
ConsumeKafka ──► ValidateRecord ► UpdateRecord ► QueryRecord ─ delivered ► MergeRecord ──► PutSplunk ◄─► RetryFlowFile
(logs.raw)            │                              └ debug ► counter only            │
   │                  └ invalid ───────────────────────────────────────────────────────┴──► UpdateAttribute ► PublishKafka (logs.dlq)
   └ parse failure ───────────────────────────────────────────────────────────────────────►                   └ failure ► PutFile
```

Each stage is a separate Process Group connected by ports. The `error.stage` attribute tells where a record failed: `ingest-parse`, `validate-transform`, `deliver`.

## Before you start

```bash
make lab-setup        # the topics logs.raw (6 partitions) and logs.dlq
make lab-services     # the Splunk emulator splunk-sim (part of make up)
docker exec splunk-sim sh -c ': > /data/splunk_in.log'     # zero the emulator
```

Parameter Context `logs-prod`: `kafka.bootstrap=kafka1:19092`, `splunk.host=splunk-sim`, `splunk.port=5514`, `dlq.topic=logs.dlq`, `out.dir=/opt/nifi/data/final`.

## Steps

1. **Services:** `Kafka3ConnectionService`, an `AvroSchemaRegistry` with the schemas `LogEvent.in` and `LogEvent.out` (as in [[labs/lab-6|Lab 6]]: the output schema has `ingest_ts` first and no `pwd`), Readers/Writers that take the schema by name.
2. **Build the groups in order:** `ingest` → `validate-transform` → `deliver` → `errors`, and connect their ports.
   - `ingest`: `ConsumeKafka` (`logs.raw`, Group ID `nifi-logs`, `RECORD`, `earliest`) → `UpdateCounter` `final.accepted`; `parse failure` → errors.
   - `validate-transform`: `ValidateRecord` → `UpdateRecord` (`/ingest_ts`) → `QueryRecord` with two queries: `delivered` = `level <> 'DEBUG'`, `debug` = `level = 'DEBUG'` (→ `UpdateCounter` `final.filtered.debug`) → `MergeRecord`.
   - `deliver`: `UpdateCounter` `final.delivered` → `PutSplunk`; `failure` → `RetryFlowFile` (5 attempts, with a penalty) → back, exhausted → errors.
   - `errors`: `UpdateAttribute` (`error.time`) → `UpdateCounter` `final.dlq` → `PublishKafka` into `logs.dlq` (one message per line, headers `error.*`); its `failure` → `PutFile` into `#{out.dir}/dlq`.
3. **Back pressure:** moderate thresholds on the connections between groups, high ones on the connections into `errors` — the DLQ must never block the main flow.
4. **Start everything and send the load:**

   ```bash
   make lab-final-load n=100000     # lab-services/produce_logs.py → logs.raw
   ```

   The mix: ~5% broken events (`{"ts":"bad"}`), the levels INFO / ERROR / DEBUG in equal parts, every event carries a secret `user.pwd`.

5. **Reconcile** the counters and check the destination (see below).
6. **Break it** — the failure tests below.
7. **Tune it** — [[labs/lab-9|Lab 9]] on this pipeline: stable throughput without a growing Kafka lag, GC pauses within the SLO; write down the final settings (heap, threads, Concurrent Tasks, batch sizes, repositories).

## Expected behavior

- At the destination: events **without** `pwd`, **without** `DEBUG`, with `ingest_ts` first.
- In `logs.dlq`: the broken events (about 5% of the load) with `error.*` headers.
- The counters add up: `final.accepted = final.delivered + final.filtered.debug + final.dlq`. Example on 2,000 events: 2000 = 1256 + 636 + 108.
- The queues into `errors` stay below their thresholds; the main flow does not stop when the DLQ grows.
- Provenance: for a random event the path Kafka → … → `SEND` is visible.

## Verification

```bash
make lab-splunk-count                                                                  # lines in the emulator = final.delivered
docker exec splunk-sim sh -c 'grep -c pwd /data/splunk_in.log; grep -c DEBUG /data/splunk_in.log'   # 0 and 0
docker exec kafka1 kafka-console-consumer --bootstrap-server kafka1:19092 --topic logs.dlq \
   --from-beginning --timeout-ms 10000 --property print.headers=true | head
```

## Failure tests

| # | Do | Expect |
|---|---|---|
| 1 | `docker stop splunk-sim` during the load, later `docker start splunk-sim` | the queue before `PutSplunk` grows, back pressure stops reading from Kafka, the lag grows (`kafka-consumer-groups --describe --group nifi-logs`) — nothing is lost; after the start the backlog drains |
| 2 | `docker restart nifi` during the load | the queues are restored, `ConsumeKafka` continues from the committed offsets; some events may be delivered twice (at-least-once) — decide whether that is acceptable |
| 3 | send 100% garbage: `yes '{"ts":"bad"}' \| head -1000 \| docker exec -i kafka1 kafka-console-producer --bootstrap-server kafka1:19092 --topic logs.raw` | the main flow keeps working, all of it goes to `logs.dlq` |

```bash
docker exec kafka1 kafka-consumer-groups --bootstrap-server kafka1:19092 --describe --group nifi-logs   # the Kafka lag
```

## Re-running

Stop `ConsumeKafka`, then:

```bash
docker exec kafka1 kafka-consumer-groups --bootstrap-server kafka1:19092 --delete --group nifi-logs
docker exec splunk-sim sh -c ': > /data/splunk_in.log'
```

Reset the counters (☰ → Counters). For exact numbers, also recreate the topics: `kafka-topics --delete --topic logs.raw` / `logs.dlq`, then `make lab-setup`.

## Without the stand

```bash
python3 - <<'EOF' | docker exec -i kafka1 kafka-console-producer --bootstrap-server kafka1:19092 --topic logs.raw
import json, random, time
for i in range(100000):
    if random.random() < 0.05:
        print('{"ts":"bad"}')                     # a broken event
    else:
        print(json.dumps({"ts": time.strftime("%Y-%m-%dT%H:%M:%SZ"), "level": random.choice(["INFO","ERROR","DEBUG"]),
                          "svc": "api", "msg": "event %d" % i, "user": {"id": i % 100, "pwd": "s3cret"}}))
EOF
```

Reference flow: `make import f=nifi/flows/lab10-final-pipeline.json`.
