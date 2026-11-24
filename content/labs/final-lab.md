---
title: "Final lab — an end-to-end pipeline"
aliases:
  - exercises/final-lab
---

> From the materials of [[materials/day-3|Day 3 — Production, Security, and Performance]]. The lab stand is described on the [[labs/index|Labs]] page; loading the reference solutions — on the [[solutions/index|Solutions]] page.
> Solution: [[solutions/final-lab|Final lab — solution]].

**Goal.** Build a production-style pipeline from everything of the three days: read logs from Kafka, validate and clean them, publish the clean events to another Kafka topic, send every error to a DLQ — then break it on purpose and check that nothing is lost.

**Where the data goes.**

| | On the stand |
|---|---|
| Input | Kafka topic `logs.raw` (6 partitions), load from `make lab-final-load` |
| Output | Kafka topic `logs.clean` — without `DEBUG`, without `user.pwd`, with `ingest_ts` first; the message key is the event `id` |
| Errors | Kafka topic `logs.dlq` (headers `error.*`); if Kafka does not accept them — files in `nifi/data/final/dlq` |
| Reconciliation | counters `final.accepted`, `final.delivered`, `final.filtered.debug`, `final.dlq` (☰ → **Counters**) |

```text
ingest              validate-transform                                    deliver                                  errors
ConsumeKafka ──► ValidateRecord ► UpdateRecord ► QueryRecord ─ delivered ► MergeRecord ──► PublishKafka ─ success ► counter
(logs.raw)            │                              └ debug ► counter only                  │  ▲  (logs.clean)
   │                  │                                                         failure ─────┘  └── retry ── RetryFlowFile
   │                  └ invalid ──────────────────────────────────────────────────────────────────► exhausted ──┐
   └ parse failure ──────────────────────────────────────────────────────────────────────────────────────────────┴─► UpdateAttribute ► PublishKafka (logs.dlq)
                                                                                                                                          └ failure ► PutFile
```

Each stage is a separate Process Group connected by ports. The `error.stage` attribute tells where a record failed: `ingest-parse`, `validate-transform`, `deliver`.

## Before you start

```bash
make lab-setup        # the topics logs.raw (6 partitions), logs.clean (6 partitions) and logs.dlq
```

Parameter Context `logs-prod`: `kafka.bootstrap=kafka1:19092`, `out.topic=logs.clean`, `dlq.topic=logs.dlq`, `out.dir=/opt/nifi/data/final`.

## Steps

1. **Services:** `Kafka3ConnectionService`, an `AvroSchemaRegistry` with the schemas `LogEvent.in` and `LogEvent.out` (as in [[labs/lab-6|Lab 6]]: the output schema has `ingest_ts` first and no `pwd`; both have the event `id`), Readers/Writers that take the schema by name.
2. **Build the groups in order:** `ingest` → `validate-transform` → `deliver` → `errors`, and connect their ports.
   - `ingest`: `ConsumeKafka` (`logs.raw`, Group ID `nifi-logs`, `RECORD`, `earliest`) → `UpdateCounter` `final.accepted`; `parse failure` → errors.
   - `validate-transform`: `ValidateRecord` → `UpdateRecord` (`/ingest_ts`) → `QueryRecord` with two queries: `delivered` = `level <> 'DEBUG'`, `debug` = `level = 'DEBUG'` (→ `UpdateCounter` `final.filtered.debug`) → `MergeRecord`.
   - `deliver`: `PublishKafka` into `#{out.topic}` (Record Reader/Writer with `LogEvent.out`, *Message Key Field* `id`); `success` → `UpdateCounter` `final.delivered`; `failure` → `RetryFlowFile` (*Maximum Retries* `5`, *Penalty Duration* `10 sec`) → back to `PublishKafka`, exhausted → errors.
     - Count `final.delivered` **after** `success`, not in front of `PublishKafka`: in front of it the counter counts attempts, and a batch that later goes to the DLQ would be counted twice.
     - Keep **one** retry mechanism: `RetryFlowFile`, with *Penalty Duration* `0 sec` on `PublishKafka` and no framework retry on it. Two mechanisms multiply: 3 framework retries with a growing penalty inside each of 5 `RetryFlowFile` rounds keep a FlowFile away from the DLQ for ~20 minutes (see [[labs/lab-8|Lab 8]]).
   - `errors`: `UpdateAttribute` (`error.time`) → `UpdateCounter` `final.dlq` → `PublishKafka` into `logs.dlq` (one message per line, headers `error.*`); its `failure` → `PutFile` into `#{out.dir}/dlq`.
3. **Back pressure:** moderate thresholds on the connections between groups, high ones on the connections into `errors` — the DLQ must never block the main flow.
4. **Start everything and send the load:**

   ```bash
   make lab-final-load n=20000     # lab-services/produce_logs.py → logs.raw
   ```

   The mix: ~5% broken events (`{"ts":"bad"}`), the levels INFO / ERROR / DEBUG in equal parts, every event carries a unique `id` and a secret `user.pwd`.

5. **Reconcile** the counters and check the destination (see below).
6. **Break it** — the failure tests below.
7. **Measure it** (if time is left) — the method of [[labs/lab-9|Lab 9]] on this pipeline: stop `ConsumeKafka`, send 200,000 events (they wait in Kafka as a fixed workload), start it and time until `final.accepted` reaches 200,000 and the queues are empty. Find the bottleneck, change one thing — for example *Concurrent Tasks* of `ConsumeKafka` up to the 6 partitions of `logs.raw`, or of the busiest processor — and measure again.

## Expected behavior

- In `logs.clean`: events **without** `pwd`, **without** `DEBUG`, with `ingest_ts` first; the message key is the event `id`.
- In `logs.dlq`: the broken events (about 5% of the load) with `error.*` headers.
- The counters add up: `final.accepted = final.delivered + final.filtered.debug + final.dlq`. On the stand, 20,000 events: 20000 = 12760 + 6276 + 964.
- The queues into `errors` stay below their thresholds; the main flow does not stop when the DLQ grows.
- Provenance: for a random event the path Kafka → … → `SEND` to `logs.clean` is visible.

The balance has two limits: `final.accepted` counts only what `ConsumeKafka` could parse, while a `parse failure` (a message that is not JSON at all) still increases `final.dlq`; and the counters live in memory — **a restart of NiFi resets them to 0**, so reconcile only within one run of NiFi.

## Verification

```bash
docker exec kafka1 kafka-console-consumer --bootstrap-server kafka1:19092 --topic logs.clean \
   --from-beginning --timeout-ms 10000 > /tmp/clean.ndjson
grep -c '"id"' /tmp/clean.ndjson                           # messages in logs.clean = final.delivered
grep -c pwd /tmp/clean.ndjson; grep -c '"DEBUG"' /tmp/clean.ndjson   # 0 and 0
docker exec kafka1 kafka-console-consumer --bootstrap-server kafka1:19092 --topic logs.dlq \
   --from-beginning --timeout-ms 10000 --property print.headers=true | head
```

Count the messages by reading the topic, not by its offsets: `PublishKafka` writes in transactions, and every transaction leaves a commit marker that takes an offset too.

## Failure tests

| # | Do | Expect |
|---|---|---|
| 1 | The destination refuses writes: forbid writing to `logs.clean` (commands below) and send `make lab-final-load n=20000` | Reading from `logs.raw` goes on (the Kafka lag stays at 0), `final.delivered` stops growing, the batches go round `RetryFlowFile`; after ~1 minute (5 × 10 sec) they land in `logs.dlq` with `error.stage=deliver`. The balance holds all the time. After the permission is back, new events reach `logs.clean` at once; what is already in the DLQ stays there |
| 2 | `docker restart nifi` right after sending the load | NiFi is back within seconds; the queues are restored from the FlowFile repository and processed; compare the `id`s of the sent events with `logs.clean` — nothing missing, nothing twice (on the stand: 12,676 of 12,676). The counters start again from 0 |
| 3 | Send 100% garbage: `yes '{"ts":"bad"}' \| head -1000 \| docker exec -i kafka1 kafka-console-producer --bootstrap-server kafka1:19092 --topic logs.raw` | the main flow keeps working, all of it goes to `logs.dlq` |

Test 1 — forbid and allow again (the broker of the stand has ACLs enabled and allows everything that no ACL forbids):

```bash
docker exec kafka1 kafka-acls --bootstrap-server kafka1:19092 --add --deny-principal User:ANONYMOUS --operation Write --topic logs.clean
docker exec kafka1 kafka-acls --bootstrap-server kafka1:19092 --remove --force --deny-principal User:ANONYMOUS --operation Write --topic logs.clean
```

Questions:

- In test 1 the Kafka lag does **not** grow, although the destination is broken. Why? What would have to change for back pressure to reach `ConsumeKafka` — and would you want that?
- Test 2 showed no duplicates. When can at-least-once still produce them, and how would a consumer of `logs.clean` deal with them (hint: the key)?
- What happens if Kafka itself stops (`docker stop kafka1`) — the input and both outputs at once?

```bash
docker exec kafka1 kafka-consumer-groups --bootstrap-server kafka1:19092 --describe --group nifi-logs   # the Kafka lag
```

## Re-running

Stop `ConsumeKafka`, then:

```bash
docker exec kafka1 kafka-consumer-groups --bootstrap-server kafka1:19092 --delete --group nifi-logs
```

Reset the counters (☰ → Counters). For exact numbers, also recreate the topics: `kafka-topics --delete --topic logs.raw` / `logs.clean` / `logs.dlq`, then `make lab-setup`.

## Without the stand

```bash
python3 - <<'EOF' | docker exec -i kafka1 kafka-console-producer --bootstrap-server kafka1:19092 --topic logs.raw
import json, random, time, uuid
for i in range(20000):
    if random.random() < 0.05:
        print('{"ts":"bad"}')                     # a broken event
    else:
        print(json.dumps({"id": uuid.uuid4().hex, "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                          "level": random.choice(["INFO","ERROR","DEBUG"]),
                          "svc": "api", "msg": "event %d" % i, "user": {"id": i % 100, "pwd": "s3cret"}}))
EOF
```

Reference flow: `make import f=nifi/flows/lab10-final-pipeline.json`.
