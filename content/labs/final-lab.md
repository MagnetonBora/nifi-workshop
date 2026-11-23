---
title: "Final lab — an end-to-end pipeline"
aliases:
  - exercises/final-lab
---

> From the materials of [[materials/day-3|Day 3 — Production, Security, and Performance]]. The lab stand is described on the [[labs/index|Labs]] page; loading the reference solutions — on the [[solutions/index|Solutions]] page.
> Solution: [[solutions/final-lab|Final lab — solution]].

## Goal and result

Build a production-oriented pipeline: **Kafka → validation and transformation of JSON/NDJSON (Record) → delivery (Splunk or a file) → error handling → a performance check and tuning**.

## Sequence

1. Prepare the stand ([[materials/Appendix-A|Appendix A]]); create the topics `logs.raw` (for example, 6 partitions) and `logs.dlq` — on the stand `make lab-setup` does this.
2. Create the Parameter Context and the Controller Services (the schemas!).
3. Build the groups in the order: ingest → validate-transform → deliver → errors; connect the ports.
4. Configure back pressure on the inter-group connections; on the connections into `errors` — high thresholds.
5. Enable framework retry on `PutSplunk`/`InvokeHTTP`; `RetryFlowFile` for those that exhausted their attempts.
6. Start the load generator: a mix of valid and invalid events (5–10% invalid), DEBUG events, events with a secret. On the stand: `make lab-final-load n=100000` (the code is `lab-services/produce_logs.py`). Manually:

```bash
python3 - <<'EOF' | docker exec -i kafka1 kafka-console-producer --bootstrap-server kafka1:19092 --topic logs.raw
import json, random, time
for i in range(100000):
    r = random.random()
    if r < 0.05:
        print('{"ts":"bad"}')                     # an invalid record
    else:
        print(json.dumps({"ts": time.strftime("%Y-%m-%dT%H:%M:%SZ"), "level": random.choice(["INFO","ERROR","DEBUG"]),
                          "svc": "api", "msg": "event %d" % i, "user": {"id": i % 100, "pwd": "s3cret"}}))
EOF
```

## Correctness check

- At the destination: events **without** `pwd`, without `DEBUG`, with `ingest_ts` as the first field.
- In `logs.dlq` — invalid records (about 5%) with error attributes/headers.
- The sum: `accepted = delivered + filtered (DEBUG) + to the DLQ` (reconciled through `record.count`/the `UpdateCounter` counters).
- There is no growth of the queues in `errors` above the thresholds; the main queue is not blocked when the DLQ grows.
- Provenance: for a random event the path Kafka → … → SEND is visible.

On the stand:

```bash
make lab-splunk-count                                    # lines in the emulator = final.delivered
docker exec splunk-sim sh -c 'grep -c pwd /data/splunk_in.log; grep -c DEBUG /data/splunk_in.log'   # we expect 0 and 0
docker exec kafka1 kafka-console-consumer --bootstrap-server kafka1:19092 --topic logs.dlq \
   --from-beginning --timeout-ms 10000 --property print.headers=true | head
```

An example of reconciliation on 2,000 events: `final.accepted` 2000 = `final.delivered` 1256 + `final.filtered.debug` 636 + `final.dlq` 108 (☰ → Counters).

**Re-running.** The consumer group `nifi-logs` stores the offsets; before a new run from the start of the topic, stop `ConsumeKafka` and run `docker exec kafka1 kafka-consumer-groups --bootstrap-server kafka1:19092 --delete --group nifi-logs`; zero the emulator: `docker exec splunk-sim sh -c ': > /data/splunk_in.log'`.

## Failure testing

1. Stop the destination (the Splunk emulator; on the stand `docker stop splunk-sim`, then `docker start splunk-sim`): the queue grows, back pressure stops the ingest; the Kafka lag grows (there is no loss; `docker exec kafka1 kafka-consumer-groups --bootstrap-server kafka1:19092 --describe --group nifi-logs`); after it returns — the backlog drains.
2. Restart NiFi during the load (on the stand `docker restart nifi`): the queues are restored, the consumer continues from the committed offsets; repeats are possible (at-least-once) — check idempotency/acceptability.
3. Submit a "batch of garbage" (100% invalid): the main flow does not stop; the DLQ accepts it.

## Performance check and tuning

Perform [[labs/lab-9|Lab 9]] on this pipeline: the goal is stable throughput without growth of the lag, GC pauses within the SLO; record the final settings (heap, thread pool, concurrent tasks, batch sizes, repositories).
