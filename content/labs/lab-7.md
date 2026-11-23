---
title: "Lab 7 — Delivering events to Splunk"
aliases:
  - exercises/lab-7
---

> From the materials of [[materials/day-2|Day 2 — Data Processing and Integrations]]. The lab stand is described on the [[labs/index|Labs]] page; loading the reference solutions — on the [[solutions/index|Solutions]] page.
> Solution: [[solutions/lab-7|Lab 7 — solution]].

**Goal.** Deliver log events to Splunk in two ways — over TCP with `PutSplunk` and over HTTP Event Collector (HEC) with `InvokeHTTP` — check that every event arrives, and measure how the batch size and parallelism change the throughput.

**Where the data goes.**

| | On the stand |
|---|---|
| Input | `GenerateRecord`: 100 log events per second (NDJSON, one event per line) |
| Variant A | `PutSplunk` → TCP port `5514` |
| Variant B | `InvokeHTTP` → HEC on port `8088`, header `Authorization: Splunk 00000000-0000-0000-0000-000000000000` |
| Destination | the Splunk emulator `splunk-sim` (default) or a real Splunk `splunk` — see below |

```text
A:  GenerateRecord (100/s) ► MergeRecord (10,000 or 10 s) ► PutSplunk (TCP)
B:  GenerateRecord (100/s) ► MergeRecord (10,000 or 10 s) ► InvokeHTTP (HEC)
```

**Two possible destinations.** Both accept the same TCP and HEC traffic; you switch between them with the Parameter Context.

| | Emulator `splunk-sim` | Real Splunk `splunk` |
|---|---|---|
| Start | already running (`make lab-services`, part of `make up`) | `make splunk` — not part of `make up`, the first start takes a few minutes, needs several GB of memory |
| `splunk.host` | `splunk-sim` | `splunk` |
| `hec.url` | `http://splunk-sim:8088/services/collector/event` | `http://splunk:8088/services/collector/raw` — the events are plain NDJSON lines; `/event` would expect the envelope `{"event": …}` |
| Result | counts the received lines: `make lab-splunk-count` | search in the UI `http://localhost:8000` (login `admin`, password from `make urls`), index `nifi` |

The emulator (`lab-services/splunk_sim.py`) answers HEC like Splunk (`{"text":"Success","code":0}`) and writes what it receives to `/data/splunk_in.log` (TCP) and `/data/hec_in.log` (HEC) — exact counts, nothing else. The real Splunk is preconfigured: TCP input `5514` and HEC token `00000000-…` write to the index `nifi`, sourcetype `nifi:json`, the JSON fields are extracted at search time.

## Before you start

Emulator:

```bash
docker exec splunk-sim sh -c ': > /data/splunk_in.log; : > /data/hec_in.log'   # zero the counters
```

Real Splunk:

```bash
make splunk                                                           # wait until http://localhost:8000 opens
docker exec nifi curl -s http://splunk:8088/services/collector/health # {"text":"HEC is healthy","code":17}
```

Parameter Context `lab7-params`: `splunk.host`, `splunk.port=5514`, `hec.url`, `hec.token=00000000-0000-0000-0000-000000000000` — `splunk.host` and `hec.url` as in the table above.

## Steps

1. **Build two groups** with the same source: `GenerateRecord` (a log event schema, *Number of Records* `100`, *Run Schedule* `1 sec`, NDJSON writer) → `MergeRecord` (*Minimum/Maximum Number of Records* `10000`, *Max Bin Age* `10 sec`).
   - **A**: → `PutSplunk` — *Hostname* `#{splunk.host}`, *Port* `#{splunk.port}`, *Protocol* `TCP`, *Message Delimiter* `\n`.
   - **B**: → `InvokeHTTP` — `POST` to `#{hec.url}`, *Request Content-Type* `application/json`, the dynamic header `Authorization` = `Splunk #{hec.token}`.
   - `failure` (and `Retry` / `No Retry` / `Failure` of `InvokeHTTP`) → `LogAttribute`.

2. **Check the delivery.** Run **A** for a minute, stop the generator, wait until the queues are empty. Compare the number of events at the destination with the number of records that left `MergeRecord` (the *Out* counter of the sender, or `record.count` in its provenance).
   - Emulator: `make lab-splunk-count`.
   - Real Splunk, *Search & Reporting*:

     ```
     index=nifi | stats count
     index=nifi | head 5
     index=nifi | stats count by level
     ```

     Every event must be a separate entry with the fields `level`, `svc`, `msg`, `user.id` extracted.

3. **Compare TCP and HEC.** Stop A, run **B** for the same time, compare.
   - Emulator: the two lines of `make lab-splunk-count` — `splunk_in.log` (TCP) and `hec_in.log` (HEC).
   - Real Splunk: `index=nifi | stats count by source` — the TCP input and HEC have different `source` values.

4. **The experiment.** For each variant run three settings, 1–2 minutes each, and measure events per second:

   | # | Setting | How |
   |---|---|---|
   | 1 | small FlowFiles of 100 events | bypass `MergeRecord`: connect `GenerateRecord` directly to the sender |
   | 2 | batches of 10,000 | the flow as built |
   | 3 | batches + parallelism | *Concurrent Tasks* `4` on `PutSplunk` / `InvokeHTTP` |

   - Raise the load first — *Number of Records* of `GenerateRecord` `1000`–`5000`: at 100 events per second every variant is limited by the generator, not by the delivery.
   - Events per second: emulator — the growth of `make lab-splunk-count` ÷ seconds; real Splunk — the graph `index=nifi earliest=-5m | timechart span=10s count`.
   - For every run write down: events/s, *Tasks/Time* of the sender, errors in `LogAttribute (failure)`.

5. **Conclusions.**
   - How much faster are batches than small FlowFiles, and why (tasks, connections and HTTP requests per event)?
   - Which is faster on your stand, TCP or HEC, and what does HEC give in return (a response per request, a token)?
   - Did *Concurrent Tasks* help, and where is the bottleneck: NiFi, the network or Splunk?

## Expected behavior

- Every event sent is received: the count at the destination equals the number of records that left `MergeRecord` (for example, 21 FlowFiles of 100 events → exactly 2,100 events).
- In provenance, the `SEND` events of `PutSplunk` / `InvokeHTTP` show the destination address.
- Batches send the same events with far fewer tasks; the effect of *Concurrent Tasks* depends on the destination.

## Verification

```bash
make lab-splunk-count       # emulator: lines in /data/splunk_in.log (TCP) and /data/hec_in.log (HEC)
```

Real Splunk: `index=nifi | stats count by source` in *Search & Reporting*.

## Re-running

- Emulator: zero the files before every measurement — `docker exec splunk-sim sh -c ': > /data/splunk_in.log; : > /data/hec_in.log'`.
- Real Splunk: no need to clean the index — limit every search to the time of the run (the time picker *Last 5 minutes* or `earliest=-5m`).
- When you are done with the real Splunk: `make splunk-down` (the emulator keeps running).

## Without the stand

```bash
# A TCP input
nc -lk 5514 > /tmp/splunk_in.log
# A minimal HEC receiver
python3 - <<'EOF'
from http.server import BaseHTTPRequestHandler, HTTPServer
class H(BaseHTTPRequestHandler):
    def do_POST(self):
        n = int(self.headers.get('Content-Length', 0)); body = self.rfile.read(n)
        open('/tmp/hec_in.log','ab').write(body + b'\n')
        self.send_response(200); self.end_headers(); self.wfile.write(b'{"text":"Success","code":0}')
HTTPServer(('0.0.0.0', 8088), H).serve_forever()
EOF
```

Count with `wc -l /tmp/splunk_in.log /tmp/hec_in.log`.

Reference flow: `make import f=nifi/flows/lab7-splunk-delivery.json` (the parameters point to the emulator).
