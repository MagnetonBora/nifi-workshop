---
title: "Lab 7 — Delivering events to Splunk"
aliases:
  - exercises/lab-7
---

> From the materials of [[materials/day-2|Day 2 — Data Processing and Integrations]]. The lab stand is described on the [[labs/index|Labs]] page; loading the reference solutions — on the [[solutions/index|Solutions]] page.
> Solution: [[solutions/lab-7|Lab 7 — solution]].

**Goal.** Send the NDJSON events from Lab 6 to Splunk or to an emulated endpoint and measure throughput.

**Emulation.** On the stand, the Splunk emulator is already running (`make lab-services`, the container `splunk-sim`, the code is `lab-services/splunk_sim.py`): the TCP input on `splunk-sim:5514` writes events to `/data/splunk_in.log`, and HEC at `http://splunk-sim:8088/services/collector/event` writes to `/data/hec_in.log` and responds like Splunk (`{"text":"Success","code":0}`). Without the stand, the same effect is given by:

```bash
# Emulating a Splunk TCP input
nc -lk 5514 > /tmp/splunk_in.log
# Or in Python: a simple HTTP receiver for HEC
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

**The event source.** `GenerateRecord` (a log event schema, 100 records per FlowFile, `Run Schedule 1 sec`) ► `MergeRecord` (Min/Max Records `10000`, Max Bin Age `10 sec`). It is convenient to build two groups: A — `PutSplunk` (TCP), B — `InvokeHTTP` (HEC) — and to run them in turn.

**The experiment.**

1. Prepare 1 million events (repeated `GenerateRecord`/`GenerateFlowFile`). On a laptop, start with ~100,000 and increase while watching the heap.
2. Variant 1: small FlowFiles of 100 events. Variant 2: batches of 10,000 through `MergeRecord`. Variant 3: `Concurrent Tasks=4`.
3. Measure events/s using the queue counters and time (on the stand: `make lab-splunk-count`).

**Verification.** `make lab-splunk-count` (or `wc -l /tmp/splunk_in.log` without the stand) equals the number of events sent — for example, 21 FlowFiles of 100 events give exactly 2,100 lines over both TCP and HEC; in provenance the `SEND` events show the destination address. Before a new measurement, zero the emulator files: `docker exec splunk-sim sh -c ': > /data/splunk_in.log; : > /data/hec_in.log'`.
