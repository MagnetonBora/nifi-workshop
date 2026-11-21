---
title: "Lab 7 — Retry and error handling"
aliases:
  - exercises/lab-7
---

> From the materials of [[materials/day-3|Day 3 — Production, Security, and Performance]]. The lab stand is described on the [[labs/index|Labs]] page; loading the reference solutions — on the [[solutions/index|Solutions]] page.
> Solution: [[solutions/lab-7|Lab 7 — solution]].

**Goal.** Build a flow that survives temporary failures of the destination and does not get blocked by "bad" data.

The Parameter Context `lab7-params` (the stand): `flaky.url=http://flaky:9099/events`, `ok.dir=/opt/nifi/data/ok`, `dlq.dir=/opt/nifi/data/dlq` (on the host — `nifi/data/ok`, `nifi/data/dlq`).

**A data source with invalid records.** `GenerateRecord` produces records strictly according to its schema, so without preparation the `invalid` branch is empty. Declare the fields in the generator schema as `["null", …]` and set `Nullable Fields = true`, `Null Value Percentage = 5`, and in `ValidateRecord` (Schema Access Strategy `Use 'Schema Text' Property`) validate against a strict schema in which all fields are required: records with `null` will go to `invalid`.

**Simulating an unstable API.** On the stand the service is already running: the container `flaky` (`make lab-services`, the code is `lab-services/flaky.py`), the address from NiFi is `http://flaky:9099/events`. Without the stand:

```bash
python3 - <<'EOF'
import random
from http.server import BaseHTTPRequestHandler, HTTPServer
class H(BaseHTTPRequestHandler):
    def do_POST(self):
        self.rfile.read(int(self.headers.get('Content-Length', 0)))
        r = random.random()
        code = 200 if r < 0.6 else (503 if r < 0.9 else 400)   # 60% ok, 30% 503, 10% 400
        self.send_response(code); self.end_headers(); self.wfile.write(b'{}')
HTTPServer(('0.0.0.0', 9099), H).serve_forever()
EOF
```

**Verification** (from the stand host):

- After a few minutes most FlowFiles are in `nifi/data/ok` (`ls nifi/data/ok | wc -l`); `400` responses — in `nifi/data/dlq/http-400`, invalid records — in `nifi/data/dlq/validation`; `503` responses pass through `RetryFlowFile` (FlowFiles with a penalty are visible in the `InvokeHTTP → RetryFlowFile` queue).
- `retries_exceeded` almost never occurs while `flaky` is running: four `503` responses in a row are needed (~1%). Stop the service — `docker stop flaky`: requests go to `Failure`, after 3 retries with a penalty `nifi/data/dlq/retries-exceeded` appears, the queue before `InvokeHTTP` grows, and then back pressure kicks in; after `docker start flaky` the queue drains *without intervention*.
- Check provenance: a FlowFile with several attempts has a chain of `ROUTE`/`SEND` events; the `flowfile.retries` attribute shows the number of attempts.
