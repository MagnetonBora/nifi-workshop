---
title: "Lab 8 — Retry and error handling"
aliases:
  - exercises/lab-8
---

> From the materials of [[materials/day-3|Day 3 — Production, Security, and Performance]]. The lab stand is described on the [[labs/index|Labs]] page; loading the reference solutions — on the [[solutions/index|Solutions]] page.
> Solution: [[solutions/lab-8|Lab 8 — solution]].

**Goal.** Build a flow that sends events to an unreliable HTTP API, survives its temporary failures without losing data, and is not blocked by bad records: every kind of error ends up in its own DLQ directory.

**Where the data goes.**

| | On the stand |
|---|---|
| Input | `GenerateRecord`: 5 events every 2 seconds, ~5% of the fields are `null` |
| Destination | the unstable API `http://flaky:9099/events`: 60% `200`, 30% `503`, 10% `400` |
| Delivered | `/opt/nifi/data/ok` (on the host: `nifi/data/ok`) |
| Errors | `/opt/nifi/data/dlq/<error.stage>` (on the host: `nifi/data/dlq/…`) |

```text
GenerateRecord ► ValidateRecord ─ valid ─► InvokeHTTP ─ Original (2xx) ─► PutFile (ok)
                     │                        ├ Retry (5xx) / Failure ─► RetryFlowFile ─ retry ─► back to InvokeHTTP
                     │                        │                              └ retries_exceeded ─► DLQ
                     │                        └ No Retry (4xx) ─► DLQ
                     └ invalid ─► DLQ

DLQ group: UpdateAttribute (error.stage, error.time) ► PutFile #{dlq.dir}/${error.stage}
```

| Error | `error.stage` | Directory |
|---|---|---|
| a record with `null` in a required field | `validation` | `nifi/data/dlq/validation` |
| the API answered `400` | `http-400` | `nifi/data/dlq/http-400` |
| `503` four times in a row, or the API is down | `retries-exceeded` | `nifi/data/dlq/retries-exceeded` |

## Before you start

```bash
make lab-setup        # creates nifi/data/ok and nifi/data/dlq
make lab-services     # the container flaky is running (part of make up)
```

Parameter Context `lab8-params`: `flaky.url=http://flaky:9099/events`, `ok.dir=/opt/nifi/data/ok`, `dlq.dir=/opt/nifi/data/dlq`, `event.schema.strict` — the strict schema for validation.

## Steps

1. **A source with bad records.** `GenerateRecord` produces records strictly by its schema, so normally there is nothing invalid. Declare the fields in the generator schema as `["null", …]`, set *Nullable Fields* `true` and *Null Value Percentage* `5`; *Number of Records* `5`, *Run Schedule* `2 sec`.
2. **`ValidateRecord`** against `#{event.schema.strict}`, where every field is required: records with `null` go to `invalid`.
3. **`InvokeHTTP`**: `POST` to `#{flaky.url}`, *Connection* and *Socket Read Timeout* `5 sec`. Route: `Original` → `PutFile` into `#{ok.dir}`; `Retry` and `Failure` → `RetryFlowFile`; `No Retry` → DLQ.
4. **`RetryFlowFile`**: *Maximum Retries* `3`, *Penalize Retries* `true`; `retry` → back into `InvokeHTTP`, `retries_exceeded` → DLQ.
5. **The DLQ group**: on each error path an `UpdateAttribute` sets `error.stage` (see the table) → input port of the DLQ group → `UpdateAttribute` (`error.time`) → `PutFile` into `#{dlq.dir}/${error.stage}`.
6. **Start the group** and let it run for a few minutes.
7. **Break the API:** `docker stop flaky`. Watch the queue before `InvokeHTTP`, then bring it back: `docker start flaky`.

## Expected behavior

| Situation | What happens |
|---|---|
| normal work | most FlowFiles land in `nifi/data/ok`; FlowFiles waiting for a retry are visible with a penalty in the queue `InvokeHTTP → RetryFlowFile` |
| `400` | straight into `dlq/http-400`, no retries |
| `null` in a field | into `dlq/validation`; the reason is in the FlowFile attribute `error.reason` (visible in provenance) |
| `503` | retried; `retries-exceeded` is rare while `flaky` works (four `503` in a row, ~1%) |
| `flaky` stopped | requests go to `Failure`, after 3 retries with a penalty `dlq/retries-exceeded` appears; the queue before `InvokeHTTP` grows until back pressure stops the generator |
| `flaky` started again | the queue drains on its own, without restarting anything |

In provenance, a FlowFile retried several times has a chain of `SEND` / `ROUTE` events; its attribute `flowfile.retries` shows the number of attempts.

## Verification

From the root of the stand repository:

```bash
ls nifi/data/ok | wc -l                  # grows all the time
ls nifi/data/dlq                         # http-400, validation (and retries-exceeded after docker stop flaky)
ls nifi/data/dlq/http-400 | wc -l        # roughly 10% of the requests
```

## Re-running

```bash
rm -rf nifi/data/ok/* nifi/data/dlq/*
docker start flaky                       # if it was stopped
```

## Without the stand

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

Reference flow: `make import f=nifi/flows/lab8-retry-errors.json`.
