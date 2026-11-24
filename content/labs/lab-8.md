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
3. **`InvokeHTTP`**: `POST` to `#{flaky.url}`, *Connection* and *Socket Read Timeout* `5 sec`; on the *Settings* tab *Penalty Duration* `0 sec` (see the note below). Route: `Original` → `PutFile` into `#{ok.dir}`; `Retry` and `Failure` → `RetryFlowFile`; `No Retry` → DLQ.
4. **`RetryFlowFile`**: *Maximum Retries* `3`, *Penalize Retries* `true`; on the *Settings* tab *Penalty Duration* `10 sec` — this is the pause before each retry. `retry` → back into `InvokeHTTP`, `retries_exceeded` → DLQ.

   > **Why `0 sec` on `InvokeHTTP`.** `InvokeHTTP` itself penalizes every FlowFile it sends to `Retry` (5xx) with its own *Penalty Duration* (`30 sec` by default), and then `RetryFlowFile` adds its own penalty. With the defaults each retry waits 30 + 10 = 40 seconds, and the FlowFiles sit penalized in the queue `InvokeHTTP → RetryFlowFile`, as if `RetryFlowFile` were not picking them up. Keep the backoff in one place — in `RetryFlowFile`.
5. **The DLQ group**: on each error path an `UpdateAttribute` sets `error.stage` (see the table) → input port of the DLQ group → `UpdateAttribute` (`error.time`) → `PutFile` into `#{dlq.dir}/${error.stage}`.
6. **Start the group** and let it run for a few minutes.
7. **Break the API:** `docker stop flaky`. Watch the queue `RetryFlowFile → InvokeHTTP` and the directory `dlq/retries-exceeded` for a couple of minutes, then bring the API back: `docker start flaky`.

## Expected behavior

| Situation | What happens |
|---|---|
| normal work | most FlowFiles land in `nifi/data/ok`; FlowFiles waiting for a retry are visible with a penalty (~10 sec) in the queue `RetryFlowFile → InvokeHTTP` |
| `400` | straight into `dlq/http-400`, no retries |
| `null` in a field | into `dlq/validation`; the reason is in the FlowFile attribute `error.reason` (visible in provenance) |
| `503` | retried; `retries-exceeded` is rare while `flaky` works (four `503` in a row, ~1%) |
| `flaky` stopped | requests go to `Failure` (`java.net.UnknownHostException`: the stopped container no longer resolves); after 4 attempts (~30 sec) every FlowFile lands in `dlq/retries-exceeded`, about one every 2 seconds — as fast as the generator produces them. The queue `RetryFlowFile → InvokeHTTP` first rises (up to ~25 in the first ~40 sec), then holds at ~15 FlowFiles (those waiting for their next attempt) and does not grow: back pressure is not reached and the generator is not stopped |
| `flaky` started again | within ~20 sec the retry queue drains on its own and `ok` grows again, without restarting anything; what already landed in `retries-exceeded` stays there |

In provenance, a FlowFile retried several times has a chain of `SEND` / `ROUTE` events; its attribute `flowfile.retries` shows the number of attempts.

## Verification

From the root of the stand repository:

```bash
ls nifi/data/ok | wc -l                  # grows all the time
ls nifi/data/dlq                         # http-400, validation (and retries-exceeded after docker stop flaky)
ls nifi/data/dlq/http-400 | wc -l        # roughly 10% of the requests
```

## Measuring the whole scenario (optional)

Instead of watching the canvas, run the scenario automatically and get a report with charts:

```bash
make import f=nifi/flows/lab8-retry-errors.json   # if the group is not on the canvas yet
make lab08-measure                                # ~9 min
```

The command needs exactly one group *Lab 8 - Retry and error handling* on the canvas and the `flaky` container running. It enables the group's services, starts the group, and goes through three phases: 4 min of normal work, 2.5 min with `flaky` stopped (it runs `docker stop flaky` itself), 1.5 min after `docker start flaky`. Then it stops the generator, waits until the retry loop is empty, and stops the group. `flaky` is started again even if the run is interrupted.

The outcomes are counted from provenance (the `SEND` events of both `PutFile`), so files already lying in `nifi/data/ok` and `nifi/data/dlq` do not matter. The report goes to `labs/lab8-measure-<date-time>.txt` and `.html` in the root of the stand repository:

| In the report | What to look at |
|---|---|
| tiles | the shares of `ok`, `http-400`, `retries-exceeded` in the normal phase and of FlowFiles with invalid records — next to the values expected from `flaky.py` |
| *Retry loop* | the queue between `InvokeHTTP` and `RetryFlowFile` over time: low in normal work, a plateau while `flaky` is down, back to normal after the restart |
| *Where the FlowFiles ended up* | cumulative `ok` / `validation` / `http-400` / `retries-exceeded`: `ok` stops during the outage, `retries-exceeded` takes over |
| two bar charts | the outcome of a valid FlowFile and the number of attempts a delivered FlowFile needed, measured vs expected |

Short run for a demo: `NIFI_PASSWORD=… PHASE1=60 PHASE2=45 PHASE3=30 python3 nifi/common/lab8-measure.py` (the numbers get noisier).

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
