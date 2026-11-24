---
title: "Lab 8 — Retry and error handling"
aliases:
  - exercises/lab-8-solution
  - labs/lab-8-solution
---

> From the materials of [[materials/day-3|Day 3 — Production, Security, and Performance]]. The lab stand is described on the [[labs/index|Labs]] page; loading the reference solutions — on the [[solutions/index|Solutions]] page.
> Task: [[labs/lab-8|Lab 8 — Retry and error handling]].

**Structure of the reference flow.** One group *Lab 8 - Retry and error handling* with the Parameter Context `lab8-params` and two shared services (`r-event`, `w-event`). The main flow lives in it; the error handling is moved into a child group *DLQ group* with a single input port `dlq-in`. Every error path ends in that port, so the way errors are stored is defined in one place.

The idea of the solution is to split errors by their nature:

| Kind of error | Example | What to do |
|---|---|---|
| **Data error** | a required field is `null` | retrying will not help — straight into the DLQ |
| **Permanent API error** | `400 Bad Request` | the request itself is bad — straight into the DLQ, no retries |
| **Temporary API error** | `503`, connection refused, timeout | retry with a pause; give up after a limit and put it into the DLQ |

## Architecture

```text
GenerateRecord ► ValidateRecord ─ invalid / failure ─► UpdateAttribute (error.stage=validation) ──────────────┐
                     │ valid                                                                                  │
                     ▼                                                                                        │
                InvokeHTTP ◄──────────── retry ──────────── RetryFlowFile                                      │
                 │  │  │  └─ Retry (5xx) / Failure ──────────► │                                             │
                 │  │  │                                       └─ retries_exceeded / failure ─►              │
                 │  │  │                                          UpdateAttribute (error.stage=retries-exceeded) ─┤
                 │  │  └─ No Retry (4xx) ─► UpdateAttribute (error.stage=http-${invokehttp.status.code}) ────────┤
                 │  └──── Response ─► LogAttribute (Response)                                                 │
                 └─────── Original (2xx) ─► UpdateAttribute (filename) ─► PutFile #{ok.dir}                   │
                                                                                                              ▼
[DLQ group]  dlq-in ─► UpdateAttribute (error.time, filename) ─► PutFile #{dlq.dir}/${error.stage}
```

## Configuration

**Parameter Context `lab8-params`.**

| Parameter | Value | Purpose |
|---|---|---|
| `flaky.url` | `http://flaky:9099/events` | the unstable API: 60% `200`, 30% `503`, 10% `400` |
| `ok.dir` | `/opt/nifi/data/ok` | delivered events (on the host: `nifi/data/ok`) |
| `dlq.dir` | `/opt/nifi/data/dlq` | errors, one subdirectory per `error.stage` (on the host: `nifi/data/dlq`) |
| `event.schema.strict` | the `Event` schema where every field is required | the contract the data is validated against |

**Controller Services.**

| Service | Type | Key properties |
|---|---|---|
| `r-event` | `JsonTreeReader` | Schema Access `Use 'Schema Text' Property` — the **lenient** `Event` schema: every field is `["null", …]`, so records with `null` can be read at all |
| `w-event` | `JsonRecordSetWriter` | Schema Access `Inherit Record Schema`; Schema Write Strategy `Do Not Write Schema`; Output Grouping `One Line Per Object`; Pretty Print `false` |

Two schemas are used on purpose: the lenient one (in the Reader and the generator) describes what *can* arrive, the strict one (`event.schema.strict` in `ValidateRecord`) — what we *agree to* send further.

**Main flow.**

| Component | Settings |
|---|---|
| `GenerateRecord` | Record Writer `w-event`; Schema Text — the lenient schema; Number of Records `5`; Nullable Fields `true`; Null Value Percentage `5`; Run Schedule `2 sec`; Execution `Primary node` (in a cluster the generator must not run on every node) |
| `ValidateRecord` | Reader `r-event`; Writer `w-event`; Schema Access Strategy `Use 'Schema Text' Property` = `#{event.schema.strict}`; Strict Type Checking `true`; Validation Details Attribute Name `validation.details` |
| `InvokeHTTP` | HTTP Method `POST`; HTTP URL `#{flaky.url}`; Request Content-Type `application/json`; Connection Timeout and Socket Read Timeout `5 sec`; Concurrent Tasks `2`; **Penalty Duration (Settings) `0 sec`** |
| `RetryFlowFile` | Maximum Retries `3`; Penalize Retries `true`; Retry Attribute — the default `flowfile.retries`; **Penalty Duration (Settings) `10 sec`** — the pause before every retry |
| `error.stage=validation` (`UpdateAttribute`) | `error.stage` = `validation`; `error.reason` = `${validation.details}` |
| `error.stage=http-${invokehttp.status.code}` (`UpdateAttribute`) | `error.stage` = `http-${invokehttp.status.code}`; `error.reason` = `${invokehttp.status.message}` |
| `error.stage=retries-exceeded` (`UpdateAttribute`) | `error.stage` = `retries-exceeded`; `error.reason` = `last status ${invokehttp.status.code:replaceEmpty(${invokehttp.java.exception.class})}` — the HTTP status, or the exception class when the last attempt got no response (`java.net.UnknownHostException` when `flaky` is stopped) |
| `filename (ok)` (`UpdateAttribute`) | `filename` = `${uuid}.json` |
| `PutFile (ok)` | Directory `#{ok.dir}`; Conflict Resolution `replace` |
| `LogAttribute (Response)` | Log Level `info`; `success` auto-terminated |

**DLQ group.**

| Component | Settings |
|---|---|
| input port `dlq-in` | the only entry into the group |
| `UpdateAttribute` | `error.time` = `${now():format("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")}`; `filename` = `${uuid}.json` |
| `PutFile (DLQ)` | Directory `#{dlq.dir}/${error.stage}` (the subdirectory is created automatically); Conflict Resolution `replace` |

**Connections.**

| From | Relationships | To |
|---|---|---|
| `GenerateRecord` | `success` | `ValidateRecord` |
| `ValidateRecord` | `valid` | `InvokeHTTP` |
| `ValidateRecord` | `invalid`, `failure` | `error.stage=validation` |
| `InvokeHTTP` | `Original` | `filename (ok)` |
| `InvokeHTTP` | `Response` | `LogAttribute (Response)` |
| `InvokeHTTP` | `Retry`, `Failure` | `RetryFlowFile` (with a bend point, see below) |
| `InvokeHTTP` | `No Retry` | `error.stage=http-…` |
| `RetryFlowFile` | `retry` | `InvokeHTTP` (with a bend point) |
| `RetryFlowFile` | `retries_exceeded`, `failure` | `error.stage=retries-exceeded` |
| the three `error.stage=…` | `success` | `dlq-in` — Back Pressure Object Threshold `100000`, Size Threshold `10 GB` |

## What happens to the data

**1. Generation.** Every 2 seconds `GenerateRecord` creates a FlowFile with 5 records. Each of the 4 fields is `null` with a probability of 5%, so a record is fully filled with a probability of 0.95⁴ ≈ 81%, and a whole FlowFile of 5 records is clean only in ~35% of cases.

**2. Validation.** `ValidateRecord` reads the FlowFile with the lenient Reader and checks every record against the strict schema. It does not reject the FlowFile as a whole: the valid records are written into one FlowFile (`valid`), the invalid ones into another (`invalid`). A FlowFile with 4 good and 1 bad record therefore produces two FlowFiles. The reason is written into the attribute `validation.details` (which fields failed) and copied into `error.reason`.

**3. Sending.** `InvokeHTTP` POSTs the content of the `valid` FlowFile to `flaky` and routes it by the response:

| Response | Relationship | What travels | Where to |
|---|---|---|---|
| `2xx` | `Original` | the original FlowFile (our events) | `nifi/data/ok` |
| `2xx` | `Response` | a new FlowFile with the response body (`{}`) | the log |
| `5xx` | `Retry` | the original FlowFile, penalized by `InvokeHTTP` | `RetryFlowFile` |
| `4xx` and other non-retryable codes | `No Retry` | the original FlowFile | the DLQ `http-400` |
| no response (connection refused, DNS, timeout) | `Failure` | the original FlowFile | `RetryFlowFile` |

`Original` and `Response` are both produced on success. The events are in `Original`, so that is what goes to `ok`; `Response` is only useful for diagnostics.

`InvokeHTTP` adds `invokehttp.status.code`, `invokehttp.status.message`, `invokehttp.request.url`, `invokehttp.response.body` (the beginning of the body), `invokehttp.request.duration`, and `invokehttp.tx.id`. On `Failure` there is no status code; instead the exception is described by `invokehttp.java.exception.class` and `invokehttp.java.exception.message`.

**4. Retrying.** `RetryFlowFile` keeps a counter in `flowfile.retries`. Each time a FlowFile arrives the counter grows by one:

| Arrival | `flowfile.retries` | Result |
|---|---|---|
| 1st (after the first attempt failed) | `1` | `retry`, penalty 10 sec |
| 2nd | `2` | `retry`, penalty 10 sec |
| 3rd | `3` | `retry`, penalty 10 sec |
| 4th | over the limit | `retries_exceeded` → DLQ |

So a FlowFile gets **4 attempts** in total (the first one and 3 retries) and spends about 30 seconds in the loop before it gives up. A penalized FlowFile stays in the queue `RetryFlowFile → InvokeHTTP`; `InvokeHTTP` takes the other FlowFiles in the meantime, so a retry does not block new data.

**Why `Penalty Duration` of `InvokeHTTP` is `0 sec`.** `InvokeHTTP` penalizes a FlowFile itself before sending it to `Retry`, with its own *Penalty Duration*. With the default `30 sec` the FlowFile first waits 30 seconds in the queue `InvokeHTTP → RetryFlowFile`, then another 10 in `RetryFlowFile → InvokeHTTP`: each retry takes 40 seconds instead of 10, and in the UI it looks as if `RetryFlowFile` is not taking anything. The backoff should live in one place — `RetryFlowFile`.

**5. The DLQ.** Every error path sets `error.stage` and `error.reason` and sends the FlowFile into `dlq-in`. Inside the group `error.time` and a unique `filename` are added, and `PutFile` writes the content into `#{dlq.dir}/${error.stage}`:

| `error.stage` | Directory | Content |
|---|---|---|
| `validation` | `nifi/data/dlq/validation` | only the invalid records of a FlowFile |
| `http-400` | `nifi/data/dlq/http-400` | a batch of valid records the API refused |
| `retries-exceeded` | `nifi/data/dlq/retries-exceeded` | a batch of valid records that could not be delivered after 4 attempts |

The DLQ connections have a high back pressure threshold (`100000` / `10 GB`): if the DLQ cannot keep up (the disk is slow or full), the main flow must not be the first to stop.

## Expected numbers

For a valid FlowFile, with 60% / 30% / 10% from `flaky`:

| Outcome | Probability | Why | Measured on the stand |
|---|---|---|---|
| delivered (`ok`) | ~85% | `0.6 × (1 + 0.3 + 0.3² + 0.3³)` — success on one of the 4 attempts | 86.7% |
| `http-400` | ~14% | a `400` on one of the attempts ends the loop at once | 12.5% |
| `retries-exceeded` | ~1% | four `503` in a row: `0.3⁴ ≈ 0.8%` | 0.8% |

A delivered FlowFile needed 1 / 2 / 3 / 4 attempts in ~71 / 21 / 6 / 2% of cases (measured 64 / 30 / 4 / 2%); a generated FlowFile has at least one invalid record with a probability of `1 − 0.95²⁰ ≈ 64%` (measured 64.2%). The measured column is one run of `make lab08-measure` (4 min of normal work, 120 valid FlowFiles; 243 FlowFiles generated over the whole run) — with samples this small, a few percentage points of difference are noise.

The `retries-exceeded` directory therefore appears only after several minutes of normal work, or right away if the API is stopped.

**When `flaky` is stopped** (`docker stop flaky`), every request ends in `Failure` with `java.net.UnknownHostException` (the stopped container no longer resolves), every FlowFile passes 4 attempts and lands in `retries-exceeded` about 30 seconds after it was generated. Nothing reaches `ok`. The queue `RetryFlowFile → InvokeHTTP` holds the FlowFiles that are waiting for their next attempt — on the stand it rose to ~25 in the first ~40 seconds, then a batch left for the DLQ and the queue held at ~15 (one FlowFile every 2 seconds × ~30 seconds in the loop) and does **not** grow: FlowFiles leave the loop as fast as they enter it, so back pressure is never reached and the generator keeps running. The data is not lost, but it all piles up in the DLQ. After `docker start flaky` the FlowFiles still in the loop are delivered on their next attempt (on the stand the retry queue drained within ~20 seconds) — nothing has to be restarted. The ones already in `retries-exceeded` stay there: returning them is a separate replay step (see below).

## How to check

- **The canvas.** The counters *In/Out* of `ValidateRecord`, `InvokeHTTP`, and `RetryFlowFile` over the last 5 minutes; the queue `RetryFlowFile → InvokeHTTP` shows the penalized FlowFiles (the clock icon in *List queue*).
- **Data Provenance** on `PutFile (DLQ)`: the attributes `error.stage`, `error.reason`, `error.time`, `flowfile.retries`, `invokehttp.status.code`. For a FlowFile that was retried, the lineage shows several `SEND` events from `InvokeHTTP` in a row.
- **The whole scenario at once.** `make lab08-measure` runs normal work, an outage, and the recovery on the imported group and writes a report with charts to `labs/` of the stand repository (see the task, "Measuring the whole scenario").
- **The host.**

  ```bash
  ls nifi/data/ok | wc -l
  for d in nifi/data/dlq/*/; do echo "$d $(ls "$d" | wc -l)"; done
  cat "$(ls -t nifi/data/dlq/validation/* | head -1)"     # the invalid records: there is a null in each of them
  ```

## Simplifications of the reference solution

The flow is built for the lab; for production, close these gaps:

- **`PutFile` has `failure` auto-terminated** in both `ok` and the DLQ: if the disk is full, the data is silently lost. In production route `failure` back into `PutFile` (or into framework retry) and alert on it.
- **`flowfile.retries` is not removed after success**: a FlowFile delivered on the 3rd attempt keeps `flowfile.retries=2`. If the flow had another `RetryFlowFile` with the same attribute, the counter would continue from that value (see [[materials/day-3#312-failure-and-retry-relationships-framework-level-retry-retryflowfile|Day 3, §3.1.2]]). Delete it in `filename (ok)` (*Delete Attributes Expression* `flowfile\.retries.*`) or use different *Retry Attribute* names.
- **The pause is fixed** (10 sec). For an API under load, an exponential backoff is better: framework retry on the `Retry` relationship with *Backoff Policy* `Penalize` and *Maximum Backoff Period*, or `UpdateAttribute` that computes the penalty from `flowfile.retries`.
- **The DLQ is write-only.** Only the content is stored in files; the attributes (`error.*`, the HTTP status) are visible only in provenance, which expires. In production write the DLQ to a place that keeps metadata (Kafka with headers, a table, or `PutFile` + an attribute file via `AttributesToJSON`) and add a replay flow: `GetFile` from `dlq/retries-exceeded` → back into `InvokeHTTP`.

## Common mistakes

- **A loop without a counter** (`Retry` → straight back into `InvokeHTTP`) — a FlowFile that always gets `503` circulates forever. Always put `RetryFlowFile` or framework retry with a limit in the loop.
- **`retries_exceeded` routed back to the input** — the same endless loop, just longer.
- **`400` sent to `RetryFlowFile`** — retrying a bad request is pointless: it will fail every time and only load the API.
- **`Failure` left unconnected or auto-terminated** — when the API is down, the data disappears without a trace, or the processor becomes invalid.
- **Retries take ~40 sec instead of 10** and FlowFiles hang penalized in the queue in front of `RetryFlowFile` — *Penalty Duration* of `InvokeHTTP` was left at `30 sec`.
- **The `invalid` branch is always empty** — the generator schema has no `["null", …]` fields or *Nullable Fields* is `false`: `GenerateRecord` produces only records valid against its own schema.
- **Everything lands in one `dlq` directory** — `error.stage` is not set on one of the paths, and `${error.stage}` evaluates to an empty string.
- **`GenerateRecord` / `ValidateRecord` are invalid with "references a Controller Service that does not exist"** — the processors point to service ids that are not in this NiFi. On import (upload or pasting the JSON onto the canvas) NiFi creates the services with new ids and rewrites the references — but on paste only for properties marked `identifiesControllerService: true` in `propertyDescriptors` of the Flow Definition; a hand-edited JSON without these descriptors breaks on paste. The reference files have them; for your own flow select `r-event` / `w-event` again in the processor properties.
- **The "overlapping connections" warning** — `InvokeHTTP → RetryFlowFile` and `RetryFlowFile → InvokeHTTP` lie on one line and look like one connection. Double-click a connection to add a bend point and drag it aside.
- **The DLQ fills the disk** — add a cleanup and monitoring (`MonitorDiskUsage`, an alert on the size of `dlq`).

**Reference solution:** [lab8-retry-errors.json](https://github.com/MagnetonBora/nifi-playground/blob/master/nifi/flows/lab8-retry-errors.json).
