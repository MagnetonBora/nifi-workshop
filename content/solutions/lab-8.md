---
title: "Lab 8 — Retry and error handling"
aliases:
  - exercises/lab-8-solution
  - labs/lab-8-solution
---

> From the materials of [[materials/day-3|Day 3 — Production, Security, and Performance]]. The lab stand is described on the [[labs/index|Labs]] page; loading the reference solutions — on the [[solutions/index|Solutions]] page.
> Task: [[labs/lab-8|Lab 8 — Retry and error handling]].

**Architecture.**

```text
GenerateRecord ► ValidateRecord ─ invalid ─► [DLQ group]
                     │ valid
                     ▼
              InvokeHTTP (an endpoint that simulates failures)
                 ├ Original (2xx) ► PutFile #{ok.dir}
                 ├ Response ► LogAttribute
                 ├ Retry ──► RetryFlowFile ─ retry ► (penalize) ► InvokeHTTP
                 │                 └ retries_exceeded ► [DLQ group]
                 ├ No Retry ───────────────────────────────► [DLQ group]
                 └ Failure ► RetryFlowFile (the same one)
[DLQ group]: UpdateAttribute (error.*) ► PutFile #{dlq.dir}/${error.stage} (with a high back pressure threshold)
```

`Original` is the source FlowFile of the request after a 2xx response (it is the one that makes sense to put into `ok`); `Response` is the response body (here `{}`).

**Configuration.**

| Component | Setting |
|---|---|
| `GenerateRecord` | Schema Text — a schema with `["null", …]` fields; Number of Records `5`; Nullable Fields `true`; Null Value Percentage `5`; Run Schedule `2 sec` |
| `ValidateRecord` | Schema Access Strategy `Use 'Schema Text' Property` — a strict schema (all fields required); Strict Type Checking `true`; Validation Details Attribute Name `validation.details` |
| `InvokeHTTP` | URL `#{flaky.url}`; POST; Connection/Read Timeout — `5 sec` each; Concurrent Tasks `2` |
| `RetryFlowFile` | Maximum Retries `3`; Penalize Retries `true`; Retry Attribute — the default; the dynamic property `error.stage` = `retries_exceeded`. The penalty value is *Penalty Duration* on the Settings tab (for example, `10 sec`) |
| The connection `...► DLQ` | Back Pressure Object Threshold `100000`, Size `10 GB` |
| `UpdateAttribute` (before the DLQ) | `error.stage` = `validation` / `http-${invokehttp.status.code}` / `retries-exceeded` (depending on the error path); `filename` = `${uuid}.json` |
| `PutFile` (DLQ) | Directory `#{dlq.dir}/${error.stage}`; Conflict Resolution `replace` (names by `${uuid}`) |

**Mistakes and diagnostics.** A loop without a counter (retry → the same processor, without `RetryFlowFile`) — endless retries; the `Failure` queue is not empty and blocks the source — it must be routed to a sink; the DLQ directory fills the disk — add monitoring (`MonitorDiskUsage`) and cleanup; the `invalid` branch is always empty — the generator produces only records that are valid according to its own schema (see "A data source with invalid records").

**Reference solution:** [lab8-retry-errors.json](https://github.com/MagnetonBora/nifi-playground/blob/master/nifi/flows/lab8-retry-errors.json).
