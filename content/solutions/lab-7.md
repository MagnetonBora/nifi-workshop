---
title: "Lab 7 — Delivering events to Splunk"
aliases:
  - exercises/lab-7-solution
  - labs/lab-7-solution
---

> From the materials of [[materials/day-2|Day 2 — Data Processing and Integrations]]. The lab stand is described on the [[labs/index|Labs]] page; loading the reference solutions — on the [[solutions/index|Solutions]] page.
> Task: [[labs/lab-7|Lab 7 — Delivering events to Splunk]].

**Architecture.** The group *Lab 7 - Splunk delivery* with the shared services (`r-event`, `w-event-ndjson`) and two groups inside, each with its own generator — run them one at a time:

```text
A - PutSplunk (TCP):   GenerateRecord (100/s) ► MergeRecord (10,000 or 10 s) ► PutSplunk ─ failure ─► LogAttribute
B - InvokeHTTP (HEC):  GenerateRecord (100/s) ► MergeRecord (10,000 or 10 s) ► InvokeHTTP ─ Failure / Retry / No Retry ─► LogAttribute
```

**`PutSplunk` configuration:** Protocol `TCP`; Hostname `#{splunk.host}`; Port `#{splunk.port}` (`5514`); Message Delimiter `\n`; Concurrent Tasks `1` (then an experiment with `2`, `4`). The `Connection Per FlowFile` property does not exist in NiFi 2.12 (see 2.3.2).

**`InvokeHTTP` (HEC) configuration:** HTTP Method `POST`; HTTP URL `#{hec.url}`; Request Content-Type `application/json`; the dynamic property `Authorization` = `Splunk #{hec.token}` — the dynamic properties of `InvokeHTTP` become request headers. `Response` and `Original` are auto-terminated.

## Destination: emulator or real Splunk

The flow does not change — only two parameters. There are two reference files that differ only in these values: `lab7-splunk-delivery-mock.json` (Parameter Context `lab7-params-mock`) and `lab7-splunk-delivery-splunk.json` (`lab7-params-splunk`); the contexts have different names, so both groups can be imported into the same NiFi side by side.

| Parameter | Emulator (`lab7-splunk-delivery-mock.json`) | Real Splunk (`lab7-splunk-delivery-splunk.json`, needs `make splunk`) |
|---|---|---|
| `splunk.host` | `splunk-sim` | `splunk` |
| `hec.url` | `http://splunk-sim:8088/services/collector/event` | `http://splunk:8088/services/collector/raw` |
| `splunk.port`, `hec.token` | `5514`, `00000000-0000-0000-0000-000000000000` | the same |

> **The HEC body format.** The generator writes plain NDJSON — one event per line. The `/services/collector/event` endpoint of a real Splunk accepts events only in the envelope `{"time": …, "sourcetype": …, "event": {…}}` (see 2.3.4) and rejects "bare" NDJSON; the emulator accepts any body. That is why for the real Splunk the lab uses **`/services/collector/raw`**: Splunk splits the body into lines itself, one line — one event. The alternative is to keep `/event` and wrap the records before `InvokeHTTP`, for example with `JoltTransformRecord` using the specification `[{"operation":"shift","spec":{"*":"event.&"}},{"operation":"default","spec":{"sourcetype":"nifi:json"}}]` (Reader `r-event`, Writer `w-event-ndjson`; checked on the stand: Splunk answers `Success` and accepts several such objects in one request body; do not set `index` — the HEC token already writes to `nifi`) — see also exercise 4 in [[labs/lab-6|Lab 6]].

The real Splunk on the stand is preconfigured by the app `splunk/apps/nifi_playground`: the TCP input `5514` and the HEC input with the token `00000000-…` both write to the index `nifi` with the sourcetype `nifi:json` (`KV_MODE = json` — the fields are extracted at search time; the event time = the time of arrival, because the generator produces random timestamps).

## Checking the result

**Emulator.** `make lab-splunk-count` — the lines in `/data/splunk_in.log` (TCP, group A) and `/data/hec_in.log` (HEC, group B). Zero the files before each run.

**Real Splunk** (*Search & Reporting*, `http://localhost:8000`):

| Search | What it shows | Example on the stand |
|---|---|---|
| `index=nifi \| stats count` | the number of events — must equal the records that left `MergeRecord` | |
| `index=nifi \| head 5` | every event is a separate entry with the fields extracted | |
| `index=nifi \| stats count by level` | the JSON fields are parsed | `ERROR`, `INFO`, `WARN` |
| `index=nifi \| stats count by source, sourcetype` | which way the events came: TCP or HEC | `tcp:5514` (group A), `http:nifi` (group B); sourcetype `nifi:json` |
| `index=nifi earliest=-5m \| timechart span=10s count` | the throughput of the current run | |

Limit every search to the time of the run (`earliest=-5m` or the time picker) instead of cleaning the index.

## The experiment

| # | Setting | How | What to expect |
|---|---|---|---|
| 1 | small FlowFiles of 100 events | `GenerateRecord` directly to the sender | one task, one TCP write / one HTTP request per 100 events — the most overhead per event |
| 2 | batches of 10,000 | the flow as built | ~100 times fewer tasks and requests for the same events |
| 3 | batches + *Concurrent Tasks* `4` | on `PutSplunk` / `InvokeHTTP` | helps only if the sender is the bottleneck; with a slow destination it just moves the queue |

With the default load (100 events per second) all three settings deliver exactly what the generator produces — raise *Number of Records* to `1000`–`5000` to see a difference. Typical conclusions: batches matter most; HEC costs an HTTP request and a response per batch, but gives an explicit result (status code) and a token; TCP is the simplest, but `success` of `PutSplunk` only means "written to the socket".

## Common mistakes

- `PutSplunk` goes to `failure` — the destination is unavailable or closed the connection (the real Splunk is not started: `make splunk`; `splunk.host` still points to the other destination).
- All events arrive as one entry — there is no delimiter between events (*Message Delimiter* `\n`).
- HEC replies `{"text":"No data","code":5}` — bare NDJSON sent to `/services/collector/event` of a real Splunk: use `/raw` or wrap the events in `{"event": …}`.
- HEC replies `401/403` — the header is not `Authorization: Splunk <token>`, or the token is wrong.
- Nothing is found in Splunk — the search is not on `index=nifi`, the time picker does not cover the run, or `hec.url` / `splunk.host` still point to `splunk-sim` (the events are in the emulator's files, not in Splunk).
- Splunk does not parse the time — the format or field order does not match the sourcetype settings (on the stand the time of arrival is used deliberately).

**Reference solution:** [lab7-splunk-delivery-mock.json](https://github.com/MagnetonBora/nifi-playground/blob/master/nifi/flows/lab7-splunk-delivery-mock.json) — the emulator `splunk-sim`; [lab7-splunk-delivery-splunk.json](https://github.com/MagnetonBora/nifi-playground/blob/master/nifi/flows/lab7-splunk-delivery-splunk.json) — the real Splunk (`splunk`, HEC `/raw`).
