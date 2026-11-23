---
title: "Lab 7 — Delivering events to Splunk"
aliases:
  - exercises/lab-7-solution
  - labs/lab-7-solution
---

> From the materials of [[materials/day-2|Day 2 — Data Processing and Integrations]]. The lab stand is described on the [[labs/index|Labs]] page; loading the reference solutions — on the [[solutions/index|Solutions]] page.
> Task: [[labs/lab-7|Lab 7 — Delivering events to Splunk]].

**Architecture.**

```text
...► MergeRecord (NDJSON) ► PutSplunk (TCP) ► [Splunk TCP input / a netcat emulator]
                       └─► (an alternative) InvokeHTTP ► [HEC / an http emulator]
```

**`PutSplunk` configuration:** Protocol `TCP`; Hostname `#{splunk.host}` (`splunk-sim`); Port `#{splunk.port}` (`5514`); Message Delimiter `\n`; Concurrent Tasks `1` (then an experiment with `2`, `4`). The `Connection Per FlowFile` property does not exist in NiFi 2.12 (see 2.3.2).

**`InvokeHTTP` (HEC) configuration:** HTTP Method `POST`; HTTP URL `#{hec.url}`; Request Content-Type `application/json`; the dynamic property `Authorization` = `Splunk #{hec.token}` — the dynamic properties of `InvokeHTTP` become request headers.

> **The HEC body format.** The `/services/collector/event` endpoint of a real Splunk accepts events only in the envelope `{"time": …, "sourcetype": …, "event": {…}}` (see 2.3.4) and rejects "bare" NDJSON. The stand's emulator accepts any body, so the lab works without the envelope. For a real HEC, wrap the records before `InvokeHTTP`, for example with `JoltTransformRecord` using the specification `[{"operation":"shift","spec":{"*":"event.&"}},{"operation":"default","spec":{"sourcetype":"app:log","index":"main"}}]` (checked on 2.12.0) — or send to `/services/collector/raw`.

**Common mistakes.** `PutSplunk` goes to `failure` — the destination is unavailable/closed the connection; everything on one line — there is no delimiter between events; Splunk does not parse the time — the format/field order does not match the sourcetype settings; HEC replies `400 Invalid data format` — the event is not wrapped in the `{"event": …}` envelope.

**Reference solution:** [lab7-splunk-delivery.json](https://github.com/MagnetonBora/nifi-playground/blob/master/nifi/flows/lab7-splunk-delivery.json) (without the HEC envelope — designed for the emulator).
