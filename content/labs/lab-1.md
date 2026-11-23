---
title: "Lab 1 — The NiFi UI and a first flow"
aliases:
  - exercises/lab-1
---

> From the materials of [[materials/day-1|Day 1 — Apache NiFi Fundamentals]]. The lab stand is described on the [[labs/index|Labs]] page; loading the reference solutions — on the [[solutions/index|Solutions]] page.
> Solution: [[solutions/lab-1|Lab 1 — solution]].

**Goal.** Get to know the NiFi UI: build the simplest flow, run it, look inside the queue and trace a FlowFile in provenance.

```text
GenerateFlowFile ──success──► UpdateAttribute ──success──► LogAttribute
```

- `GenerateFlowFile` creates a small JSON every 5 seconds, for example `{"id": 598, "ts": "2026-11-19T16:00:05.141Z"}`.
- `UpdateAttribute` adds two attributes: `lab.stage = ingested` and `event.id = ${UUID()}`.
- `LogAttribute` writes the attributes to the NiFi log.

## Before you start

Open NiFi at `https://localhost:8443/nifi` (login and password: `make urls`).

## Steps

1. **Add the processors.** Drag the Processor icon onto the canvas, find `GenerateFlowFile` by name; then `UpdateAttribute` and `LogAttribute`.
2. **Configure them.**
   - `GenerateFlowFile`: *Run Schedule* `5 sec`, *Mime Type* `application/json`, *Custom Text* `{"id": ${random():mod(1000)}, "ts": "${now():format("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")}"}`.
   - `UpdateAttribute`: add two properties with **+** — `lab.stage` = `ingested`, `event.id` = `${UUID()}`.
   - `LogAttribute`: *Log Level* `info`; under *Relationships* tick **terminate** for `success`.
3. **Connect them.** Drag an arrow from one processor to the next; in the dialog tick only `success`.
4. **Start the flow** (select everything → Start). Watch the counters on the processors (*In*, *Out*, *Tasks/Time*) and the number of FlowFiles on the connections.
5. **Look into the queue.** Stop `UpdateAttribute`: FlowFiles pile up before it. Right-click the connection → **List queue** → ⓘ: the *Attributes* tab and **View content**. Start `UpdateAttribute` again.
6. **Trace a FlowFile.** Right-click `LogAttribute` → **View data provenance** → open an event (ⓘ) → **Lineage graph**.

## Expected behavior

| Where | What you see |
|---|---|
| Queue after `GenerateFlowFile` | one new FlowFile every 5 seconds |
| Attributes after `UpdateAttribute` | `lab.stage = ingested`, a unique `event.id` |
| Content | JSON like `{"id": 598, "ts": "2026-11-19T16:00:05.141Z"}` |
| Provenance for one FlowFile | three events: `CREATE` (GenerateFlowFile), `ATTRIBUTES_MODIFIED` (UpdateAttribute), `DROP` (LogAttribute, auto-terminate) |

## Verification

```bash
make logs s=nifi | grep lab.stage        # LogAttribute lines with lab.stage = ingested; Ctrl+C to stop
```

## Typical problems

- A yellow triangle on a processor — a relationship is not connected or terminated, or a required property is empty: hover over the triangle to see the reason.
- The queue does not move — the next processor is stopped, or the queue has reached its back pressure limit.

Reference flow: `make import f=nifi/flows/lab1-first-flow.json`.
