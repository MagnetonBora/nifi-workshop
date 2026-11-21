---
title: "Lab 1 — The NiFi UI and a first flow"
aliases:
  - exercises/lab-1
---

> From the materials of [[materials/day-1|Day 1 — Apache NiFi Fundamentals]]. The lab stand is described on the [[labs/index|Labs]] page; loading the reference solutions — on the [[solutions/index|Solutions]] page.
> Solution: [[solutions/lab-1|Lab 1 — solution]].

**Goal.** Get to know the NiFi UI, create, run, and trace the simplest flow, and see provenance.

**Expected result.** The flow `GenerateFlowFile → UpdateAttribute → LogAttribute`, queue statistics, provenance events, and the FlowFile content in the Content Viewer.

**Sequence.**

1. Add the processors by dragging the Processor icon onto the canvas (search by name).
2. Connect them with a `success` connection; in the Connection dialog check only `success`.
3. For `LogAttribute`, enable `success` under *Settings → Automatically Terminate Relationships*.
4. Start the flow (Start). Watch the *In/Out* counters, *Tasks/Time*, and the queue on the connection.
5. Right-click the connection → **List queue** → view attributes and content (to see it with the destination stopped, stop `UpdateAttribute` and the queue will fill).
6. Right-click the processor → **View data provenance** → open an event and the **Lineage graph**.

**Verification.** In `logs/nifi-app.log` (on the lab stand: `make logs s=nifi`) `LogAttribute` entries with the attributes `lab.stage=ingested` appear; provenance shows three events for one `uuid`; in the Content Viewer — JSON of the form `{"id": 598, "ts": "2026-11-19T16:00:05.141Z"}`.
