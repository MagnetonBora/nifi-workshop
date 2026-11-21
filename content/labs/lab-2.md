---
title: "Lab 2 — Several sources and routing"
aliases:
  - exercises/lab-2
---

> From the materials of [[materials/day-1|Day 1 — Apache NiFi Fundamentals]]. The lab stand is described on the [[labs/index|Labs]] page; loading the reference solutions — on the [[solutions/index|Solutions]] page.
> Solution: [[solutions/lab-2|Lab 2 — solution]].

**Goal.** Receive data from several sources (files, HTTP, a generator) and route it by attributes.

**Build order.**

1. Create the Parameter Context `lab-params` and bind it to the groups (*Configure Process Group → Parameter Context*).
2. Build the `ingest` group: three sources and an output port inside.
3. Build the `route` group; connect the port of the `ingest` group to the input port of `route`.
4. Set the `NewestFlowFileFirstPrioritizer` priority on the connection after the merge (for demonstration).
5. Send a test event and put in files (commands from the lab stand host, from the repository root):

```bash
curl -X POST -H 'Content-Type: application/json' \
     -d '{"id":1,"type":"click"}' http://localhost:8081/events
echo '{"id":10,"type":"file"}' > nifi/data/in/sample.json
printf 'id,type\n2,view\n' > nifi/data/in/sample.csv
```

**Verification.** FlowFiles appear in the `json`/`csv` queues; the `source` attribute matches the source; the HTTP event goes to `json` with `mime.type=application/json`; the provenance events show a `ROUTE` event.
