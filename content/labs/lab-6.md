---
title: "Lab 6 — An NDJSON transformation pipeline"
aliases:
  - exercises/lab-6
---

> From the materials of [[materials/day-2|Day 2 — Data Processing and Integrations]]. The lab stand is described on the [[labs/index|Labs]] page; loading the reference solutions — on the [[solutions/index|Solutions]] page.
> Solution: [[solutions/lab-6|Lab 6 — solution]].

**Goal.** Build a pipeline: NDJSON logs → validation → removing a secret → adding `ingest_ts` to the start of the object → merging into large NDJSON files.

The Parameter Context `lab6-params`: `out.dir=/opt/nifi/data/out`, `quarantine.dir=/opt/nifi/data/quarantine` (on the stand host — `nifi/data/out`, `nifi/data/quarantine`), `log.schema.in` and `log.schema.out` — the schema texts (one schema is used by several services).

**Input data (`Custom Text` in `GenerateFlowFile`, Mime Type `application/x-ndjson`):**

```json
{"ts":"2026-11-15T10:00:00Z","level":"INFO","svc":"api","msg":"started","user":{"id":7,"pwd":"s3cret"}}
{"ts":"2026-11-15T10:00:01Z","level":"ERROR","svc":"api","msg":"db timeout","user":{"id":9,"pwd":"x"}}
{"ts":"bad","level":"INFO"}
```

**Verification** (from the stand host, from the repository root):

```bash
head -n 2 nifi/data/out/*.ndjson
# we expect: {"ingest_ts":"2026-11-15T...","ts":"2026-11-15T10:00:00Z","level":"INFO",...,"user":{"id":7}}
grep -c pwd nifi/data/out/*.ndjson   # we expect 0
cat nifi/data/quarantine/*.ndjson    # we expect {"ts":"bad","level":"INFO"}
```
