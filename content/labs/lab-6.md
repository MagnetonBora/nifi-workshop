---
title: "Lab 6 — An NDJSON transformation pipeline"
aliases:
  - exercises/lab-6
---

> From the materials of [[materials/day-2|Day 2 — Data Processing and Integrations]]. The lab stand is described on the [[labs/index|Labs]] page; loading the reference solutions — on the [[solutions/index|Solutions]] page.
> Solution: [[solutions/lab-6|Lab 6 — solution]].

**Goal.** Clean up NDJSON logs: drop broken lines into quarantine, remove the password from every event, put the ingestion time first, and merge the events into large NDJSON files.

**Where the data goes.**

| | On the stand |
|---|---|
| Input | `GenerateFlowFile`: the same 3 log lines every 10 seconds |
| Output | NDJSON files in `/opt/nifi/data/out` (on the host: `nifi/data/out`) |
| Broken lines | files in `/opt/nifi/data/quarantine` (on the host: `nifi/data/quarantine`) |

```text
GenerateFlowFile ► ValidateRecord ─ valid ─► UpdateRecord (ingest_ts) ► MergeRecord ► PutFile (out)
                        └ invalid ─► PutFile (quarantine)
```

**The input** (`Custom Text` of `GenerateFlowFile`, *Mime Type* `application/x-ndjson`): two correct events and one broken.

```json
{"ts":"2026-11-15T10:00:00Z","level":"INFO","svc":"api","msg":"started","user":{"id":7,"pwd":"s3cret"}}
{"ts":"2026-11-15T10:00:01Z","level":"ERROR","svc":"api","msg":"db timeout","user":{"id":9,"pwd":"x"}}
{"ts":"bad","level":"INFO"}
```

**The trick of the lab:** there is no separate step that deletes `pwd` or reorders fields. Both are done by the **output schema** of the Writer: it has no `user.pwd` and has `ingest_ts` as the first field, so the Writer simply writes the record in that shape.

| Schema | Fields |
|---|---|
| `log.schema.in` — reading | `ts`, `level`, `svc`, `msg` (all required), `user` {`id`, `pwd`} |
| `log.schema.out` — writing | **`ingest_ts`**, `ts`, `level`, `svc`, `msg`, `user` {`id`} — no `pwd` |

## Before you start

```bash
make lab-setup        # creates nifi/data/out and nifi/data/quarantine
```

Parameter Context `lab6-params`: `out.dir=/opt/nifi/data/out`, `quarantine.dir=/opt/nifi/data/quarantine`, `log.schema.in` and `log.schema.out` — the schema texts (one schema is used by several services, so it lives in a parameter). The schemas are in the [[solutions/lab-6|solution]].

## Steps

1. **Create the services:** a `JsonTreeReader` with `#{log.schema.in}`, a `JsonRecordSetWriter` with `#{log.schema.out}` (*One Line Per Object*), and a Writer that inherits the record schema — for the invalid lines.
2. **`GenerateFlowFile`** with the text above, *Run Schedule* `10 sec`.
3. **`ValidateRecord`** against the reader schema, *Strict Type Checking* `true`: `valid` → next, `invalid` → `PutFile` into `#{quarantine.dir}`.
4. **`UpdateRecord`**: the property `/ingest_ts` = `${now():format("yyyy-MM-dd'T'HH:mm:ss.SSSXXX", "UTC")}`, *Replacement Value Strategy* `Literal Value`, Writer with `log.schema.out`.
5. **`MergeRecord`**: *Minimum Number of Records* `1000`, *Maximum* `10000`, *Max Bin Age* `30 sec` → `UpdateAttribute` (`filename` = `${uuid}.ndjson`) → `PutFile` into `#{out.dir}`.
6. **Start the group** and wait about a minute.

## Expected behavior

| Line | What happens |
|---|---|
| the two correct events | go through; in the output file: `ingest_ts` first, `user` without `pwd` |
| `{"ts":"bad","level":"INFO"}` | `invalid` — the required `svc` and `msg` are missing → a file in quarantine, unchanged |

- A new file in quarantine every 10 seconds.
- The output files appear about every **30 seconds**: there are far fewer than 1000 records, so `MergeRecord` closes the bin by *Max Bin Age*. Each file holds ~6 events (3 runs of the generator × 2 correct events).

## Verification

From the root of the stand repository:

```bash
head -n 2 nifi/data/out/*.ndjson
# {"ingest_ts":"2026-11-23T...","ts":"2026-11-15T10:00:00Z","level":"INFO","svc":"api","msg":"started","user":{"id":7}}
grep -c pwd nifi/data/out/*.ndjson     # 0 in every file
cat nifi/data/quarantine/*.ndjson      # {"ts":"bad","level":"INFO"}
```

## Re-running

```bash
rm -f nifi/data/out/*.ndjson nifi/data/out/jolt/*.ndjson nifi/data/quarantine/*.ndjson
```

## Part 2: the same with Jolt

In part 1 the shape of the output comes from the Writer schema. Now do the same transformation with Jolt only: the specification itself removes `pwd` and puts `ingest_ts` first, and the Writer just writes what it gets.

1. Replace `UpdateRecord` with two processors:
   - **`UpdateAttribute`**: the attribute `ingest.ts` = `${now():format("yyyy-MM-dd'T'HH:mm:ss.SSSXXX", "UTC")}`.
   - **`JoltTransformRecord`**: *Record Reader* `reader-input-schema`, *Record Writer* `writer-as-is` (*Inherit Record Schema* — no output schema), *Jolt Transform* `Chain`, *Jolt Specification*:

   ```json
   [
     { "operation": "remove", "spec": { "user": { "pwd": "" } } },
     { "operation": "shift",  "spec": { "#${ingest.ts}": "ingest_ts", "*": "&" } }
   ]
   ```

   - `remove` deletes `user.pwd`.
   - `shift` with `#<value>` writes a constant into `ingest_ts`; NiFi substitutes `${ingest.ts}` from the attribute before Jolt runs. `"*": "&"` copies all other fields under their own names. The constant is written first, so `ingest_ts` is the first field.
   - Because of Expression Language every FlowFile gets its own specification — set *Transform Cache Size* to `10` or more, otherwise NiFi recompiles it all the time.

2. Connect `success` → `MergeRecord`, `failure` → quarantine. In `MergeRecord` set the *Record Reader* to a new `JsonTreeReader` with *Infer Schema* and the *Record Writer* to `writer-as-is` — otherwise its `log.schema.out` reshapes the records again and you cannot see what Jolt did. Keep `ValidateRecord` in front: Jolt does not validate anything — the broken line `{"ts":"bad","level":"INFO"}` would pass through it as `{"ingest_ts":"…","ts":"bad","level":"INFO"}`.
3. Clear the output directories, start the group and run the same verification: the result must be identical to part 1.

   In the reference flow part 2 is the group *Part 2 - Jolt*; it has its own generator and writes to `nifi/data/out/jolt`, so the two parts can be compared side by side: `head -n 2 nifi/data/out/jolt/*.ndjson`.

**Exercises.** Each one is applied to every record; the input is the first line of the lab data:

```json
{"ts":"2026-11-15T10:00:00Z","level":"INFO","svc":"api","msg":"started","user":{"id":7,"pwd":"s3cret"}}
```

To try a specification without starting the flow, use `JoltTransformJSON` → right-click → **Advanced**: it has an input field and shows the result immediately.

**1. Rename and flatten** (`shift`). Only the described fields get into the result — `pwd` disappears without a separate `remove`:

```json
[ { "operation": "shift", "spec": {
    "ts": "timestamp", "level": "severity", "svc": "service", "msg": "message",
    "user": { "id": "user_id" } } } ]
```

```json
{"timestamp":"2026-11-15T10:00:00Z","severity":"INFO","service":"api","message":"started","user_id":7}
```

**2. Add constants** (`default`). Adds only the keys that are missing, at any depth:

```json
[ { "operation": "default", "spec": { "env": "workshop", "source": "nifi", "user": { "role": "guest" } } } ]
```

```json
{"ts":"2026-11-15T10:00:00Z","level":"INFO","svc":"api","msg":"started","user":{"id":7,"pwd":"s3cret","role":"guest"},"env":"workshop","source":"nifi"}
```

**3. Change values and compute a field** (`modify-overwrite-beta`). The functions run in order, so `summary` already sees the upper-cased `svc`:

```json
[ { "operation": "modify-overwrite-beta", "spec": {
    "level": "=toLower", "svc": "=toUpper",
    "summary": "=concat(@(1,svc),': ',@(1,msg))" } } ]
```

```json
{"ts":"2026-11-15T10:00:00Z","level":"info","svc":"API","msg":"started","user":{"id":7,"pwd":"s3cret"},"summary":"API: started"}
```

**4. Wrap into an envelope** (`remove` + `shift`) — the shape that Splunk HEC expects (used in [[labs/lab-7|Lab 7]]):

```json
[ { "operation": "remove", "spec": { "user": { "pwd": "" } } },
  { "operation": "shift",  "spec": { "ts": "time", "#nifi-logs": "sourcetype", "*": "event.&" } } ]
```

```json
{"sourcetype":"nifi-logs","time":"2026-11-15T10:00:00Z","event":{"level":"INFO","svc":"api","msg":"started","user":{"id":7}}}
```

**Questions.** When is the Writer schema (part 1) better than Jolt, and when the other way round? What happens in exercise 1 if a new field appears in the input?

Reference flow: `make import f=nifi/flows/lab6-ndjson-pipeline.json` — the group *Lab 6 - NDJSON pipeline* with the shared services and two groups inside: *Part 1 - Writer schema* (writes to `nifi/data/out`) and *Part 2 - Jolt* (writes to `nifi/data/out/jolt`). Start one part at a time or both.
