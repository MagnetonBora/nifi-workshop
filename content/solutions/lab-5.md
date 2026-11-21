---
title: "Lab 5 — An NDJSON transformation pipeline"
aliases:
  - exercises/lab-5-solution
  - labs/lab-5-solution
---

> From the materials of [[materials/day-2|Day 2 — Data Processing and Integrations]]. The lab stand is described on the [[labs/index|Labs]] page; loading the reference solutions — on the [[solutions/index|Solutions]] page.
> Task: [[labs/lab-5|Lab 5 — An NDJSON transformation pipeline]].

**Architecture.**

```text
GenerateFlowFile(NDJSON) ► ValidateRecord ─ invalid / failure ─► UpdateAttribute(filename) ► PutFile (#{quarantine.dir}) / PublishKafka(dlq)
        │ valid
        ▼
UpdateRecord (writer schema: ingest_ts first, without user.pwd) ► MergeRecord (NDJSON) ► UpdateAttribute(filename) ► PutFile (#{out.dir})
```

**Configuration.**

| Component | Key properties |
|---|---|
| `JsonTreeReader` `r-log` | Schema Access: `Use 'Schema Text' Property` = `#{log.schema.in}` (the input schema with `ts`, `level`, `svc`, `msg`, `user{id,pwd}`; `pwd` is allowed) |
| `JsonTreeReader` `r-log-out` | Schema Access: `Use 'Schema Text' Property` = `#{log.schema.out}` (the `LogEvent` schema) — the Reader for `MergeRecord` |
| `JsonRecordSetWriter` `w-log` | `Use 'Schema Text' Property` = `#{log.schema.out}` (the `LogEvent` schema from section 2.2.4); Output Grouping `One Line Per Object`; Pretty Print `false` |
| `JsonRecordSetWriter` `w-raw` | `Inherit Record Schema`; `One Line Per Object` — for invalid rows (keeps them as is) |
| `ValidateRecord` | Reader `r-log`; Writer `w-log`; Record Writer for Invalid Records `w-raw`; Schema Access Strategy `Use Reader's Schema`; Strict Type Checking `true` |
| `UpdateRecord` | Reader `r-log`; Writer `w-log`; Strategy `Literal Value`; `/ingest_ts` = `${now():format("yyyy-MM-dd'T'HH:mm:ss.SSSXXX", "UTC")}` |
| `MergeRecord` | Reader `r-log-out`, Writer `w-log` (both — the `LogEvent` schema); Min Records `1000`; Max Records `10000`; **Max Bin Age** `30 sec` |
| `UpdateAttribute` | `filename` = `${uuid}.ndjson` (before each `PutFile`) |
| `PutFile` | Directory `#{out.dir}` (for invalid ones — `#{quarantine.dir}`); Conflict Resolution `replace` |

**What happens to the data.** After `ValidateRecord`, line 3 goes to `invalid` (a type error/missing fields). The remaining records in `UpdateRecord` get `ingest_ts` as the first field and lose `user.pwd`; `MergeRecord` assembles batches of up to 10,000 records or on timeout.

**Common mistakes.** The `ingest_ts` field does not appear — it is absent from the Writer schema; the key order is "wrong" — the Writer uses `Inherit Record Schema` rather than an explicit schema; `MergeRecord` does not release data — `Max Bin Age` is not set and *Minimum Number of Records* has not been reached (the generator produces 2 valid records every 10 seconds — without `Max Bin Age` the bin will never close); the secret remains — you are removing it from the Reader schema, not from the Writer schema (fields that are absent from the Writer schema are discarded precisely in the Writer); in `MergeRecord` you cannot specify a Writer as a Reader — a separate `JsonTreeReader` with the `LogEvent` schema is needed.

**Reference solution:** [lab5-ndjson-pipeline.json](https://github.com/MagnetonBora/nifi-playground/blob/master/nifi/flows/lab5-ndjson-pipeline.json).
