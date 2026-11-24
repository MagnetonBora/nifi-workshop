---
title: "Lab 6 — An NDJSON transformation pipeline"
aliases:
  - exercises/lab-6-solution
  - labs/lab-6-solution
---

> From the materials of [[materials/day-2|Day 2 — Data Processing and Integrations]]. The lab stand is described on the [[labs/index|Labs]] page; loading the reference solutions — on the [[solutions/index|Solutions]] page.
> Task: [[labs/lab-6|Lab 6 — An NDJSON transformation pipeline]].

**Structure of the reference flow.** One group *Lab 6 - NDJSON pipeline* with the Parameter Context `lab6-params` and the shared services (`reader-input-schema`, `reader-output-schema`, `writer-output-schema`, `writer-as-is`); inside it two independent groups, each with its own generator:

| Group | How the output gets its shape | Output |
|---|---|---|
| *Part 1 - Writer schema* | `UpdateRecord` adds `ingest_ts`; the Writer schema `log.schema.out` puts it first and drops `user.pwd` | `#{out.dir}` (`nifi/data/out`) |
| *Part 2 - Jolt* | `JoltTransformRecord` removes `user.pwd` and puts `ingest_ts` first; the Writers only inherit the schema | `#{out.dir}/jolt` (`nifi/data/out/jolt`) |

Both send the invalid lines to `#{quarantine.dir}`.

## Part 1 — Writer schema

**Architecture.**

```text
GenerateFlowFile(NDJSON) ► ValidateRecord ─ invalid / failure ─► UpdateAttribute(filename) ► PutFile (#{quarantine.dir})
        │ valid
        ▼
UpdateRecord (writer schema: ingest_ts first, without user.pwd) ► MergeRecord (NDJSON) ► UpdateAttribute(filename) ► PutFile (#{out.dir})
```

**Configuration.**

| Component | Key properties |
|---|---|
| `JsonTreeReader` `reader-input-schema` | Schema Access: `Use 'Schema Text' Property` = `#{log.schema.in}` (the input schema with `ts`, `level`, `svc`, `msg`, `user{id,pwd}`; `pwd` is allowed) |
| `JsonTreeReader` `reader-output-schema` | Schema Access: `Use 'Schema Text' Property` = `#{log.schema.out}` (the `LogEvent` schema) — the Reader for `MergeRecord` |
| `JsonRecordSetWriter` `writer-output-schema` | `Use 'Schema Text' Property` = `#{log.schema.out}` (the `LogEvent` schema from section 2.2.4); Output Grouping `One Line Per Object`; Pretty Print `false` |
| `JsonRecordSetWriter` `writer-as-is` | `Inherit Record Schema`; `One Line Per Object` — for invalid rows (keeps them as is) |
| `ValidateRecord` | Reader `reader-input-schema`; Writer `writer-output-schema`; Record Writer for Invalid Records `writer-as-is`; Schema Access Strategy `Use Reader's Schema`; Strict Type Checking `true` |
| `UpdateRecord` | Reader `reader-input-schema`; Writer `writer-output-schema`; Strategy `Literal Value`; `/ingest_ts` = `${now():format("yyyy-MM-dd'T'HH:mm:ss.SSSXXX", "UTC")}` |
| `MergeRecord` | Reader `reader-output-schema`, Writer `writer-output-schema` (both — the `LogEvent` schema); Min Records `1000`; Max Records `10000`; **Max Bin Age** `30 sec` |
| `UpdateAttribute` | `filename` = `${uuid}.ndjson` (before each `PutFile`) |
| `PutFile` | Directory `#{out.dir}` (for invalid ones — `#{quarantine.dir}`); Conflict Resolution `replace` |

**What happens to the data.** After `ValidateRecord`, line 3 goes to `invalid` (a type error/missing fields). The remaining records in `UpdateRecord` get `ingest_ts` as the first field and lose `user.pwd`; `MergeRecord` assembles batches of up to 10,000 records or on timeout.

**Common mistakes (part 1).** The `ingest_ts` field does not appear — it is absent from the Writer schema; the key order is "wrong" — the Writer uses `Inherit Record Schema` rather than an explicit schema; `MergeRecord` does not release data — `Max Bin Age` is not set and *Minimum Number of Records* has not been reached (the generator produces 2 valid records every 10 seconds — without `Max Bin Age` the bin will never close); the secret remains — you are removing it from the Reader schema, not from the Writer schema (fields that are absent from the Writer schema are discarded precisely in the Writer); in `MergeRecord` you cannot specify a Writer as a Reader — a separate `JsonTreeReader` with the `LogEvent` schema is needed.

## Part 2 — Jolt

**Architecture.**

```text
GenerateFlowFile(NDJSON) ► ValidateRecord ─ invalid / failure ─► UpdateAttribute(filename) ► PutFile (#{quarantine.dir})
        │ valid                                                              ▲
        ▼                                                                    │ failure
UpdateAttribute (ingest.ts) ► JoltTransformRecord ─────────────────────────────┘
                                   │ success
                                   ▼
                              MergeRecord ► UpdateAttribute(filename) ► PutFile (#{out.dir}/jolt)
```

**Configuration** (only what differs from part 1).

| Component | Key properties |
|---|---|
| `UpdateAttribute` `ingest.ts` | `ingest.ts` = `${now():format("yyyy-MM-dd'T'HH:mm:ss.SSSXXX", "UTC")}` |
| `JoltTransformRecord` | Reader `reader-input-schema`; Writer `writer-as-is` (*Inherit Record Schema*); Jolt Transform `Chain`; Transform Cache Size `10`; the specification below; `original` auto-terminated, `failure` → quarantine |
| `JsonTreeReader` `reader-inferred-schema` | *Infer Schema* — in the group *Part 2 - Jolt*, for `MergeRecord` |
| `MergeRecord` | Reader `reader-inferred-schema`, Writer `writer-as-is`; Min Records `1000`; Max Records `10000`; Max Bin Age `30 sec` |
| `PutFile` | Directory `#{out.dir}/jolt` |

```json
[
  { "operation": "remove", "spec": { "user": { "pwd": "" } } },
  { "operation": "shift",  "spec": { "#${ingest.ts}": "ingest_ts", "*": "&" } }
]
```

**What happens to the data.** `ValidateRecord` works exactly as in part 1. `UpdateAttribute` puts the current time into the attribute `ingest.ts`; NiFi substitutes it into the specification before Jolt runs (*Jolt Specification* supports Expression Language over FlowFile attributes). `remove` deletes `user.pwd`; in `shift`, `#<value>` writes a constant into `ingest_ts` and `"*": "&"` copies the remaining fields under their own names — the constant is written first, so `ingest_ts` is the first field. `MergeRecord` reads with *Infer Schema* and writes with *Inherit Record Schema*, so no schema reshapes the records after Jolt: the output must be the same as in part 1.

**Part 1 or part 2?** The Writer schema is a contract: fields that are not in it never reach the output, the order is fixed, a new field in the input is dropped automatically. Jolt is more flexible — renaming, nesting, computed values (see the exercises in the lab) — but a `"*": "&"` passes through any new field, including a new secret. In practice they are combined: Jolt for the structure, an explicit Writer schema as the final filter.

**Common mistakes (part 2).**

- The broken line reaches the output — `ValidateRecord` was removed: Jolt does not validate anything, `{"ts":"bad","level":"INFO"}` goes through it unchanged plus `ingest_ts`.
- The output in `out/jolt` looks exactly like part 1 although the specification does something else — `MergeRecord` still uses `reader-output-schema` / `writer-output-schema`, and the schema `log.schema.out` reshapes the records again.
- `ingest_ts` is empty or literally `${ingest.ts}` — the attribute is not set before `JoltTransformRecord`, or the time format is written straight into the specification: quotes inside Expression Language break the JSON of the specification — compute it in `UpdateAttribute`.
- NiFi is slow on `JoltTransformRecord` — *Transform Cache Size* is left at `1`: because of Expression Language every FlowFile has its own specification, and it is recompiled every time.

**Reference solution:** [lab6-ndjson-pipeline.json](https://github.com/MagnetonBora/nifi-playground/blob/master/nifi/flows/lab6-ndjson-pipeline.json) — both parts.
