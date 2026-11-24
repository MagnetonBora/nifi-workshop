---
title: "Lab 5 — A database + REST + Kafka with record processing"
aliases:
  - exercises/lab-5-solution
  - labs/lab-5-solution
---

> From the materials of [[materials/day-2|Day 2 — Data Processing and Integrations]]. The lab stand is described on the [[labs/index|Labs]] page; loading the reference solutions — on the [[solutions/index|Solutions]] page.
> Task: [[labs/lab-5|Lab 5 — A database + REST + Kafka with record processing]].

**Architecture.**

```text
ConsumeKafka(topic: orders.raw, Processing=RECORD, reader: infer) ─ parse failure ─► DLQ
   │ success
   ▼
ValidateRecord (schema order.v1) ─ invalid / failure ─► DLQ
   │ valid  (written with the order.v1 schema, the schema.name attribute)
   ▼
LookupRecord (RestLookupService /customers/${customer_id}) ─ unmatched / failure ─► DLQ
   │ matched
   ▼
SplitRecord (Records Per Split = 1) ─ failure ─► DLQ
   │ splits
   ▼
PutDatabaseRecord (UPSERT orders_stage) ─ failure ─► DLQ
   │ success                          └ retry ─► framework retry (3 attempts) ─► DLQ
   ▼
PublishKafka (orders.enriched, key = order_id) ─ failure ─► DLQ

DLQ: UpdateAttribute (error.stage = validate | lookup | db | publish) ► PublishKafka (orders.dlq, one message per row)
```

`SplitRecord` before the database is needed because `PutDatabaseRecord` executes the whole FlowFile in one transaction: `o-1001` and `o-1002` arrive in one FlowFile, and a `CHECK` error on `o-1002` would roll back `o-1001` too. Splitting by single records isolates errors at the cost of throughput; for large volumes the alternative is to keep the batches and handle `failure` separately.

**Controller Services.**

| Service | Configuration |
|---|---|
| `Kafka3ConnectionService` | Bootstrap Servers `#{kafka.bootstrap}`; Security Protocol depends on the environment (on the stand `PLAINTEXT`) |
| `JsonTreeReader` (`reader-infer`) | Schema Access Strategy `Infer Schema` — for `ConsumeKafka`, `ValidateRecord`, `SplitRecord`, `PutDatabaseRecord`, `PublishKafka` |
| `JsonTreeReader` (`reader-order`) | Schema Access Strategy: `Use 'Schema Name' Property` (`${schema.name}`); Schema Registry: `AvroSchemaRegistry` — for `LookupRecord` |
| `JsonRecordSetWriter` (`writer-order-ndjson`) | Schema Access: `Use 'Schema Name' Property` = `order.v1`; Schema Write Strategy: `Set 'schema.name' Attribute`; Output Grouping: `One Line Per Object`; Pretty Print `false` |
| `JsonRecordSetWriter` (`writer-ndjson`) | Schema Access `Inherit Record Schema`; `One Line Per Object` — for the remaining processors (keeps the fields added by `LookupRecord`) |
| `AvroSchemaRegistry` | The dynamic property `order.v1` with the schema from Block 1.3 |
| `DBCPConnectionPool` | URL `#{db.url}`; Driver `org.postgresql.Driver`; Driver Locations `#{db.driver}`; User `#{db.user}`; Password `#{db.password}` |
| `RestLookupService` | URL `#{customers.api}/customers/${customer_id}`; Record Reader — `JsonTreeReader` (Infer Schema) |

`ConsumeKafka` reads with `Infer Schema`, not with the `order.v1` schema: with a strict schema the record `o-1004` (`"amount":"not-a-number"`) cannot be parsed at all, whereas with schema inference it lands in a separate FlowFile (*Schema Conflict Resolution* `Create New FlowFile`) and is honestly rejected by validation. In `RECORD` mode `ConsumeKafka` has a relationship **`parse failure`** — messages that the Reader could not parse; route it to the DLQ, otherwise the processor is invalid.

**`ValidateRecord` configuration:** Record Reader `reader-infer`; Record Writer `writer-order-ndjson`; Record Writer for Invalid Records `writer-ndjson`; Schema Access Strategy `Use 'Schema Name' Property` = `order.v1`; *Allow Extra Fields* `false`; ***Strict Type Checking* `false`**; *Validation Details Attribute Name* `validation.details`. Relationships: `valid`, `invalid`, `failure`.

`Strict Type Checking = false` allows coercion of compatible types: a JSON number → `decimal`, `long` → `timestamp-millis`. With `true`, **all** records are rejected (JSON does not carry Avro logical types); `o-1004` remains invalid with `false` too — the string `not-a-number` cannot be coerced to a number, and the required `created_at` is missing. Valid records are written with the `order.v1` schema (with the correct `decimal` and `timestamp`) and get the `schema.name` attribute.

**`LookupRecord` configuration:** Reader `reader-order`; Writer `writer-ndjson`; Lookup Service `RestLookupService`; Routing Strategy `Route to 'matched' or 'unmatched'`; Record Result Contents `Insert Record Fields`; Record Update Strategy `Use Property`; Result RecordPath `/`; the dynamic property `customer_id` = `/customer_id`. The API responds with the fields `customer_name`, `customer_tier` — their names match the columns of `orders_stage`, so `LookupRecord` places them at the root of the record and `PutDatabaseRecord` writes them without mapping. "Customer not found" is a `404` response with an empty body: `RestLookupService` returns "no match", and the record goes to `unmatched`.

**`PutDatabaseRecord` configuration:** Database Type `PostgreSQL`; Statement Type `UPSERT`; Table Name `orders_stage`; Update Keys `order_id`; Unmatched Field Behavior `Ignore Unmatched Fields`; Unmatched Column Behavior `Ignore Unmatched Columns`; Batch Size — by measurement (a starting point: 1000). *Settings → Retry*: the `retry` relationship, 3 attempts; those that exhausted the attempts and `failure` go to the DLQ.

**DLQ configuration:** `UpdateAttribute` on every error path sets `error.stage` (`validate`, `lookup`, `db`, `publish`) and `error.time`; `PublishKafka` (`orders.dlq`) with *Message Demarcator* `\n` (one NDJSON line — one message) and *FlowFile Attribute Header Pattern* `error\..*|validation\..*` (the attributes go into the Kafka headers).

**Common mistakes and diagnostics.**

| Symptom | Cause | Action |
|---|---|---|
| `ConsumeKafka` reads nothing | A different `Group ID` and `Auto Offset Reset=latest`; or the group has already read the topic | Set `earliest` for the new Group ID; reset the group (see "Re-running") |
| `ConsumeKafka` is invalid: `Relationship 'parse failure' is not connected` | In RECORD mode the processor has a `parse failure` relationship | Route it to the DLQ |
| Endless retries of `PutDatabaseRecord` | `retry` is connected to the processor's input | Limit the number of attempts (framework retry), send the exhausted ones to the DLQ |
| Validation rejects all records | `Strict Type Checking = true` + the schema's logical types (`decimal`, `timestamp-millis`) against JSON numbers | `Strict Type Checking = false`, or bring the schema in line with the actual data (`double`, `long`) |
| There are no records in the database although some orders are correct | Correct and erroneous records are in one FlowFile: one error rolls back the transaction of the whole FlowFile | `SplitRecord` before `PutDatabaseRecord`, or separate handling of `failure` |
| Orders reach `matched`, but without customer fields (`orders.enriched` looks like `orders.raw`) | *Result RecordPath* of `LookupRecord` is empty (the default): the lookup only routes, it writes the answer nowhere | Result RecordPath `/`, Record Result Contents `Insert Record Fields` |
| The customer is there, but as one nested field (`"customer": {…}`); the columns stay empty | Record Result Contents is `Insert Entire Record` (the default) and/or Result RecordPath points to a new field | `Insert Record Fields` into `/` — the fields land next to `order_id` and match the columns |
| The customer fields disappear after `LookupRecord` | The Writer of `LookupRecord` (or a later Reader/Writer) uses the fixed schema `order.v1`, which has no customer fields | A Writer with *Inherit Record Schema* after the lookup, `Infer Schema` / inherit further on |
| Empty `customer_name` / `customer_tier` in the database | The names of the API response fields do not match the columns | Align the names (or `UpdateRecord` with `Record Path Value`) |
| Duplicates in the database | at-least-once + INSERT | Use UPSERT/a key |
| A "Failed to connect" bulletin | A wrong bootstrap/TLS errors | Check `Kafka3ConnectionService` and the availability of the broker from the NiFi container |

**Reference solution:** [lab5-db-rest-kafka.json](https://github.com/MagnetonBora/nifi-playground/blob/master/nifi/flows/lab5-db-rest-kafka.json) (the customers REST API — `lab-services/customers_api.py`).
