---
title: "Lab 4 — Connecting NiFi to a database"
aliases:
  - exercises/lab-4-solution
  - labs/lab-4-solution
---

> From the materials of [[materials/day-2|Day 2 — Data Processing and Integrations]]. The lab stand is described on the [[labs/index|Labs]] page; loading the reference solutions — on the [[solutions/index|Solutions]] page.
> Task: [[labs/lab-4|Lab 4 — Connecting NiFi to a database]].

**Architecture.**

```text
ExecuteSQLRecord (gold customers) ─ success ─► PutDatabaseRecord (UPSERT customers_gold) ─ success ─► LogAttribute (copied)
          └ failure ─────────────────────────────────┴ failure, retry ─► LogAttribute (db error)

QueryDatabaseTableRecord (customers, max id) ─ success ─► LogAttribute (new rows)
```

**Controller services** (all in the group, the Parameter Context `lab4-params`):

| Service | Settings |
|---|---|
| `db-pool` — `DBCPConnectionPool` | *Database Connection URL* `#{db.url}`, *Database Driver Class Name* `org.postgresql.Driver`, *Database Driver Locations* `#{db.driver}`, *Database User* `#{db.user}`, *Password* `#{db.password}` |
| `writer-json` — `JsonRecordSetWriter` | *Output Grouping* `One Line Per Object`, *Schema Write Strategy* `Do Not Write Schema`, *Schema Access Strategy* `Inherit Record Schema` |
| `reader-json` — `JsonTreeReader` | *Schema Access Strategy* `Infer Schema` |

**Processors.**

| Processor | Settings | Scheduling |
|---|---|---|
| `Read gold customers` — `ExecuteSQLRecord` | *SQL Query* `SELECT id, name, tier, country FROM customers WHERE tier = 'gold' ORDER BY id`, *Record Writer* `writer-json` | 1 min, Primary node; started with **Run Once** |
| `Copy to customers_gold` — `PutDatabaseRecord` | *Record Reader* `reader-json`, *Database Type* `PostgreSQL`, *Statement Type* `UPSERT`, *Table Name* `customers_gold`, *Update Keys* `id`, *Unmatched Field Behavior* `Ignore Unmatched Fields`, *Unmatched Column Behavior* `Ignore Unmatched Columns` | 0 sec |
| `Read new customers` — `QueryDatabaseTableRecord` | *Database Type* `PostgreSQL`, *Table Name* `customers`, *Columns to Return* `id, name, tier, country`, *Maximum-value Columns* `id`, *Initial Load Strategy* `Start at Beginning`, *Record Writer* `writer-json` | 15 sec, Primary node |
| `LogAttribute (copied / db error / new rows)` | *Log Level* `info` (`error` for db error), `success` auto-terminated | 0 sec |

**What happens to the data.**

- `ExecuteSQLRecord` puts the whole result set into one FlowFile (*Max Rows Per FlowFile* = 0) and writes `executesql.row.count`. One FlowFile with 333 records means one transaction and one JDBC batch in `PutDatabaseRecord`, not 333 separate inserts.
- `PutDatabaseRecord` with `UPSERT` and the `PostgreSQL` type generates `INSERT … ON CONFLICT (id) DO UPDATE`. With `Generic` the processor does not know the dialect and UPSERT is not available. `copied_at` is filled by the column default because the records have no such field and *Unmatched Column Behavior* allows it.
- `QueryDatabaseTableRecord` adds `WHERE id > <state>` to its query and keeps the new maximum in the processor state (`customers@!@id`). The output FlowFile carries `querydbtable.row.count` and `maxvalue.id`. When nothing is new, it produces nothing. It does not notice updated or deleted rows: for that you need CDC.
- In a cluster both readers must run on the Primary node only, otherwise every node reads the same rows.

**Result on the stand.** After the steps of the lab: `customers` — 1010 rows; `customers_gold` — 343 rows (`max(id) = 1010`); the state of `Read new customers` is `id = 1010`.

**Common mistakes.**

- The URL contains `localhost` — inside the NiFi container that is NiFi itself; the database host is `postgres`.
- **Verify** or enabling the service reports that the driver class cannot be loaded — a wrong path in *Database Driver Locations*; the JAR is in `/opt/nifi/drivers` (on the host: `nifi/drivers`).
- *Password* references a non-sensitive parameter — NiFi does not allow it: `db.password` must be marked sensitive.
- `PutDatabaseRecord` sends everything to `failure` complaining about the column `copied_at` — *Unmatched Column Behavior* is left at the default `Fail on Unmatched Columns`, and the records have no such field.
- UPSERT is not offered or fails — *Database Type* is `Generic`; set `PostgreSQL`.
- A repeated INSERT fails with a duplicate key — expected; this is why idempotent loads use UPSERT.
- `QueryDatabaseTableRecord` reads nothing after the table was reset — the state still holds the old maximum: stop the processor and **Clear State**. If the state is empty and the table is large, the first run reads the whole table at once — set *Max Rows Per FlowFile* or *Initial Load Strategy* `Start at Current Maximum Values`.

**Reference solution:** [lab4-db-basics.json](https://github.com/MagnetonBora/nifi-playground/blob/master/nifi/flows/lab4-db-basics.json).
