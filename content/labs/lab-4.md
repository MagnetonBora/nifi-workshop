---
title: "Lab 4 — Connecting NiFi to a database"
aliases:
  - exercises/lab-4
---

> From the materials of [[materials/day-2|Day 2 — Data Processing and Integrations]]. The lab stand is described on the [[labs/index|Labs]] page; loading the reference solutions — on the [[solutions/index|Solutions]] page.
> Solution: [[solutions/lab-4|Lab 4 — solution]].

**Goal.** Connect NiFi to PostgreSQL and move data in both directions: read with a query, read a table incrementally, write records into another table.

**The tables.** Create them with `make lab-setup`. The source table `customers` holds 1000 customers; every third one is `gold` (333 rows):

```sql
CREATE TABLE IF NOT EXISTS customers (
    id          serial       PRIMARY KEY,
    name        varchar(128) NOT NULL,
    tier        varchar(16)  NOT NULL,     -- gold / silver / bronze
    country     varchar(2)   NOT NULL,
    updated_at  timestamptz  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS customers_gold (
    id         integer      PRIMARY KEY,
    name       varchar(128) NOT NULL,
    tier       varchar(16)  NOT NULL,
    country    varchar(2)   NOT NULL,
    copied_at  timestamptz  NOT NULL DEFAULT now()
);
```

Check that the data is there:

```bash
docker exec nifi-postgres psql -U nifi -d nifi -c "SELECT tier, count(*) FROM customers GROUP BY tier"
```

**Parameter Context `lab4-params`** (values for the stand, [[materials/Appendix-A#a5-addresses-for-the-labs|Appendix A.5]]): `db.url=jdbc:postgresql://postgres:5432/nifi`, `db.user=nifi`, `db.password=nifi` (sensitive), `db.driver=/opt/nifi/drivers/postgresql-42.7.14.jar`.

## Steps

Create a Process Group *Lab 4 - Database basics* and assign `lab4-params` to it.

1. **The connection.** Add a `DBCPConnectionPool` controller service: the URL, driver class `org.postgresql.Driver`, driver location, user and password — all from the parameters. Before enabling it, click **Verify** in the service settings: NiFi opens a test connection and shows the result. Then break it on purpose — set a wrong password, then a wrong driver path — and look at what **Verify** and the bulletins report. Restore the values and enable the service.

2. **Read with a query.** `ExecuteSQLRecord` with a `JsonRecordSetWriter` (one line per object):

   ```sql
   SELECT id, name, tier, country FROM customers WHERE tier = 'gold' ORDER BY id
   ```

   Connect `success` to a stopped processor, do **Run Once** and open the FlowFile in the queue: the content is 333 JSON lines; the attributes `executesql.row.count` and `executesql.query.duration` show the number of rows and the query time.

3. **Write.** Send the result to `PutDatabaseRecord`: a `JsonTreeReader` (*Infer Schema*), *Database Type* `PostgreSQL`, table `customers_gold`, *Statement Type* `INSERT`, *Unmatched Column Behavior* `Ignore Unmatched Columns` (the table has the column `copied_at`, the records do not). Route `failure` and `retry` to a `LogAttribute`.
   - **Run Once** on `ExecuteSQLRecord`: 333 rows appear in `customers_gold`.
   - **Run Once** again: the FlowFile goes to `failure` — the primary key already exists.
   - Switch to *Statement Type* `UPSERT` with *Update Keys* `id` and repeat: `success`, still 333 rows. A repeated load is now idempotent.

4. **Read incrementally.** `QueryDatabaseTableRecord`: table `customers`, *Database Type* `PostgreSQL`, *Columns to Return* `id, name, tier, country`, *Maximum-value Columns* `id`, the same Writer, *Run Schedule* 15 sec.
   - Start it: one FlowFile with all 1000 rows (`querydbtable.row.count`, `maxvalue.id`).
   - Right-click → **View State**: the processor remembers `id = 1000`.
   - Add rows: `make lab4-insert n=10`. Within 15 s a FlowFile with only the 10 new rows comes out, the state becomes 1010.
   - Nothing new in the table — no FlowFiles at all.
   - Stop the processor, **Clear State**, start it again: the whole table is read again.

5. **Bring it together.** **Run Once** on `ExecuteSQLRecord` once more: the 10 new gold customers are added, the existing 333 are updated.

## Expected behavior

| Action | Expectation |
|---|---|
| **Verify** with correct parameters | the connection succeeds |
| **Verify** with a wrong password / driver path | the verification fails: password authentication / the driver cannot be loaded |
| `ExecuteSQLRecord`, Run Once | one FlowFile, `executesql.row.count = 333` |
| `PutDatabaseRecord` INSERT, the second run | `failure`: duplicate key (`customers_gold_pkey`) |
| `PutDatabaseRecord` UPSERT, any run | `success`, the number of rows does not grow on repeats |
| `QueryDatabaseTableRecord`, first run | 1000 rows, state `id = 1000` |
| after `make lab4-insert n=10` | 10 rows, state `id = 1010` |
| final Run Once of `ExecuteSQLRecord` | `customers_gold` has 343 rows |

## Verification

```bash
docker exec nifi-postgres psql -U nifi -d nifi -c "SELECT count(*), max(id) FROM customers_gold"     # 343 | 1010
docker exec nifi-postgres psql -U nifi -d nifi -c "SELECT count(*), max(id) FROM customers"          # 1010 | 1010
```

## Re-running

Return the tables to the initial state and clear the state of `QueryDatabaseTableRecord` (stopped → **View State** → **Clear State**):

```bash
docker exec nifi-postgres psql -U nifi -d nifi -c "DELETE FROM customers WHERE id > 1000; TRUNCATE customers_gold"
```
