---
title: "Lab 5 — A database + REST + Kafka with record processing"
aliases:
  - exercises/lab-5
---

> From the materials of [[materials/day-2|Day 2 — Data Processing and Integrations]]. The lab stand is described on the [[labs/index|Labs]] page; loading the reference solutions — on the [[solutions/index|Solutions]] page.
> Solution: [[solutions/lab-5|Lab 5 — solution]].

**Goal.** Build a flow: order events are read from Kafka, enriched with data from a REST API, written to a database, and the result is published to an output topic. The database connection (`DBCPConnectionPool`, `PutDatabaseRecord` UPSERT) works the same way as in [[labs/lab-4|Lab 4]].

**The database table.** On the stand it is created by `make lab-setup` (the file `postgres/init/02-lab5-orders.sql`):

```sql
CREATE TABLE IF NOT EXISTS orders_stage (
    order_id       varchar(64)    PRIMARY KEY,
    customer_id    varchar(64),
    customer_name  varchar(256),
    customer_tier  varchar(32),
    amount         numeric(12, 2) NOT NULL CONSTRAINT orders_stage_amount_positive CHECK (amount > 0),
    currency       varchar(3)     NOT NULL,
    created_at     timestamptz,
    loaded_at      timestamptz    NOT NULL DEFAULT now()
);
```

The `CHECK (amount > 0)` constraint is the business rule on which `o-1002` goes to the DLQ.

**Parameter Context `lab5-params`** (values for the stand, [[materials/Appendix-A#a5-addresses-for-the-labs|Appendix A.5]]): `kafka.bootstrap=kafka1:19092`, `db.url=jdbc:postgresql://postgres:5432/nifi`, `db.user=nifi`, `db.password=nifi` (sensitive), `db.driver=/opt/nifi/drivers/postgresql-42.7.14.jar`, `customers.api=http://customers-api:8000`.

**Preparing test data.** On the stand: `make lab5-data`, or manually:

```bash
docker exec -i kafka1 kafka-console-producer \
  --bootstrap-server kafka1:19092 --topic orders.raw <<'EOF'
{"order_id":"o-1001","customer_id":"c-1","amount":120.50,"currency":"EUR","created_at":1731660000000}
{"order_id":"o-1002","customer_id":"c-2","amount":-5.00,"currency":"EUR","created_at":1731660001000}
{"order_id":"o-1003","customer_id":"c-9999","amount":45.00,"currency":"USD","created_at":1731660002000}
{"order_id":"o-1004","amount":"not-a-number","currency":"EUR"}
EOF
```

**Expected behavior.**

| Record | Expectation |
|---|---|
| `o-1001` | Passes through the whole chain; an UPSERT in the database (with `customer_name = Alice Example`, `customer_tier = gold`); into `orders.enriched` with the key `o-1001` |
| `o-1002` (a negative amount) | Violates the `CHECK` in the database → `failure` → the DLQ with `error.stage=db`; does not affect `o-1001` thanks to `SplitRecord` |
| `o-1003` (no such customer) | `unmatched` in `LookupRecord` → the DLQ with `error.stage=lookup` |
| `o-1004` | `invalid` in `ValidateRecord` (no `created_at`, `amount` is not a number) → the DLQ with `error.stage=validate` and `validation.details` |

**Verification.**

```bash
docker exec kafka1 kafka-console-consumer --bootstrap-server kafka1:19092 \
   --topic orders.enriched --from-beginning --timeout-ms 10000 --property print.key=true
docker exec kafka1 kafka-console-consumer --bootstrap-server kafka1:19092 \
   --topic orders.dlq --from-beginning --timeout-ms 10000 --property print.headers=true
docker exec nifi-postgres psql -U nifi -d nifi \
   -c "SELECT order_id, customer_name, customer_tier, amount, currency FROM orders_stage ORDER BY order_id"
```

**Re-running.** The consumer group `nifi-lab5` stores the offsets it has read: on a re-run `ConsumeKafka` will read nothing. Reset the group (with the processor stopped) and clear the table:

```bash
docker exec kafka1 kafka-consumer-groups --bootstrap-server kafka1:19092 --delete --group nifi-lab5
docker exec nifi-postgres psql -U nifi -d nifi -c "TRUNCATE orders_stage"
```
