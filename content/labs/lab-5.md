---
title: "Lab 5 — A database + REST + Kafka with record processing"
aliases:
  - exercises/lab-5
---

> From the materials of [[materials/day-2|Day 2 — Data Processing and Integrations]]. The lab stand is described on the [[labs/index|Labs]] page; loading the reference solutions — on the [[solutions/index|Solutions]] page.
> Solution: [[solutions/lab-5|Lab 5 — solution]].

**Goal.** Build a flow that reads orders from Kafka, checks them, adds customer data from a REST API, writes them to the database and publishes them to an output topic. Every order that fails along the way goes to a dead-letter queue (DLQ) with the stage where it failed.

**Where the data goes.**

| | On the stand |
|---|---|
| Input | Kafka topic `orders.raw` |
| Customer data | REST API `http://customers-api:8000/customers/<customer_id>` |
| Output | table `orders_stage` and Kafka topic `orders.enriched` (key = `order_id`) |
| Errors | Kafka topic `orders.dlq`, with the header `error.stage` = `validate`, `lookup` or `db` |

```text
ConsumeKafka ► ValidateRecord ► LookupRecord ► SplitRecord ► PutDatabaseRecord ► PublishKafka (orders.enriched)
                     │               │                              │
                  invalid         unmatched                      failure
                     └───────────────┴──────────► UpdateAttribute (error.stage) ► PublishKafka (orders.dlq)
```

## Before you start

```bash
make lab-setup       # the table orders_stage and the topics orders.raw / orders.enriched / orders.dlq
```

The table has a business rule: `CHECK (amount > 0)`. An order with a negative amount is rejected by the database itself and must end up in the DLQ.

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

**Parameter Context `lab5-params`** (values for the stand, [[materials/Appendix-A#a5-addresses-for-the-labs|Appendix A.5]]): `kafka.bootstrap=kafka1:19092`, `db.url=jdbc:postgresql://postgres:5432/nifi`, `db.user=nifi`, `db.password=nifi` (sensitive), `db.driver=/opt/nifi/drivers/postgresql-42.7.14.jar`, `customers.api=http://customers-api:8000`, `topic.in=orders.raw`, `topic.out=orders.enriched`, `topic.dlq=orders.dlq`.

## Steps

1. **Build the flow** in a Process Group *Lab 5* with `lab5-params`, stage by stage. The settings of every component are in the [[solutions/lab-5|solution]].
   - `ConsumeKafka` from `#{topic.in}`, Group ID `nifi-lab05`, *Processing Strategy* `RECORD`, offset reset `earliest`.
   - `ValidateRecord` against the schema `order.v1`: `invalid` → DLQ.
   - `LookupRecord` with a `RestLookupService` on `#{customers.api}/customers/${customer_id}` (its *Record Reader* — a `JsonTreeReader` with *Infer Schema*): `unmatched` → DLQ. Three settings of `LookupRecord` decide whether the order is enriched at all:
     - the dynamic property **`customer_id`** = `/customer_id` — the lookup key; its name is what `${customer_id}` in the URL refers to;
     - **Result RecordPath** `/` — where to put the answer of the API. Empty by default: then `LookupRecord` only routes the record to `matched` and does not change it — the order goes on without the customer;
     - **Record Result Contents** `Insert Record Fields` — copy the fields of the answer (`customer_name`, `customer_tier`, `country`) into the record itself. The default `Insert Entire Record` puts the whole answer into one nested field, and the columns `customer_name` / `customer_tier` of `orders_stage` stay empty.

     The *Record Writer* of `LookupRecord` and everything after it must not use the fixed schema `order.v1` (it has no customer fields): use a Writer with *Inherit Record Schema*.
   - `SplitRecord` (1 record per FlowFile), then `PutDatabaseRecord` UPSERT into `orders_stage` by `order_id`: `failure` → DLQ. The database connection is the same as in [[labs/lab-4|Lab 4]].
   - `PublishKafka` into `#{topic.out}` with the key from `order_id`.
   - DLQ: on every error path an `UpdateAttribute` sets `error.stage`, then one `PublishKafka` into `#{topic.dlq}`.

   Stuck or short on time — import the reference flow and study it instead: `make import f=nifi/flows/lab5-db-rest-kafka.json`, then *Enable all controller services*.

2. **Start the group** — all processors.

3. **Send the orders:**

   ```bash
   make lab05-data
   ```

   This puts 100 orders into `orders.raw` (the file `lab-services/data/lab5-orders.ndjson`): 82 correct ones and 6 for each kind of error.

4. **Watch the flow.**
   - After `ConsumeKafka` there are only **3 FlowFiles**, not 100: in `RECORD` mode it packs many messages into one FlowFile and splits them by record structure — the correct orders, the orders with `"amount":"n/a"`, and `o-1004` without `customer_id`. **List queue** → ⓘ → the attribute `record.count` shows how many orders each FlowFile holds.
   - The content of such a FlowFile is NDJSON, one order per line. The Content Viewer in the *formatted* mode shows only the first one — switch **View** to *original* to see all of them.
   - `PutDatabaseRecord` shows ERROR bulletins `violates check constraint "orders_stage_amount_positive"` — one for each order with a negative amount. This is expected: the database rejected the order, and it went to the DLQ.

5. **Check the result** (see below) and explain for each kind of order why it ended up where it did.

## Expected behavior

| Orders | Example | What happens |
|---|---|---|
| 82 correct orders | `o-1001` | the whole chain: a row in `orders_stage` with `customer_name` and `customer_tier` from the API (`o-1001` → `Alice Example`, `gold`), a message in `orders.enriched` |
| 6 with a negative amount | `o-1002` | pass validation and the lookup, the database rejects them by `CHECK` → DLQ, `error.stage=db`; the other orders of the same batch are not affected thanks to `SplitRecord` |
| 6 with an unknown customer | `o-1003` (`c-9999`) | the API answers 404 → `unmatched` in `LookupRecord` → DLQ, `error.stage=lookup` |
| 6 broken orders | `o-1004` | no `created_at`, `amount` is not a number → `invalid` in `ValidateRecord` → DLQ, `error.stage=validate`, the reason in `validation.details` |

## Verification

After one `make lab05-data` on clean topics: **82** rows in `orders_stage`, **82** messages in `orders.enriched`, **18** in `orders.dlq` — 6 for each `error.stage`.

```bash
docker exec nifi-postgres psql -U nifi -d nifi -c "SELECT count(*) FROM orders_stage"                    # 82
docker exec nifi-postgres psql -U nifi -d nifi \
   -c "SELECT order_id, customer_name, customer_tier, amount, currency FROM orders_stage ORDER BY order_id LIMIT 5"
docker exec kafka1 kafka-console-consumer --bootstrap-server kafka1:19092 \
   --topic orders.enriched --from-beginning --timeout-ms 10000 --property print.key=true
docker exec kafka1 kafka-console-consumer --bootstrap-server kafka1:19092 \
   --topic orders.dlq --from-beginning --timeout-ms 10000 --property print.headers=true \
   | grep -o 'error.stage:[a-z]*' | sort | uniq -c                                                       # 6 db, 6 lookup, 6 validate
```

Every `make lab05-data` sends the same 100 orders again: the table stays at 82 rows (UPSERT), but the topics `orders.enriched` and `orders.dlq` grow by 82 and 18.

## Re-running

The consumer group `nifi-lab05` remembers what it has read: without a reset, `ConsumeKafka` reads only new messages. Stop `ConsumeKafka`, then:

```bash
docker exec kafka1 kafka-consumer-groups --bootstrap-server kafka1:19092 --delete --group nifi-lab05
docker exec nifi-postgres psql -U nifi -d nifi -c "TRUNCATE orders_stage"
```

For exact counts in the topics, recreate them as well:

```bash
for t in orders.raw orders.enriched orders.dlq; do
  docker exec kafka1 kafka-topics --bootstrap-server kafka1:19092 --delete --topic $t
done
make lab-setup
```

## Without the stand

Send four orders by hand — one of each kind:

```bash
docker exec -i kafka1 kafka-console-producer \
  --bootstrap-server kafka1:19092 --topic orders.raw <<'EOF'
{"order_id":"o-1001","customer_id":"c-1","amount":120.50,"currency":"EUR","created_at":1731660000000}
{"order_id":"o-1002","customer_id":"c-2","amount":-5.00,"currency":"EUR","created_at":1731660001000}
{"order_id":"o-1003","customer_id":"c-9999","amount":45.00,"currency":"USD","created_at":1731660002000}
{"order_id":"o-1004","amount":"not-a-number","currency":"EUR"}
EOF
```

Expected: 1 row in the database and 1 message in `orders.enriched` (`o-1001`), 3 messages in `orders.dlq`.
