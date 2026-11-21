---
title: "Lab 4 — БД + REST + Kafka на record-обработке"
aliases:
  - exercises/lab-4
---

> Из материалов [[materials/day-2|Day 2 — Data Processing and Integrations]]. Стенд и загрузка эталонов — на странице [[labs/index|Labs]].

**Цель.** Собрать flow: события заказов читаются из Kafka, обогащаются данными из REST API, записываются в БД, а результат публикуется в выходной топик.

**Архитектура.**

```text
ConsumeKafka(topic: orders.raw, Processing=RECORD, reader: infer) ─ parse failure ─► DLQ
   │ success
   ▼
ValidateRecord (schema order.v1) ─ invalid / failure ─► DLQ
   │ valid  (записано схемой order.v1, атрибут schema.name)
   ▼
LookupRecord (RestLookupService /customers/${customer_id}) ─ unmatched / failure ─► DLQ
   │ matched
   ▼
SplitRecord (Records Per Split = 1) ─ failure ─► DLQ
   │ splits
   ▼
PutDatabaseRecord (UPSERT orders_stage) ─ failure ─► DLQ
   │ success                          └ retry ─► framework retry (3 попытки) ─► DLQ
   ▼
PublishKafka (orders.enriched, key = order_id) ─ failure ─► DLQ

DLQ: UpdateAttribute (error.stage = validate | lookup | db | publish) ► PublishKafka (orders.dlq, по сообщению на строку)
```

`SplitRecord` перед БД нужен потому, что `PutDatabaseRecord` выполняет весь FlowFile одной транзакцией: `o-1001` и `o-1002` приходят одним FlowFile, и ошибка `CHECK` у `o-1002` откатила бы и `o-1001`. Разбиение по одной записи изолирует ошибки ценой пропускной способности; альтернатива для больших объёмов — оставить пачки и разбирать `failure` отдельно.

**Таблица в БД.** На стенде создаётся `make lab-setup` (файл `postgres/init/01-lab4-orders.sql`):

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

Ограничение `CHECK (amount > 0)` — бизнес-правило, на котором `o-1002` уходит в DLQ.

**Parameter Context `lab4-params`** (значения для стенда, [[materials/Appendix-A#a5-addresses-for-the-labs|Приложение A.5]]): `kafka.bootstrap=kafka1:19092`, `db.url=jdbc:postgresql://postgres:5432/nifi`, `db.user=nifi`, `db.password=nifi` (sensitive), `db.driver=/opt/nifi/drivers/postgresql-42.7.14.jar`, `customers.api=http://customers-api:8000`.

**Controller Services.**

| Сервис | Конфигурация |
|---|---|
| `Kafka3ConnectionService` | Bootstrap Servers `#{kafka.bootstrap}`; Security Protocol по окружению (на стенде `PLAINTEXT`) |
| `JsonTreeReader` (`reader-infer`) | Schema Access Strategy `Infer Schema` — для `ConsumeKafka`, `ValidateRecord`, `SplitRecord`, `PutDatabaseRecord`, `PublishKafka` |
| `JsonTreeReader` (`reader-order`) | Schema Access Strategy: `Use 'Schema Name' Property` (`${schema.name}`); Schema Registry: `AvroSchemaRegistry` — для `LookupRecord` |
| `JsonRecordSetWriter` (`writer-order-ndjson`) | Schema Access: `Use 'Schema Name' Property` = `order.v1`; Schema Write Strategy: `Set 'schema.name' Attribute`; Output Grouping: `One Line Per Object`; Pretty Print `false` |
| `JsonRecordSetWriter` (`writer-ndjson`) | Schema Access `Inherit Record Schema`; `One Line Per Object` — для остальных процессоров (сохраняет поля, добавленные `LookupRecord`) |
| `AvroSchemaRegistry` | Динамическое свойство `order.v1` со схемой из Block 1.3 |
| `DBCPConnectionPool` | URL `#{db.url}`; Driver `org.postgresql.Driver`; Driver Locations `#{db.driver}`; User `#{db.user}`; Password `#{db.password}` |
| `RestLookupService` | URL `#{customers.api}/customers/${customer_id}`; Record Reader — `JsonTreeReader` (Infer Schema) |

`ConsumeKafka` читает с `Infer Schema`, а не со схемой `order.v1`: со строгой схемой запись `o-1004` (`"amount":"not-a-number"`) не разбирается вообще, а с выводом схемы она попадает в отдельный FlowFile (*Schema Conflict Resolution* `Create New FlowFile`) и честно отклоняется валидацией. В режиме `RECORD` у `ConsumeKafka` есть relationship **`parse failure`** — сообщения, которые Reader не смог разобрать; направьте его в DLQ, иначе процессор невалиден.

**Конфигурация `ValidateRecord`:** Record Reader `reader-infer`; Record Writer `writer-order-ndjson`; Record Writer for Invalid Records `writer-ndjson`; Schema Access Strategy `Use 'Schema Name' Property` = `order.v1`; *Allow Extra Fields* `false`; ***Strict Type Checking* `false`**; *Validation Details Attribute Name* `validation.details`. Relationships: `valid`, `invalid`, `failure`.

`Strict Type Checking = false` разрешает приведение совместимых типов: JSON-число → `decimal`, `long` → `timestamp-millis`. При `true` отклоняются **все** записи (JSON не несёт логических типов Avro); `o-1004` остаётся невалидным и при `false` — строку `not-a-number` к числу не привести, а обязательного `created_at` нет. Valid-записи пишутся схемой `order.v1` (с правильными `decimal` и `timestamp`) и получают атрибут `schema.name`.

**Конфигурация `LookupRecord`:** Reader `reader-order`; Writer `writer-ndjson`; Lookup Service `RestLookupService`; Routing Strategy `Route to 'matched' or 'unmatched'`; Record Result Contents `Insert Record Fields`; Record Update Strategy `Use Property`; Result RecordPath `/`; динамическое свойство `customer_id` = `/customer_id`. API отвечает полями `customer_name`, `customer_tier` — их имена совпадают с колонками `orders_stage`, поэтому `LookupRecord` кладёт их в корень записи, а `PutDatabaseRecord` пишет без маппинга. «Клиент не найден» — ответ `404` с пустым телом: `RestLookupService` возвращает «нет совпадения», запись уходит в `unmatched`.

**Конфигурация `PutDatabaseRecord`:** Database Type `PostgreSQL`; Statement Type `UPSERT`; Table Name `orders_stage`; Update Keys `order_id`; Unmatched Field Behavior `Ignore Unmatched Fields`; Unmatched Column Behavior `Ignore Unmatched Columns`; Batch Size — по замерам (стартовая точка: 1000). *Settings → Retry*: relationship `retry`, 3 попытки; исчерпавшие и `failure` — в DLQ.

**Конфигурация DLQ:** `UpdateAttribute` на каждом пути ошибки ставит `error.stage` (`validate`, `lookup`, `db`, `publish`) и `error.time`; `PublishKafka` (`orders.dlq`) с *Message Demarcator* `\n` (одна строка NDJSON — одно сообщение) и *FlowFile Attribute Header Pattern* `error\..*|validation\..*` (атрибуты уходят в заголовки Kafka).

**Подготовка тестовых данных.** На стенде: `make lab4-data`, или вручную:

```bash
docker exec -i kafka1 kafka-console-producer \
  --bootstrap-server kafka1:19092 --topic orders.raw <<'EOF'
{"order_id":"o-1001","customer_id":"c-1","amount":120.50,"currency":"EUR","created_at":1731660000000}
{"order_id":"o-1002","customer_id":"c-2","amount":-5.00,"currency":"EUR","created_at":1731660001000}
{"order_id":"o-1003","customer_id":"c-9999","amount":45.00,"currency":"USD","created_at":1731660002000}
{"order_id":"o-1004","amount":"not-a-number","currency":"EUR"}
EOF
```

**Ожидаемое поведение.**

| Запись | Ожидание |
|---|---|
| `o-1001` | Проходит всю цепочку; в БД UPSERT (с `customer_name = Alice Example`, `customer_tier = gold`); в `orders.enriched` с ключом `o-1001` |
| `o-1002` (отрицательная сумма) | Нарушает `CHECK` в БД → `failure` → DLQ с `error.stage=db`; на `o-1001` не влияет благодаря `SplitRecord` |
| `o-1003` (клиента нет) | `unmatched` в `LookupRecord` → DLQ с `error.stage=lookup` |
| `o-1004` | `invalid` в `ValidateRecord` (нет `created_at`, `amount` не число) → DLQ с `error.stage=validate` и `validation.details` |

**Проверка.**

```bash
docker exec kafka1 kafka-console-consumer --bootstrap-server kafka1:19092 \
   --topic orders.enriched --from-beginning --timeout-ms 10000 --property print.key=true
docker exec kafka1 kafka-console-consumer --bootstrap-server kafka1:19092 \
   --topic orders.dlq --from-beginning --timeout-ms 10000 --property print.headers=true
docker exec nifi-postgres psql -U nifi -d nifi \
   -c "SELECT order_id, customer_name, customer_tier, amount, currency FROM orders_stage ORDER BY order_id"
```

**Повторный прогон.** Consumer group `nifi-lab4` хранит прочитанные offsets: при повторном запуске `ConsumeKafka` ничего не прочитает. Сбросьте группу (при остановленном процессоре) и очистите таблицу:

```bash
docker exec kafka1 kafka-consumer-groups --bootstrap-server kafka1:19092 --delete --group nifi-lab4
docker exec nifi-postgres psql -U nifi -d nifi -c "TRUNCATE orders_stage"
```

**Типичные ошибки и диагностика.**

| Симптом | Причина | Действие |
|---|---|---|
| `ConsumeKafka` ничего не читает | Другой `Group ID` и `Auto Offset Reset=latest`; или группа уже прочитала топик | Поставьте `earliest` для нового Group ID; сбросьте группу (см. «Повторный прогон») |
| `ConsumeKafka` невалиден: `Relationship 'parse failure' is not connected` | В режиме RECORD у процессора есть relationship `parse failure` | Направьте его в DLQ |
| Бесконечные повторы `PutDatabaseRecord` | `retry` соединён с входом процессора | Ограничить число попыток (framework retry), отправить исчерпавшие в DLQ |
| Валидация отклоняет все записи | `Strict Type Checking = true` + логические типы схемы (`decimal`, `timestamp-millis`) против JSON-чисел | `Strict Type Checking = false` или привести схему к фактическим данным (`double`, `long`) |
| В БД нет ни одной записи, хотя часть заказов корректна | Корректные и ошибочные записи в одном FlowFile: ошибка одной откатывает транзакцию всего FlowFile | `SplitRecord` перед `PutDatabaseRecord` или отдельная обработка `failure` |
| В БД пустые `customer_name` / `customer_tier` | Имена полей ответа API не совпадают с колонками | Привести имена (или `UpdateRecord` с `Record Path Value`) |
| Дубли в БД | at-least-once + INSERT | Использовать UPSERT/ключ |
| Bulletin «Failed to connect» | Неверный bootstrap/ошибки TLS | Проверить `Kafka3ConnectionService` и доступность брокера из контейнера NiFi |

**Эталонное решение:** [lab4-db-rest-kafka.json](https://github.com/MagnetonBora/nifi-playground/blob/master/nifi/flows/lab4-db-rest-kafka.json) (REST API клиентов — `lab-services/customers_api.py`).
