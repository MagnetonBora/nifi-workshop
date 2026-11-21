---
title: "Финальная лабораторная — сквозной конвейер"
---

> Из материалов [[materials/day-3|Day 3 — Production, Security, and Performance]]. Стенд и загрузка эталонов — на странице [[exercises/index|Exercises]].

## Цель и результат

Собрать производственно-ориентированный конвейер: **Kafka → валидация и преобразование JSON/NDJSON (Record) → доставка (Splunk или файл) → обработка ошибок → проверка производительности и тюнинг**.

## Архитектура

```text
                         ┌───────────────────────── PG: ingest ─────────────────────────┐
 Kafka topic logs.raw ──►│ ConsumeKafka (Processing=RECORD, Group=nifi-logs, Output=Value)│
                         │   └ parse failure ───────────────────────────► PG: errors     │
                         └───────────────┬───────────────────────────────────────────────┘
                                         ▼
                         ┌───────────────── PG: validate-transform ───────────────────┐
                         │ ValidateRecord (schema LogEvent.in)                         │
                         │   ├ invalid ─────────────────────────────► PG: errors        │
                         │ UpdateRecord (ingest_ts; writer-schema без секретов,         │
                         │               порядок полей: ingest_ts первым)               │
                         │ QueryRecord (WHERE level <> 'DEBUG')                         │
                         │ MergeRecord (NDJSON, до 10 000 записей, Max Bin Age 30 sec)  │
                         └───────────────┬───────────────────────────────────────────────┘
                                         ▼
                         ┌───────────────────── PG: deliver ─────────────────────────┐
                         │ PutSplunk (TCP)  или  InvokeHTTP→HEC  или PutFile           │
                         │   ├ failure/retry ► RetryFlowFile ► (исчерпано) ► PG: errors │
                         └───────────────┬───────────────────────────────────────────────┘
                                         ▼
                         ┌───────────────────── PG: errors ───────────────────────────┐
                         │ UpdateAttribute(error.*) ► PublishKafka (logs.dlq) / PutFile │
                         └───────────────────────────────────────────────────────────────┘
```

## Controller Services и параметры

| Сервис/параметр | Значение |
|---|---|
| `Kafka3ConnectionService` | `#{kafka.bootstrap}` |
| `AvroSchemaRegistry` | `LogEvent.in`, `LogEvent.out` |
| `JsonTreeReader` | Schema Name Property, registry; без Infer |
| `JsonRecordSetWriter` | `LogEvent.out`, `One Line Per Object`, без Pretty Print |
| Parameter Context `logs-prod` | `kafka.bootstrap`, `splunk.host`, `splunk.port`, `dlq.topic`, `out.dir` |

Значения для стенда ([[materials/day-3#a5-адреса-для-лабораторных|Приложение A.5]]): `kafka.bootstrap=kafka1:19092`, `splunk.host=splunk-sim`, `splunk.port=5514`, `dlq.topic=logs.dlq`, `out.dir=/opt/nifi/data/final` (резервный сток DLQ; на хосте — `nifi/data/final`).

В режиме `RECORD` у `ConsumeKafka` есть relationship **`parse failure`** (сообщения, которые Reader не разобрал) — направьте его в `errors`, иначе процессор невалиден. Для сверки удобно поставить `UpdateCounter` с Delta `${record.count}` в точках «принято» (после `ConsumeKafka`), «доставлено» (перед `PutSplunk`), «отфильтровано» (выход `debug` второго запроса `QueryRecord`: `SELECT * FROM FLOWFILE WHERE level = 'DEBUG'`) и «в DLQ» (в группе `errors`).

## Последовательность

1. Подготовьте стенд ([[materials/day-3#приложение-a-лабораторный-стенд-docker-compose|Приложение A]]); создайте топики `logs.raw` (например, 6 партиций) и `logs.dlq` — на стенде это делает `make lab-setup`.
2. Создайте Parameter Context, Controller Services (схемы!).
3. Постройте группы в порядке: ingest → validate-transform → deliver → errors; соедините порты.
4. Настройте back pressure на межгрупповых связях; на связях в `errors` — высокие пороги.
5. Включите framework retry на `PutSplunk`/`InvokeHTTP`; `RetryFlowFile` для исчерпавших.
6. Запустите генератор нагрузки: смесь валидных и невалидных событий (5–10 % невалидных), события DEBUG, события с секретом. На стенде: `make lab-final-load n=100000` (код — `lab-services/produce_logs.py`). Вручную:

```bash
python3 - <<'EOF' | docker exec -i kafka1 kafka-console-producer --bootstrap-server kafka1:19092 --topic logs.raw
import json, random, time
for i in range(100000):
    r = random.random()
    if r < 0.05:
        print('{"ts":"bad"}')                     # невалидная запись
    else:
        print(json.dumps({"ts": time.strftime("%Y-%m-%dT%H:%M:%SZ"), "level": random.choice(["INFO","ERROR","DEBUG"]),
                          "svc": "api", "msg": "event %d" % i, "user": {"id": i % 100, "pwd": "s3cret"}}))
EOF
```

## Проверка корректности

- В приёмнике: события **без** `pwd`, без `DEBUG`, с `ingest_ts` первым полем.
- В `logs.dlq` — невалидные записи (примерно 5 %) с атрибутами/заголовками ошибки.
- Сумма: `принято = доставлено + отфильтровано (DEBUG) + в DLQ` (сверка через `record.count`/счётчики `UpdateCounter`).
- Отсутствие роста очередей в `errors` свыше порогов; основная очередь не блокируется при росте DLQ.
- Provenance: для случайного события виден путь Kafka → … → SEND.

На стенде:

```bash
make lab-splunk-count                                    # строк в эмуляторе = final.delivered
docker exec splunk-sim sh -c 'grep -c pwd /data/splunk_in.log; grep -c DEBUG /data/splunk_in.log'   # ожидаем 0 и 0
docker exec kafka1 kafka-console-consumer --bootstrap-server kafka1:19092 --topic logs.dlq \
   --from-beginning --timeout-ms 10000 --property print.headers=true | head
```

Пример сверки на 2000 событий: `final.accepted` 2000 = `final.delivered` 1256 + `final.filtered.debug` 636 + `final.dlq` 108 (☰ → Counters).

**Повторный прогон.** Consumer group `nifi-logs` хранит offsets; перед новым прогоном с начала топика остановите `ConsumeKafka` и выполните `docker exec kafka1 kafka-consumer-groups --bootstrap-server kafka1:19092 --delete --group nifi-logs`; обнулите эмулятор: `docker exec splunk-sim sh -c ': > /data/splunk_in.log'`.

## Тест отказов

1. Остановите приёмник (Splunk-эмулятор; на стенде `docker stop splunk-sim`, затем `docker start splunk-sim`): очередь растёт, back pressure останавливает ingest; Kafka lag растёт (потери нет; `docker exec kafka1 kafka-consumer-groups --bootstrap-server kafka1:19092 --describe --group nifi-logs`); после возврата — рассасывание.
2. Перезапустите NiFi во время нагрузки (на стенде `docker restart nifi`): очереди восстанавливаются, consumer продолжает с зафиксированных offsets; возможны повторы (at-least-once) — проверить идемпотентность/приемлемость.
3. Подайте «пачку мусора» (100 % невалидных): основной поток не останавливается; DLQ принимает.

## Performance check и tuning

Выполните Lab 8 на этом конвейере: цель — стабильная пропускная способность без роста lag, GC-паузы в пределах SLO; зафиксируйте итоговые настройки (heap, thread pool, concurrent tasks, размер пачек, репозитории).

## Типичные ошибки и диагностика

| Симптом | Диагностика |
|---|---|
| Данные не появляются | Статус `ConsumeKafka` (Group/Offset Reset), bulletins; `kafka-consumer-groups.sh --describe` (на стенде `docker exec kafka1 kafka-consumer-groups --bootstrap-server kafka1:19092 --describe --group nifi-logs`); группа уже прочитала топик — см. «Повторный прогон» |
| `ConsumeKafka` невалиден | Не подключён relationship `parse failure` |
| Всё в DLQ | Схема `LogEvent.in` не соответствует данным (типы/вложенные структуры) — смотрите атрибуты ошибки |
| Низкий throughput | Tasks/Time, очереди; не хватает партиций; мелкие FlowFile; один поток на `PutSplunk` |
| Дубли в приёмнике | Перезапуски/ребалансы; нужна идемпотентность |

**Эталонное решение:** [lab9-final-pipeline.json](https://github.com/MagnetonBora/nifi-playground/blob/master/nifi/flows/lab9-final-pipeline.json). Тесты отказов (остановка приёмника, перезапуск NiFi под нагрузкой) на эталоне не прогонялись.
