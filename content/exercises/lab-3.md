---
title: "Lab 3 — Split-flow против Record-flow"
---

> Из материалов [[materials/day-1|Day 1 — Apache NiFi Fundamentals]]. Стенд и загрузка эталонов — на странице [[exercises/index|Exercises]].

**Цель.** Количественно сравнить два варианта обработки одного набора данных.

**Исходные данные.** `GenerateRecord` (процессор появился в 1.20.0) создаёт FlowFile с N случайных записей по заданной схеме. Либо `GenerateFlowFile` с NDJSON-текстом.

Нужны **две схемы**. Схема генератора (`Schema Text` у `GenerateRecord`, Writer — `JsonRecordSetWriter` с `Output Grouping = One Line Per Object`): `currency` объявлен enum, иначе генератор выдаёт случайные строки и фильтр по валюте не сработает:

```json
{"type": "record", "name": "GeneratedOrder", "fields": [
  {"name": "order_id", "type": "string"},
  {"name": "customer", "type": "string"},
  {"name": "amount",   "type": "double"},
  {"name": "currency", "type": {"type": "enum", "name": "Currency", "symbols": ["EUR", "USD", "GBP"]}}
]}
```

Схема Reader (`JsonTreeReader`, `Use 'Schema Text' Property`) — та же, но `currency` объявлен строкой: SQL-движок `QueryRecord` (Apache Calcite) не сравнивает enum со строками в `IN` / `NOT IN`:

```json
{"type": "record", "name": "Order", "fields": [
  {"name": "order_id", "type": "string"},
  {"name": "customer", "type": "string"},
  {"name": "amount",   "type": "double"},
  {"name": "currency", "type": "string"}
]}
```

**Вариант A (Split-based).**

```text
GenerateRecord ► SplitRecord(Records Per Split=1) ► EvaluateJsonPath ► RouteOnAttribute ► UpdateAttribute ► MergeContent ► LogAttribute
```

**Вариант B (Record-based).**

```text
GenerateRecord ► QueryRecord ─ valid ─► UpdateRecord ► LogAttribute
                              └ rejected ─► LogAttribute
```

QueryRecord, динамическое свойство `valid`:

```sql
SELECT order_id, customer, amount, currency
FROM FLOWFILE
WHERE amount > 0 AND currency IN ('EUR', 'USD')
```

и `rejected`:

```sql
SELECT * FROM FLOWFILE WHERE amount <= 0 OR currency NOT IN ('EUR', 'USD')
```

**Методика замера.**

1. Одинаковый вход: например, 100 FlowFile по 100 000 записей (настраивается в `GenerateRecord`: *Number of Records*). Учтите масштаб: вариант A превращает такой вход в ~10 млн FlowFile, что не помещается в учебный heap (на стенде 1–2 ГБ). На ноутбуке начинайте с 1 FlowFile по 10 000 записей (*Run Once* у генератора) и увеличивайте объём, следя за heap и back pressure.
2. Заморозьте начальные условия: чистые очереди, одинаковые `Concurrent Tasks`.
3. Запустите вариант A, дождитесь обработки всех данных; запишите: время обработки, значения *Tasks/Time*, *Bytes Read/Written*, число FlowFile в очередях, heap и GC (System Diagnostics), число provenance-событий.
4. Остановите, очистите очереди, запустите вариант B с теми же входными данными; повторите замеры.
5. Повторите 3 раза и берите медиану.

**Ожидаемое поведение (качественно).** Вариант B обрабатывает данные заметно быстрее и с меньшей нагрузкой на heap/GC; число provenance-событий и записей в FlowFile Repository на порядки меньше. Абсолютные значения зависят от железа — **цифры нужно получить в вашем окружении**, а не брать из учебника.

**Проверка корректности.** Сумма записей в `valid` и `rejected` = входному числу; атрибуты `record.count` (`QueryRecord` ставит его на выходные FlowFile) совпадают. Например, при 10 000 записей на входе: `valid` 4910 + `rejected` 5090 (событие `ROUTE` у `QueryRecord` в provenance).

**Типичные ошибки.** `QueryRecord` падает с ошибкой SQL — имя таблицы должно быть `FLOWFILE`; тип поля не совпал (decimal vs double) — задайте схему явно; ошибка `Values passed to NOT IN operator must have compatible types` — поле в схеме Reader объявлено enum, объявите его строкой; в варианте A `MergeContent` ждёт нужного числа элементов — задайте *Max Bin Age*. Одинаковые имена процессоров в вариантах A и B (например, два `LogAttribute (valid)`) путают при поиске в provenance — давайте им разные имена.

**Эталонное решение:** [lab3-split-vs-record.json](https://github.com/MagnetonBora/nifi-playground/blob/master/nifi/flows/lab3-split-vs-record.json).
