---
title: "Lab 5 — NDJSON transformation pipeline"
aliases:
  - exercises/lab-5
---

> Из материалов [[materials/day-2|Day 2 — Data Processing and Integrations]]. Стенд и загрузка эталонов — на странице [[labs/index|Labs]].

**Цель.** Построить конвейер: NDJSON-логи → валидация → удаление секрета → добавление `ingest_ts` в начало объекта → склейка в крупные NDJSON-файлы.

**Архитектура.**

```text
GenerateFlowFile(NDJSON) ► ValidateRecord ─ invalid / failure ─► UpdateAttribute(filename) ► PutFile (#{quarantine.dir}) / PublishKafka(dlq)
        │ valid
        ▼
UpdateRecord (writer schema: ingest_ts first, без user.pwd) ► MergeRecord (NDJSON) ► UpdateAttribute(filename) ► PutFile (#{out.dir})
```

Parameter Context `lab5-params`: `out.dir=/opt/nifi/data/out`, `quarantine.dir=/opt/nifi/data/quarantine` (на хосте стенда — `nifi/data/out`, `nifi/data/quarantine`), `log.schema.in` и `log.schema.out` — тексты схем (одна схема используется несколькими сервисами).

**Входные данные (`Custom Text` в `GenerateFlowFile`, Mime Type `application/x-ndjson`):**

```json
{"ts":"2026-11-15T10:00:00Z","level":"INFO","svc":"api","msg":"started","user":{"id":7,"pwd":"s3cret"}}
{"ts":"2026-11-15T10:00:01Z","level":"ERROR","svc":"api","msg":"db timeout","user":{"id":9,"pwd":"x"}}
{"ts":"bad","level":"INFO"}
```

**Конфигурация.**

| Компонент | Ключевые свойства |
|---|---|
| `JsonTreeReader` `r-log` | Schema Access: `Use 'Schema Text' Property` = `#{log.schema.in}` (входная схема с `ts`, `level`, `svc`, `msg`, `user{id,pwd}`; `pwd` допускается) |
| `JsonTreeReader` `r-log-out` | Schema Access: `Use 'Schema Text' Property` = `#{log.schema.out}` (схема `LogEvent`) — Reader для `MergeRecord` |
| `JsonRecordSetWriter` `w-log` | `Use 'Schema Text' Property` = `#{log.schema.out}` (схема `LogEvent` из раздела 2.2.4); Output Grouping `One Line Per Object`; Pretty Print `false` |
| `JsonRecordSetWriter` `w-raw` | `Inherit Record Schema`; `One Line Per Object` — для невалидных строк (сохраняет их как есть) |
| `ValidateRecord` | Reader `r-log`; Writer `w-log`; Record Writer for Invalid Records `w-raw`; Schema Access Strategy `Use Reader's Schema`; Strict Type Checking `true` |
| `UpdateRecord` | Reader `r-log`; Writer `w-log`; Strategy `Literal Value`; `/ingest_ts` = `${now():format("yyyy-MM-dd'T'HH:mm:ss.SSSXXX", "UTC")}` |
| `MergeRecord` | Reader `r-log-out`, Writer `w-log` (обе — схема `LogEvent`); Min Records `1000`; Max Records `10000`; **Max Bin Age** `30 sec` |
| `UpdateAttribute` | `filename` = `${uuid}.ndjson` (перед каждым `PutFile`) |
| `PutFile` | Directory `#{out.dir}` (для невалидных — `#{quarantine.dir}`); Conflict Resolution `replace` |

**Что происходит с данными.** После `ValidateRecord` строка 3 уходит в `invalid` (ошибка типа/отсутствующие поля). Остальные записи в `UpdateRecord` получают `ingest_ts` первым полем, теряют `user.pwd`; `MergeRecord` собирает пачки до 10 000 записей или по таймауту.

**Проверка** (с хоста стенда, из корня репозитория):

```bash
head -n 2 nifi/data/out/*.ndjson
# ожидаем: {"ingest_ts":"2026-11-15T...","ts":"2026-11-15T10:00:00Z","level":"INFO",...,"user":{"id":7}}
grep -c pwd nifi/data/out/*.ndjson   # ожидаем 0
cat nifi/data/quarantine/*.ndjson    # ожидаем {"ts":"bad","level":"INFO"}
```

**Типичные ошибки.** Поле `ingest_ts` не появляется — оно отсутствует в Writer-схеме; порядок ключей «не тот» — Writer использует `Inherit Record Schema`, а не явную схему; `MergeRecord` не выпускает данные — не задан `Max Bin Age` и не набрано *Minimum Number of Records* (генератор даёт 2 валидные записи раз в 10 секунд — без `Max Bin Age` бин не закроется никогда); секрет остался — удаляете из Reader-схемы, а не из Writer-схемы (поля, которых нет в Writer-схеме, отбрасываются именно в Writer); в `MergeRecord` нельзя указать Writer в качестве Reader — нужен отдельный `JsonTreeReader` со схемой `LogEvent`.

**Эталонное решение:** [lab5-ndjson-pipeline.json](https://github.com/MagnetonBora/nifi-playground/blob/master/nifi/flows/lab5-ndjson-pipeline.json).
