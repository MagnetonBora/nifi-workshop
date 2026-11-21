---
title: "Lab 1 — Интерфейс NiFi и первый flow"
aliases:
  - exercises/lab-1
---

> Из материалов [[materials/day-1|Day 1 — Apache NiFi Fundamentals]]. Стенд и загрузка эталонов — на странице [[labs/index|Labs]].

**Цель.** Освоить UI NiFi, создать, запустить и отследить простейший flow, увидеть provenance.

**Ожидаемый результат.** Flow `GenerateFlowFile → UpdateAttribute → LogAttribute`, статистика очередей, события provenance и содержимое FlowFile в Content Viewer.

**Архитектура решения.**

```text
GenerateFlowFile ──success──► UpdateAttribute ──success──► LogAttribute (auto-terminate success)
```

**Процессоры и конфигурация.**

| Процессор | Свойство | Значение |
|---|---|---|
| `GenerateFlowFile` | Scheduling → Run Schedule | `5 sec` |
| | File Size | `0 B` (если задан Custom Text) |
| | Custom Text | `{"id": ${random():mod(1000)}, "ts": "${now():format("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")}"}` |
| | Mime Type | `application/json` |
| `UpdateAttribute` | Dynamic property `lab.stage` | `ingested` |
| | Dynamic property `event.id` | `${UUID()}` |
| `LogAttribute` | Log Level | `info`; Relationship `success` → Auto-terminate |

**Последовательность.**

1. Добавьте процессоры перетаскиванием иконки Processor на холст (поиск по имени).
2. Соедините связью `success`; в диалоге Connection отметьте только `success`.
3. Для `LogAttribute` в *Settings → Automatically Terminate Relationships* включите `success`.
4. Запустите (Start). Наблюдайте счётчики *In/Out*, *Tasks/Time*, очередь на связи.
5. ПКМ на связи → **List queue** → просмотр атрибутов и контента (при остановленном приёмнике — остановите `UpdateAttribute`, очередь заполнится).
6. ПКМ на процессоре → **View data provenance** → откройте событие и **Lineage graph**.

**Что происходит с FlowFile.** `GenerateFlowFile` создаёт FlowFile (событие `CREATE`); `UpdateAttribute` фиксирует `ATTRIBUTES_MODIFIED`; `LogAttribute` пишет в `nifi-app.log` и FlowFile завершается (`DROP`).

**Проверка.** В `logs/nifi-app.log` (на стенде: `make logs s=nifi`) появляются записи `LogAttribute` с атрибутами `lab.stage=ingested`; в provenance видны три события на один `uuid`; в Content Viewer — JSON вида `{"id": 598, "ts": "2026-11-19T16:00:05.141Z"}`.

**Типичные ошибки.** Процессор остаётся с жёлтым треугольником (невалиден): нет подключённых/auto-terminated relationships или не заданы обязательные свойства. Очередь «не двигается»: приёмник остановлен или достигнут back pressure. В контенте вместо даты сырой текст `${now():format(...)}` — Expression Language не разобрал выражение: строку формата с литералом `'T'` пишите в **двойных** кавычках (`format("yyyy-MM-dd'T'...")`); вариант `format('yyyy-MM-dd\'T\'...')` в NiFi 2.12 не вычисляется. Процессор при этом остаётся валидным — ошибка видна только по содержимому FlowFile.

**Эталонное решение:** [lab1-first-flow.json](https://github.com/MagnetonBora/nifi-playground/blob/master/nifi/flows/lab1-first-flow.json) ([[materials/Appendix-A#a8-reference-solutions|Приложение A.8]]).
