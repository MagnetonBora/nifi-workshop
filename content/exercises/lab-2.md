---
title: "Lab 2 — Несколько источников и маршрутизация"
---

> Из материалов [[materials/day-1|Day 1 — Apache NiFi Fundamentals]]. Стенд и загрузка эталонов — на странице [[exercises/index|Exercises]].

**Цель.** Принять данные из нескольких источников (файлы, HTTP, генератор) и смаршрутизировать их по атрибутам.

**Архитектура.**

```text
[PG: ingest]                                   [PG: route]
 ListFile → FetchFile ───────┐
 ListenHTTP (port 8081) ─────┼─► Output Port ─► RouteOnAttribute ─┬─ json  ─► (PG: process_json)
 GenerateFlowFile ───────────┘   "all-sources"                    ├─ csv   ─► (PG: process_csv)
                                                                  └─ unmatched ─► LogAttribute / PutFile (quarantine)
```

**Конфигурация.**

| Компонент | Ключевые свойства |
|---|---|
| `ListFile` | Input Directory `#{ingest.dir}`; File Filter `.*\.(json|csv)`; Listing Strategy по умолчанию (tracking timestamps) |
| `FetchFile` | File to Fetch `${absolute.path}/${filename}`; Completion Strategy `None` (лабораторный режим) |
| `ListenHTTP` | Listening Port `8081`; Base Path `events`; **HTTP Headers for Attributes `Content-Type`**; Для лабораторной без TLS (в production — SSL Context Service) |
| `UpdateAttribute` (на каждом входе) | `source` = `file` / `http` / `generator`; на входе HTTP дополнительно `mime.type` = `${'Content-Type':substringBefore(';'):trim()}` — `ListenHTTP` сам `mime.type` не выставляет |
| `GenerateFlowFile` | Custom Text — JSON; Mime Type `application/json` |
| `RouteOnAttribute` | Routing Strategy `Route to Property name`; `json`=`${mime.type:equals('application/json'):or(${filename:endsWith('.json')})}`, `csv`=`${filename:endsWith('.csv')}` |
| `PutFile` (quarantine) | Directory `#{quarantine.dir}` |
| Parameter Context `lab-params` | `ingest.dir=/opt/nifi/data/in`, `quarantine.dir=/opt/nifi/data/quarantine` (на хосте стенда — `nifi/data/in`, `nifi/data/quarantine`, см. [[materials/day-3#a5-адреса-для-лабораторных|Приложение A.5]]) |

**Порядок построения.**

1. Создайте Parameter Context `lab-params` и привяжите к группам (*Configure Process Group → Parameter Context*).
2. Постройте группу `ingest`, внутри — три источника и выходной порт.
3. Постройте группу `route`; соедините порт группы `ingest` с входным портом `route`.
4. Настройте приоритет `NewestFlowFileFirstPrioritizer` на связи после объединения (для демонстрации).
5. Отправьте тестовое событие и положите файлы (команды с хоста стенда, из корня репозитория):

```bash
curl -X POST -H 'Content-Type: application/json' \
     -d '{"id":1,"type":"click"}' http://localhost:8081/events
echo '{"id":10,"type":"file"}' > nifi/data/in/sample.json
printf 'id,type\n2,view\n' > nifi/data/in/sample.csv
```

**Проверка.** В очередях `json`/`csv` появляются FlowFile; атрибут `source` соответствует источнику; HTTP-событие уходит в `json` с `mime.type=application/json`; события provenance показывают событие `ROUTE`.

**Ошибки.** `ListenHTTP` не отвечает — порт занят или не проброшен в контейнер (на стенде порт 8081 проброшен только у single NiFi); `ListFile` не видит файлы — права пользователя NiFi на директорию; `unmatched` растёт — не заданы mime.type/filename (проверьте атрибуты через List queue). Частый случай: HTTP-события уходят в `unmatched`, потому что `ListenHTTP` не переносит заголовок `Content-Type` в `mime.type` — нужны `HTTP Headers for Attributes` и `UpdateAttribute` из таблицы выше.

**Эталонное решение:** [lab2-multi-source-routing.json](https://github.com/MagnetonBora/nifi-playground/blob/master/nifi/flows/lab2-multi-source-routing.json).
