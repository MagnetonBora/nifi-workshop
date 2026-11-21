---
title: Labs
aliases:
  - exercises/index
---

Практическая часть воркшопа: восемь лабораторных по ходу трёх дней и финальная сквозная лабораторная. Каждая
лабораторная — самостоятельное задание с архитектурой, настройками компонентов, командами проверки и разбором типичных
ошибок. Все задания проверены на Apache NiFi 2.12.0.

## Лабораторные

| Лабораторная | День | Что отрабатывается | Эталонное решение |
|---|---|---|---|
| [[labs/lab-1\|Lab 1 — Интерфейс NiFi и первый flow]] | Day 1 | UI, запуск flow, очереди, provenance, Content Viewer | [lab1-first-flow.json](https://github.com/MagnetonBora/nifi-playground/blob/master/nifi/flows/lab1-first-flow.json) |
| [[labs/lab-2\|Lab 2 — Несколько источников и маршрутизация]] | Day 1 | ListFile/FetchFile, ListenHTTP, Process Groups и порты, Parameter Context, RouteOnAttribute | [lab2-multi-source-routing.json](https://github.com/MagnetonBora/nifi-playground/blob/master/nifi/flows/lab2-multi-source-routing.json) |
| [[labs/lab-3\|Lab 3 — Split-flow против Record-flow]] | Day 1 | GenerateRecord, SplitRecord против QueryRecord/UpdateRecord, замер throughput | [lab3-split-vs-record.json](https://github.com/MagnetonBora/nifi-playground/blob/master/nifi/flows/lab3-split-vs-record.json) |
| [[labs/lab-4\|Lab 4 — БД + REST + Kafka на record-обработке]] | Day 2 | ConsumeKafka, ValidateRecord, LookupRecord + REST, PutDatabaseRecord UPSERT, DLQ | [lab4-db-rest-kafka.json](https://github.com/MagnetonBora/nifi-playground/blob/master/nifi/flows/lab4-db-rest-kafka.json) |
| [[labs/lab-5\|Lab 5 — NDJSON transformation pipeline]] | Day 2 | ValidateRecord, удаление секрета и порядок полей через Writer-схему, MergeRecord | [lab5-ndjson-pipeline.json](https://github.com/MagnetonBora/nifi-playground/blob/master/nifi/flows/lab5-ndjson-pipeline.json) |
| [[labs/lab-6\|Lab 6 — Доставка событий в Splunk]] | Day 2 | PutSplunk (TCP), InvokeHTTP → HEC, размер пачек и throughput | [lab6-splunk-delivery.json](https://github.com/MagnetonBora/nifi-playground/blob/master/nifi/flows/lab6-splunk-delivery.json) |
| [[labs/lab-7\|Lab 7 — Retry и обработка ошибок]] | Day 3 | InvokeHTTP, RetryFlowFile, DLQ, back pressure при сбое приёмника | [lab7-retry-errors.json](https://github.com/MagnetonBora/nifi-playground/blob/master/nifi/flows/lab7-retry-errors.json) |
| [[labs/lab-8\|Lab 8 — Тест производительности и тюнинг]] | Day 3 | Базовая линия, сценарии тюнинга, метрики и отчёт | [lab8-performance.json](https://github.com/MagnetonBora/nifi-playground/blob/master/nifi/flows/lab8-performance.json) |
| [[labs/final-lab\|Финальная лабораторная — сквозной конвейер]] | Day 3 | Kafka → валидация и трансформация → Splunk → DLQ, сверка и тесты отказов | [lab9-final-pipeline.json](https://github.com/MagnetonBora/nifi-playground/blob/master/nifi/flows/lab9-final-pipeline.json) |

## Стенд

Лабораторные выполняются на готовом стенде [NiFi playground](https://github.com/MagnetonBora/nifi-playground):
NiFi (одиночный узел или кластер), NiFi Registry, PostgreSQL, Kafka, Prometheus и Grafana, Splunk и заглушки внешних
систем (REST API клиентов, эмулятор Splunk, нестабильный API). Нужен Docker с памятью от 8 ГБ и `make`.

```bash
git clone https://github.com/MagnetonBora/nifi-playground.git
cd nifi-playground
make up          # NiFi и всё окружение; первый старт — несколько минут
make lab-setup   # таблица и топики для лабораторных
make urls        # адреса и логины
```

NiFi откроется на `https://localhost:8443/nifi`. Адреса, которые указываются в лабораторных (Kafka, PostgreSQL,
каталоги, заглушки), собраны в [[materials/Appendix-A|Приложении A]] материалов Day 3.

## Эталонные решения

Для каждой лабораторной есть проверенный Flow Definition — готовая Process Group с Parameter Context, где уже
указаны адреса стенда. Сначала постройте flow сами, затем сравните с эталоном.

- **В UI:** скачайте файл по ссылке из таблицы, перетащите на холст значок **Process Group** и нажмите значок
  загрузки файла.
- **Из терминала:** `make import f=nifi/flows/<файл>.json` (для кластера — `make import-cluster`).

После импорта включите Controller Services группы (*Enable all controller services*) и запустите её.

## Перед повторным прогоном

Лабораторные с Kafka (Lab 4 и финальная) запоминают прочитанные сообщения в consumer group. Чтобы прогнать их
заново с начала топика, сбросьте группу и очистите результаты — команды в
[[materials/Appendix-A#a7-test-data-and-reset|Приложении A.7]].
