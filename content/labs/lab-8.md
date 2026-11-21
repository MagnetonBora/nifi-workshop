---
title: "Lab 8 — Тест производительности и тюнинг"
aliases:
  - exercises/lab-8
---

> Из материалов [[materials/day-3|Day 3 — Production, Security, and Performance]]. Стенд и загрузка эталонов — на странице [[labs/index|Labs]].

**Цель.** Измерить throughput конвейера из Lab 5, найти узкое место и улучшить показатели.

**Методика (по шагам).**

1. **Базовая линия.** Зафиксируйте версию, конфигурацию (heap, thread pool, concurrent tasks), железо. Подготовьте генератор нагрузки (`GenerateRecord`, Kafka producer perf-test).
2. **Метрики.** События/с на выходе; Tasks/Time по процессорам; очереди (где растут); heap/GC; CPU; `iostat`; размер/время событий provenance.
3. **Сценарии.**
   - A: базовая конфигурация.
   - B: явные схемы (убрать Infer).
   - C: увеличить `Concurrent Tasks` узкого звена.
   - D: увеличить размер пачек (`MergeRecord`).
   - E: перенести Provenance на отдельный том/уменьшить индексацию.
   - F: увеличить heap и thread pool (по росту числа ядер).
4. **Один параметр за раз;** по 3 прогона; медиана.
5. **Отчёт:** таблица «изменение → эффект → вывод», обновлённые настройки, рекомендации.

**На стенде ([[materials/Appendix-A|Приложение A]]).**

- **Генератор:** `GenerateRecord` (схема лог-события с `user.pwd`, Number of Records `10000`, Run Schedule `1 sec`) перед конвейером Lab 5 — около 10 000 событий/с; нагрузку увеличивают через *Number of Records* и *Run Schedule*. Пока узким местом остаётся генератор, это и есть базовая линия.
- **Счётчик выхода:** `UpdateCounter` после `MergeRecord` с Delta `${record.count}` — events/s = прирост счётчика (☰ → Counters) за интервал.
- **Сценарий A** — Reader с `Infer Schema`; **B** — переключите Record Reader у `ValidateRecord` на `JsonTreeReader` с явной схемой.
- **Сценарий D** — *Minimum/Maximum Number of Records* у `MergeRecord`.
- **Сценарий E** — на Docker Desktop все volumes лежат на одном виртуальном диске, поэтому эффект отдельного тома не воспроизвести; уменьшите индексацию provenance в `conf/nifi.properties` (volume `nifi_conf`): `nifi.provenance.repository.indexed.fields`, `nifi.provenance.repository.indexed.attributes`, `nifi.provenance.repository.max.storage.time`, затем перезапуск `make nifi` (**[проверить]** влияние на вашей версии).
- **Сценарий F** — heap: `NIFI_JVM_HEAP_INIT` / `NIFI_JVM_HEAP_MAX` в `nifi/single/docker-compose-nifi-single.yml` и `make nifi` (учитывайте лимит памяти Docker Desktop); thread pool: ☰ → Controller Settings → *Maximum Timer Driven Thread Count*.
- **Метрики:** Prometheus уже собирает метрики NiFi (`http://localhost:9090`, job `nifi`), графики — Grafana `http://localhost:3000`.
- **Выход** пишется в `nifi/data/lab8-out` с именами по секундам (`lab8-${now():format('ss')}.ndjson`, Conflict Resolution `replace`) — на диске не больше 60 файлов при любой нагрузке.

**Шаблон таблицы результатов.**

| Сценарий | Изменение | events/s | CPU % | GC pause (p95) | I/O await (мс) | Вывод |
|---|---|---|---|---|---|---|
| A | базовая | | | | | |
| B | явные схемы | | | | | |
| C | больше Concurrent Tasks на узком месте | | | | | |
| D | крупнее пачки `MergeRecord` | | | | | |
| E | Provenance на отдельном томе / меньше индексации | | | | | |
| F | больше heap и пул потоков | | | | | |

**Полезные команды.**

```bash
bin/nifi.sh dump /tmp/nifi-dump.txt           # thread dump
bin/nifi.sh diagnostics /tmp/diag.txt         # диагностика
iostat -x 5                                   # нагрузка на диски
jstat -gcutil <pid> 5000                      # GC
```

На стенде те же команды выполняются внутри контейнера, а нагрузка видна через `docker stats`:

```bash
docker exec nifi /opt/nifi/nifi-current/bin/nifi.sh dump /tmp/nifi-dump.txt
docker exec nifi /opt/nifi/nifi-current/bin/nifi.sh diagnostics /tmp/diag.txt
docker exec nifi sh -c 'jstat -gcutil $(pgrep -f org.apache.nifi.NiFi | head -1) 5000'   # [проверить]
docker stats nifi                                                                     # CPU / память контейнера
```

**Типичные ошибки.** Измерение без стабильного входа; одновременное изменение нескольких параметров; игнорирование прогрева JVM (первые минуты не показательны); сравнение без одинаковых данных; выход на диск без ограничения объёма (заполняет диск за минуты).

**Эталонное решение:** [lab8-performance.json](https://github.com/MagnetonBora/nifi-playground/blob/master/nifi/flows/lab8-performance.json).
