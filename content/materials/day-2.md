---
title: "Day 2 — Data Processing and Integrations"
---

> Документ 2 из 3. Связанные документы: **Day 1** (`day-1.md`), **Day 3** (`day-3.md`). Обозначения ([2.x], [1.x], [dep], [rem], [проверить]) и принципы достоверности — в начале Day 1.

## Содержание

- [Block 2.1: Connecting to databases, messaging systems, REST APIs](#block-21-connecting-to-databases-messaging-systems-rest-apis)
- [Block 2.2: Transforming JSON and NDJSON](#block-22-transforming-json-and-ndjson)
- [Block 2.3: Splunk integration](#block-23-splunk-integration)
- [Block 2.4: Migration from NiFi 1.x to 2.x](#block-24-migration-from-nifi-1x-to-2x)
- [Block 2.5: Day 2 recap and Q&A](#block-25-day-2-recap-and-qa)

---

## Block 2.1: Connecting to databases, messaging systems, REST APIs

### 2.1.1 Базы данных (JDBC)

#### Connection pool как Controller Service

Подключение к БД инкапсулировано в Controller Service `DBCPConnectionPool` (или `HikariCPConnectionPool`). Пул разделяется всеми процессорами, которые на него ссылаются, поэтому размер пула ограничивает суммарный параллелизм обращений к БД.

| Свойство | Назначение | Рекомендации |
|---|---|---|
| Database Connection URL | JDBC URL, например `jdbc:postgresql://db:5432/orders` | Через параметр: `#{db.url}` |
| Database Driver Class Name | Например `org.postgresql.Driver` | |
| Database Driver Location(s) | Путь к JAR драйвера (файл/директория/URL) | Драйвер кладётся вне `lib/`, например `/opt/nifi/drivers/` |
| Database User / Password | Учётные данные | Password — sensitive parameter |
| Max Wait Time | Сколько ждать свободное соединение | Короче, чем таймаут запросов, чтобы ошибки были заметны |
| Max Total Connections | Размер пула | ≥ суммарных Concurrent Tasks процессоров, использующих пул (с запасом), но ≤ лимитов БД |
| Validation Query | Проверка живости соединения | `SELECT 1` (или оставить проверку драйвера, если он поддерживает `isValid`) |

Замечания безопасности и совместимости:

- В 1.22.0 на `DBCPConnectionPool`/`HikariCPConnectionPool` перестали поддерживаться подключения H2 по результатам CVE-валидации URL; в 1.23.0 для ряда компонентов (в т. ч. `DBCPConnectionPool`) введено ограничение *Reference Remote Resources* ([Migration Guidance](https://cwiki.apache.org/confluence/display/NIFI/Migration+Guidance)). Если вы используете fine-grained политики, пользователям, настраивающим эти компоненты, нужно выдать соответствующее разрешение. Ограничения *restricted components* как механизм авторизации удалены в 2.10.0 (см. Block 3.3).
- В 2.7.0 добавлена аутентификация AWS RDS IAM для DBCP ([Release Notes 2.7.0](https://cwiki.apache.org/confluence/display/NIFI/Release+Notes)).
- Для разных диалектов SQL с 2.2.0 есть **Database Dialect Service** ([Release Notes 2.2.0](https://cwiki.apache.org/confluence/display/NIFI/Release+Notes)).

#### JDBC-драйвер: где взять, куда положить, как подключить

NiFi подключается к БД через стандартный интерфейс JDBC, но **сам драйвер конкретной СУБД вам нужно добавить отдельно**: в стандартную поставку JDBC-драйверы внешних СУБД не входят (**[проверить]** для вашего дистрибутива/образа). Процессор не работает с драйвером напрямую: драйвер подключается к Controller Service `DBCPConnectionPool`, а процессоры ссылаются на этот сервис.

```text
JAR драйвера на диске узла ──► DBCPConnectionPool (URL, класс драйвера, путь к JAR, user/password)
                                      ▲
   ExecuteSQLRecord / PutDatabaseRecord / QueryDatabaseTableRecord ── ссылка на сервис
```

##### Шаг 1. Скачать драйвер

Скачивайте драйвер только из **официального источника**: сайт производителя СУБД или Maven Central (координаты Maven — ниже). Проверяйте контрольную сумму (SHA-256) и/или подпись, указанную на странице загрузки. Версию драйвера подбирайте так, чтобы она поддерживала **вашу версию СУБД** и **Java 21** (для NiFi 2.x): старые драйверы, собранные под Java 8, часто работают, но актуальная линейка предпочтительнее.

| СУБД | Maven-координаты (ориентир) | Класс драйвера | Формат JDBC URL |
|---|---|---|---|
| PostgreSQL | `org.postgresql:postgresql` | `org.postgresql.Driver` | `jdbc:postgresql://host:5432/dbname` |
| MySQL | `com.mysql:mysql-connector-j` | `com.mysql.cj.jdbc.Driver` | `jdbc:mysql://host:3306/dbname` |
| MariaDB | `org.mariadb.jdbc:mariadb-java-client` | `org.mariadb.jdbc.Driver` | `jdbc:mariadb://host:3306/dbname` |
| Microsoft SQL Server | `com.microsoft.sqlserver:mssql-jdbc` | `com.microsoft.sqlserver.jdbc.SQLServerDriver` | `jdbc:sqlserver://host:1433;databaseName=dbname;encrypt=true` |
| Oracle | `com.oracle.database.jdbc:ojdbc11` (линейка под современные JDK; выбирайте по матрице совместимости Oracle) | `oracle.jdbc.OracleDriver` | `jdbc:oracle:thin:@//host:1521/SERVICE_NAME` |
| ClickHouse | `com.clickhouse:clickhouse-jdbc` | `com.clickhouse.jdbc.ClickHouseDriver` | `jdbc:clickhouse://host:8123/dbname` |

Класс драйвера и формат URL **обязательно сверьте** с документацией выбранной версии драйвера (**[проверить]**): они иногда меняются между мажорными версиями. Некоторым драйверам нужны дополнительные JAR (например, Oracle — вспомогательные библиотеки для интернационализации/пулов; SQL Server с интегрированной аутентификацией Windows — нативные компоненты) — читайте инструкцию производителя.

##### Шаг 2. Положить драйвер на узлы NiFi

**Рекомендация:** хранить драйверы в **отдельном каталоге вне `NIFI_HOME/lib`**, например `/opt/nifi/drivers/` (в Docker-образе NIFI_HOME обычно `/opt/nifi/nifi-current`, каталог драйверов монтируется как volume — см. Приложение A, где уже есть `./drivers:/opt/nifi/drivers`).

Почему не в `lib/`:

- `lib/` — каталог фреймворка NiFi; добавление туда сторонних JAR может приводить к **конфликтам classpath** с библиотеками NiFi.
- Файлы в `lib/` теряются или требуют ручного переноса при обновлении версии NiFi.
- Драйвер, подключённый через сервис, загружается **отдельным class loader'ом** этого сервиса, поэтому разные пулы могут использовать **разные версии драйвера** одновременно (**[проверить]** для вашей версии).

Требования к размещению:

| Требование | Пояснение |
|---|---|
| **Одинаковый путь на каждом узле кластера** | Путь в настройках сервиса разрешается локально на каждом узле; если JAR есть только на одном узле, сервис на остальных будет невалиден |
| Права чтения для пользователя, под которым работает NiFi | Например, владелец `nifi:nifi`, права `640` на файлы и `750` на каталог |
| Каталог можно указывать целиком | Сервис загрузит все JAR из каталога (удобно, когда у драйвера несколько файлов). Не смешивайте в одном каталоге разные версии одного драйвера |
| Не хранить в каталоге лишнее | Каждый JAR попадает в class loader сервиса |

```bash
# Пример для Linux-узла
sudo mkdir -p /opt/nifi/drivers/postgresql
sudo cp postgresql-<версия>.jar /opt/nifi/drivers/postgresql/
sudo chown -R nifi:nifi /opt/nifi/drivers
sudo chmod 750 /opt/nifi/drivers /opt/nifi/drivers/postgresql
sudo chmod 640 /opt/nifi/drivers/postgresql/*.jar
# Повторить (или раскатить через Ansible/образ) на всех узлах кластера
```

**Лабораторный стенд.** Драйвер PostgreSQL уже лежит в каталоге `nifi/drivers` репозитория стенда и смонтирован во все узлы NiFi (single и cluster) как `/opt/nifi/drivers` (только чтение): в `DBCPConnectionPool` укажите `/opt/nifi/drivers/postgresql-42.7.14.jar`. Новые драйверы просто кладите в `nifi/drivers` — перезапуск не нужен, JAR подгружается при включении сервиса (Приложение A.6).

**Контейнеры и Kubernetes.** В Docker — volume с драйверами (см. Приложение A); в Kubernetes — либо собственный образ с драйвером, либо `initContainer`, скачивающий JAR в общий volume, либо PersistentVolume. Главное — чтобы путь был одинаков во всех подах.

**Загрузка через NiFi (2.x).** В 2.0.0 добавлены REST-сервисы для загрузки *assets* и пользовательских NAR ([Release Notes 2.0.0](https://cwiki.apache.org/confluence/display/NIFI/Release+Notes)); в NiFi 2.x параметры Parameter Context могут ссылаться на загруженные файлы (assets), что позволяет не раскладывать драйверы вручную по узлам. Возможность и способ указания такого параметра в *Database Driver Location(s)* проверьте в документации вашей версии (**[проверить]**).

##### Шаг 3. Создать и настроить `DBCPConnectionPool`

1. Откройте область, где будет жить сервис: правый клик на холсте нужной **Process Group → Controller Services** (или *Configure*), нажмите **«+»**, выберите `DBCPConnectionPool`. Сервис виден процессорам этой группы и вложенных групп; сервис в корневой группе виден всем.
2. Заполните свойства:

| Свойство | Пример | Комментарий |
|---|---|---|
| Database Connection URL | `#{db.url}` → `jdbc:postgresql://pg:5432/orders` | Через параметр окружения. Пароль в URL не указывайте |
| Database Driver Class Name | `org.postgresql.Driver` | Из документации драйвера |
| **Database Driver Location(s)** | `/opt/nifi/drivers/postgresql` | Путь к JAR, каталогу или URL; несколько значений — через запятую |
| Database User | `#{db.user}` | Учётная запись с минимально необходимыми правами |
| Password | `#{db.password}` | **Sensitive parameter**, а не открытый текст |
| Max Wait Time | `10 sec` (по умолчанию `500 millis`) | Сколько процессор ждёт свободного соединения; значение по умолчанию очень малое — под нагрузкой оно приводит к ошибкам «timeout waiting for idle object» |
| Max Total Connections | `16` (по умолчанию `8`) | Не превышайте лимиты БД с учётом всех узлов кластера (соединения умножаются на число узлов!). Родственные свойства: Max Idle Connections (`8`), Minimum Idle Connections (`0`), Max Connection Lifetime (`-1`, без ограничения), Minimum Evictable Idle Time (`30 mins`) |
| Validation Query | `SELECT 1` (Oracle: `SELECT 1 FROM DUAL`) | Проверка живости соединения при выдаче из пула |
| Dynamic properties | например, `sslmode = verify-full` | Динамические свойства передаются драйверу как JDBC-параметры соединения (поддерживают EL по переменным окружения); имя с префиксом `SENSITIVE.` (например, `SENSITIVE.password`) обрабатывается как sensitive-значение. Так задают TLS и специфические опции |

3. **Apply** → сервис должен стать *Disabled* без красного значка. Если сервис *Invalid*, наведите курсор на значок — причина указана (путь, класс, обязательное свойство).
4. **Enable** (значок молнии). Включение создаёт пул и (в зависимости от настроек) проверяет соединение.
5. Если в вашей версии доступна проверка свойств (**Verify** в диалоге конфигурации), запустите её: она покажет, загружается ли драйвер и устанавливается ли соединение, без запуска flow.

##### Шаг 4. Подключить процессор

В процессоре откройте *Properties* и в свойстве **Database Connection Pooling Service** выберите созданный сервис (или создайте его прямо из выпадающего списка «Create new service…»).

| Процессор | Что настроить | Заметка |
|---|---|---|
| `ExecuteSQLRecord` | Database Connection Pooling Service; SQL select query; Record Writer; Max Rows Per Flow File; Fetch Size | Чтение; результат — записи в выбранном формате |
| `QueryDatabaseTableRecord` | Pooling Service; Table Name; Maximum-value Columns; Record Writer | Инкрементальное чтение; Primary Node Only |
| `PutDatabaseRecord` | Pooling Service; Record Reader; Statement Type; Table Name; Update Keys; Batch Size | Запись; см. подробности выше |

Пример минимального теста чтения: `GenerateFlowFile` (запуск один раз) → `ExecuteSQLRecord` с запросом `SELECT 1 AS ok` → `LogAttribute`. Если в `LogAttribute` появился FlowFile с `record.count = 1`, драйвер, URL и учётные данные настроены верно.

##### Диагностика проблем с драйвером

| Симптом | Вероятная причина | Что сделать |
|---|---|---|
| Сервис *Invalid*: путь не найден / `Database Driver Location(s)` | Неверный путь; нет прав; JAR только на части узлов | Проверить путь на **каждом** узле, владельца и права |
| `Cannot load JDBC driver class '...'` | Опечатка в имени класса; JAR не содержит класс; нужные зависимости драйвера отсутствуют | Сверить класс с документацией драйвера; добавить недостающие JAR |
| `No suitable driver` | Префикс URL не соответствует драйверу | Проверить URL (`jdbc:postgresql://`, `jdbc:mysql://` и т. п.) |
| `UnsupportedClassVersionError` | Драйвер собран под более новую Java, чем запущена | Подобрать версию драйвера под Java NiFi |
| Ошибка аутентификации/SSL | Неверные учётные данные, политика TLS БД, сертификаты | Параметры TLS в URL/динамических свойствах; truststore драйвера |
| `Timeout waiting for idle object` / не хватает соединений | Малый `Max Total Connections` относительно числа `Concurrent Tasks` | Увеличить пул или уменьшить параллелизм; учесть лимиты БД |
| После замены JAR старая версия «живёт» | Class loader сервиса уже создан | Отключить и снова включить сервис; при необходимости перезапуск NiFi |
| Сервис включается, но «разрывы» соединений | Простой пула, firewall закрывает idle-соединения | Validation Query, параметры idle/lifetime пула, keepalive на сети |

Замечания безопасности и совместимости: учитывайте ограничения на компоненты (в 1.22.0 запрещены H2-подключения через `DBCPConnectionPool`/`HikariCPConnectionPool`; в 1.23.0 введено ограничение *Reference Remote Resources* — [Migration Guidance](https://cwiki.apache.org/confluence/display/NIFI/Migration+Guidance)); пользователь с правом настройки такого сервиса получает доступ к файловой системе и сети от имени NiFi, поэтому ограничивайте права на изменение Controller Services (Block 3.3).

#### DBCPConnectionPool и DBCPConnectionPoolLookup: в чём разница

Оба сервиса реализуют один и тот же интерфейс `DBCPService`, поэтому в процессорах (`ExecuteSQLRecord`, `PutDatabaseRecord` и др.) выбираются из одного и того же свойства *Database Connection Pooling Service*. Но роли у них разные.

**`DBCPConnectionPool`** — **реальный пул соединений** к **одной** базе данных: хранит URL, драйвер, логин/пароль и размеры пула; именно он открывает и держит соединения.

**`DBCPConnectionPoolLookup`** — **не пул, а «маршрутизатор»**: он ничего не подключает сам, а на каждый запрос соединения **выбирает другой `DBCPService`** (обычно `DBCPConnectionPool`) по значению **атрибута FlowFile `database.name`**. Сервис доступен с NiFi 1.7.0 ([пост сообщества](https://community.cloudera.com/t5/Support-Questions/Nifi-Multiple-databases-same-query-how-do-pass-database-name/m-p/240247)) и присутствует в документации 2.12.0 ([справочник](https://nifi.apache.org/components/org.apache.nifi.dbcp.DBCPConnectionPoolLookup)).

##### Как устроен Lookup

- Обязательных свойств у него **нет**. Настройка — через **динамические свойства**: *имя свойства* — «ключ» (значение, которое придёт в `database.name`), *значение свойства* — ссылка на включаемый Controller Service `DBCPService`. Поддержка Expression Language для этих свойств выключена; sensitive dynamic properties не поддерживаются ([справочник](https://nifi.apache.org/docs/nifi-docs/components/org.apache.nifi/nifi-dbcp-service-nar/1.28.0/org.apache.nifi.dbcp.DBCPConnectionPoolLookup)).
- Процессор, запрашивая соединение, передаёт сервису атрибуты FlowFile. Если атрибута `database.name` нет, сервис **выбрасывает исключение**; если значение не совпало ни с одним ключом — выбрать сервис невозможно (ошибка).

```text
FlowFile (database.name = "tenant_b")
        │
        ▼
 PutDatabaseRecord ── Database Connection Pooling Service: db-lookup (DBCPConnectionPoolLookup)
                                   │ динамические свойства:
                                   │   tenant_a = pg-tenant-a (DBCPConnectionPool → БД клиента A)
                                   │   tenant_b = pg-tenant-b (DBCPConnectionPool → БД клиента B)   ◄── выбран по database.name
                                   ▼
                              соединение к БД клиента B
```

##### Сравнение

| Аспект | `DBCPConnectionPool` | `DBCPConnectionPoolLookup` |
|---|---|---|
| Что это | Пул соединений | Прокси/маршрутизатор между несколькими `DBCPService` |
| К чему подключается | К одной БД (один URL, один пользователь) | Ни к чему напрямую; использует другие сервисы |
| Где хранятся URL, драйвер, пароль | В нём самом | В **целевых** `DBCPConnectionPool`; в Lookup — только таблица «ключ → сервис» |
| Выбор БД | Фиксирован настройкой | **Динамический**, на каждый FlowFile, по атрибуту `database.name` |
| Обязательные свойства | URL, класс драйвера, пользователь и др. | Нет (только динамические свойства-ключи) |
| Размер пула | Свой (`Max Total Connections`) | Нет: размеры задаются в каждом целевом пуле; суммарное число соединений = сумма по пулам |
| Требования к flow | Нет | У каждого FlowFile перед процессором должен быть атрибут `database.name` (его выставляют `UpdateAttribute`, `EvaluateJsonPath`, `PartitionRecord` и т. п.) |
| Ограничения по процессорам | Любой процессор с DBCP-свойством | Только процессоры, у которых **есть входящий FlowFile** (из его атрибутов берётся `database.name`). Источники без входа (например, `QueryDatabaseTableRecord`) и процессоры, не передающие атрибуты (например, `ListDatabaseTables`), использовать Lookup не могут (**[проверить]** по каждому процессору) |
| Отказ | Недоступна БД → ошибки подключения | Дополнительно: нет атрибута или неизвестный ключ → исключение |
| Простота | Максимальная | Сложнее: больше сервисов, зависимости, правила именования |

##### Когда использовать Lookup

- **Мультитенантность:** по отдельной БД на клиента/подразделение, одинаковая схема и одинаковый flow. Вместо копирования flow N раз — один flow + Lookup.
- **Окружения/шарды:** одни и те же запросы к нескольким шардам или региональным БД.
- **Разделение чтения и записи** (read-реплика/primary): разные ключи для разных веток flow.
- **Управляемое переключение** между экземплярами БД (например, ручной failover на другой хост) без правки процессоров — но для автоматического failover лучше решать на уровне DNS/кластера БД.

Когда **не** нужен: одна БД (используйте обычный `DBCPConnectionPool`); разные БД требуют **разных** SQL/логики (проще отдельные ветки); нужна строгая изоляция прав и сетевых политик по клиентам с ротацией секретов (допустимо и с Lookup, но ответственность за целевые сервисы остаётся).

##### Пример настройки (мультитенантный приём заказов)

1. Создать два сервиса `DBCPConnectionPool`: `pg-tenant-a` и `pg-tenant-b` (разные URL/учётные данные; одинаковый драйвер), включить.
2. Создать `DBCPConnectionPoolLookup` с именем `db-lookup`; добавить динамические свойства: `tenant_a` → `pg-tenant-a`, `tenant_b` → `pg-tenant-b`. Включить (зависимые сервисы должны быть включены).
3. Построить flow:

```text
ConsumeKafka(RECORD) ► PartitionRecord (по /tenant)  → атрибут tenant
   ► UpdateAttribute:  database.name = ${tenant}
   ► RouteOnAttribute (допустимые значения: tenant_a | tenant_b; остальное → DLQ)
   ► PutDatabaseRecord  (Database Connection Pooling Service = db-lookup)
```

Здесь `PartitionRecord` нужен, потому что **один FlowFile идёт в одну БД**: смешанные записи разных клиентов надо разделить до записи.

##### Подводные камни

- **Имя атрибута фиксировано** — `database.name`; значение должно **точно** (с учётом регистра) совпадать с именем динамического свойства.
- **Безопасность:** если значение `database.name` приходит из внешних данных (заголовок HTTP, поле сообщения), оно фактически выбирает БД, в которую пойдут данные. Валидируйте значение по **белому списку** (`RouteOnAttribute`) и не подставляйте его «как есть» из ненадёжного источника.
- Обработка ошибок: отсутствие атрибута/неизвестный ключ — ошибка на этапе получения соединения; предусмотрите ветки `failure`/DLQ и мониторинг bulletins.
- **Включение/выключение:** Lookup зависит от целевых сервисов; нельзя включить Lookup, пока не включены целевые, и нельзя выключить целевой, пока включён Lookup (UI покажет зависимые компоненты).
- **Кластер:** драйвер должен лежать на каждом узле (как для обычного пула), а общее число соединений нужно считать как «сумма пулов × число узлов».
- **Не путать** с `DatabaseRecordLookupService`/`SimpleDatabaseLookupService`: это сервисы *обогащения* (поиск значений в таблице для `LookupRecord`), а не маршрутизаторы соединений.


#### HikariCPConnectionPool (кратко)

**Что это.** Ещё один Controller Service с интерфейсом `DBCPService`, то есть реальный пул соединений к одной БД, как и `DBCPConnectionPool`. Работает на библиотеке HikariCP и выбирается в тех же процессорах из того же свойства *Database Connection Pooling Service*.

**Зачем.** Это альтернативная реализация пула: HikariCP известна небольшим размером и малыми накладными расходами на выдачу соединений. Выбирают его, когда пул под высокой конкурентной нагрузкой (много `Concurrent Tasks`) или когда команде привычны параметры HikariCP.

**Чем отличается.**

| | `DBCPConnectionPool` | `HikariCPConnectionPool` |
|---|---|---|
| Библиотека пула | Apache Commons DBCP | HikariCP |
| Назначение | Универсальный пул «по умолчанию» | Альтернативный пул с другим набором настроек |
| Набор свойств | Max Total Connections (`8`), Max Wait Time (`500 millis`), Max Idle Connections (`8`), Minimum Idle Connections (`0`), Max Connection Lifetime (`-1`), Validation Query, Kerberos User Service и др. | Max Total Connections (`10`), Max Wait Time (`500 millis`), **Minimum Idle Connections (`10`)**, Max Connection Lifetime (`-1`), Validation Query, Kerberos User Service ([справочник](https://docs.snowflake.com/en/user-guide/data-integration/openflow/controllers/hikaricpconnectionpool)). Документация HikariCP-сервиса рекомендует задавать Minimum Idle равным Max Total Connections, чтобы пул не пересоздавал соединения |
| Драйвер JDBC, URL, пользователь | Нужны | Нужны, подключаются так же (см. выше) |
| Роль в flow | Подключение к одной БД | То же |

Оба сервиса — настоящие пулы; от `DBCPConnectionPoolLookup`, который лишь выбирает между пулами, они отличаются именно этим. Если нет конкретной причины, начинайте с `DBCPConnectionPool`; переход на `HikariCPConnectionPool` оправдан по результатам замеров. Ограничение на H2-подключения (1.22.0) действует для обоих сервисов ([Migration Guidance](https://cwiki.apache.org/confluence/display/NIFI/Migration+Guidance)).

#### Чтение: ExecuteSQLRecord и QueryDatabaseTableRecord

**`ExecuteSQLRecord`** — выполнение произвольного SELECT с выводом через Record Writer (JSON/Avro/CSV).

| Свойство | Назначение |
|---|---|
| Database Connection Pooling Service | DBCP-сервис |
| SQL select query | SQL; поддерживает EL по атрибутам входящего FlowFile (`${id}`) — удобно, но опасно: см. ниже |
| Record Writer | Формат вывода (JSON NDJSON, Avro, …) |
| Max Rows Per Flow File | Размер выходного FlowFile (0 = все строки в один) |
| Output Batch Size | Сколько FlowFile отдавать в сессию до commit (снижает память при очень больших выборках) |
| Fetch Size | Размер пачки, которую драйвер забирает за раз (по умолчанию в драйвере может быть «всё в память») |
| Query timeout | Ограничение выполнения |

**Важно про память.** Некоторые JDBC-драйверы (PostgreSQL) без явного режима курсора загружают весь результат в память клиента; чтобы работал `Fetch Size`, нужны специфичные настройки соединения (для PostgreSQL — режим без автокоммита; точные условия — в разделе документации pgjdbc о получении результата курсором; **[проверить]** для вашего драйвера/версии). Всегда ограничивайте выборки по времени/ключу.

**Параметры в SQL.** Подстановка значений через EL в строку SQL уязвима к SQL-инъекциям, если значение приходит из внешних данных. Для безопасных запросов используйте параметризованный путь (атрибуты `sql.args.N.type/value` у процессоров `PutSQL`) или проверяйте значения (`RouteOnAttribute`/`ValidateRecord`) до подстановки.

**`QueryDatabaseTableRecord`** — инкрементальная выгрузка таблицы.

| Свойство | Назначение |
|---|---|
| Table Name, Columns to Return | Источник |
| **Maximum-value Columns** | Монотонно растущие столбцы (id, updated_at); процессор хранит максимум в **state** и добавляет `WHERE col > <state>` |
| Max Rows Per Flow File, Fetch Size | Контроль размера и памяти |
| Record Writer | Формат вывода |
| Scheduling | **Primary Node Only** в кластере (иначе несколько узлов начнут читать одно и то же) |

Ограничения: обновлённые (UPDATE) строки без изменения max-value столбца и удалённые строки не будут обнаружены; для CDC используйте CDC-источники (`CaptureChangeMySQL`, Debezium→Kafka). Для очень больших таблиц комбинируйте `GenerateTableFetch` (разбиение на независимые запросы) с `ExecuteSQLRecord` и load balancing.

#### Запись: PutDatabaseRecord

`PutDatabaseRecord` — основной процессор записи записей в БД (batch insert/update/upsert/delete).

| Свойство | Назначение |
|---|---|
| Record Reader | Reader входных записей (с явной схемой!) |
| Database Type / Dialect | Нужен для UPSERT и особенностей диалекта |
| **Statement Type** | `INSERT`, `UPDATE`, `DELETE`, `UPSERT` (с 1.22.0), `INSERT_IGNORE`, `SQL`, `Use statement.type Attribute`, `Use Record Path` (набор может отличаться по версии) |
| Database Connection Pooling Service | DBCP |
| Schema Name / Table Name | Целевая таблица |
| Translate Field Names | Сопоставление имён полей и столбцов без учёта регистра/подчёркиваний |
| Unmatched Field Behavior / Unmatched Column Behavior | Что делать с полями без столбцов и наоборот |
| Update Keys | Ключи для UPDATE/UPSERT |
| Batch Size | Число строк в JDBC batch (влияет на скорость и размер транзакции) |
| Rollback On Failure | Откатывать всю транзакцию (и оставлять FlowFile во входящей очереди) вместо маршрутизации в `failure` |

**Relationships:** `success`; `failure` — данные, которые БД не принимает (нарушение ограничений, неверные типы); `retry` — временные проблемы (потеря соединения, deadlock). Не соединяйте `retry` обратно с тем же процессором без ограничения (см. Block 3.1).

**Эффективность.**

1. Крупные FlowFile (тысячи записей) + **batch** — вместо одной записи на FlowFile.
2. Параллелизм: `Concurrent Tasks` ≤ размер пула; следите за блокировками на таблице.
3. Идемпотентность: для повторной доставки (at-least-once) используйте `UPSERT`/`INSERT ... ON CONFLICT DO NOTHING` (через `INSERT_IGNORE` или диалект), уникальный ключ события.
4. Для очень больших загрузок БД-специфичные пути (`COPY` в PostgreSQL, bulk-loaders) выходят за рамки NiFi — рассматривайте пакетные загрузки файлов.

Пример схемы входных записей и таблицы:

```sql
CREATE TABLE orders_stage (
    order_id   VARCHAR(64) PRIMARY KEY,
    customer   VARCHAR(128),
    amount     NUMERIC(12,2) NOT NULL,
    currency   CHAR(3) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    ingest_ts  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 2.1.2 REST API: InvokeHTTP

`InvokeHTTP` — универсальный HTTP-клиент (замена удалённых `GetHTTP`/`PostHTTP`, **[rem 2.0]**, см. [Migrating Deprecated Components](https://cwiki.apache.org/confluence/spaces/NIFI/pages/240883792/Migrating+Deprecated+Components+and+Features+for+2.0.0)).

| Свойство | Назначение |
|---|---|
| HTTP Method | `GET`, `POST`, `PUT`, `DELETE`, `PATCH` … (поддерживает EL) |
| HTTP URL | Адрес; значения в URL должны быть URL-кодированы (требование с 1.24.0, [Migration Guidance](https://cwiki.apache.org/confluence/display/NIFI/Migration+Guidance)) |
| SSL Context Service | Настройка TLS (trust store, клиентский сертификат) |
| Connection Timeout / Read Timeout | Таймауты — всегда задавайте осознанно; значения по умолчанию могут быть неподходящими для вашего API |
| Request Content-Type | Для POST/PUT (по умолчанию из атрибута `mime.type`) |
| Request OAuth2 Access Token Provider | Подключение `StandardOauth2AccessTokenProvider` (client credentials, JWT Bearer — с 2.4.0) |
| Request Username / Password | Basic-аутентификация (sensitive) |
| Dynamic properties | Добавляют HTTP-заголовки, например `Authorization`, `X-Request-Id: ${UUID()}` |
| Response Body Attribute Name | Положить тело ответа в атрибут (только для маленьких ответов) |
| Always Output Response | Выдавать ответ на `Response` даже при ошибках статуса |
| Proxy Configuration Service | Прокси |

**Relationships и маршрутизация кодов ответов.**

| Relationship | Когда |
|---|---|
| `Original` | Исходный запрос (всегда, если нужен контекст) |
| `Response` | Ответ с успешным статусом (2xx) — тело в контенте |
| `Retry` | Ответы, при которых повтор имеет смысл (в т. ч. 5xx) |
| `No Retry` | Клиентские ошибки (4xx): повтор бессмыслен |
| `Failure` | Транспортные ошибки (соединение, DNS, timeout) |

Атрибуты ответа: `invokehttp.status.code`, `invokehttp.status.message`, `invokehttp.request.url`, `invokehttp.tx.id`, `invokehttp.response.url`. Эти атрибуты используют для дальнейшей маршрутизации (`RouteOnAttribute`) и логирования.

#### Паттерн «повтор с экспоненциальной паузой»

1. На процессоре включите **framework-level Retry** (с 1.16.0): вкладка *Relationships* → отметьте галку *Retry* для `Retry`/`Failure`; задайте число попыток, **Backoff Policy** (Penalize/Yield) и **Maximum Backoff Period**. Это реализуется самим фреймворком без петли в flow.
2. Для более сложной логики — `RetryFlowFile` (Block 3.1).
3. Rate limiting для внешнего API: `ControlRate` (по числу FlowFile/объёму/атрибуту) перед `InvokeHTTP`; либо ограничение `Concurrent Tasks`.

#### Пример: обогащение событий внешним API

```text
ConsumeKafka(Record) ► PartitionRecord(by customer_id) ► InvokeHTTP(GET /customers/${customer_id})
      ├─ Response ► (JoltTransformRecord/ LookupRecord ... ) ► PutDatabaseRecord
      ├─ Retry ► (framework retry, затем) ► RetryFlowFile ► ... ► DLQ
      └─ No Retry / Failure ► DLQ (PutFile / PublishKafka dead-letter topic)
```

Если для каждой записи нужен отдельный запрос — это *per-record* вызовы; чтобы не превращать пакет в миллионы запросов, сначала сгруппируйте по ключу (`PartitionRecord`), кешируйте (`LookupRecord` с кэширующим сервисом поиска, например `RestLookupService`) или обогащайте пакетным запросом. Для lookup по HTTP в Record-потоке часто лучше `LookupRecord` + `RestLookupService`, чем `InvokeHTTP` на каждую запись (**[проверить]** конкретные свойства сервиса).

### 2.1.3 Kafka: PublishKafka и ConsumeKafka (NiFi 2.x)

> **Критически важное изменение для курса.** В программе упомянуты `PublishKafka` и `ConsumeKafkaRecord`. В NiFi **2.x** процессоры «Kafka 2.6» (`ConsumeKafka_2_6`, `ConsumeKafkaRecord_2_6`, `PublishKafka_2_6`, `PublishKafkaRecord_2_6`) **удалены** (**[rem 2.0.0]**, [Release Notes 2.0.0 — Breaking Changes](https://cwiki.apache.org/confluence/display/NIFI/Release+Notes)). Вместо них — **единые** процессоры `ConsumeKafka` и `PublishKafka` и Controller Service `Kafka3ConnectionService`. Раздельных Record-вариантов больше нет: режим Record включается свойством **Processing Strategy = RECORD** (в consume) и соответствующей стратегией публикации. Подтверждение: [описание Pull Request, внедрившего новые компоненты](https://www.mail-archive.com/issues@nifi.apache.org/msg169807.html) и [справочник ConsumeKafka](https://nifi.apache.org/components/org.apache.nifi.kafka.processors.ConsumeKafka/).

#### Архитектура

```text
Kafka3ConnectionService  (bootstrap servers, security, SSL/SASL, client timeouts)
        ▲                               ▲
        │                               │
   ConsumeKafka                    PublishKafka
 (consumer group, topics,        (topic, key, headers,
  Processing Strategy:            Publish Strategy,
  FLOW_FILE | DEMARCATOR |        delivery guarantee,
  RECORD; Output Strategy)        transactions)
```

Преимущества сервисного подхода: параметры подключения (брокеры, TLS, SASL) задаются **один раз**; процессоры не зависят от версии клиентской библиотеки; один и тот же процессор работает в режимах FlowFile/Record/Demarcator.

#### Kafka3ConnectionService: ключевые свойства

По справочнику компонента ([Kafka3ConnectionService](https://docs.snowflake.com/en/user-guide/data-integration/openflow/controllers/kafka3connectionservice)):

| Свойство | По умолчанию | Назначение |
|---|---|---|
| **Bootstrap Servers** | — | Список `host:port` через запятую (`bootstrap.servers`) |
| **Security Protocol** | `PLAINTEXT` | `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL` |
| **SASL Mechanism** | `GSSAPI` | `GSSAPI`, `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`; для PLAIN/SCRAM задаются SASL Username и Password |
| SSL Context Service | — | TLS-связь с брокерами |
| **Max Poll Records** | `10000` | Максимум записей, возвращаемых за один опрос (`max.poll.records`); определяет размер пачки у `ConsumeKafka` |
| **Client Timeout** | `60 sec` | Общий таймаут операций клиента (`default.api.timeout.ms`); `request.timeout.ms` выводится как его половина |
| **Acknowledgment Wait Time** | `5 sec` | Сколько ждать подтверждения после отправки |
| **Max Metadata Wait Time** | `5 sec` | Сколько ждать метаданные/сброс буфера при `send` (`max.block.ms`) |
| **Transaction Isolation Level** | `Read Committed` | `Read Committed` — не получать сообщения отменённых транзакций (возможна задержка); `Read Uncommitted` — получать сразу |

#### ConsumeKafka: ключевые свойства

| Свойство | Назначение |
|---|---|
| Kafka Connection Service | Ссылка на `Kafka3ConnectionService` |
| Group ID | Consumer group; **все узлы и потоки, потребляющие один набор партиций, должны иметь один Group ID** |
| Topics | Имя(на) или шаблон (pattern) топиков |
| **Auto Offset Reset** | Что делать, если нет сохранённого offset: `earliest` / `latest` / `none`; соответствует Kafka `auto.offset.reset` (по умолчанию `latest`) |
| **Processing Strategy** | `FLOW_FILE` (одно сообщение — один FlowFile), `DEMARCATOR` (несколько сообщений в один FlowFile с разделителем), `RECORD` (записи через Reader/Writer) |
| Record Reader / Record Writer | При `RECORD`: чем читать значение сообщения и чем писать |
| **Output Strategy** | При `RECORD`: `Use Content as Value`, `Use Wrapper` (запись содержит key, headers, metadata вместе со значением), `Inject Metadata` (метаданные Kafka вставляются в запись; с 2.3.0/2.5.0, [Release Notes](https://cwiki.apache.org/confluence/display/NIFI/Release+Notes)) |
| Header Name Pattern, Header Encoding | Какие заголовки Kafka превращать в атрибуты |
| Commit Offsets | Управление фиксацией offsets |
| Max Uncommitted Time | Максимальное время до фиксации offsets |

Атрибуты выходных FlowFile: `kafka.topic`, `kafka.partition`, `kafka.offset` (или минимальный в пачке), `kafka.max.offset`, `kafka.count`, `kafka.key`, `kafka.timestamp` — их используют для маршрутизации и диагностики.

#### PublishKafka: ключевые свойства

| Свойство | Назначение |
|---|---|
| Kafka Connection Service | Подключение |
| Topic Name | Целевой топик (поддерживает EL: `orders.${tenant}`) |
| Publish Strategy | Как формировать сообщение: значение = контент / wrapper-запись / разбор записей Reader'ом |
| Record Reader / Record Writer | Для публикации по записям (каждая запись — отдельное сообщение) |
| Kafka Key / Message Key Field | Ключ сообщения: либо значение свойства (если не задано — атрибут `kafka.key`), либо поле входной записи; ключ определяет партицию |
| Record Key Writer, Record Metadata Strategy | Как сериализовать ключ записи и откуда брать метаданные (топик и партиция — из метаданных записи или из свойств процессора) |
| Delivery Guarantee (`acks`) | `Best Effort`, `Guarantee Single Node Delivery`, `Guarantee Replicated Delivery`: быстрее ↔ надёжнее |
| Transactions Enabled, Transactional ID Prefix | Транзакционная публикация (`transactional.id` — сгенерированный UUID с заданным префиксом). **Включение требует `Delivery Guarantee = Guarantee Replicated Delivery`** |
| `compression.type`, `partitioner.class` и др. | Параметры клиента Kafka |

Список свойств `PublishKafka` сверен по описанию компонента ([зеркало документации](https://docs.snowflake.com/en/en/user-guide/data-integration/openflow/processors/publishkafka)); в вашей минорной версии смотрите *Help → Documentation*. Для атрибутов Kafka-заголовков и кодировок есть дополнительные свойства.

**Эволюция Kafka-компонентов 2.x (подтверждено по [Release Notes](https://cwiki.apache.org/confluence/display/NIFI/Release+Notes)):**

| Версия | Изменение |
|---|---|
| 2.0.0-M4 | Поддержка Kafka 3 для consume и publish |
| 2.4.0 | Рефакторинг Kafka-компонентов (улучшения и исправления); поддержка AWS MSK IAM |
| 2.5.0 | SASL/OAUTHBEARER; режим вывода Inject Metadata; исправления Kafka-компонентов |
| 2.7.0 | Поддержка Web Identity для `AmazonMSKConnectionService` |
| 2.8.0 / 2.9.0 | Исправлена проблема дублирования сообщений при ребалансировке consumer group в `ConsumeKafka` |

Практический вывод: **не используйте для production версии ниже 2.8.0 с интенсивными ребалансировками**, если ваш приёмник не идемпотентен — иначе возможны дубли (подтверждено строкой в Release Notes 2.8.0/2.9.0).

### 2.1.4 Эффективная интеграция с Kafka: батчинг, offsets, гарантии, параллелизм

#### Offsets и семантика доставки

Offsets хранятся **в Kafka** (топик `__consumer_offsets`), а не в NiFi; NiFi лишь фиксирует их после успешного commit сессии. Следовательно:

- Смена `Group ID` = «новый потребитель»: чтение начнётся согласно `Auto Offset Reset`.
- Сбросить offsets из NiFi нельзя; это делается средствами Kafka (`kafka-consumer-groups.sh --reset-offsets`) при остановленных потребителях.
- Для продолжения чтения после миграции со старых процессоров сохраните тот же **Group ID** — чтение продолжится с зафиксированных offsets (см. Block 2.4).

Семантика (consumer side):

| Режим | Поведение | Риск |
|---|---|---|
| Commit offsets *после* успешной обработки сессии (стандарт) | **At-least-once** | Дубли при сбое между побочным эффектом и commit; при rebalance |
| Auto-commit до обработки | At-most-once | Потери при сбое |
| Stateless-группа с Kafka consume/publish в транзакции | Ближе к **exactly-once** на цепочке Kafka→Kafka | Ограничения Stateless (память, ограниченный набор процессоров) |

Producer side: уровни подтверждения (`acks`), идемпотентный producer и транзакции Kafka определяют, возможны ли дубли и потери при публикации. Исходя из требований: *критичные* потоки — `acks=all`, идемпотентность; *метрики/логи* — более слабые гарантии ради throughput.

**Идемпотентность приёмника — главное правило.** Для at-least-once нужен либо уникальный ключ события и `UPSERT`, либо `DetectDuplicate`, либо дедупликация в приёмнике.

#### Батчинг

- **Consume.** `ConsumeKafka` за один вызов `onTrigger` читает *пачку* сообщений (опрос клиента). При Processing Strategy `RECORD` пачка превращается в один FlowFile с N записей — это ключ к пропускной способности. Размер пачки задаётся свойством сервиса **Max Poll Records** (по умолчанию `10000`) и `Run Duration` процессора (чем больше, тем крупнее пачки и выше throughput за счёт latency).
- **Publish.** В режиме `RECORD` каждая запись публикуется отдельным сообщением, но клиент Kafka сам батчит их по `linger.ms`/`batch.size`. Подбирайте эти параметры (через динамические свойства/сервис) под задержку и размер сообщений.
- **Размер сообщений.** Не публикуйте в Kafka мегабайтные FlowFile как одно сообщение; следите за `message.max.bytes` брокера и `Max Request Size` клиента.

#### Параллелизм и партиции

Параллелизм потребления ограничен числом **партиций**: на одну партицию в группе одновременно — один потребитель.

```text
Топик с 12 партициями, группа "nifi-orders"
Кластер из 3 узлов × ConsumeKafka c Concurrent Tasks = 4  →  12 потребителей → 1 партиция на поток (идеал)
Concurrent Tasks = 8 на 3 узлах → 24 потока на 12 партиций → 12 потоков простаивают
```

Правила:

1. `Concurrent Tasks × число узлов` ≤ число партиций (больше — бесполезно).
2. Если порядок внутри партиции критичен, не вставляйте после consume шаги, разрушающие порядок (параллельные `Concurrent Tasks` > 1 на следующих процессорах, round-robin балансировка).
3. Ребалансировки (изменение состава группы) приостанавливают потребление; уменьшайте их частоту: не перезапускайте `ConsumeKafka` без нужды; следите за таймаутами сессии/heartbeat.
4. Если партиций мало, а обработка тяжёлая — *после* consume распределите данные по кластеру `Round robin` на связи.

### Lab 4: БД + REST + Kafka на record-обработке

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

**Parameter Context `lab4-params`** (значения для стенда, Приложение A.5): `kafka.bootstrap=kafka1:19092`, `db.url=jdbc:postgresql://postgres:5432/nifi`, `db.user=nifi`, `db.password=nifi` (sensitive), `db.driver=/opt/nifi/drivers/postgresql-42.7.14.jar`, `customers.api=http://customers-api:8000`.

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

**Эталонное решение:** `nifi/flows/lab4-db-rest-kafka.json` (REST API клиентов — `lab-services/customers_api.py`).

---

## Block 2.2: Transforming JSON and NDJSON

### 2.2.1 Эффективная работа с NDJSON

**NDJSON** (newline-delimited JSON, он же JSON Lines) — формат, где каждая строка — самостоятельный JSON-объект:

```json
{"ts":"2026-11-15T10:00:00Z","level":"INFO","svc":"api","msg":"started","user":{"id":7,"pwd":"s3cret"}}
{"ts":"2026-11-15T10:00:01Z","level":"ERROR","svc":"api","msg":"db timeout","user":{"id":9,"pwd":"x"}}
```

Преимущества: потоковая обработка без загрузки всего массива; простое разрезание/склейка по строкам; хорошо подходит для Kafka, логов и Splunk. Недостатки: нет общего «корня»; запрещены переводы строк внутри объекта (в сжатом виде).

**Как работать в NiFi.**

1. `JsonTreeReader` читает NDJSON напрямую — отдельный парсинг не нужен.
2. На выходе `JsonRecordSetWriter` с **Output Grouping = One Line Per Object** выдаёт NDJSON; *Pretty Print JSON* должен быть выключен.
3. Не используйте `SplitText` для NDJSON ради обработки по записи — это возвращает вас к Split-антипаттерну (миллионы FlowFile). Если нужно ограничить размер пачек — `SplitRecord` с *Records Per Split* большим значением, или `MergeRecord` после обработки.
4. Для склейки мелких FlowFile перед доставкой используйте `MergeRecord` с NDJSON Writer: *Merge Strategy* `Bin-Packing Algorithm`; *Minimum/Maximum Number of Records*; *Max Bin Age* (обязательно); *Correlation Attribute* при необходимости.
5. Следите за `mime.type`: NDJSON часто имеет `application/x-ndjson` или `application/json`; процессоры доставки (HTTP) могут отправлять заголовок `Content-Type` из этого атрибута.

### 2.2.2 Ключевые процессоры

| Процессор | Назначение | Когда использовать |
|---|---|---|
| `ValidateRecord` | Проверка записей против схемы; `valid`/`invalid` | Контроль качества на входе |
| `QueryRecord` | SQL-фильтрация/проекция/агрегация в пределах FlowFile | Фильтры, маршрутизация по значениям, переименование столбцов, простые агрегаты |
| `UpdateRecord` | Изменение значений существующих полей (RecordPath + EL) | Нормализация значений, вычисляемые поля при наличии поля в схеме |
| `JoltTransformRecord` | Структурные преобразования (shift/default/remove/modify), подробно — раздел 2.2.3 | Переименование/перемещение, добавление отсутствующих полей, удаление |
| `EvaluateJsonPath` | Извлечение значений в атрибуты или контент | Только небольшие скаляры; не для обработки массивов записей |
| `MergeRecord` | Склейка FlowFile | Подготовка пачек для приёмника |
| `RemoveRecordField` | Удаление полей по RecordPath | Простое удаление |
| `PartitionRecord` | Разбиение по значению поля | Маршрутизация по ключам |

Подробные справочники по `ValidateRecord`, `QueryRecord`, `UpdateRecord` (с основами RecordPath), `EvaluateJsonPath` и `MergeRecord` — в разделе 2.2.6; по `JoltTransformRecord` — в разделе 2.2.3; по Split-процессорам — в Day 1, раздел 1.3.7.

**Совместимость 2.x.** `JoltTransformJSON` и `JoltTransformRecord` перенесены в `nifi-jolt-nar` (**2.0.0-M2**). В **2.8.0** они переведены на поддерживаемую библиотеку jolt-community; пользовательские классы трансформаций из внешних JAR нужно **перекомпилировать** ([Migration Guidance 2.8.0](https://cwiki.apache.org/confluence/display/NIFI/Migration+Guidance)). В **2.10.0** изменились типы возвращаемых значений RecordPath-функций `format()`, `toString()`, `toDate()`, `toBytes()` — теперь они возвращают корректно типизированные значения ([Migration Guidance 2.10.0](https://cwiki.apache.org/confluence/display/NIFI/Migration+Guidance)); проверьте `UpdateRecord`-выражения, использующие эти функции.

### 2.2.3 Jolt: декларативные JSON-преобразования в NiFi

#### Что такое Jolt

**Jolt** — Java-библиотека для **декларативных JSON→JSON преобразований**: вы описываете *спецификацию* (spec) — тоже JSON, в которой указано «откуда куда» переложить данные, какие значения добавить по умолчанию, что удалить и т. д. Код не пишется. Проект развивается как [jolt-community/jolt](https://github.com/jolt-community/jolt) (там же подробная документация операций и примеры).

В NiFi с Jolt работают два процессора, которые входят в бандл `nifi-jolt-nar` (туда их перенесли в версии 2.0.0-M2, см. [Release Notes](https://cwiki.apache.org/confluence/display/NIFI/Release+Notes)):

| Процессор | Как читает данные | Для чего |
|---|---|---|
| `JoltTransformJSON` | Весь FlowFile как **один JSON-документ** (объект или массив) | Разовые структурные преобразования одного документа |
| `JoltTransformRecord` | **Запись за записью** через Record Reader/Writer; спецификация применяется к каждой записи | Потоки событий, NDJSON, любые форматы, поддерживаемые Reader/Writer (JSON, Avro, CSV…) |

Для потоковых данных (NDJSON, большие пачки записей) используйте **`JoltTransformRecord`**: он обрабатывает записи потоково и согласуется с Record-подходом (Block 1.3). `JoltTransformJSON` рассчитан на один JSON-документ и разбирает его целиком в памяти.

**Версионные замечания (подтверждено по [Migration Guidance](https://cwiki.apache.org/confluence/display/NIFI/Migration+Guidance)).** В **2.8.0** оба процессора переведены на поддерживаемую библиотеку jolt-community; кастомные классы трансформаций из внешних JAR нужно **перекомпилировать** под неё. В **2.0.0-M2** процессоры перемещены из `nifi-standard-nar` в `nifi-jolt-nar` (важно, если вы собираете кастомный образ/дистрибутив).

#### Свойства `JoltTransformRecord`

Свойства по справочнику компонента ([JoltTransformRecord](https://nifi.apache.org/docs/nifi-docs/components/org.apache.nifi/nifi-jolt-record-nar/1.8.0/org.apache.nifi.processors.jolt.record.JoltTransformRecord/index.html), [2.0.0-M3](https://nifi.apache.org/documentation/nifi-2.0.0-M3/components/org.apache.nifi/nifi-jolt-nar/2.0.0-M3/org.apache.nifi.processors.jolt.JoltTransformRecord/index.html)); отображаемые имена могут немного отличаться в вашей версии (**[проверить]**):

| Свойство | Назначение |
|---|---|
| **Record Reader** | Читает входные записи и определяет их схему |
| **Record Writer** | Записывает результат |
| **Jolt Transformation DSL** (`Jolt Transform`) | Какая операция применяется: `Chain` (по умолчанию), `Shift`, `Default`, `Remove`, `Cardinality`, `Sort`, `Modify - Default`, `Modify - Define`, `Modify - Overwrite`, `Custom` |
| **Jolt Specification** | Текст спецификации **или путь к файлу** со спецификацией. Поддерживает Expression Language по атрибутам FlowFile. Игнорируется для `Sort` |
| Custom Transformation Class Name / Custom Module Directory | Только для `Custom`: класс и каталоги с JAR |
| **Transform Cache Size** (по умолчанию `1`) | Сколько скомпилированных трансформаций держать в памяти. Компиляция дорога; если в спецификации используется Expression Language, для разных FlowFile нужны разные трансформации — кэш можно увеличить |

Relationships: `success` (результат), `original` (исходный FlowFile), `failure` (ошибка разбора или трансформации). Выходные атрибуты: `record.count`, `mime.type`.

#### Как работает Jolt

Принцип — **обход дерева входных данных и дерева спецификации одновременно**. Спецификация — это «шаблон» той же вложенности, что и данные; на каждом уровне движок сопоставляет ключи данных с ключами спецификации и выполняет действие, записанное в значении. Два стиля:

- **Input-driven** (`shift`, `remove` в части обхода): движок идёт по *входным* данным и для каждого узла ищет подходящее правило в спецификации. Следствие: **в результат попадает только то, что явно сопоставлено**; всё остальное отбрасывается.
- **Spec-driven** (`default`, `modify-*`, `cardinality`): движок идёт по *спецификации* и применяет правило к соответствующим узлам данных (если узла нет — для `default` он создаётся).

Операции собираются в цепочку (`chain`): выход одной операции — вход следующей. Именно `chain` — режим по умолчанию в NiFi.

##### Операции (трансформации)

| Операция (в NiFi / в спецификации) | Что делает | Стиль | Типичное применение |
|---|---|---|---|
| **Shift** / `shift` | Копирует значения из одного места входа в другое место выхода (переименование, перекладывание, «выпрямление», вложение, разворот массивов). Несопоставленные поля **не копируются** | Input-driven | Реструктуризация, переименование, whitelist полей |
| **Default** / `default` | Добавляет значения по умолчанию, если ключ отсутствует | Spec-driven | Добавление констант/значений по умолчанию, заполнение пропусков |
| **Remove** / `remove` | Удаляет перечисленные ключи (значения в спецификации игнорируются — обычно `""`) | Spec-driven | Удаление секретов, PII, служебных полей |
| **Cardinality** / `cardinality` | Меняет «кратность»: `"ONE"` — массив → скаляр (первый элемент), `"MANY"` — скаляр → массив | Spec-driven | Нормализация поля, которое то массив, то одиночное значение |
| **Modify - Overwrite** / `modify-overwrite-beta` | Вычисляет и **всегда перезаписывает** значение с помощью функций (`=toInteger`, `=concat`, `=toUpper`, …) | Spec-driven | Приведение типов, вычисляемые поля |
| **Modify - Default** / `modify-default-beta` | То же, но пишет, только если ключа **нет или значение `null`** | Spec-driven | Значения по умолчанию, зависящие от других полей |
| **Modify - Define** / `modify-define-beta` | То же, но пишет, только если ключа **нет** | Spec-driven | Определение производных полей без перезаписи существующих |
| **Sort** / `sort` | Сортирует ключи объектов по алфавиту; спецификация игнорируется | — | Детерминированный вид для сравнения/хеширования |
| **Chain** / `chainr` | Список операций, выполняемых последовательно | — | Любое нетривиальное преобразование |
| **Custom** | Ваш Java-класс-трансформация | — | Особые случаи; требует сборки JAR и сопровождения |

(Суффикс `-beta` в названиях `modify-*` — историческое имя в библиотеке Jolt; в NiFi эти операции — обычные значения списка.)

##### Операторы (wildcards) в спецификации `shift`

| Оператор | Где | Смысл | Пример |
|---|---|---|---|
| литерал | слева | точное имя ключа | `"order_id": "id"` |
| `*` | слева | любое имя (можно как часть: `"rating-*"`) | `"rating-*": "ratings.&(0,1)"` |
| `&` | справа | подстановка ключа из пути: `&` = текущий, `&1` = родительский, `&(0,1)` = первое совпадение `*` на текущем уровне | `"name": "person.&"` |
| `@` | слева/справа | ссылка на значение; `@` — текущее значение, `@(1,key)` — значение соседнего ключа на уровень выше | `"@": "copy"` |
| `$` | слева | использовать **имя ключа** как значение | `"customer": { "$": "customer_key" }` |
| `\|` | слева | «ИЛИ» между именами | `"a\|b": "x"` |
| `[]` | справа | индекс массива/принудительный массив | `"tag": "tags[]"`, `"lines[&1].sku"` |
| `#` | слева | записать литерал (константу) в указанное место | `"#EUR": "currency"` |

Правило выбора: **более конкретный ключ побеждает более общий** (литерал важнее `*`).

#### Примеры

Входная запись (одна запись в NDJSON-потоке):

```json
{"order_id":"o-1","customer":{"id":7,"name":"Ann","email":"a@x.com"},"items":[{"sku":"A","qty":"2","price":10.5},{"sku":"B","qty":"1","price":3}],"pwd":"s3cret","coupon":null}
```

**1) `Shift` — переименование, выпрямление, разворот массива.**

```json
{
  "order_id": "id",
  "customer": {
    "id":   "customer_id",
    "name": "customer_name"
  },
  "items": {
    "*": {
      "sku":   "lines[&1].sku",
      "qty":   "lines[&1].qty",
      "price": "lines[&1].price"
    }
  }
}
```

Результат:

```json
{"id":"o-1","customer_id":7,"customer_name":"Ann","lines":[{"sku":"A","qty":"2","price":10.5},{"sku":"B","qty":"1","price":3}]}
```

Здесь `&1` — ключ родительского уровня (индекс элемента `items`). Обратите внимание: `pwd`, `customer.email` и `coupon` **исчезли** — `shift` переносит только описанное. Это удобно как «белый список» полей, но опасно, если вы забыли поле. Чтобы **сохранить всё остальное**, добавьте в конец спецификации правило `"*": "&"` (копирует несопоставленные поля верхнего уровня с их именами).

**2) `Remove` — удаление полей.**

```json
{ "pwd": "", "customer": { "email": "" } }
```

Удаляет `pwd` и `customer.email`, остальное сохраняется.

**3) `Default` — добавление отсутствующих значений.**

```json
{ "currency": "EUR", "source": "nifi", "items": { "*": { "discount": 0 } } }
```

Добавит `currency` и `source` на верхнем уровне и `discount: 0` в каждый элемент `items`. Значение, которое уже есть, не изменится. Для значений `null`: по описаниям реализаций библиотеки (включая её порты) `default` заполняет и отсутствующие, и `null`-значения, но на своих данных это стоит проверить. Однозначно подтверждено для модификаторов ([release notes Jolt 0.0.22](https://github.com/bazaarvoice/jolt/releases/tag/jolt-0.0.22)): `modify-overwrite-beta` пишет всегда, `modify-default-beta` — если ключа нет **или значение `null`**, `modify-define-beta` — только если ключа нет.

**4) `Modify - Overwrite` — приведение типов и вычисления.**

```json
{
  "lines": { "*": { "qty": "=toInteger", "total": "=toDouble(@(1,price))" } },
  "customer_name": "=toUpper(@(1,customer_name))"
}
```

Функции (набор в вашей версии сверяйте по документации Jolt): `=toInteger`, `=toLong`, `=toDouble`, `=toString`, `=toBoolean`, `=toUpper`, `=toLower`, `=trim`, `=concat(...)`, `=substring(...)`, `=join(...)`, `=split(...)`, `=size`, `=sum`/`=intSum`/`=doubleSum`, `=min`/`=max`/`=avg`, `=abs`, `=firstElement`/`=lastElement`/`=elementAt`, `=toList`, `=sort`, `=squashNulls`. Аргумент `@(1,price)` означает «значение `price` на уровень выше относительно текущего узла».

**5) `Cardinality`.**

```json
{ "tags": "MANY", "primary_tag": "ONE" }
```

`tags` всегда станет массивом (скаляр оборачивается), `primary_tag` — скаляром (берётся первый элемент). Полезно, когда источник присылает поле то как строку, то как массив из одной строки.

**6) `Chain` — всё вместе (самый частый случай).**

```json
[
  {
    "operation": "shift",
    "spec": {
      "order_id": "id",
      "customer": { "id": "customer_id", "name": "customer_name" },
      "items": { "*": { "sku": "lines[&1].sku", "qty": "lines[&1].qty", "price": "lines[&1].price" } }
    }
  },
  {
    "operation": "modify-overwrite-beta",
    "spec": { "lines": { "*": { "qty": "=toInteger" } } }
  },
  {
    "operation": "default",
    "spec": { "currency": "EUR", "source": "nifi" }
  }
]
```

Результат для входной записи выше:

```json
{"id":"o-1","customer_id":7,"customer_name":"Ann","lines":[{"sku":"A","qty":2,"price":10.5},{"sku":"B","qty":1,"price":3}],"currency":"EUR","source":"nifi"}
```

**7) Expression Language в спецификации.** Свойство *Jolt Specification* поддерживает EL по атрибутам FlowFile, поэтому можно подставлять значения, известные на уровне FlowFile (константа окружения, время обработки пачки, значение атрибута):

```json
[
  {
    "operation": "default",
    "spec": { "tenant": "${tenant}", "ingest_epoch_ms": "${now():toNumber()}" }
  }
]
```

Результат EL подставляется в спецификацию **как строка до компиляции**; поэтому `ingest_epoch_ms` получится строкой — при необходимости добавьте следующим шагом `=toLong`. Значение вычисляется **один раз на FlowFile**, а не на запись. Если спецификация зависит от EL, для разных FlowFile создаются разные скомпилированные трансформации — увеличьте *Transform Cache Size* (иначе компиляция будет повторяться для каждого FlowFile).

#### Jolt в Record-потоке: что важно знать

1. **Схема входа.** Record Reader создаёт записи по своей схеме; то, чего нет в схеме Reader (при явной схеме), Jolt «не увидит»: справочник `JsonTreeReader` подтверждает, что поля JSON, отсутствующие в схеме, пропускаются. Для явных схем включайте в схему все поля, которые нужны трансформации.
2. **Схема выхода.** После Jolt структура и типы меняются (например, `=toInteger`, новые поля). Writer должен уметь записать результат: либо схема выводится по результату (Writer с `Inherit Record Schema`), либо вы задаёте явную Writer-схему. Для детерминированного результата, порядка полей и типов задавайте **явную схему Writer** (см. Block 1.3).
3. **Порядок ключей.** Порядок полей в выходном JSON определяется Writer'ом/схемой; на порядок ключей, получающийся «сам собой» в результате Jolt, полагаться нельзя. Если порядок важен (Block 2.2.4, пример `ingest_ts` первым) — управляйте им Writer-схемой.
4. **Типы.** `shift` сохраняет типы значений; изменение типа выполняйте `modify-*` и проверяйте, что Writer-схема согласована.
5. **Ошибки.** Неверная спецификация или несовместимые данные → FlowFile в `failure`. Исходный FlowFile отправляется в `original` только при успехе.

#### Когда Jolt, а когда другие инструменты

| Задача | Лучший инструмент |
|---|---|
| Переименовать/удалить пару полей | `RemoveRecordField`, `UpdateRecord`, явная Writer-схема — проще и быстрее |
| Отфильтровать записи, проекция столбцов, простые агрегаты | `QueryRecord` (SQL) |
| Сложная реструктуризация вложенных структур, разворот/сворачивание массивов, работа с неизвестной глубиной | **Jolt** (`shift`, `chain`) |
| Приведение типов и простые вычисления | `modify-*` в Jolt или `UpdateRecord` (RecordPath) |
| Нужны ветвления по условиям, внешние вызовы, сложная логика | Скриптовые/Python-процессоры или внешний код (Block 2.2.5) |

Производительность: спецификация компилируется один раз и кэшируется (*Transform Cache Size*); основная стоимость — преобразование записи в структуру для Jolt и обратно, поэтому «тривиальные» операции дешевле выполнять Record-процессорами. Слишком «умные» спецификации с множеством `*` трудно читать и отлаживать — разбивайте на `chain` из небольших шагов.

#### Отладка спецификаций

1. Берите **минимальный пример** входа (одна запись) и добавляйте правила по одному.
2. Тестируйте в NiFi на тестовом FlowFile: `GenerateFlowFile` (Custom Text с JSON) → `JoltTransformRecord` → просмотр входа и выхода в *Content Viewer* (через очередь или provenance).
3. Для изолированных проверок используйте консоль/тесты проекта jolt-community (см. репозиторий) либо онлайн-песочницы Jolt (доступность сторонних сервисов не гарантируется).
4. Фиксируйте спецификации в Git (как часть flow или в отдельном файле; свойство *Jolt Specification* допускает **путь к файлу** — удобно для версионирования, но файл должен быть на всех узлах кластера).

**Типичные ошибки.**

| Симптом | Причина |
|---|---|
| В результате пропали поля | `shift` — whitelist; добавьте правило или `"*": "&"` |
| `failure` без внятной причины | Опечатка/неверный JSON в спецификации; смотрите bulletin и `nifi-app.log` |
| Поле не добавлено `default`'ом | Поле уже есть (даже пустая строка); или значение `null` (см. выше) |
| Тип поля «не тот» | Нет явного `modify-*`; Writer-схема не соответствует |
| Спецификация «работает в песочнице, но не в NiFi» | Данные приходят как Record по схеме Reader, а не как «сырой JSON»; сравнивайте реальные записи |
| Кастомный класс не работает после апгрейда до 2.8+ | Нужна перекомпиляция под jolt-community |
| Падение производительности с EL в spec | Каждый FlowFile — новая компиляция; увеличьте *Transform Cache Size* или уберите EL |


### 2.2.4 Типичные преобразования

Исходная запись:

```json
{"ts":"2026-11-15T10:00:00Z","level":"INFO","svc":"api","msg":"started","user":{"id":7,"pwd":"s3cret"}}
```

#### 1) Удаление пары ключ/значение (`user.pwd`)

Вариант A — `JoltTransformRecord`:

```json
[
  {
    "operation": "remove",
    "spec": { "user": { "pwd": "" } }
  }
]
```

Вариант B — `RemoveRecordField` (RecordPath `/user/pwd`). Вариант C — явная схема Writer без поля `pwd` (в 1.28.1 исправлено поведение, когда Writer не отбрасывал неизвестные схеме поля; убедитесь, что используете исправленную версию, [Release Notes 1.28.1](https://cwiki.apache.org/confluence/display/NIFI/Release+Notes)).

Для данных, содержащих секреты, удаление должно происходить **до** любого логирования, provenance-просмотра контента и доставки.

#### 2) Вставка новой пары ключ/значение (например, `ingest_ts`)

Если поля нет во входных данных, `UpdateRecord` его **не создаст**, пока оно не присутствует в схеме Writer. Поэтому есть два способа:

*Способ 1 — Jolt `default`* (добавляет поле с **константным** значением, если оно отсутствует):

```json
[
  {
    "operation": "default",
    "spec": { "source_system": "nifi" }
  }
]
```

Jolt подходит для структурных добавлений (константы, значения по умолчанию, вложенные объекты). Сам Jolt время не вычисляет, но свойство *Jolt Specification* поддерживает Expression Language по атрибутам FlowFile: `"ingest_epoch_ms": "${now():toNumber()}"` (см. пример 7 в разделе 2.2.3; значение будет строкой, одно на FlowFile). Для форматированной даты и значения на уровне записи удобнее способы 2 и 3.

*Способ 2 — Writer-схема с полем + `UpdateRecord`:*

Writer (`JsonRecordSetWriter`) использует **Schema Text** с добавленным полем `ingest_ts` (`["null","string"]`, default `null`). `UpdateRecord`:

| Свойство | Значение |
|---|---|
| Replacement Value Strategy | `Literal Value` |
| Dynamic property `/ingest_ts` | `${now():format("yyyy-MM-dd'T'HH:mm:ss.SSSXXX", "UTC")}` |

Выражение выполняется один раз на FlowFile (одно значение на пачку) — для разных значений на запись используйте RecordPath-функции или `LookupRecord`. Если нужно именно «время обработки каждой записи», задавайте его в приёмнике или используйте SQL.

*Способ 3 — `QueryRecord` (чистый SQL):*

```sql
SELECT CAST(CURRENT_TIMESTAMP AS VARCHAR) AS ingest_ts, t.*
FROM FLOWFILE t
```

SQL-диалект — Apache Calcite; приведение типов проверьте на своей версии.

#### 3) Перемещение ключа в начало объекта и вставка нового ключа в начало

**Теория.** По спецификации JSON порядок ключей объекта **не имеет смысла**; но некоторые приёмники (Splunk-парсеры по регулярным выражениям, человек при чтении логов, некоторые подписанные payload'ы) зависят от порядка. NiFi `JsonRecordSetWriter` выводит поля **в порядке схемы записи (Writer schema)**. Следовательно, порядок управляется схемой:

1. Определите явную Avro-схему Writer с нужным порядком полей, например `ingest_ts` первым, `ts` вторым:

```json
{
  "type": "record", "name": "LogEvent",
  "fields": [
    {"name": "ingest_ts", "type": ["null","string"], "default": null},
    {"name": "ts",        "type": "string"},
    {"name": "level",     "type": "string"},
    {"name": "svc",       "type": "string"},
    {"name": "msg",       "type": "string"},
    {"name": "user", "type": ["null", {"type":"record","name":"User","fields":[
        {"name":"id","type":"long"}
    ]}], "default": null}
  ]
}
```

2. Reader читает данные со своей схемой (или явной, или inferred), Writer — с описанной выше (`Use 'Schema Text' Property`). Поля, которых нет в Writer-схеме, **отбрасываются** (так же удаляется `user.pwd`); недостающие — заполняются `null`/default.
3. `UpdateRecord` заполняет `/ingest_ts`.

Так в одном `UpdateRecord` + Writer решаются все три задачи: удалить поле, добавить новое, поставить в начало. Обратите внимание на цену: **схема Writer — единственный источник порядка**; при эволюции данных её надо поддерживать.

*Альтернатива* — `ReplaceText` по NDJSON (регулярка вида `^\{` → `{"ingest_ts":"${now():format(...)}",`) — работает построчно и очень быстро, но *не знает структуры JSON* и ломается на пустых объектах/пробелах; подходит только для строго контролируемого формата и должна сопровождаться валидацией.

### 2.2.5 Где преобразовывать: в NiFi или вне него

| Критерий | В NiFi | Вне NiFi (приёмник, Kafka Streams/Flink/Spark, ETL) |
|---|---|---|
| Простые структурные изменения, фильтрация, маршрутизация | **Да**: дёшево, потоково, с provenance | Избыточно |
| Нормализация формата на границе системы | **Да** | |
| Тяжёлые join/агрегации по окнам, состояние по ключам | Нет (state и память ограничены) | **Да** |
| ML-инференс, сложная бизнес-логика | Ограниченно (Python processors, scripted) | **Да** |
| Единый контракт для многих потребителей | Через Schema Registry + публикация «канонического» формата | Предпочтительно в стриминговом слое |
| Масштаб: десятки/сотни тысяч событий/с на узел | Возможно при Record-подходе | Для миллионов/с — специализированные движки |
| Надёжность, повтор, DLQ | Встроено (relationships, retry, provenance) | Нужно строить |

Практическое правило: **NiFi — для перемещения и лёгкой подготовки данных, внешние движки — для вычислений над данными**. Если трансформация меняет смысл данных (бизнес-правила) и должна версионироваться и тестироваться как код, её место — в коде приложений/стриминга, а NiFi просто доставляет.

### 2.2.6 Справочник: ValidateRecord, QueryRecord, UpdateRecord (RecordPath), EvaluateJsonPath, MergeRecord

Раздел собирает подробные описания процессоров, которые чаще всего используются в NDJSON-конвейерах (Lab 5, Lab 4, финальная лаба). Свойства сверены со справочниками компонентов NiFi ([ValidateRecord](https://nifi.apache.org/documentation/nifi-2.0.0-M2/components/org.apache.nifi/nifi-standard-nar/2.0.0-M2/org.apache.nifi.processors.standard.ValidateRecord/index.html), [QueryRecord](https://nifi.apache.org/docs/nifi-docs/components/org.apache.nifi/nifi-standard-nar/latest/org.apache.nifi.processors.standard.QueryRecord/index.html), [UpdateRecord](https://nifi.apache.org/docs/nifi-docs/components/org.apache.nifi/nifi-standard-nar/latest/org.apache.nifi.processors.standard.UpdateRecord/index.html), [EvaluateJsonPath](https://nifi.apache.org/docs/nifi-docs/components/org.apache.nifi/nifi-standard-nar/1.2.0/org.apache.nifi.processors.standard.EvaluateJsonPath/index.html), [MergeRecord](https://nifi.apache.org/docs/nifi-docs/components/org.apache.nifi/nifi-standard-nar/1.3.0/org.apache.nifi.processors.standard.MergeRecord/additionalDetails.html)); версии справочников указаны по ссылкам, поэтому имена свойств в вашей версии проверяйте во встроенной документации.

#### ValidateRecord

**Назначение.** Проверяет **каждую запись** FlowFile на соответствие схеме и **разделяет** записи на валидные и невалидные (в отличие от «всё или ничего»). Это «входные ворота» конвейера и основа dead-letter-паттерна (Block 3.1.3).

**Как работает.** Reader читает записи; процессор проверяет их по схеме; записи, прошедшие проверку, пишутся в `valid`, остальные — в `invalid`. Схема, по которой выполнялась проверка, используется и для записи валидных данных, независимо от схемы Writer.

| Свойство | По умолчанию | Назначение |
|---|---|---|
| **Record Reader** | — | Читает входные записи |
| **Record Writer** | — | Пишет `valid` (и `invalid`, если не задан отдельный Writer) |
| Invalid Record Writer | не задан | Отдельный Writer для невалидных записей (если нужен другой формат) |
| **Schema Access Strategy** | `Use Reader's Schema` | Откуда брать схему проверки: схема Reader'а, `Use 'Schema Name' Property` (из Schema Registry) или `Use 'Schema Text' Property` |
| Schema Text | `${avro.schema}` | Текст Avro-схемы (при стратегии `Schema Text`) |
| **Allow Extra Fields** | `true` | Если `false`, запись с полями, которых нет в схеме, **невалидна** |
| **Strict Type Checking** | `true` | Если `true`, поле неверного типа делает запись невалидной; если `false` — запись валидна, а поле приводится к нужному типу, если возможно |
| Force Types From Reader's Schema | `false` | Приводить поля к типам из схемы Reader'а |
| Validation Details Attribute Name | не задан | Если указан, при ошибке в этот атрибут записываются **подробности** (чем запись невалидна) |
| Maximum Validation Details Length | `1024` | Лимит длины подробностей; лишнее обрезается |

**Relationships.**

| Relationship | Что попадает |
|---|---|
| `valid` | Записи, соответствующие схеме |
| `invalid` | Записи, не соответствующие схеме |
| `failure` | **Исходный FlowFile**, если записи не удалось прочитать, проверить или записать (например, это не JSON вообще) |

Атрибуты: `mime.type`, `record.count` (число записей в FlowFile, ушедшем в relationship).

**Практика.**

1. **Разделяйте валидные и невалидные записи на входе**, а не в конце: плохие данные уходят в DLQ, основной поток не блокируется (Block 3.1.3).
2. Для production задавайте схему явно (`Schema Name`/`Schema Text`), а не `Use Reader's Schema` вместе с Infer Schema: иначе схема «подстраивается» под данные и проверять нечего.
3. Включайте **Validation Details Attribute Name** (например, `validation.details`) и сохраняйте его вместе с невалидным FlowFile — это причина ошибки для разбора DLQ без повторного прогона. Для расследования можно временно включить DEBUG-логирование логгера `org.apache.nifi.processors.standard.ValidateRecord`.
4. Выбор строгости: `Strict Type Checking=true`, `Allow Extra Fields=false` — максимальная строгость (контрактные потоки); `false/true` — мягкий режим (приведение типов, лишние поля допустимы).
5. Помните о двух уровнях ошибок: **запись** невалидна (`invalid`) или **весь FlowFile** нечитаем (`failure`).

**Пример (NDJSON-события, схема `LogEvent.in`):**

```text
ValidateRecord
  Record Reader                     = JsonTreeReader (Schema Name Property)
  Record Writer                     = JsonRecordSetWriter (One Line Per Object)
  Allow Extra Fields                = false
  Strict Type Checking              = true
  Validation Details Attribute Name = validation.details
valid   ──► дальше по конвейеру
invalid ──► UpdateAttribute (error.stage=validate) ──► PublishKafka (logs.dlq)
failure ──► то же + алерт (FlowFile нечитаем целиком)
```

#### QueryRecord (полный справочник)

**Назначение.** Выполняет SQL над записями FlowFile; результат запроса становится содержимым выходного FlowFile. Один процессор может одновременно фильтровать, проектировать (выбирать/переименовывать поля), агрегировать и маршрутизировать.

**Как работает.**

- SQL — **ANSI SQL на Apache Calcite**; таблица, к которой обращаетесь, называется `FLOWFILE`.
- Каждое **динамическое свойство** = relationship: *имя свойства* — имя relationship, *значение* — `SELECT`. Должно быть хотя бы одно свойство (значения поддерживают Expression Language).
- Если Writer наследует схему из записи (`Inherit Record Schema`), наследуется схема **результата запроса**, а не входа. У такой схемы **нет имени**, поэтому Writer не должен пытаться записать schema name в атрибут (иначе ошибка/пустое значение).

| Свойство | По умолчанию | Назначение |
|---|---|---|
| **Record Reader** | — | Читает входные записи |
| **Record Writer** | — | Пишет результаты запросов |
| **Include Zero Record FlowFiles** | `true` | Если запрос вернул 0 записей, отправлять ли пустой FlowFile в relationship. **Для производительности ставьте `false`**, иначе по каждому запросу на каждый входной FlowFile появляются пустые FlowFile |
| Default Decimal Precision / Default Decimal Scale | — | Точность и масштаб для DECIMAL при записи как Avro logical type, когда СУБД/запрос не определили их сами |
| Динамические свойства | — | Запросы = relationships |

**Relationships:** `original` (исходный FlowFile), `failure` (запрос не выполнился, например в SQL есть столбцы, которых нет во входных данных) и по одной на каждое динамическое свойство.

**Атрибуты результата:** `record.count`, `mime.type`, `QueryRecord.Route` (имя relationship, в которую ушёл FlowFile).

**Примеры для NDJSON-логов.**

```sql
-- Фильтр и проекция (одновременно «удаляет» лишние поля, например pwd):
SELECT ts, level, svc, msg FROM FLOWFILE WHERE level <> 'DEBUG'

-- Вставка нового поля первым (порядок столбцов = порядок в SELECT):
SELECT CAST(CURRENT_TIMESTAMP AS VARCHAR) AS ingest_ts, t.* FROM FLOWFILE t

-- Агрегат по пачке:
SELECT svc, level, COUNT(*) AS cnt FROM FLOWFILE GROUP BY svc, level

-- Маршрутизация по значению: два свойства = две relationship
--   errors: SELECT * FROM FLOWFILE WHERE level = 'ERROR'
--   others: SELECT * FROM FLOWFILE WHERE level <> 'ERROR'
```

**Нюансы SQL.**

- Если имя поля совпадает с зарезервированным словом SQL (например, `timestamp`, `user`, `value`) или содержит спецсимволы, берите его в **двойные кавычки**.
- Ссылаться можно только на поля, которые есть в схеме Reader'а: при явной схеме поле вне схемы недоступно (запрос уйдёт в `failure`).
- Вложенные записи и массивы доступны, но синтаксис усложняется; для глубокой реструктуризации лучше Jolt (раздел 2.2.3).
- Типы: сравнение числа со строкой (`amount > '10'`) даст ошибку или неожиданный результат — сверяйте типы схемы.

**Стоимость и производительность.**

- Каждый запрос — это **отдельный проход** по записям FlowFile и отдельная запись результата. Четыре запроса в одном процессоре — четыре прохода (**[проверить]** на вашей версии); если запросы тяжёлые, разнесите их по разным процессорам или цепочке.
- Простые `SELECT … WHERE` работают потоково (запись за записью). **Сортировка (`ORDER BY`), группировка (`GROUP BY`), `JOIN` и оконные функции** накапливают данные в памяти — на очень больших FlowFile это риск OutOfMemory; ограничивайте размер FlowFile (`SplitRecord`, `MergeRecord`) до QueryRecord.
- Схема результата кэшируется (процессор хранит ограниченное число схем и вытесняет старые), поэтому единообразные запросы быстрее разнородных.
- `Include Zero Record FlowFiles=false` и явные списки столбцов вместо `SELECT *` уменьшают объём работы.

**Когда QueryRecord, а когда другие инструменты.**

| Задача | Выбор |
|---|---|
| Фильтрация и маршрутизация по значениям полей | `QueryRecord` |
| Удалить поле, добавить поле, переставить порядок | `QueryRecord` (SELECT) или Writer-схема; для сложной структуры — Jolt |
| Изменить значение поля | `UpdateRecord` |
| Только разделить по значению ключа без SQL | `PartitionRecord` |
| Обогащение из внешнего источника | `LookupRecord` |

#### UpdateRecord и RecordPath

**Назначение.** Изменяет **значения полей** в записях. Какие поля менять — задаётся **RecordPath** (язык путей по полям записи, [RecordPath Guide](https://nifi.apache.org/docs/nifi-docs/html/record-path-guide.html)); на что менять — значением свойства.

**Как работает.** Вы добавляете одно или несколько **динамических свойств**: *имя* — RecordPath поля, которое обновляется; *значение* — либо литерал (с возможным Expression Language), либо другой RecordPath, в зависимости от свойства **Replacement Value Strategy**.

| Свойство / стратегия | Смысл |
|---|---|
| **Record Reader / Record Writer** | Как читать и писать записи |
| **Replacement Value Strategy = Literal Value** (по умолчанию) | Значение свойства (после вычисления Expression Language) — это новое значение поля. В выражении доступны переменные `field.name`, `field.type`, `field.value` (имя, тип, текущее значение обновляемого поля) и `record.index` (индекс текущей записи) |
| **Replacement Value Strategy = Record Path Value** | Значение свойства — **RecordPath**, вычисляемый по записи; результат подставляется в поле. Если путь не находит значения, полю присваивается `null`; если находит **несколько** значений, FlowFile уходит в `failure` |

Relationships: `success`, `failure` (FlowFile не удалось преобразовать; атрибут `record.error.message` содержит ошибку Reader'а/Writer'а).

**Основы RecordPath.**

| Конструкция | Смысл | Пример |
|---|---|---|
| `/a` | Поле верхнего уровня | `/level` |
| `/a/b` | Вложенное поле | `/user/id` |
| `//id` | Поле на любом уровне вложенности (descendant) | `//id` |
| `/arr[0]`, `/arr[*]` | Элемент массива по индексу / все элементы | `/items[*]/sku` |
| `..` | Родитель | `../id` |
| `[./field > 100]` | Фильтр-предикат внутри пути | `/accounts[./balance > 100]` (синтаксис см. в Guide) |

**Функции RecordPath** (полный список — в Guide; названия регистрозависимы): `concat`, `substring`, `substringBefore`/`substringAfter` (и `…Last`), `replace`, `replaceRegex`, `trim`, `toUpperCase`/`toLowerCase`, `padLeft`/`padRight`, `coalesce`, `fieldName`, `format`, `toDate`, `toString`, `toBytes`, `hash`, `uuid5`, `base64Encode`/`base64Decode`, `escapeJson`/`unescapeJson`; функции-фильтры `contains`, `matchesRegex`, `startsWith`, `endsWith`, `not`, `isEmpty`, `isBlank`. Версионное замечание: в **2.10.0** изменились типы значений, которые возвращают `format()`, `toString()`, `toDate()`, `toBytes()` ([Migration Guidance](https://cwiki.apache.org/confluence/display/NIFI/Migration+Guidance)) — проверьте существующие выражения.

**Примеры (NDJSON-логи).**

| Задача | Replacement Value Strategy | Свойство (имя → значение) |
|---|---|---|
| Записать константу | Literal Value | `/svc` → `api` |
| Привести значение: верхний регистр | Literal Value | `/level` → `${field.value:toUpper()}` |
| Положить атрибут FlowFile в каждую запись | Literal Value | `/tenant` → `${tenant}` |
| Скопировать поле в другое | Record Path Value | `/user_id` → `/user/id` |
| Объединить поля | Record Path Value | `/full_name` → `concat(/first, ' ', /last)` |
| Проставить время обработки пачки | Literal Value | `/ingest_ts` → `${now():format("yyyy-MM-dd'T'HH:mm:ss.SSSXXX", "UTC")}` (одно значение на FlowFile) |
| Нормализовать формат даты | Record Path Value | `/ts_norm` → `format(toDate(/ts, "yyyy-MM-dd'T'HH:mm:ssX"), "yyyy-MM-dd HH:mm:ss")` (проверьте форматы `java.time`) |

**Важное ограничение.** `UpdateRecord` обновляет поля, **которые есть в схеме записи/Writer'а**; для нового поля добавьте его в Writer-схему (как в Block 2.2.4) или используйте Jolt `default`/`QueryRecord`. Поведение при отсутствии поля проверяйте на своих данных (**[проверить]**).

**Типичные ошибки.** Путь указывает на несколько полей, а Record Path Value возвращает несколько значений → `failure`; ожидали новое поле, а Writer-схема его не содержит → поле не появилось; EL в литерале вычисляется один раз на FlowFile (для разных значений по записям используйте RecordPath-функции); путаница с форматами дат после перехода на `java.time` (Block 2.4).

#### EvaluateJsonPath

**Назначение.** Выполняет выражения **JsonPath** над JSON-документом и записывает результат в **атрибуты** FlowFile или в **контент**. Это не Record-процессор: он работает с одним JSON-документом целиком.

**Как работает.** Каждое динамическое свойство — выражение JsonPath; если *Destination* = `flowfile-attribute`, **имя свойства** — имя атрибута, в который кладётся результат.

| Свойство | По умолчанию | Назначение |
|---|---|---|
| **Destination** | `flowfile-content` | Куда писать результат: в контент (допускается **только одно** выражение, имя свойства игнорируется) или в атрибуты |
| **Return Type** | `auto-detect` | `json` или `scalar`. `auto-detect` выбирает `json` для контента и `scalar` для атрибутов. Если выражение даёт массив/объект, а тип `scalar`, FlowFile остаётся неизменным и уходит в `failure` |
| **Path Not Found Behavior** | `ignore` | Только для Destination=атрибут: `ignore` (создаётся атрибут с пустой строкой), `warn` (то же + предупреждение в лог), `skip` (атрибут не создаётся) |
| **Null Value Representation** | `empty string` | Как представлять `null`: пустая строка или строка `null` |
| **Max String Length** | `20 MB` | Максимальная длина строкового значения при разборе |

**Relationships.**

| Relationship | Когда |
|---|---|
| `matched` | JsonPath успешно вычислен и FlowFile изменён. При Destination=атрибут и ненайденном пути FlowFile **всегда** идёт в `matched` (атрибуты будут пустыми или отсутствовать, смотря по *Path Not Found Behavior*) |
| `unmatched` | Только при Destination=контент: путь не найден, контент не менялся |
| `failure` | JsonPath нельзя применить (например, содержимое не валидный JSON), либо несовместимый тип результата |

**Примеры.**

```text
Destination = flowfile-attribute
  user.id   = $.user.id
  first.sku = $.items[0].sku
  → атрибуты user.id и first.sku; дальше RouteOnAttribute по ${user.id}

Destination = flowfile-content, Return Type = json
  (единственное свойство) = $.payload
  → содержимое FlowFile заменяется объектом payload
```

**Когда использовать и когда нет.**

- **Подходит:** достать **небольшие скаляры** (идентификатор, тип события, ключ маршрутизации) в атрибуты для `RouteOnAttribute`, имени файла, ключа Kafka; «вытащить» вложенный объект в контент.
- **Не подходит:** массивы записей, большие значения (атрибуты живут в heap — Block 1.1.3), NDJSON (много документов подряд — это не один JSON-документ); для них используйте Record-процессоры (`QueryRecord`, `PartitionRecord`, `UpdateRecord`).
- Ловушка: при `ignore` ненайденный путь даёт **пустой атрибут, а не отсутствующий**; в `RouteOnAttribute` проверяйте `${user.id:isEmpty()}`, либо используйте `skip` и `${user.id:isNull()}`.
- Сравните: `EvaluateJsonPath` + `RouteOnAttribute` на каждую запись (после `SplitJson`) заменяется одним `QueryRecord` на весь FlowFile.

#### MergeRecord

**Назначение.** Склеивает **много мелких FlowFile** в **крупные** (по числу записей, размеру, времени), сохраняя схему и формат через Reader/Writer. Это обратная сторона Split и главный инструмент «подготовки пакета» перед приёмником (БД, Splunk, Kafka, файл).

**Как работает.** FlowFile раскладываются по **bin'ам** («корзинам»): в один bin попадают только «похожие» FlowFile — с **одинаковой схемой** (по определению Reader'а) и, если задан *Correlation Attribute Name*, с одинаковым значением этого атрибута. Bin выпускается (сливается), когда выполнены условия готовности.

**Правила готовности bin'а** ([Additional Details](https://nifi.apache.org/docs/nifi-docs/components/org.apache.nifi/nifi-standard-nar/1.3.0/org.apache.nifi.processors.standard.MergeRecord/additionalDetails.html)):

1. Bin считается **полным**, когда выполнены **оба** минимума: *Minimum Bin Size* и *Minimum Number of Records* (незаданный игнорируется).
2. Bin сливается, как только достигнут **любой один** максимум: *Maximum Bin Size* или *Maximum Number of Records*.
3. Если достигнут **Max Bin Age**, bin сливается **даже без минимумов** — защита от «застрявшего хвоста» данных.
4. Если создано максимальное число bin'ов и новый FlowFile некуда положить, **самый старый bin сливается**, чтобы освободить место.
5. В режиме `Defragment` bin готов, когда число FlowFile в нём равно `fragment.count`.

| Свойство | По умолчанию | Назначение |
|---|---|---|
| **Record Reader / Record Writer** | — | Как читать входные и писать объединённый FlowFile |
| **Merge Strategy** | `Bin-Packing Algorithm` | `Bin-Packing Algorithm` — набирает bin'ы по размеру/записям; `Defragment` — собирает части обратно по атрибутам `fragment.identifier` и `fragment.count` (обязательны у всех FlowFile) |
| **Minimum Number of Records** | `1` | Нижняя граница записей в bin'е. **Значение по умолчанию даёт мелкие выходы** — повышайте |
| **Maximum Number of Records** | `1000` | Верхняя граница записей; это «мягкий» лимит: если добавляется FlowFile, в bin попадают все его записи, поэтому результат может немного превысить лимит |
| Minimum Bin Size / Maximum Bin Size | `0 B` / не задан | Границы по размеру |
| **Max Bin Age** | не задан | Максимальное время жизни bin'а, формат `<число> <секунды/минуты/часы>`. **Обязателен на практике** |
| **Maximum Number of Bins** | — | Сколько bin'ов хранить в памяти. Не меньше числа `Concurrent Tasks`, иначе bin'ы часто состоят из одного FlowFile |
| Correlation Attribute Name | не задан | Объединять только FlowFile с одинаковым значением атрибута (например, `tenant`) |
| Attribute Strategy | — | Какие атрибуты попадают в результат: `Keep Only Common Attributes` (только общие у всех, с одинаковыми значениями) или `Keep All Unique Attributes` |

**Relationships:** `merged` (объединённый FlowFile), `original` (исходные FlowFile, из которых собран результат), `failure`.

**Атрибуты результата:** `record.count`, `mime.type`, `merge.count` (сколько FlowFile объединено), `merge.bin.age` (возраст bin'а при выпуске), `merge.uuid`, `merge.completion.reason` (почему bin выпущен: достигнут максимум, истёк возраст, превышено число bin'ов и т. д.) — удобно для диагностики.

**Практика.**

1. **Всегда задавайте Max Bin Age** и осознанно Min/Max Records; пример для NDJSON перед Splunk: Min Records `5000`, Max Records `20000`, Max Bin Age `30 sec`.
2. **Не планируйте `MergeRecord` по CRON** — справочник прямо не рекомендует.
3. Для очень больших итоговых FlowFile — **двухступенчатое слияние**: первый MergeRecord собирает по 1 000, его `merged` идёт во второй MergeRecord, тоже по 1 000 (так собирают миллион записей без раздувания числа bin'ов в памяти).
4. Разные схемы **не склеиваются** — они попадут в разные bin'ы; следите, чтобы источник не «плавал» по схеме (Infer Schema, Block 1.3.4).
5. Группировка по ключу (тенант, топик): установите атрибут (через `UpdateAttribute`/`PartitionRecord`) и укажите его в *Correlation Attribute Name*.
6. Баланс: чем больше bin, тем выше throughput приёмника, но выше latency и память; подбирайте по замерам (Lab 8).

**Типичные ошибки.** Нет `Max Bin Age` → последние записи «зависают»; `Minimum Number of Records` = 1 (по умолчанию) → на выходе миллион мелких FlowFile; Maximum Number of Bins меньше числа потоков → фрагментация; ожидание строгого лимита записей, хотя он «мягкий»; `Defragment` без атрибутов `fragment.*`.

#### Шпаргалка: операции из Lab 5 и чем они выполняются

| Операция над NDJSON | Основной инструмент | Альтернатива |
|---|---|---|
| Проверка на схему, отделение невалидных | `ValidateRecord` | — |
| Удалить поле | Writer-схема, `RemoveRecordField` или `QueryRecord` (SELECT) | Jolt `remove` |
| Изменить значение поля | `UpdateRecord` | Jolt `modify-*` |
| Вставить поле (константа/время) | `UpdateRecord` + Writer-схема | Jolt `default`, `QueryRecord` |
| Переместить поле в начало | Порядок полей в Writer-схеме / `SELECT` | — |
| Отфильтровать и разделить по условию | `QueryRecord` | `PartitionRecord` |
| Достать скаляр в атрибут | `EvaluateJsonPath` (небольшие значения) | `PartitionRecord`, `LookupRecord` |
| Склеить в крупные пакеты | `MergeRecord` | `MergeContent` (байтовая склейка) |


### Lab 5: NDJSON transformation pipeline

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

**Эталонное решение:** `nifi/flows/lab5-ndjson-pipeline.json`.

---

## Block 2.3: Splunk integration

### 2.3.1 Варианты интеграции

| Компонент | Транспорт | Назначение |
|---|---|---|
| `PutSplunk` | Raw TCP или UDP на порт Splunk (**TCP/UDP Data Input**) | Отправка содержимого FlowFile как «сырых» событий; без аутентификации Splunk и без подтверждения индексирования |
| `PutSplunkHTTP` | HTTP(S) → HTTP Event Collector (HEC) | События с токеном, `source`/`sourcetype`/`index`; FlowFile получает атрибут с идентификатором для проверки индексирования |
| `QuerySplunkIndexingStatus` | HEC indexer acknowledgement | Подтверждение, что событие реально проиндексировано (at-least-once от начала до конца) |
| `InvokeHTTP` → HEC `/services/collector/event` или `/raw` | HTTP(S) | Гибкое формирование запросов, пакеты, собственные заголовки |
| Универсальный forwarder / Kafka → Splunk Connect | Внешние агенты | Когда NiFi не должен быть «последней милей» |

### 2.3.2 PutSplunk: конфигурация

| Свойство | Назначение |
|---|---|
| Protocol | `TCP` или `UDP` |
| Hostname / Port | Адрес Splunk (индексер, heavy forwarder или балансировщик) |
| **Message Delimiter** | Разделитель, добавляемый между сообщениями (обычно `\n`); определяет, как содержимое FlowFile режется на события |
| Connection Per FlowFile | **Нет в NiFi 2.12** (было в 1.x): открывать ли новое соединение на каждый FlowFile. В 2.x `PutSplunk` держит соединения и закрывает простаивающие по *Idle Connection Expiration* |
| Idle Connection Expiration | Закрытие простаивающих соединений |
| Timeout | Таймаут соединения/записи |
| Max Size of Socket Send Buffer | Размер буфера отправки (влияет на throughput по высоколатентной сети) |
| SSL Context Service | TLS для TCP (должен соответствовать настройке Splunk input) |
| Character Set | Кодировка (обычно UTF-8) |

Relationships: `success`, `failure`.

**Принцип работы.** Содержимое FlowFile читается потоково и пишется в сокет; разделитель добавляется после каждого «сообщения» (если контент уже содержит `\n` между событиями — NDJSON — отдельный разделитель не нужен, но обеспечьте, чтобы последняя строка тоже оканчивалась `\n`). Для UDP нет подтверждений и повторов; для TCP успех означает, что данные записаны в сокет, **а не проиндексированы Splunk**.

### 2.3.3 Пропускная способность: что влияет

1. **Размер FlowFile и число соединений.** Один FlowFile с 10 000 событий через одно долгоживущее TCP-соединение — быстро. 10 000 FlowFile по одному событию — заметно медленнее (накладные расходы на каждый FlowFile; в 1.x с `Connection Per FlowFile` ещё и handshake на каждый).
2. **Concurrent Tasks.** Каждая задача держит своё соединение. Больше задач — больше параллельных потоков в Splunk (до пределов input'а и сети). Для упорядоченной доставки — одно соединение.
3. **Сетевой буфер** и задержка до Splunk: при высокой RTT увеличивайте буфер отправки.
4. **Парсинг на стороне Splunk.** Если Splunk разрезает события по `LINE_BREAKER` и ищет метку времени, формат (NDJSON с `ts` в начале!) влияет на скорость индексации. Это та самая причина, по которой в Block 2.2 важно положение `ingest_ts`/времени в начале объекта.
5. **Back pressure.** Если Splunk принимает медленнее, очередь перед `PutSplunk` растёт — это нормально: back pressure остановит upstream; настройте пороги и мониторинг.
6. **TLS** добавляет CPU-накладные расходы на обеих сторонах.

### 2.3.4 Оптимизации и альтернативные подходы

- **Склейка в крупные NDJSON-пачки** (`MergeRecord`, размер сотни КБ — единицы МБ) перед отправкой.
- **HEC вместо raw TCP**, если нужны: аутентификация, метаданные (`sourcetype`, `index`), подтверждения (ack), HTTP-балансировка и прозрачная диагностика кодов ответов. Для HEC в `InvokeHTTP`:

```text
HTTP Method : POST
HTTP URL    : https://splunk.example.com:8088/services/collector/event
Header      : Authorization = Splunk ${hec.token}      (токен — sensitive parameter / атрибут из secrets)
Content-Type: application/json
Body        : несколько JSON-событий подряд, например
  {"time":1731664800,"sourcetype":"app:log","index":"main","event":{"msg":"started"}}
  {"time":1731664801,"sourcetype":"app:log","index":"main","event":{"msg":"db timeout"}}
```

(Формат тела HEC — см. [документацию Splunk HEC](https://docs.splunk.com/Documentation/Splunk/latest/Data/FormateventsforHTTPEventCollector); NiFi лишь передаёт HTTP-запрос.)

- **Индексаторский ack** (`PutSplunkHTTP` + `QuerySplunkIndexingStatus`) даёт end-to-end подтверждение ценой дополнительной обратной связи и сложности.
- **Распределение нагрузки на индексеры:** DNS round robin / L4-балансировщик; несколько `PutSplunk` на разные индексеры; load balancing на связях NiFi.
- **Фильтрация до отправки.** Не отправляйте шум (DEBUG, health-checks): `QueryRecord` с фильтром `WHERE level <> 'DEBUG'` экономит лицензию Splunk и сеть.
- **Альтернативы архитектуры:** Kafka как буфер с Splunk Connect for Kafka; Universal Forwarder на конечных хостах. Выбор зависит от того, кто должен отвечать за доставку (платформа данных или команда Splunk) и каковы требования к гарантиям.

### Lab 6: доставка событий в Splunk (или в имитацию)

**Цель.** Отправить NDJSON-события из Lab 5 в Splunk либо в эмулированный endpoint и измерить throughput.

**Архитектура.**

```text
...► MergeRecord (NDJSON) ► PutSplunk (TCP) ► [Splunk TCP input / netcat-эмулятор]
                       └─► (альтернатива) InvokeHTTP ► [HEC / http-эмулятор]
```

**Эмуляция.** На стенде эмулятор Splunk уже запущен (`make lab-services`, контейнер `splunk-sim`, код — `lab-services/splunk_sim.py`): TCP input на `splunk-sim:5514` пишет события в `/data/splunk_in.log`, HEC на `http://splunk-sim:8088/services/collector/event` — в `/data/hec_in.log` и отвечает как Splunk (`{"text":"Success","code":0}`). Без стенда тот же эффект даёт:

```bash
# Эмуляция TCP-input Splunk
nc -lk 5514 > /tmp/splunk_in.log
# Либо на Python: простой приёмник HTTP для HEC
python3 - <<'EOF'
from http.server import BaseHTTPRequestHandler, HTTPServer
class H(BaseHTTPRequestHandler):
    def do_POST(self):
        n = int(self.headers.get('Content-Length', 0)); body = self.rfile.read(n)
        open('/tmp/hec_in.log','ab').write(body + b'\n')
        self.send_response(200); self.end_headers(); self.wfile.write(b'{"text":"Success","code":0}')
HTTPServer(('0.0.0.0', 8088), H).serve_forever()
EOF
```

**Источник событий.** `GenerateRecord` (схема лог-события, 100 записей на FlowFile, `Run Schedule 1 sec`) ► `MergeRecord` (Min/Max Records `10000`, Max Bin Age `10 sec`). Удобно собрать две группы: A — `PutSplunk` (TCP), B — `InvokeHTTP` (HEC) — и запускать их по очереди.

**Конфигурация `PutSplunk`:** Protocol `TCP`; Hostname `#{splunk.host}` (`splunk-sim`); Port `#{splunk.port}` (`5514`); Message Delimiter `\n`; Concurrent Tasks `1` (затем эксперимент с `2`, `4`). Свойства `Connection Per FlowFile` в NiFi 2.12 нет (см. 2.3.2).

**Конфигурация `InvokeHTTP` (HEC):** HTTP Method `POST`; HTTP URL `#{hec.url}`; Request Content-Type `application/json`; динамическое свойство `Authorization` = `Splunk #{hec.token}` — динамические свойства `InvokeHTTP` становятся заголовками запроса.

> **Формат тела HEC.** Эндпоинт `/services/collector/event` настоящего Splunk принимает события только в конверте `{"time": …, "sourcetype": …, "event": {…}}` (см. 2.3.4), а «голый» NDJSON отклоняет. Эмулятор стенда принимает любое тело, поэтому лабораторная работает и без конверта. Для реального HEC оберните записи перед `InvokeHTTP`, например `JoltTransformRecord` со спецификацией `[{"operation":"shift","spec":{"*":"event.&"}},{"operation":"default","spec":{"sourcetype":"app:log","index":"main"}}]` (**[проверить]** на вашей версии) — либо отправляйте на `/services/collector/raw`.

**Эксперимент.**

1. Подготовьте 1 млн событий (многократный `GenerateRecord`/`GenerateFlowFile`). На ноутбуке начните с ~100 000 и увеличивайте, следя за heap.
2. Вариант 1: мелкие FlowFile по 100 событий. Вариант 2: пачки по 10 000 через `MergeRecord`. Вариант 3: `Concurrent Tasks=4`.
3. Измеряйте события/с по счётчикам очереди и времени (на стенде: `make lab-splunk-count`).

**Проверка.** `make lab-splunk-count` (или `wc -l /tmp/splunk_in.log` без стенда) равно числу отправленных событий — например, 21 FlowFile по 100 событий дают ровно 2100 строк и по TCP, и по HEC; в provenance события `SEND` показывают адрес приёмника. Перед новым замером обнулите файлы эмулятора: `docker exec splunk-sim sh -c ': > /data/splunk_in.log; : > /data/hec_in.log'`.

**Типичные ошибки.** `PutSplunk` в `failure` — приёмник недоступен/закрыл соединение; всё в одну строку — нет разделителя между событиями; Splunk не разбирает время — формат/порядок полей не соответствует настройкам sourcetype; HEC отвечает `400 Invalid data format` — событие не завёрнуто в конверт `{"event": …}`.

**Эталонное решение:** `nifi/flows/lab6-splunk-delivery.json` (без конверта HEC — рассчитано на эмулятор).

---

## Block 2.4: Migration from NiFi 1.x to 2.x

### 2.4.1 Ключевые принципы и различия 1.x ↔ 2.x

**Статус версии.** NiFi 2.0.0 — первый GA релиз (4 ноября 2024). Основные архитектурные и функциональные изменения (подтверждено по [Release Notes](https://cwiki.apache.org/confluence/display/NIFI/Release+Notes) и [Release Goals](https://cwiki.apache.org/confluence/display/NIFI/NiFi+2.0+Release+Goals)):

| Область | NiFi 1.x | NiFi 2.x | Причина изменения |
|---|---|---|---|
| Java | 8/11/17 (в зависимости от релиза) | **Java 21** (минимум) | Зависимости (Jetty 12, Spring 6, Servlet 6) требуют новых версий Java; NAR plugin 2.0.0 «move to Java 21 … to align with NiFi 2» |
| Формат flow | `flow.xml.gz` (+ `flow.json.gz` с 1.16) | Только `flow.json.gz` | Устранение дублирования представлений |
| Templates | XML Templates | **[rem]** — JSON Flow Definitions / Registry | Дублирующая функциональность |
| Variable Registry | Variables | **[rem]** — Parameter Contexts; EL scope `ENVIRONMENT` | Дублирующая функциональность |
| Event-driven scheduling | Доступна на части процессоров | **[rem]** — только Timer driven (и CRON) | Экспериментально, без выигрыша |
| UI | Старый UI | Полностью новый UI (Angular), dark mode | Модернизация стека |
| API | `nifi-api` в составе проекта | Независимо версионируемый `nifi-api` 2.x | Развязка расширений и фреймворка |
| Kafka | `*_1_0`, `*_2_0`, `*_2_6`, Record-варианты | **Единые `ConsumeKafka`/`PublishKafka`** + `Kafka3ConnectionService`; все 2.6-процессоры удалены | Единая архитектура, независимость от версии клиента |
| Скрипты | ECMAScript, Lua, Ruby, Jython, Groovy | Groovy; Python-расширения через новый Python API | Нет поддержки устаревших движков |
| Python | — | Нативный Python API для процессоров | Новая возможность |
| Stateless | Отдельный runtime | Stateless execution mode на уровне Process Group | Транзакционные цепочки в основном NiFi |
| Flow Registry | NiFi Registry | Git-клиенты (GitHub, GitLab, Bitbucket, Azure DevOps); NiFi Registry deprecated (2.8.0) | Упрощение SDLC |
| Bootstrap | Сложный протокол | Переработан, мониторинг по HTTP | Простота |
| Репозитории | Поддерживалось шифрование repo | **[rem]** — поддержка шифрования репозиториев удалена | См. раздел рисков |
| Безопасность | Knox SSO и Kerberos SPNEGO для REST API, `encrypt-config`/property protection | **[rem]** (2.0.0-M4) | Устаревшие/неподдерживаемые механизмы |
| Windows scripts | `run-nifi.bat` и др. | `nifi.cmd` с подкомандами | Унификация |

#### Удалённые компоненты (по [Release Notes](https://cwiki.apache.org/confluence/display/NIFI/Release+Notes) и [Migration Guidance](https://cwiki.apache.org/confluence/display/NIFI/Migration+Guidance))

| Версия | Удалено |
|---|---|
| 2.0.0-M1 | Часть модулей переведена в optional build profiles (Hadoop, HBase, Kudu, Parquet, Solr NAR — теперь отдельная загрузка) |
| 2.0.0-M2 | InfluxDB processors; Bootstrap Notification Services; MiNiFi C2 Server; переход с `SimpleDateFormat` на `DateTimeFormatter`; Jolt-процессоры перенесены в `nifi-jolt-nar` |
| 2.0.0-M3 | Accumulo 2; Atlas Reporting Task; Hive 3; Ranger Plugin; Solr; Cassandra 3; **ConvertAvroToJSON**; Couchbase; Datadog Reporting Task; **ExecuteStateless** processor; InvokeAWSGatewayApi; Spark Livy |
| 2.0.0-M4 | Application property protection и `encrypt-config`; REST Knox SSO; REST Kerberos SPNEGO |
| 2.0.0 | PutIceberg (и сервисы); HBase; **Kafka 2.6 Processors**; ListenBeats/ListenRELP/ListenSMTP/ListenTCPRecord; DecryptContent(+Compatibility); Kudu; Yandex; QueryDNS/QueryWhois; ExtractTNEFAttachments; **поддержка Repository Encryption**; Shell User Group Provider; сервисы `Distributed*` переименованы |
| 2.7.0 | OCSP-валидация клиентских сертификатов в фреймворке |
| 2.8.0 | Asana-компоненты |
| 2.10.0 | Авторизация по restricted components / required permissions |

Ниже компоненты сгруппированы **по областям**: JSON и конвертация, Kafka, HTTP, выполнение запросов к БД. Колонка «Источник» показывает, насколько факт подтверждён: **Apache** — официальные [Release Notes](https://cwiki.apache.org/confluence/display/NIFI/Release+Notes) или страница [Migrating Deprecated Components and Features for 2.0.0](https://cwiki.apache.org/confluence/spaces/NIFI/pages/240883792/Migrating+Deprecated+Components+and+Features+for+2.0.0); **вторичный** — документация производителя дистрибутива (Cloudera, [Removed NiFi processors](https://docs.cloudera.com/dataflow/cloud/support-matrix/topics/cdf-removed-processors.html)), где версии удаления относятся к их сборкам; такие пункты нужно сверять с [Deprecated Components and Features](https://cwiki.apache.org/confluence/display/NIFI/Deprecated+Components+and+Features) и вашим `nifi-deprecation.log` (**[проверить]**).

#### Удалённые компоненты по областям

##### JSON, Avro и конвертация форматов

| Компонент (1.x) | Статус в 2.x | Чем заменить | Источник |
|---|---|---|---|
| `ConvertAvroToJSON` | **[rem]** 2.0.0-M3 | `ConvertRecord` (`AvroReader` → `JsonRecordSetWriter`) | Apache (удаление); замена — вторичный |
| `ConvertJSONToAvro`, `ConvertCSVToAvro`, `ConvertAvroSchema`, `InferAvroSchema`, `ConvertAvroToORC` | Устаревшие «безрекордные» конвертеры; в списке Cloudera «удалено между NiFi 1.26 и 2.0» и в их линейке 1.25 | `ConvertRecord` с нужными Reader/Writer; для `InferAvroSchema` — `ExtractRecordSchema`; для `ConvertAvroToORC` замены нет | вторичный; **[проверить]** версию удаления в самом Apache |

Что **не** удалено и продолжает работать: `EvaluateJsonPath`, `SplitJson`, `JoltTransformJSON`, `JoltTransformRecord`, `JsonTreeReader`/`JsonPathReader`, `JsonRecordSetWriter`, `QueryRecord`, `ValidateRecord`. Но есть **изменения поведения**, которые ломают flow без удаления компонентов:

- форматы дат/времени в Record-компонентах — `java.time` вместо `SimpleDateFormat` (2.0.0-M2);
- Jolt-процессоры в `nifi-jolt-nar` (2.0.0-M2) и на библиотеке jolt-community (2.8.0): кастомные классы нужно перекомпилировать;
- типы значений RecordPath-функций `format()`, `toString()`, `toDate()`, `toBytes()` (2.10.0);
- более «мягкие» JSON-читатели и гибче JSON Writer (2.10.0).

(Подробно — Block 2.2.)

##### Kafka

| Компонент (1.x) | Статус в 2.x | Чем заменить | Источник |
|---|---|---|---|
| `ConsumeKafka_2_6` | **[rem]** 2.0.0 | `ConsumeKafka`, Processing Strategy = `FLOW_FILE` или `DEMARCATOR` | Apache |
| `ConsumeKafkaRecord_2_6` | **[rem]** 2.0.0 | `ConsumeKafka`, Processing Strategy = `RECORD` + Record Reader/Writer | Apache |
| `PublishKafka_2_6` | **[rem]** 2.0.0 | `PublishKafka` (режим FlowFile/Demarcator) | Apache |
| `PublishKafkaRecord_2_6` | **[rem]** 2.0.0 | `PublishKafka` (режим Record) | Apache |
| `ConsumeKafka_1_0`/`_2_0`, `ConsumeKafkaRecord_1_0`/`_2_0`, `PublishKafka_1_0`/`_2_0`, `PublishKafkaRecord_1_0`/`_2_0` (и более старые линейки) | Более старые версии процессоров; в списке Cloudera «удалено между NiFi 1.26 и 2.0» значатся `ConsumeKafkaRecord_1_0`/`_2_0` (замена на `*_2_6`) | Сначала перевести на `*_2_6` на 1.x (до апгрейда), затем пересобрать на `ConsumeKafka`/`PublishKafka` | вторичный; **[проверить]** |
| Параметры подключения внутри процессора (Kafka Brokers, Security Protocol, SASL, SSL Context) | Больше не задаются в процессоре | Controller Service `Kafka3ConnectionService` | Apache (новые компоненты) |

Автоматического преобразования старых процессоров в новые **нет**: после загрузки flow в 2.x они остаются «призрачными» (ghosted). Порядок миграции и перенос Group ID — раздел 2.4.3.

##### HTTP

| Компонент / свойство (1.x) | Статус в 2.x | Чем заменить | Источник |
|---|---|---|---|
| `GetHTTP` | **[rem]** 2.0.0 (deprecated с 1.9.0) | `InvokeHTTP`: HTTP Method `GET`; имя файла из URL — Response FlowFile Naming Strategy `URL_PATH` | Apache |
| `PostHTTP` | **[rem]** 2.0.0 (deprecated с 1.9.0) | `InvokeHTTP`: HTTP Method `POST`. Режим *Send as FlowFile* (пакет атрибутов и контента для `ListenHTTP`) заменяется связкой `MergeContent` (формат FlowFile v3) → `InvokeHTTP` либо Site-to-Site | Apache |
| Свойства `InvokeHTTP`: Proxy Host, Proxy Port, Proxy Type, Proxy Username, Proxy Password | Удалены (deprecated с 1.18.0) | Controller Service `StandardProxyConfigurationService` и свойство *Proxy Configuration Service* | Apache |
| `InvokeAWSGatewayApi` | **[rem]** 2.0.0-M3 | `InvokeHTTP` с нужной аутентификацией | Apache (удаление); замена — по смыслу, **[проверить]** |

Различия поведения при замене на `InvokeHTTP` (по официальной странице миграции): SSL Context Service для публичных HTTPS-адресов не нужен (используются стандартные корневые сертификаты JVM); таймауты сокета по умолчанию **меньше**, чем у `GetHTTP`/`PostHTTP`, — задавайте их явно; `filename` по умолчанию — случайный UUID. С 1.24.0 значения в URL должны быть URL-кодированы.

##### Базы данных и выполнение запросов

**Основные SQL-процессоры и пулы соединений** — `ExecuteSQL`, `ExecuteSQLRecord`, `QueryDatabaseTable`, `QueryDatabaseTableRecord`, `GenerateTableFetch`, `PutSQL`, `PutDatabaseRecord`, `DBCPConnectionPool`, `HikariCPConnectionPool`, `DBCPConnectionPoolLookup` — в перечнях удаляемых компонентов Release Notes и Migration Guidance **не значатся** (**[проверить]** по вашему `nifi-deprecation.log`). `DBCPConnectionPoolLookup` присутствует в документации 2.12.0. Менялись условия использования: подключения H2 запрещены (1.22.0), введено ограничение *Reference Remote Resources* (1.23.0), в 2.10.0 удалена авторизация по restricted components, в 2.2.0 добавлен Database Dialect Service.

**Удалены компоненты конкретных СУБД и хранилищ** (flow, в которых они есть, не будут работать):

| Область | Что удалено | Версия | Источник |
|---|---|---|---|
| Hive | Компоненты Hive 3 (процессоры чтения, записи, создания/обновления таблиц, стриминга Hive 3 и их сервисы) | 2.0.0-M3 | Apache (семейство); точные имена классов — **[проверить]** |
| Cassandra | Cassandra 3 | 2.0.0-M3 | Apache |
| Accumulo | Accumulo 2 | 2.0.0-M3 | Apache |
| Couchbase | Компоненты Couchbase | 2.0.0-M3 | Apache |
| Solr | Компоненты Solr | 2.0.0-M3 (в M1 — отдельный optional-профиль сборки) | Apache |
| InfluxDB | Процессоры InfluxDB | 2.0.0-M2 | Apache |
| HBase | Компоненты HBase | 2.0.0 | Apache |
| Kudu | Компоненты Kudu | 2.0.0 | Apache |
| Iceberg | `PutIceberg` и связанные сервисы | 2.0.0 | Apache |
| Ignite, Flume, Kite | `GetIgniteCache`, `PutIgniteCache`, `ExecuteFlumeSource`/`ExecuteFlumeSink`, `StoreInKiteDataset` | у производителя удалены в линейке 1.25 | вторичный; **[проверить]** |

Прямой замены у большинства нет: используйте JDBC-совместимый доступ (`PutDatabaseRecord`/`ExecuteSQLRecord`, если у СУБД есть драйвер), внешние коннекторы (Kafka Connect, СУБД-специфичные средства) или Python-расширения NiFi. Для компонентов, которые нужны вам, проверьте, поставляются ли они отдельным NAR в вашем дистрибутиве (часть модулей в 2.x вынесена в optional build profiles).

##### Другие удалённые компоненты из официального списка 2.0.0

Для полноты (официальная страница миграции): `Base64EncodeContent` → `EncodeContent`; `HashContent` → `CryptographicHashContent`; `HashAttribute` → `UpdateAttribute` с EL-функцией `hash`; `GetJMSQueue`/`GetJMSTopic` → `ConsumeJMS`; `PutJMS` → `PublishJMS`; `GetTwitter` → `ConsumeTwitter`. Дополнительно по данным производителя (вторичный источник, **[проверить]**): `PutAzureBlobStorage` → `PutAzureBlobStorage_v12`; `PutBigQueryBatch`/`PutBigQueryStreaming` → `PutBigQuery`; `HBase_1_1_2_*` → `HBase_2_*`; `HortonworksSchemaRegistry` не имеет прямого аналога в Apache.

**Как найти такие компоненты в вашем flow.** Запустите последнюю 1.x и проверьте `logs/nifi-deprecation.log` (с 1.18.0 он содержит предупреждения о компонентах и свойствах, предназначенных к удалению; [описание](https://nifi.apache.org/docs/nifi-docs/html/administration-guide.html#deprecation-logging)), либо составьте инвентаризацию типов из `flow.json.gz` командой из раздела 2.4.5 и сверьте с таблицами выше.

### 2.4.2 Совместимость существующих flow

NiFi 2.x умеет стартовать с **«призрачными» (ghosted) компонентами** — flow загружается, но удалённые процессоры/сервисы отображаются как недоступные, чтобы их можно было заменить (цель [Release Goals](https://cwiki.apache.org/confluence/display/NIFI/NiFi+2.0+Release+Goals), «migration tools»). Поэтому «успешный старт» ещё **не означает**, что flow работает.

Классы несовместимостей:

1. **Удалённые/перемещённые компоненты** — нужна замена (см. таблицы).
2. **Изменённые свойства** — удалены deprecated-свойства (Keytab-свойства процессоров заменены на `KerberosUserService`; `KeytabCredentialsService` → `KerberosKeytabUserService`). Процессоры становятся *invalid*, пока свойства не перенастроены.
3. **Переименование** — Distributed Map/Set Cache → Map/Set Cache (`MapCacheServer`, `MapCacheClientService`, …).
4. **Поведение Record/дат** — переход на `java.time` меняет допустимые шаблоны форматов и точность (2.0.0-M2); меняются RecordPath-функции (2.10.0).
5. **Кастомные компоненты** — NAR, собранные под 1.x, как правило, не загрузятся: нужно пересобрать с `nifi-api` 2.x и Java 21; Servlet API 3 → 6 (`javax.servlet` → `jakarta.servlet`); `PropertyDescriptor` с `defaultValue(null)` больше не поддерживается; additional details — `.md` вместо `.html`.
6. **Scripted-компоненты** — ECMAScript/Lua/Ruby (и Jython) недоступны, переписать на Groovy/Python API. В 2.7.0/2.7.1 известна проблема миграции значения Script Engine у `ExecuteScript` — исправлено в 2.7.1.

### 2.4.3 Kafka: особенности миграции

1. Все процессоры `*Kafka*_2_6` (и более старые) **удалены**. После загрузки flow в 2.x они «призраки». **Автоматического преобразования в `ConsumeKafka`/`PublishKafka` нет** — по информации разработчиков, это «не позволяет выполнить апгрейд без нарушения совместимости» ([Cloudera: Breaking changes](https://docs.cloudera.com/cfm/4.10.0/release-notes/topics/cfm-nifi2-breaking-changes.html); вторичный источник, **[проверить]** актуальность для вашей версии).
2. Новая модель: параметры подключения — в `Kafka3ConnectionService`; в процессорах остаётся логика потребления/публикации.
3. Таблица соответствия (смысловое, **проверяйте точные имена свойств по документации вашей версии**):

| Было (1.x, Kafka 2.6) | Стало (2.x) |
|---|---|
| `ConsumeKafka_2_6` | `ConsumeKafka`, Processing Strategy = `FLOW_FILE` или `DEMARCATOR` |
| `ConsumeKafkaRecord_2_6` | `ConsumeKafka`, Processing Strategy = `RECORD` + Record Reader/Writer |
| `PublishKafka_2_6` | `PublishKafka` (режим FlowFile/Demarcator) |
| `PublishKafkaRecord_2_6` | `PublishKafka` (режим Record) |
| Kafka Brokers, Security Protocol, SASL, SSL Context | Свойства `Kafka3ConnectionService` |
| Topic Name(s) | Topics |
| Group ID | Group ID (сохранить прежний!) |
| Offset Reset | Auto Offset Reset |

4. **Сохранение позиции чтения.** Offsets хранятся в Kafka по паре *(Group ID, topic, partition)*. Если новый `ConsumeKafka` использует **тот же Group ID**, чтение продолжится с зафиксированной позиции. Порядок переключения: *остановить* старые потребители (иначе два потребителя в одной группе будут делить партиции; либо с «призраками» они и так не работают) → запустить новые.
5. Проверить различия semantic: формат атрибутов (`kafka.*`), обработка заголовков, key (`Output Strategy`), поведение ошибок разбора (куда уходят «битые» сообщения).
6. **Не используйте NiFi < 2.8.0** для потребителей, чувствительных к дублям при ребалансировке (исправление в 2.8.0).
7. Обсуждение в dev-списке упоминало временную возможность загружать NAR старых Kafka-процессоров из Maven Central (`2.0.0-M4`), но поддержка/совместимость с GA не подтверждены — не рассчитывайте на это как на стратегию ([пример обсуждения](https://go.mail-archive.com/dev@nifi.apache.org/msg22909.html), **[проверить]**).

Другие интеграции, где возможны сюрпризы: Hive/HBase/Solr/Couchbase/Cassandra 3/Accumulo/Kudu/Iceberg (старый PutIceberg) — удалены; Pulsar (в дистрибутивах — ручная установка NAR); gRPC — как внешние NAR (по документации дистрибутивов).

### 2.4.4 Upgrade routine: in-place vs fresh installation

| Критерий | In-place upgrade (сохранение данных и flow) | Fresh installation + пересборка flow |
|---|---|---|
| Что переносится | `flow.json.gz`, state, репозитории (FlowFile/Content/Provenance), users/policies | Только то, что вы перенесёте осознанно (flow definitions, параметры, политики) |
| Данные «в полёте» | Сохраняются (при условии совместимости репозиториев) | Должны быть дренированы или перенесены отдельно |
| Скорость | Быстрее при небольших изменениях | Дольше, но чище |
| Риск | Накопленный «технический долг» (устаревшие компоненты, невалидные настройки) переезжает; откат сложнее (формат репозиториев, state) | Минимальный перенос проблем; естественный reset конфигурации |
| Откат | Восстановление из backup (потребуется точка отката; см. 2.4.6) | Старый кластер остаётся нетронутым — откат = переключение трафика |
| Тестирование | На клоне prod или staging | Параллельный запуск (blue/green) |
| Когда выбирать | Мало кастомных компонентов, flows уже на `flow.json.gz`, нужно сохранить state (`List*` указатели и т. п.) и очереди | Много устаревших компонентов, смена топологии/ОС/Java, желание пересмотреть архитектуру, жёсткие требования к простому откату |

**Критерии выбора (чеклист решения).**

1. Есть ли данные в очередях, которые нельзя дренировать? → in-place (или аккуратный перенос).
2. Нужно ли сохранить state `List*`/`QueryDatabaseTable*` (иначе повторная загрузка)? → in-place, либо запланированный перенос state/повторная загрузка с идемпотентным приёмником.
3. Сколько кастомных NAR и скриптов? Много → fresh + переписывание.
4. Используется ли шифрование репозиториев (удалено)? → предварительный план выхода из шифрования.
5. Допускается ли downtime? Нет → параллельный кластер (blue/green) + переключение источников/Group ID.
6. Достаточно ли уверенности в тестировании? Нет → fresh/parallel.

Рекомендуемая стратегия для production: **blue/green** — собрать новый кластер 2.x, перенести flows (через JSON definitions/Registry), прогнать тесты на копиях/в «shadow-режиме» (потребление из Kafka отдельной consumer group в `earliest` и запись в тестовые приёмники), затем переключить боевой Group ID/источники и остановить старый кластер, **не удаляя** его до окончания периода наблюдения.

### 2.4.5 Подготовка и prerequisites

**Версионный путь (версия-путь).** Для миграции на 2.0.0 требуется **предварительный апгрейд до 1.27.0** ([Migration Guidance: Migrating to 2.0.0](https://cwiki.apache.org/confluence/display/NIFI/Migration+Guidance)). Для 2.0.0-M4/M3 требовалась 1.26.0, для M2 — 1.25.0, для M1 — 1.24.0. Практически: обновитесь до последней 1.x (например, 1.28.1), устраните deprecation-сообщения, затем идите на 2.x. Затем движение по минорам 2.x подчиняется обычным правилам совместимости, но читайте Migration Guidance каждой версии на пути.

**Предмиграционный чеклист.**

| № | Шаг | Детали |
|---|---|---|
| 1 | Инвентаризация | Версия NiFi, Java, ОС; список процессоров/сервисов по типам; кастомные NAR; скрипты; интеграции |
| 2 | Deprecation-аудит на 1.x | `logs/nifi-deprecation.log` (с 1.18.0 содержит предупреждения о deprecated-компонентах и альтернативах); поиск в UI по `event` (event-driven scheduling) |
| 3 | Обновление до 1.27.0+ | Минимально 1.27.0; рекомендуется последняя 1.x |
| 4 | Устранение Variables/Templates | Конвертация Variables → Parameters; XML templates → JSON flow definitions/Registry |
| 5 | Формат flow | Убедиться, что используется `flow.json.gz` (с 1.16+; `nifi.flow.configuration.file`) |
| 6 | Java 21 | Установить на все узлы; проверить совместимость JDBC-драйверов и внешних JAR |
| 7 | Безопасность | Узнайте, не используете ли Knox SSO/SPNEGO/`encrypt-config`/шифрование репозиториев → планируйте замену |
| 8 | Backup | `conf/` целиком (включая `flow.json.gz`, `authorizers.xml`, `users.xml`, `authorizations.xml`, `login-identity-providers.xml`), `nifi.sensitive.props.key`, `state/`, при in-place — репозитории (требует остановки) |
| 9 | Кластерные зависимости | ZooKeeper: версии, ACL; убедиться в доступности |
| 10 | Окно и плана отката | Время простоя, критерии «go/no-go», ответственные |
| 11 | Staging | Развернуть копию окружения; прогнать flows на тестовых данных |

Для проверки списка компонентов можно использовать экспортированный flow:

```bash
# Список типов процессоров в flow.json.gz (для инвентаризации)
zcat conf/flow.json.gz | jq -r '.. | objects | select(has("type") and has("bundle")) | .type' | sort | uniq -c | sort -rn
```

### 2.4.6 Выполнение, валидация и откат

#### Последовательность in-place upgrade (кластер)

1. Заморозить изменения flow; остановить источники (или перевести в безопасное состояние); дождаться дренирования очередей (по возможности).
2. Остановить кластер; снять backup (см. выше), включая `state/` и репозитории.
3. Установить NiFi 2.x (новая директория, не поверх старой); перенести `conf/` **осознанно**: нельзя слепо копировать `nifi.properties` из 1.x — свойства изменились (например, `nifi.flow.configuration.file` → `flow.json.gz`; Java; bootstrap). Используйте `nifi.properties` 2.x как основу и накатывайте свои значения.
4. Перенести `flow.json.gz`, `users.xml`/`authorizations.xml`, sensitive key, при in-place — каталоги репозиториев/state; проверить права.
5. Запустить один узел, дождаться загрузки; найти *invalid*/ghost-компоненты (UI: поиск `invalid`, `Unsupported`; логи `nifi-app.log`).
6. Заменить удалённые компоненты; заново настроить Kafka-процессоры; пересоздать сервисы с переименованными типами.
7. Запустить остальные узлы; проверить flow election и состояние кластера.
8. Постепенно включать источники (Kafka сначала с мониторингом дублей).

#### Валидация после миграции

- **Технические проверки:** все узлы CONNECTED; нет ghost/invalid компонентов; Controller Services enabled; нет повторяющихся bulletins; TLS и аутентификация работают; репозитории доступны; метрики (Prometheus) поступают.
- **Функциональные:** сравнение выходных данных старого и нового потоков на одинаковых входах (контрольные суммы, `record.count`, схемы); проверка edge-cases (пустые значения, даты/временные зоны — из-за `java.time`; decimal; unicode).
- **Нефункциональные:** throughput/latency, heap/GC, I/O; сравнение с базовой линией 1.x.
- **Интеграции:** Kafka (lag по consumer group, отсутствие дублей/потерь), БД (число записей), Splunk/REST (коды ответов).
- **Безопасность:** политики доступа, аудит, ротация секретов.

#### Rollback strategy

| Подход | Описание | Ограничения |
|---|---|---|
| **Blue/green (рекомендуется)** | Старый кластер 1.x не трогают; откат = вернуть потребителей/трафик (Kafka Group ID) на старый кластер | Необходимы идемпотентные приёмники и контроль двойного потребления |
| **Restore from backup** | Остановить 2.x; вернуть старые `conf/`, репозитории, state; запустить 1.x | Данные, обработанные уже 2.x, нужно учитывать; **формат state/репозиториев мог измениться** |
| **Частичный откат flow** | Через Registry вернуть версию Process Group | Не решает проблемы платформы (Java, форматы) |

Важные факты для отката:

- Репозитории NiFi с 1.1.0 спроектированы с поддержкой отката записанного новой версией состояния на предыдущую ([Release Notes 1.1.0](https://cwiki.apache.org/confluence/display/NIFI/Release+Notes)), но это **не** гарантирует откат между мажорными версиями; тестируйте на staging.
- В **2.14.0** локальное состояние компонентов пишется в новом формате write-ahead log (serializer version 1); **старые версии NiFi не могут его прочитать и не стартуют**. Для отката: восстановить backup `state/local`, сделанный до апгрейда, или удалить этот каталог (что **сбросит локальное состояние** всех компонентов) ([Migration Guidance 2.14.0](https://cwiki.apache.org/confluence/display/NIFI/Migration+Guidance)). Следовательно, backup `state/local` обязателен.
- Flow-конфигурация: `conf/archive/` содержит предыдущие версии flow; но их формат совместим только с соответствующими версиями.
- Данные из очередей после старта 2.x: если flow изменён (замена компонентов), откат вернёт только старую конфигурацию, но не «разработанные заново» FlowFile.

### 2.4.7 Типичные проблемы и способы решения

| Проблема | Причина | Решение |
|---|---|---|
| NiFi 2.x не стартует | Java < 21; неверные пути/`nifi.properties` из 1.x | Java 21; конфигурация 2.x с миграцией значений |
| Компоненты «Ghost»/«Unsupported» | Удалённые процессоры/сервисы | Заменить по таблице; для Kafka — пересоздать |
| Процессоры `invalid` после апгрейда | Удалены/переименованы свойства (Kerberos keytab, Cassandra и др.) | Перенастроить; Kerberos → `KerberosUserService` |
| Flow использует Variables | Variable Registry удалён | Перенести в Parameter Context до апгрейда (на 1.x есть инструменты конвертации в UI) |
| `${var}` в EL не вычисляется | Изменение scope | Использовать Parameters (`#{}`) или `ENVIRONMENT` scope |
| Ошибки дат в Record-компонентах | `SimpleDateFormat` → `DateTimeFormatter` | Исправить шаблоны (`yyyy-MM-dd'T'HH:mm:ss.SSSXXX`) |
| Jolt-спецификации с кастомными JAR | Смена библиотеки (2.8.0) | Перекомпилировать классы на jolt-community |
| Dashboards Prometheus сломались | Изменения метрик/меток (2.12.0: убрана метка `connected_nodes`) | Пересчитать из `cluster_connected_node_count` и `cluster_total_node_count` |
| Кластер не собирается по TLS (2.12+) | `nifi.cluster.node.address`/`load.balance.host` — IP, а не DNS | Использовать DNS-имена и сертификаты со SAN |
| PEM-ключи не читаются (2.13+) | Исторические форматы (PKCS1/OpenSSL) удалены; нужен PKCS8 | Конвертировать ключи в PKCS8 |
| Дубли в Kafka-потреблении при ребалансе | Ошибка в ранних версиях `ConsumeKafka` | Обновиться до ≥ 2.8.0 |
| `ExecuteScript` с неверным Script Engine после апгрейда (2.7.0) | Известная ошибка | Обновиться до 2.7.1+ |
| SSL Context Service в Management Controller Services невалиден (2.7.x) | Миграция Key Password (NIFI-15340) | Исправлено позже; проверить пароль ключа |
| Не стартует старая версия после 2.14 | Новый формат local state | Backup `state/local` до апгрейда или очистка |
| Loss of custom NARs | Несовместимость API | Пересобрать под `nifi-api` 2.x |

### Mini-lab: миграционный чеклист вашей среды

**Цель.** Составить собственный план миграции.

**Шаги.**

1. Экспортируйте инвентаризацию компонентов (команда `jq` выше) и сопоставьте с таблицами удалений.
2. Обсудите [Migration Guidance](https://cwiki.apache.org/confluence/display/NIFI/Migration+Guidance): пройдите разделы для всех версий на пути от вашей текущей до целевой и отметьте изменения, которые затрагивают ваше окружение.
3. Определите порядок: апгрейд до 1.27.0+ → устранение deprecated → staging 2.x → прогон → окно → blue/green.
4. Обсудите «дерево решений»: что делаем при невозможности обновить кастомный NAR / при несовместимости Kafka-потока.

---

## Block 2.5: Day 2 recap and Q&A

### Ключевые выводы

1. **Базы данных:** `DBCPConnectionPool` + `ExecuteSQLRecord`/`QueryDatabaseTableRecord` для чтения; `PutDatabaseRecord` с UPSERT и batch для записи; следите за памятью выборок и идемпотентностью.
2. **REST:** `InvokeHTTP`; корректно маршрутизируйте `Retry`/`No Retry`/`Failure`; задавайте таймауты; ограничивайте скорость запросов.
3. **Kafka 2.x:** `ConsumeKafka`/`PublishKafka` + `Kafka3ConnectionService`; режимы FlowFile/Demarcator/Record; offsets — в Kafka; параллелизм ≤ партиций; версия ≥ 2.8.0 для устранения дублей при rebalance.
4. **JSON/NDJSON:** Reader/Writer с явной схемой; NDJSON — `One Line Per Object`; порядок полей — по схеме Writer; удаление/добавление полей через Jolt/Writer-схему.
5. **Splunk:** `PutSplunk` (raw TCP/UDP) vs HEC; throughput зависит от размера пачек, числа соединений и парсинга на стороне Splunk.
6. **Миграция:** Java 21, предварительно 1.27.0, удалённые компоненты (в т. ч. Kafka 2.6), Variables/Templates, scripted-движки, blue/green и backup `state/local`.

### Вопросы для самопроверки

1. Почему в NiFi 2.x нельзя найти `ConsumeKafkaRecord`, и как получить его поведение?
2. Как сохранить позицию чтения Kafka при замене процессоров?
3. Почему `UpdateRecord` не добавляет новое поле, и как решить задачу?
4. В чём разница между «TCP-запись успешна» и «событие проиндексировано в Splunk»?
5. Какие артефакты необходимо сохранить перед апгрейдом до 2.14?

### Предпросмотр Day 3

Отказоустойчивость и обработка ошибок, мониторинг и troubleshooting, безопасность и governance, тюнинг производительности и финальная лабораторная работа.
