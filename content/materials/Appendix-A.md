---
title: "Appendix A"
---

A training stand for all the labs (not for production) — the stand repository (the compose project `nifi-playground`). All services are started through `make`, run in one Docker network `nifi-net`, and keep their data in named volumes. Verified on NiFi 2.12.0.

### A.1 Components of the stand

| Service | Image | Container | Address in the `nifi-net` network | Host port | Compose file |
|---|---|---|---|---|---|
| NiFi (single) | `apache/nifi:2.12.0` | `nifi` | `https://nifi:8443` | `8443`, `8081` (ListenHTTP, Lab 2) | `nifi/single/docker-compose-nifi-single.yml` |
| NiFi (cluster, 3 nodes) | `apache/nifi:2.12.0` | `nifi1`–`nifi3` | `https://nifi1:8443` … | `9443`–`9445` | `nifi/cluster/docker-compose-nifi-cluster.yml` |
| ZooKeeper (for the cluster) | `zookeeper:3.9` | `nifi-cluster-zookeeper` | `zookeeper:2181` | — | the same file |
| NiFi Registry (HTTPS) + LDAP (`lldap`) | `apache/nifi-registry:2.12.0` | `nifi-registry` | `https://nifi-registry:18443` | `18443`; the LDAP UI `17170` | `nifi/registry/docker-compose-nifi-registry.yml` |
| PostgreSQL 18 | `postgres:18-alpine` | `nifi-postgres` | `postgres:5432` | `5432` | `postgres/docker-compose-postgres.yml` |
| pgAdmin (a UI for PostgreSQL) | `dpage/pgadmin4` | `pgadmin` | — | `127.0.0.1:5050` | the same file |
| Kafka (KRaft, 1 broker) | `confluentinc/cp-kafka:8.0.0` | `kafka1` | `kafka1:19092` | `9092` | `kafka/docker-compose-kafka-single-node.yml` |
| Conduktor Console (a Kafka UI) | `conduktor/conduktor-console` | — | — | `8080` | the same file |
| Prometheus + kafka-exporter | `prom/prometheus` | `prometheus`, `kafka-exporter` | `prometheus:9090` | `9090`, `9308` | `monitoring/prometheus/docker-compose-prometheus.yml` |
| Grafana | `grafana/grafana` | `grafana` | `grafana:3000` | `3000` | `monitoring/grafana/docker-compose-grafana.yml` |
| A stub of the customers REST API (Lab 5) | `python:3.12-alpine` | `customers-api` | `http://customers-api:8000` | — | `lab-services/docker-compose-lab-services.yml` |
| The Splunk emulator (Lab 7) | `python:3.12-alpine` | `splunk-sim` | TCP `splunk-sim:5514`, HEC `http://splunk-sim:8088` | — | the same file |
| An unstable API (Lab 8) | `python:3.12-alpine` | `flaky` | `http://flaky:9099/events` | — | the same file |
| Splunk Enterprise (trial, optional) | `splunk/splunk` | `splunk` | TCP `splunk:5514`, HEC `http://splunk:8088` | `8000` (UI), `8088` | `splunk/docker-compose-splunk.yml` |

Splunk is not part of `make up` and is started separately (`make splunk`; the first start takes several minutes; on Apple Silicon the amd64 image runs through emulation). For the labs the `splunk-sim` emulator is enough; a real Splunk is needed to see the events in a search (`index=nifi`). The ports are the same as the emulator's — only the host changes in the Parameter Context: `splunk-sim` → `splunk`.

### A.2 The repository structure

```text
Makefile                      all the stand's commands (make help)
nifi/
  single/  cluster/           NiFi compose files, authorizers.xml
  registry/                   NiFi Registry + LDAP, authorizers.xml
  common/                     certificate generation, an empty flow, policy and import scripts
  flows/                      reference solutions for the labs (lab*.json) + README
  certs/                      the common CA and the node certificates (generated, not committed)
  data/                       mounted into NiFi as /opt/nifi/data (the labs' inputs and outputs)
  drivers/                    JDBC drivers, mounted as /opt/nifi/drivers
kafka/                        Kafka + Conduktor
postgres/                     PostgreSQL + pgAdmin; init/ — the labs' tables, pgadmin/ — the stand's server
monitoring/prometheus/        Prometheus, kafka-exporter, prometheus.yml
monitoring/grafana/           Grafana and data source provisioning
splunk/                       Splunk Enterprise; apps/ — the nifi index, TCP/HEC inputs, sourcetype
lab-services/                 stubs: customers_api.py, splunk_sim.py, flaky.py; the produce_logs.py generator
```

### A.3 Starting

Requirements: Docker (Docker Desktop) with ~8 GB of memory, `make`, and `python3` on the host (only for `make lab-final-load`).

```bash
make up              # single NiFi + Registry, Postgres, Kafka, monitoring, stubs
make up-cluster      # the same with a 3-node NiFi cluster (≈4–5 GB of memory for the cluster alone)
make lab-setup       # the customers, customers_gold and orders_stage tables, Kafka topics, the nifi/data/* directories (idempotent)
make ps              # the state of the containers
make logs s=nifi     # the container's logs
make urls            # addresses and logins
make down            # stop everything, the data in volumes is preserved
make destroy         # delete everything together with the data (asks for confirmation)
```

Separate stacks: `make nifi`, `make cluster`, `make registry`, `make postgres`, `make kafka`, `make monitoring`, `make lab-services`, `make splunk` and the corresponding `*-down`. NiFi takes 1–3 minutes to start; `make nifi` / `make cluster` wait for readiness and grant the required access policies.

### A.4 Access

| What | Address | Login |
|---|---|---|
| NiFi (single) | `https://localhost:8443/nifi` | `admin` / the password from `make urls` |
| NiFi (cluster) | `https://localhost:9443/nifi` (nodes 2–3: `:9444`, `:9445`) | the same |
| NiFi Registry | `https://localhost:18443/nifi-registry` | `admin` / the password from `make urls` (the users are in LDAP) |
| Registry users (lldap) | `http://localhost:17170` | the same `admin` |
| Splunk (if started) | `http://localhost:8000`, search `index=nifi` | `admin` / the password from `make urls` |
| PostgreSQL | `localhost:5432`, the database `nifi` | `nifi` / `nifi` |
| pgAdmin | `http://localhost:5050`; the server `nifi-playground` is already in the list (it will ask for the database password `nifi` on the first connection) | `admin@example.com` / the password from `make urls` |
| Kafka | `localhost:9092` | — |
| Conduktor | `http://localhost:8080` | on the first login |
| Prometheus | `http://localhost:9090` | — |
| Grafana | `http://localhost:3000` | `admin` / `admin` |

NiFi runs over HTTPS with a certificate from the stand's own CA; so that the browser does not warn you, import `nifi/certs/ca.crt` as a trusted root certificate. In the cluster, work through one node: the login token is issued by a node, and when switching between ports you will have to log in again.

### A.5 Addresses for the labs

What to specify in the Parameter Context and in commands instead of abstract addresses:

| Purpose | The value on the stand |
|---|---|
| Kafka from NiFi (`bootstrap.servers`) | `kafka1:19092`, Security Protocol `PLAINTEXT` |
| The Kafka CLI | `docker exec -i kafka1 kafka-console-producer --bootstrap-server kafka1:19092 ...` (likewise `kafka-console-consumer`, `kafka-topics`, `kafka-consumer-groups`) |
| The JDBC URL | `jdbc:postgresql://postgres:5432/nifi` (user `nifi` / `nifi`) |
| The JDBC driver | `org.postgresql.Driver`, `/opt/nifi/drivers/postgresql-42.7.14.jar` |
| The SQL console | `docker exec nifi-postgres psql -U nifi -d nifi -c "..."` |
| Directories for `ListFile` / `PutFile` | `/opt/nifi/data/<name>` in NiFi = `nifi/data/<name>` on the host |
| The customers REST API | `http://customers-api:8000/customers/<id>` |
| Splunk | the emulator: TCP `splunk-sim:5514`, HEC `http://splunk-sim:8088/services/collector/event`; a real Splunk: TCP `splunk:5514`, HEC `http://splunk:8088/services/collector/raw` (NDJSON as is) or `/services/collector/event` (the envelope `{"event": …}`), the header `Authorization: Splunk 00000000-0000-0000-0000-000000000000` |
| The unstable API | `http://flaky:9099/events` |
| Kafka topics of the final lab | `logs.raw` (6 partitions, input), `logs.clean` (6 partitions, output), `logs.dlq` (errors) — created by `make lab-setup` |
| NiFi Registry (the Registry Client) | `https://nifi-registry:18443` — the client is created automatically (`make nifi` / `make cluster`), an SSL Context Service is not needed |

### A.6 Specifics of the stand

- **Single and cluster.** Port `8081` for `ListenHTTP` (Lab 2) is forwarded only for the single NiFi. In the cluster, the generator sources and `ListFile` should be run on the Primary Node (*Execution: Primary node*), otherwise each node generates its own data.
- **Security.** All NiFi nodes use a common CA (`nifi/certs`), authorization is `managed-authorizer`, login is single-user `admin`. Administrator and node permissions are granted automatically with `make nifi` / `make cluster`.
- **NiFi metrics.** Prometheus scrapes `/nifi-api/flow/metrics/prometheus` over mTLS with the client certificate `CN=prometheus` (read-only on `/flow`).
- **Drivers and data.** `nifi/drivers` and `nifi/data` are mounted into all nodes at identical paths; a new JAR is picked up when `DBCPConnectionPool` is enabled, without restarting NiFi.
- **One compose project.** The stacks are started through `make` with `docker compose -p nifi-playground`; when you run the files directly, specify the same `-p`, otherwise Compose will create new empty volumes.

### A.7 Test data and reset

```bash
make lab03-measure                # Lab 3 measurements: split vs record, report in nifi/data/results/
make lab08-measure                # Lab 8 measurements: retries, DLQ, API outage (~9 min), report in labs/ (.txt + .html)
make lab09-measure                # Lab 9 measurements: scenarios A-D x 3 runs (~6 min, checks free disk), report in labs/
make lab4-insert n=10             # 10 new gold customers in the table customers (Lab 4)
make lab05-data                   # 100 test orders into orders.raw (Lab 5)
make lab-final-load n=100000      # the load for the final lab into logs.raw
make lab-splunk-count             # how many lines the Splunk emulator received
make import f=nifi/flows/<file>.json            # load a flow into the single NiFi
make import-cluster f=nifi/flows/<file>.json    # the same for the cluster
```

Before re-running the labs with Kafka, reset the consumer group (with `ConsumeKafka` stopped), otherwise the flow will not read the messages that have already been read:

```bash
docker exec kafka1 kafka-consumer-groups --bootstrap-server kafka1:19092 --delete --group nifi-lab05   # Lab 5
docker exec kafka1 kafka-consumer-groups --bootstrap-server kafka1:19092 --delete --group nifi-logs   # the final one
docker exec nifi-postgres psql -U nifi -d nifi -c "DELETE FROM customers WHERE id > 1000; TRUNCATE customers_gold"   # Lab 4 (+ Clear State)
docker exec nifi-postgres psql -U nifi -d nifi -c "TRUNCATE orders_stage"                             # Lab 5
docker exec splunk-sim sh -c ': > /data/splunk_in.log; : > /data/hec_in.log'                          # the Splunk emulator
```

### A.8 Reference solutions

The `nifi/flows` directory holds the Flow Definitions of all the labs verified on the stand (`lab1-first-flow.json` … `lab9-performance.json`, `lab10-final-pipeline.json` — the final one; Lab 7 has two variants: `lab7-splunk-delivery-mock.json` for the emulator and `lab7-splunk-delivery-splunk.json` for the real Splunk). Import: drag the **Process Group** icon onto the canvas and click the file upload icon, or `make import f=...`. After importing, enable the group's Controller Services (*Enable all controller services*) and start it. The environment values are moved into the Parameter Context of each lab. To change a reference solution, fix the flow in NiFi, export it (right-click the group → *Download flow definition*), and replace the file in `nifi/flows`.
