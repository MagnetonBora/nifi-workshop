---
title: "NiFi 2.x Migration and Deployment Guide"
---
## Introduction

This is a practical guide to moving to Apache NiFi 2.x and preparing a production deployment. It focuses on the topics covered in the workshop: JDBC, `PutDatabaseRecord`, Controller Services, Site-to-Site, Reporting Tasks, security, monitoring, and operations.

It covers two scenarios: installing NiFi 2.x from scratch and moving an existing flow from NiFi 1.x. It is not only about replacing binaries — it also describes a target deployment architecture.

## 1. Decisions to Make Before Migration

Fix the following first:

| Area | What to Decide |
|---|---|
| **NiFi version** | The exact 2.x version |
| **Runtime** | Bare metal, Docker, or Kubernetes |
| **Java** | The JDK version supported by that release (NiFi 2.x requires Java 21) |
| **Database** | PostgreSQL, MySQL, SQL Server, etc. |
| **Topology** | Standalone or cluster |
| **Security** | HTTPS, authentication, authorization |
| **Monitoring** | Prometheus or Site-to-Site |
| **Storage** | Separate disks for the repositories, encryption at rest |
| **Deployment** | Manual installation or CI/CD |

**Recommendation:** do not start by moving the existing flows. Prepare and verify the infrastructure first, then move the flows.

## 2. Migrating from NiFi 1.x to NiFi 2.x

This is the most important stage when you upgrade an existing installation.

### 2.1. Upgrade Path

The official Migration Guidance requires upgrading to **1.27.0** first and only then moving to 2.0.0.

```
NiFi 1.x
   |
   v
NiFi 1.27.0
   |
   v
NiFi 2.0.0
   |
   v
Target NiFi 2.x release
```

Before each step, read the Migration Guidance for the target version: it lists behavior changes, removed components, and incompatibilities.

### 2.2. What to Check in Existing Flows

| Area | What to Check |
|---|---|
| **Processors** | Removed and renamed components |
| **Controller Services** | Compatibility and new implementations |
| **Reporting Tasks** | Whether the tasks you use still exist |
| **Custom NARs** | Compatibility with NiFi 2.x |
| **JOLT** | Compatibility of custom transformation classes |
| **Scripts** | Compatibility with the newer Java and API |
| **Parameters** | Sensitive properties and Parameter Contexts |
| **Registry** | Compatibility of the flow versioning mechanism |

Some specific changes:
- `DistributedMapCacheServer` is renamed to `MapCacheServer`.
- `DistributedSetCacheServer` is renamed to `SetCacheServer`.
- `DistributedMapCacheClientService` is renamed to `MapCacheClientService`.
- `JoltTransformJSON` and `JoltTransformRecord` use an updated JOLT library; custom transformation classes from external JARs must be rebuilt.
- In NiFi 2.8 the NiFi Registry project is deprecated; for flow versioning, consider Git-based Flow Registry Clients.

Full list: [Apache NiFi Migration Guidance](https://cwiki.apache.org/confluence/spaces/NIFI/pages/57905503/Migration%2BGuidance).

### 2.3. Repositories and Encryption

**A critical change in NiFi 2.0:** built-in repository encryption is removed for:
- the FlowFile Repository;
- the Content Repository;
- the Provenance Repository;
- the FlowFile Swap Manager.

This is not just a configuration change — the implementation itself was removed from NiFi. In NiFi 2.x this data must be protected by the infrastructure:
- LUKS / dm-crypt;
- cloud disk encryption;
- encrypted persistent volumes;
- restricted access to the OS and the backup storage.

Do not confuse repository encryption with the encryption of **sensitive properties** — the latter still exists.

**Encrypting individual FlowFiles:**

| Old Approach | New Approach |
|---|---|
| `EncryptContent` | `EncryptContentAge` / `EncryptContentPGP` |
| `DecryptContent` | `DecryptContentAge` / `DecryptContentPGP` |
| `DecryptContentCompatibility` | Migrate old data according to its encryption format |

The old `EncryptContent` / `DecryptContent` family is removed in favor of the dedicated Age and PGP processors.

**Important:** if you already have encrypted data, check in advance that the new components can decrypt it. Do not delete old repositories or switch encryption settings without a tested recovery plan.

## 3. Installing NiFi 2.x

### 3.1. Runtime

1. Choose a specific NiFi version.
2. Check the Java requirements of that version.
3. Download the binary distribution.
4. Verify the checksum or OpenPGP signature.
5. Create a dedicated OS user `nifi`.
6. Prepare directories for configuration, logs, and repositories.
7. Start NiFi and check that the UI is available.

Official sources:
- [Apache NiFi Downloads](https://nifi.apache.org/download/)
- [System Administrator's Guide](https://nifi.apache.org/nifi-docs/administration-guide.html)

Do not use `latest` in production — pin a specific version.

### 3.2. Repository Layout

Separate the data by purpose, for example:

```
/opt/nifi/
    bin/
    conf/
    lib/
    drivers/

/var/lib/nifi/
    flowfile_repository/
    content_repository/
    provenance_repository/

/var/log/nifi/
```

This is an example, not a required layout.

In production, use separate disks or volumes for the repositories, especially the Content and Provenance Repositories:
- I/O isolation;
- independent capacity management;
- less competition for disk bandwidth;
- separate control of backup and retention.

Do not put the repositories on slow network storage without checking its compatibility and performance.

### 3.3. Configuration Files

| File | Purpose |
|---|---|
| `nifi.properties` | Main NiFi configuration |
| `bootstrap.conf` | JVM and startup parameters |
| `authorizers.xml` | Authorization |
| `login-identity-providers.xml` | Authentication |
| `state-management.xml` | State management |
| `logback.xml` | Logging |
| `zookeeper.properties` | ZooKeeper, if used |

Do not edit the configuration directly on running production instances without a controlled deployment process.

## 4. JDBC Drivers and Databases

### 4.1. Organizing JDBC Drivers

NiFi has no built-in package manager for JDBC drivers (nothing like `pip` or `npm`). A practical approach:
1. Choose the driver version.
2. Download the JAR from the official source or an internal artifact repository.
3. Verify its checksum and origin.
4. Put the JAR into a dedicated directory.
5. Point `DBCPConnectionPool` to it.

```
/opt/nifi/drivers/
    postgresql-42.7.8.jar
    mysql-connector-j-9.4.0.jar
```

For PostgreSQL:
```bash
mkdir -p /opt/nifi/drivers

curl -L \
  https://jdbc.postgresql.org/download/postgresql-42.7.8.jar \
  -o /opt/nifi/drivers/postgresql-42.7.8.jar
```

`DBCPConnectionPool` settings:

| Property | Value |
|---|---|
| **Database Driver Class Name** | `org.postgresql.Driver` |
| **Database Driver Locations** | `/opt/nifi/drivers/postgresql-42.7.8.jar` |
| **Database Connection URL** | `jdbc:postgresql://db-host:5432/mydb` |
| **Database User** | Database user |
| **Database Password** | Password |

*Database Driver Locations* is optional if the driver is already on the classpath, but a dedicated directory with explicit versions is easier to manage.

**Docker and cluster:**
- In Docker, the JAR must be available inside the container.
- In Kubernetes, bake it into your own image or mount it as a volume.
- In a NiFi cluster, the driver must be available on every node at the same path.
- All nodes must use the same driver version.

For production, build the JDBC drivers into your own Docker image and version them together with the NiFi image.

### 4.2. Controller Services

Do not create a new connection pool for every processor. One `DBCPConnectionPool` can serve many processors:

```
DBCPConnectionPool "Postgres Production"
   ├── PutDatabaseRecord
   ├── ExecuteSQL
   ├── QueryDatabaseTable
   └── other DB processors
```

Create a separate Controller Service for each database.

`DBCPConnectionPoolLookup` is needed only when one of several connection pools must be selected dynamically, for example by a FlowFile attribute. For a single database it is unnecessary.

### 4.3. PutDatabaseRecord

Use `PutDatabaseRecord` to load record data into a relational database.

| Property | Value |
|---|---|
| **Record Reader** | `JsonTreeReader` / `CSVReader` / `AvroReader` |
| **Database Connection Pooling Service** | `DBCPConnectionPool` |
| **Statement Type** | `INSERT` |
| **Table Name** | Target table |
| **Maximum Batch Size** | Tuned to the load |

```
ConsumeKafka
     |
     v
ConvertRecord
     |
     v
PutDatabaseRecord
     |
     +---- success
     +---- retry
     +---- failure
```

The processor reads records through the Record Reader, builds SQL statements, and executes them in a transaction. Plan separately for:
- idempotency;
- primary keys;
- reprocessing;
- behavior on partial errors;
- batch size;
- a DLQ.

`failure` and `retry` are different scenarios: a temporarily unavailable database needs a retry, invalid data needs a quarantine / DLQ.

## 5. Security

Treat security as three independent levels.

### 5.1. Data at Rest

| What to Protect | Mechanism |
|---|---|
| **FlowFile Repository** | Disk / volume encryption |
| **Content Repository** | Disk / volume encryption |
| **Provenance Repository** | Disk / volume encryption |
| **Backups** | Backup storage encryption |
| **Sensitive properties** | Built-in NiFi encryption |

Remember `nifi.sensitive.props.key` and `nifi.sensitive.props.algorithm`. Store the key separately from the configuration backup: if the key is lost, NiFi cannot decrypt the stored sensitive properties.

### 5.2. Data in Motion

Use TLS on every external and internal connection:

| Boundary | Protection |
|---|---|
| **Browser → NiFi UI** | HTTPS |
| **REST API** | HTTPS |
| **Between cluster nodes** | TLS / mutual TLS |
| **Site-to-Site** | Secure Site-to-Site |
| **NiFi → PostgreSQL** | JDBC TLS |
| **NiFi → Kafka** | TLS |
| **NiFi → HTTP services** | HTTPS |

For PostgreSQL, consider `sslmode=verify-full` so that both the server certificate and the hostname are verified.

In a cluster, DNS hostnames and TLS endpoint identification must be configured correctly. NiFi 2.12.0 in particular has a related requirement for `nifi.cluster.load.balance.host` and `nifi.cluster.node.address`.

### 5.3. PII and Data Access

- Keep PII out of FlowFile attributes as much as possible.
- Mask data before logging.
- Restrict the *view the data* permission.
- Do not log full payloads with `LogAttribute` or `LogMessage`.
- Restrict access to provenance.
- Do not store credentials directly in scripts or in plain-text flow configuration.

Remember that provenance can contain FlowFile attributes and give access to the original content.

## 6. Site-to-Site Between NiFi Instances

Site-to-Site transfers FlowFiles between NiFi instances:

```
NiFi A                                   NiFi B
Remote Process Group  ── Secure S2S ──►  Input Port ──► Process Group
                      (TLS / authN / authZ)
```

Setup:
1. Configure Secure Site-to-Site on the receiving NiFi.
2. Create an Input Port.
3. Configure the required access policies.
4. On the sending NiFi, create a Remote Process Group.
5. Enter the URL of the receiving NiFi.
6. Select the Input Port.
7. Check that FlowFiles are transferred.

In a cluster, also check that all nodes are reachable and the network configuration is correct.

### SiteToSiteStatusReportingTask

This is a **Reporting Task**, not a processor. It sends status events about NiFi components over Site-to-Site to another NiFi instance:

```
NiFi A
  |
  +-- SiteToSiteStatusReportingTask
              |
              | Site-to-Site
              v
          NiFi B
              |
              v
          Input Port
              |
              v
       Monitoring pipeline
```

Configured in **NiFi Settings → Reporting Tasks → +**. Main properties:
- Destination URL;
- Input Port Name;
- Transport Protocol;
- Component Type Filter Regex;
- Component Name Filter Regex;
- Batch Size.

It is useful for collecting the state of several NiFi installations in one place. Note that it sends **status events**, not regular business FlowFiles.

## 7. Monitoring and Alerting

Split monitoring into three areas.

### 7.1. Metrics

Prometheus collects NiFi metrics over HTTP from the endpoint `/nifi-api/flow/metrics/prometheus`. Check the authentication requirements in the documentation of your version.

Typical metrics:
- JVM heap / GC;
- CPU;
- active threads;
- FlowFiles in queues;
- queue sizes;
- incoming and outgoing FlowFiles;
- component state;
- cluster node status.

```
NiFi ──► Prometheus ──► Grafana
```

Metrics are not logs and not a history of individual FlowFiles.

### 7.2. Logs

Ship the logs to a central system (Splunk, OpenSearch, Elasticsearch, Google Cloud Logging):
- `nifi-app.log`;
- `nifi-bootstrap.log`;
- `nifi-user.log`;
- audit logs, if enabled.

Watch especially for:
- Controller Service errors;
- database connection errors;
- Site-to-Site errors;
- component stops and restarts;
- authentication / authorization failures.

### 7.3. Alerts

Example: PostgreSQL is temporarily unavailable, and `PutDatabaseRecord` routes FlowFiles to `success`, `retry`, or `failure`.

| Situation | Action |
|---|---|
| **DB timeout** | Retry |
| **Connection refused** | Retry + alert when retries are exhausted |
| **Invalid SQL** | DLQ |
| **Invalid data** | DLQ |
| **Authentication failure** | Alert the operator |
| **Repeated errors** | Quarantine + alert |

External notifications can go to Slack, Teams, email, or PagerDuty. For HTTP integrations, for example, route `401` and `403` to a separate `RouteOnAttribute` branch that sends a notification.

**Important:** a bulletin is a notification inside the NiFi UI, not a full external alerting system.

## 8. Reference Production Architecture

```
            Load Balancer / DNS / TLS / Identity Provider
                     |                    |
              NiFi Node 1           NiFi Node 2
                     |                    |
     +---------------+--------------------+---------------+
     |                                                    |
 Integrations                                       Observability
   PostgreSQL (JDBC / TLS)                            Prometheus (metrics)
   Kafka (TLS)                                        Splunk (logs)
                                                      Grafana / alerting
```

This is a conceptual diagram. A real cluster also needs consistent settings for cluster coordination, repositories, network interfaces, and state management.

## 9. Deployment Checklist

Use it as acceptance criteria before the production rollout.

**Infrastructure**
- [ ] NiFi and Java versions are pinned
- [ ] OS user and permissions are prepared
- [ ] Disks / volumes for the repositories are allocated
- [ ] Backup and restore are configured
- [ ] The deployment model is chosen (Docker / VM / Kubernetes)

**Migration**
- [ ] Deprecated and removed components are checked
- [ ] Custom NARs and scripts are checked
- [ ] JOLT transformations are checked
- [ ] Controller Services are checked
- [ ] Parameter Contexts and sensitive properties are checked
- [ ] Old encrypted payloads can still be decrypted

**Database**
- [ ] JDBC drivers are prepared
- [ ] Driver versions are pinned
- [ ] `DBCPConnectionPool` is configured
- [ ] TLS to the database is verified
- [ ] `PutDatabaseRecord` is tested on `success` / `retry` / `failure`
- [ ] Idempotency and the DLQ are verified

**Security**
- [ ] HTTPS for the UI / API
- [ ] Authentication and authorization
- [ ] TLS for the cluster and Site-to-Site
- [ ] Disk and backup encryption
- [ ] Sensitive properties are checked
- [ ] Access to PII and provenance is restricted

**Monitoring**
- [ ] Prometheus metrics are collected
- [ ] Logging is centralized
- [ ] External alerts are configured
- [ ] Reporting Tasks are checked
- [ ] Failure and recovery scenarios are tested

**Go-live**
- [ ] A test deployment is done
- [ ] The rollback plan is tested
- [ ] Restore from backup is tested
- [ ] Behavior after a restart is verified
- [ ] A load test is done
- [ ] Operations documentation is ready

## 10. Rollout Order

Do not try to do everything at once.

| Stage | Result |
|---|---|
| **1. Runtime** | NiFi is running, the UI is available |
| **2. Storage** | Repositories and backup are ready |
| **3. Security** | HTTPS, authentication, authorization |
| **4. JDBC** | Drivers, connection pools, database connectivity |
| **5. Dataflows** | Flows are imported and tested |
| **6. Site-to-Site** | Transfer between NiFi instances works |
| **7. Monitoring** | Metrics, logs, alerts |
| **8. Production test** | Load and recovery tests |

## Brief Recap
- Prepare and verify the infrastructure first, then move the flows.
- Upgrade path: 1.x → 1.27.0 → 2.0.0 → target 2.x; read the Migration Guidance for every step.
- Repository encryption is gone in 2.x: use disk / volume encryption; sensitive property encryption remains.
- `EncryptContent` / `DecryptContent` are replaced by the Age and PGP processors.
- JDBC drivers: pinned versions, one path on every node, ideally built into the image; one `DBCPConnectionPool` per database.
- `PutDatabaseRecord`: `retry` for temporary database problems, DLQ for bad data.
- TLS on every boundary; restrict PII, *view the data*, and provenance.
- Monitoring = metrics (Prometheus endpoint) + central logs + external alerts; bulletins are not alerting.
