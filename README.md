# Apache NiFi Workshop

A three-day hands-on workshop that takes participants from the fundamental concepts of Apache NiFi to production-oriented data flow practices.

The workshop combines conceptual explanations, configuration examples, diagrams, labs, common mistakes, and operational guidance. It is designed to show not only how to build a flow, but also how NiFi moves and stores data, integrates with external systems, and behaves in a clustered production environment. The target version is Apache NiFi 2.x; NiFi 1.x is covered where it helps explain architecture or migration.

## Who This Workshop Is For

The material is intended for:

- data engineers building ingestion and streaming pipelines;
- platform engineers operating NiFi clusters;
- teams integrating NiFi with databases, Kafka, REST APIs, and Splunk;
- teams planning a migration from NiFi 1.x to 2.x;
- technical leads evaluating data flow patterns.

Basic knowledge of JSON, SQL, and Linux is useful. Previous NiFi experience is not required.

## Learning Outcomes

By the end of the workshop, participants should be able to:

- explain the NiFi architecture, repositories, and the FlowFile lifecycle;
- build flows with routing, filtering, merging, back pressure, and prioritization;
- use record-based processing with Readers, Writers, and schemas instead of Split-based flows;
- install and configure a NiFi cluster and manage flow versions;
- integrate NiFi with databases, REST APIs, Kafka, and Splunk;
- transform JSON and NDJSON with Record processors and Jolt;
- plan and carry out a migration from NiFi 1.x to 2.x;
- apply error handling, monitoring, security, and performance tuning practices.

## Presentations

Start from the [workshop presentation index](content/presentation/index.html). It provides access to all three workshop days.

| Day | Presentation | Main Topics |
| --- | --- | --- |
| 1 | [Apache NiFi Fundamentals](content/presentation/nifi_day1/index.html) | NiFi architecture, FlowFiles, building data flows, record-based processing, installation, and cluster setup |
| 2 | [Data Processing and Integrations](content/presentation/nifi_day2/index.html) | Databases, REST APIs, Kafka, JSON/NDJSON transformations, Jolt, Splunk, and migration from 1.x to 2.x |
| 3 | [Production, Security, and Performance](content/presentation/nifi_day3/index.html) | Fault tolerance, monitoring, provenance, troubleshooting, security, governance, performance tuning, and the final lab |

## Day 1: Apache NiFi Fundamentals

Day 1 introduces NiFi as a data-in-motion platform and establishes its core vocabulary. Participants explore the NiFi architecture, the FlowFile, Content, and Provenance repositories, and the lifecycle of a FlowFile.

The day then covers building flows with routing, filtering, merging, back pressure, and prioritization, followed by record-based processing with Readers, Writers, and schemas. It ends with installation options, cluster roles, load balancing, and flow versioning.

## Day 2: Data Processing and Integrations

Day 2 focuses on connecting NiFi to external systems: databases over JDBC, REST APIs with InvokeHTTP, and Kafka with attention to batching, offsets, and delivery guarantees.

The day also covers JSON and NDJSON transformations with Record processors and Jolt, Splunk integration and throughput, and a structured approach to migrating from NiFi 1.x to 2.x.

## Day 3: Production, Security, and Performance

Day 3 moves from flow development to operational reliability. It covers error handling and retries, backup and restore, monitoring, Provenance, troubleshooting, and flow rollback.

The day then addresses TLS, authentication, access policies, audit trails, encryption, and performance tuning of the JVM, thread pools, and storage. It ends with an end-to-end hands-on lab that brings the three days together.

## Presentation Controls

The presentations are static HTML files and do not require a build step.

- Use `Right Arrow` or `Space` to move forward.
- Use `Left Arrow` to move backward.
- Swipe horizontally on a touch device.
- Select a navigation dot to open a specific slide.
- Use a URL hash such as `#s12` to link directly to a slide.

All presentations share the same visual style, navigation controller, and brand assets from `content/presentation/shared/`.

## Credits

The workshop was produced on commission for [NobleProg](https://www.nobleprog.com/).

This workshop website and its learning materials are powered by [Quartz](https://quartz.jzhao.xyz/).
