---
title: Agenda
---

## Day 1: Apache NiFi Fundamentals

### Part 1: NiFi Fundamentals

**09:00–10:00 | 1 hour**

1. Data in motion vs. data at rest
2. NiFi architecture
3. Repositories and the FlowFile lifecycle
4. Key concepts and typical use cases
5. Lab 1: the NiFi UI and a first flow

**Break: 15 minutes**

### Part 2: Building Data Flows

**10:15–11:15 | 1 hour**

1. Ingesting different types of data
2. Routing and filtering
3. Merging flows, back pressure, and prioritization
4. Flow design practices
5. Lab 2: a multi-source flow with routing

**Break: 15 minutes**

### Part 3: Record-Based Processing

**11:30–12:30 | 1 hour**

1. The Record concept
2. Readers, Writers, Schemas, and Schema Registry
3. Record processors vs. Split processors
4. JsonTreeReader: schema inference, limitations, explicit schemas

**Lunch break: 20 minutes**

### Part 4: Record-Based Processing, Continued

**12:50–14:20 | 1.5 hours**

1. Tuning Readers and Writers for performance
2. Core Record processors
3. Split processors, QueryRecord, and RecordPath
4. Lab 3: replacing a Split flow with a Record flow and comparing throughput

**Break: 10 minutes**

### Part 5: Installation, Cluster Configuration, and Setup

**14:30–16:00 | 1.5 hours**

1. NiFi configuration files
2. Single node vs. cluster
3. Cluster roles and coordination
4. Load balancing between nodes
5. Deployment options: Docker, Helm, Ansible
6. Repository locations
7. Flow versioning and change management

### Day 1 Outcome

Recap, self-check questions, and Q&A.

---

## Day 2: Data Processing and Integrations

### Part 1: Connecting to Databases and REST APIs

**09:00–10:00 | 1 hour**

1. Recap of the previous day
2. Databases (JDBC)
3. REST APIs with InvokeHTTP

**Break: 15 minutes**

### Part 2: Kafka Integration

**10:15–11:15 | 1 hour**

1. PublishKafka and ConsumeKafka in NiFi 2.x
2. Efficient Kafka integration: batching, offsets, delivery guarantees, parallelism
3. Lab 4: databases, REST, and Kafka with record processing

**Break: 15 minutes**

### Part 3: Transforming JSON and NDJSON

**11:30–12:30 | 1 hour**

1. Working efficiently with NDJSON
2. Key processors
3. Jolt: declarative JSON transformations
4. Typical transformations
5. Where to transform: inside or outside NiFi

**Lunch break: 20 minutes**

### Part 4: JSON Pipelines and Splunk Integration

**12:50–14:20 | 1.5 hours**

1. ValidateRecord, QueryRecord, UpdateRecord (RecordPath), EvaluateJsonPath, MergeRecord
2. Lab 5: an NDJSON transformation pipeline
3. Splunk integration options
4. PutSplunk configuration
5. Throughput factors, optimizations, and alternative approaches
6. Lab 6: delivering events to Splunk (or a mock)

**Break: 10 minutes**

### Part 5: Migration from NiFi 1.x to 2.x

**14:30–16:00 | 1.5 hours**

1. Key principles and differences between 1.x and 2.x
2. Compatibility of existing flows
3. Kafka migration specifics
4. Upgrade routine: in-place vs. fresh installation
5. Preparation, execution, validation, and rollback
6. Common issues and how to resolve them
7. Mini-lab: a migration checklist for your environment

### Day 2 Outcome

Recap, self-check questions, and Q&A.

---

## Day 3: Production, Security, and Performance

### Part 1: Fault Tolerance and Error Handling

**09:00–10:00 | 1 hour**

1. Recap of the previous day
2. Designing resilient flows with minimal bottlenecks
3. Failure and retry: relationships, framework-level retry, RetryFlowFile
4. Keeping the main flow healthy when failure queues fill up
5. Restarting flows efficiently with minimal manual intervention
6. Backup and restore: configuration, flow definitions, keys
7. Lab 7: a flow with retry and error handling

**Break: 15 minutes**

### Part 2: Monitoring, Provenance, and Troubleshooting

**10:15–11:15 | 1 hour**

1. NiFi UI, metrics, Provenance, and bulletins
2. Monitoring and autonomous recovery
3. Common issues and root-cause analysis
4. Recovery strategies: restart and failover
5. Flow rollback to a previous version
6. NiFi Settings: Controller Services, Reporting Tasks, Flow Analysis Rules, Registry Clients, Parameter Providers

**Break: 15 minutes**

### Part 3: Security and Governance

**11:30–12:30 | 1 hour**

1. TLS and authentication
2. Access control: users, groups, policies, roles
3. Audit trails for configuration and access changes
4. Encryption: sensitive properties and data at rest
5. Governance checklist

**Lunch break: 20 minutes**

### Part 4: Performance Tuning, Hardware, and Storage

**12:50–14:20 | 1.5 hours**

1. JVM, heap, thread pools, concurrent tasks, cluster settings
2. Scaling and hardware selection
3. Storage layout and Provenance Repository I/O load
4. Reducing flow design bottlenecks, prioritization, throughput control
5. Lab 8: performance testing and pipeline tuning

**Break: 10 minutes**

### Part 5: Final Hands-On Lab: End-to-End Pipeline

**14:30–16:00 | 1.5 hours**

1. Architecture, Controller Services, and parameters
2. Building and validating the pipeline
3. Failure testing
4. Performance check and tuning
5. Common mistakes and diagnostics

### Course Wrap-up

Key takeaways from the three days, recommended next steps for the team, final Q&A, and feedback.
