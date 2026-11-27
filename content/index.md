# Apache NiFi Workshop

Welcome to this three-day hands-on workshop on Apache NiFi. The course progresses from NiFi fundamentals, flow design and record-based processing to integrations with databases, Kafka, REST APIs and Splunk, migration from NiFi 1.x to 2.x, fault tolerance, monitoring and provenance, security and governance, and performance tuning. It ends with an end-to-end hands-on lab.

# Materials

[[intro.pdf|NobleProg]]

[[agenda|Agenda]]

<a href="/presentation/" data-router-ignore>Presentation</a>

[[materials/day-1|Day 1 — Apache NiFi Fundamentals]]

[[materials/day-2|Day 2 — Data Processing and Integrations]]

[[materials/day-3|Day 3 — Production, Security, and Performance]]

[[materials/Appendix-A|Appendix A]]

[[materials/Appendix-B|Appendix B]]

[[materials/additional/index|Additional]]

[[labs/index|Labs]]

[[solutions/index|Solutions]]

# Github repositories

<a href="https://github.com/MagnetonBora/nifi-workshop">Workshop Presentation Repository</a>

Source of this website: the workshop materials for all three days, the labs and their solutions, and the presentation.

<a href="https://github.com/MagnetonBora/nifi-playground">NiFi playground</a>

A Docker-based lab environment with Apache NiFi 2.x (a single node or a three-node cluster), NiFi Registry, PostgreSQL, Kafka, Prometheus, Grafana, Splunk and mock services for the labs. The whole environment starts with a single `make up` command.

# Additional Github repository

<a href="https://github.com/MagnetonBora/nifi-latest-dedup-processors">NiFi custom processor</a>

An example of a custom NiFi 2.x processor, `LatestRecordDeduplicator`. Within a FlowFile it removes duplicate records and keeps only the one with the latest timestamp for each key. The repository includes the Java source code built on the Record API, tests, and a demo flow that you can import into NiFi.

# Note on repository versions

The GitHub repositories above are actively maintained and may change in future editions of this workshop. To use exactly the version that was used during our sessions, check out the tag **`workshop-2026-11-norway`** in each repository.

```bash
# Clone the repositories
git clone https://github.com/MagnetonBora/nifi-workshop.git
git clone https://github.com/MagnetonBora/nifi-playground.git
git clone https://github.com/MagnetonBora/nifi-latest-dedup-processors.git

# Check out the workshop version in each repository
cd nifi-workshop && git checkout tags/workshop-2026-11-norway -b workshop-2026-11-norway && cd ..
cd nifi-playground && git checkout tags/workshop-2026-11-norway -b workshop-2026-11-norway && cd ..
cd nifi-latest-dedup-processors && git checkout tags/workshop-2026-11-norway -b workshop-2026-11-norway && cd ..
```

Alternatively, clone each repository directly at the tag:

```bash
git clone --branch workshop-2026-11-norway https://github.com/MagnetonBora/nifi-workshop.git
git clone --branch workshop-2026-11-norway https://github.com/MagnetonBora/nifi-playground.git
git clone --branch workshop-2026-11-norway https://github.com/MagnetonBora/nifi-latest-dedup-processors.git
```
