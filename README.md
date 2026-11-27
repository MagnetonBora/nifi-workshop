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

## Workshop Materials

Besides the presentations, the workshop website contains written materials, labs, and solutions:

- [Agenda](content/agenda.md): the schedule for all three days.
- Day materials: [Day 1](content/materials/day-1.md), [Day 2](content/materials/day-2.md), [Day 3](content/materials/day-3.md).
- [Appendix A](content/materials/Appendix-A.md): the lab stand, its components, addresses, test data, and reference solutions.
- [Appendix B](content/materials/Appendix-B.md): the sources used to confirm facts in the materials.
- [Labs](content/labs/index.md) and [Solutions](content/solutions/index.md).

## Labs

The hands-on part has nine labs and a final end-to-end lab. Every lab has a task, a solution with a component-by-component explanation, and a reference Flow Definition. All labs are verified on Apache NiFi 2.12.0.

| Lab | Day | Topic |
| --- | --- | --- |
| [Lab 1](content/labs/lab-1.md) | 1 | The NiFi UI and a first flow |
| [Lab 2](content/labs/lab-2.md) | 1 | Several sources and routing |
| [Lab 3](content/labs/lab-3.md) | 1 | A Split flow versus a Record flow |
| [Lab 4](content/labs/lab-4.md) | 2 | Connecting NiFi to a database |
| [Lab 5](content/labs/lab-5.md) | 2 | A database + REST + Kafka with record processing |
| [Lab 6](content/labs/lab-6.md) | 2 | An NDJSON transformation pipeline |
| [Lab 7](content/labs/lab-7.md) | 2 | Delivering events to Splunk |
| [Lab 8](content/labs/lab-8.md) | 3 | Retry and error handling |
| [Lab 9](content/labs/lab-9.md) | 3 | A performance test and tuning |
| [Final lab](content/labs/final-lab.md) | 3 | An end-to-end pipeline: Kafka → validation and transformation → Splunk → DLQ |

The labs run on the [NiFi playground](https://github.com/MagnetonBora/nifi-playground) stand: NiFi (a single node or a cluster), NiFi Registry, PostgreSQL, Kafka, Prometheus and Grafana, Splunk, and mocks of external systems. It needs Docker with at least 8 GB of memory and `make`:

```bash
git clone https://github.com/MagnetonBora/nifi-playground.git
cd nifi-playground
make up          # NiFi and the whole environment
make lab-setup   # the table and topics for the labs
make urls        # addresses and logins
```

## Running the Website in Docker with make

The website is built with Quartz and runs in a Docker container. The `Makefile` wraps all Docker commands.

### Prerequisites

- Docker (Docker Desktop or Docker Engine) running;
- `make` (preinstalled on macOS and most Linux distributions).

### Step by Step

1. Clone the repository and go to its directory:

   ```bash
   git clone https://github.com/MagnetonBora/nifi-workshop.git
   cd nifi-workshop
   ```

2. Build the Docker image (named `nifi-workshop` by default). The first build installs npm dependencies and Quartz plugins and takes a few minutes:

   ```bash
   make build
   ```

3. Run the container in the background:

   ```bash
   make up
   ```

4. Open <http://localhost:8081> in a browser. Quartz builds the site on start, so the first page may take a few seconds to appear. To watch the build progress, run `make logs` (press `Ctrl+C` to stop following).

5. Stop the container when you are done:

   ```bash
   make stop
   ```

### Editing the Content

The `content/` directory is mounted into the container, so changes to Markdown files are picked up and the page reloads without rebuilding the image. Port `3001` is used by Quartz for live reload. After changes to `package.json`, `quartz/`, or the Quartz configuration, rebuild the image:

```bash
make rebuild
```

### All make Targets

| Command | Description |
| --- | --- |
| `make help` | Show the list of targets |
| `make build` | Build the Docker image |
| `make up` | Run the container in the background |
| `make rebuild` | Stop, rebuild, and run the container |
| `make restart` | Restart the existing container |
| `make stop` | Stop and remove the container |
| `make logs` | Follow container logs |
| `make shell` | Open a shell in the running container |
| `make clean` | Remove the container and image |

### Configuration

The defaults can be overridden with variables:

| Variable | Default | Description |
| --- | --- | --- |
| `PORT` | `8081` | Host port for the website |
| `WS_PORT` | `3001` | Host port for live reload |
| `IMAGE` | `nifi-workshop` | Docker image name |
| `CONTAINER` | `nifi-workshop` | Container name |

For example, if port `8081` is busy:

```bash
make up PORT=8090 WS_PORT=3002
```

### Running without Docker

Use Node.js 22 or later:

```bash
npm install
npx quartz plugin install
npx quartz build --serve
```

## Repository Structure

```text
content/
├── index.md          # website home page
├── agenda.md         # workshop schedule
├── intro.pdf         # NobleProg introduction
├── materials/        # day materials and appendices
├── labs/             # lab tasks
├── solutions/        # lab solutions
├── images/           # images used in the materials
└── presentation/     # static HTML presentations for the three days
quartz/               # Quartz framework sources
quartz.ts, quartz.config.default.yaml   # Quartz configuration
Dockerfile, Makefile  # local run in Docker
```

## Presentation Controls

The presentations are static HTML files and do not require a build step.

- Use `Right Arrow` or `Space` to move forward.
- Use `Left Arrow` to move backward.
- Swipe horizontally on a touch device.
- Select a navigation dot to open a specific slide.
- Use a URL hash such as `#s12` to link directly to a slide.

All presentations share the same visual style, navigation controller, and brand assets from `content/presentation/shared/`.

## Versions and tags

This repository keeps evolving, and future editions of the workshop may change it. Every delivery of the workshop is marked with a Git tag on GitHub, so you can always get exactly the version that was used during your course.

| Tag | Course | Dates |
|---|---|---|
| [`workshop-2026-11-norway`](https://github.com/MagnetonBora/nifi-workshop/tree/workshop-2026-11-norway) | Apache NiFi workshop, Norway | November 23–25, 2026 |

To clone the repository at a specific tag:

```bash
git clone --branch workshop-2026-11-norway https://github.com/MagnetonBora/nifi-workshop.git
```

If you already have a clone:

```bash
git fetch --tags
git checkout tags/workshop-2026-11-norway -b workshop-2026-11-norway
```

## Credits

The workshop was produced on commission for [NobleProg](https://www.nobleprog.com/).

This workshop website and its learning materials are powered by [Quartz](https://quartz.jzhao.xyz/).
