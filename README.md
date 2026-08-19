📡 RAN Behavioral Intelligence Big Data Platform

<p align="center">
  <img src="docs/assets/ran-intelligence-cover.png" alt="RAN Behavioral Intelligence Platform cover" width="100%" />
</p>

<p align="center">
  <strong>From Raw RAN Telemetry → Behavioral Intelligence → Proactive Network Operations</strong>
</p>

<p align="center">
  <a href="https://kafka.apache.org/"><img src="https://img.shields.io/badge/Apache%20Kafka-3.x-000000?style=for-the-badge&logo=apachekafka&logoColor=white" alt="Apache Kafka"></a>
  <a href="https://spark.apache.org/"><img src="https://img.shields.io/badge/Apache%20Spark-3.x-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" alt="Apache Spark"></a>
  <a href="https://hadoop.apache.org/"><img src="https://img.shields.io/badge/Hadoop-HDFS-FFCC00?style=for-the-badge&logo=apachehadoop&logoColor=black" alt="HDFS"></a>
  <a href="https://clickhouse.com/"><img src="https://img.shields.io/badge/ClickHouse-Analytics-E34F26?style=for-the-badge&logo=clickhouse&logoColor=white" alt="ClickHouse"></a>
  <a href="https://www.docker.com/"><img src="https://img.shields.io/badge/Docker-Swarm-2496ED?style=for-the-badge&logo=docker&logoColor=white" alt="Docker Swarm"></a>
  <a href="https://grafana.com/"><img src="https://img.shields.io/badge/Grafana-Observability-F46800?style=for-the-badge&logo=grafana&logoColor=white" alt="Grafana"></a>
  <a href="https://tailscale.com/"><img src="https://img.shields.io/badge/Tailscale-Mesh%20VPN-111111?style=for-the-badge&logo=tailscale&logoColor=white" alt="Tailscale"></a>
  <a href="https://www.python.org/"><img src="https://img.shields.io/badge/Python-Streaming%20%26%20ML-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python"></a>
</p>

<p align="center">
  <b>Analyze · Detect · Prioritize · Predict · Prevent · Optimize</b>
</p>

🧭 Navigate

📖 The Story

⚠️ The Problem

💡 The Solution

🏗️ Lambda Architecture

🔄 End-to-End Data Flow

📡 Telemetry Data

⚡ Kafka Ingestion

🔥 Spark Processing

🐘 HDFS Data Lake

🗄️ ClickHouse Serving Layer

🧠 Behavioral Intelligence

📊 Observability

🖥️ Cluster Topology

🧪 Fault-Tolerance Testing

📁 Repository Structure

🚀 Getting Started

🔮 Future Vision

👥 Team

📖 The Story

Modern RAN infrastructure constantly produces performance measurements.

Every base station and sector tells us something about the network:

📶 traffic and data volume

👥 active users

🔗 RRC connections

📊 resource-block utilization

📡 CQI and radio quality

⚡ radio-unit and baseband energy consumption

🧠 MIMO performance

🔄 technology-specific KPIs for 2G, 4G, and 5G

Our source data is historical, but the platform should behave as if the network is live.

So the project starts with a simple idea:

Take periodic RAN performance measurements, replay them as events, and build a distributed intelligence layer that can understand what is happening now while preserving the complete historical picture.

That idea becomes a full Big Data platform.

📡 RAN Measurements
        ↓
🐍 Telemetry Replay
        ↓
⚡ Kafka
        ↓
┌───────────────────────────────┐
│                               │
▼                               ▼
🔥 Speed Layer              🐘 Batch Layer
Spark Streaming             HDFS + Spark Batch
│                               │
└──────────────┬────────────────┘
               ▼
        🗄️ ClickHouse
               ↓
        🧠 Intelligence
               ↓
          📊 Grafana
               ↓
       👨‍💻 Network Operations

⚠️ The Problem

A telecom NOC can easily receive thousands of KPI measurements and alerts, but raw measurements do not automatically become network intelligence.

A static rule such as:

IF RB Utilization > 80%
THEN ALERT

is often not enough.

A value that is normal for one cell may be abnormal for another, and degradation usually appears through multiple changing KPIs before a hard failure occurs.

For example:

          ↓ Data Volume
          ↑ Active Users
          ↑ RB Utilization
          ↓ CQI
          ↑ Energy Consumption
               │
               ▼
       ⚠️ Possible degradation

The platform therefore focuses on a more useful question:

Is this cell behaving differently from its own normal behavior, and how urgently should an engineer investigate it?

💡 The Solution

The platform combines distributed ingestion, Lambda-style processing, historical storage, analytical serving, and behavioral analytics.

📡 Observe
   ↓
⚡ Ingest
   ↓
🐘 Preserve History
   ↓
🔥 Process in Real Time + Batch
   ↓
🧠 Detect Abnormal Behavior
   ↓
🛡️ Score Risk
   ↓
📊 Visualize
   ↓
👨‍💻 Prioritize Action

MVP focus

The current MVP focuses on:

telemetry ingestion

timestamp-based streaming simulation

distributed Kafka ingestion

raw-data preservation

real-time processing

baseline anomaly detection

analytical serving

cluster observability

high-availability testing

The long-term goal is predictive failure and degradation intelligence.

🏗️ Lambda Architecture

The architecture is designed around two processing paths that share the same telemetry source.

flowchart LR
    A["📡 RAN / PM Data"] --> B["🐍 Python Producer"]
    B --> C["⚡ Kafka\nran-telemetry"]

    C --> D["🔥 Speed Layer\nSpark Structured Streaming"]
    C --> E["🐘 Batch Layer\nHDFS Raw Data Lake"]

    E --> F["🔥 Spark Batch"]

    D --> G["🧠 Real-Time Features\nAnomaly / Risk"]
    F --> H["📚 Historical Features\nAggregations / ML"]

    G --> I["🗄️ ClickHouse"]
    H --> I

    I --> J["📊 Grafana"]
    G --> J

⚡ Speed Layer

The Speed Layer answers:

What is happening right now?

Kafka
  ↓
Spark Structured Streaming
  ↓
Parsing / Validation
  ↓
Real-Time KPIs
  ↓
Anomaly Detection
  ↓
Risk / Alerts
  ↓
ClickHouse / Grafana

🐘 Batch Layer

The Batch Layer answers:

What happened historically, and what can we learn from it?

Kafka / Raw Events
       ↓
      HDFS
       ↓
  Spark Batch
       ↓
Historical KPIs
       ↓
Features / Aggregations
       ↓
ML / Analytics
       ↓
ClickHouse

🗄️ Serving Layer

The Serving Layer turns processed data into fast, queryable outputs.

Speed Results ──┐
                ├──→ ClickHouse ──→ Grafana
Batch Results ──┘

🔄 End-to-End Data Flow

<p align="center">
  <img src="docs/assets/data-flow.png" alt="RAN Intelligence data flow" width="100%" />
</p>

The logical data flow is:

┌──────────────────────┐
│ 📡 RAN / PM Dataset  │
│ LTE / NR / GSM       │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│ 🐍 Python Producer   │
│ CSV → JSON Events    │
│ timestamp replay     │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│ ⚡ Kafka Cluster     │
│ ran-telemetry        │
│ 5 brokers            │
│ partitions + RF=3    │
└──────────┬───────────┘
           │
      ┌────┴────┐
      │         │
      ▼         ▼
┌──────────┐  ┌─────────────────┐
│ 🐘 HDFS  │  │ 🔥 Spark Stream │
│ Raw Data │  │ Real-Time       │
│ Lake     │  │ Processing      │
└────┬─────┘  └────────┬────────┘
     │                 │
     ▼                 ▼
┌──────────┐     ┌───────────────┐
│ 🔥 Spark │     │ 🧠 Anomaly /  │
│ Batch    │     │ Risk Features │
└────┬─────┘     └───────┬───────┘
     │                   │
     └─────────┬─────────┘
               ▼
        ┌──────────────┐
        │ 🗄️ ClickHouse│
        └──────┬───────┘
               ▼
        ┌──────────────┐
        │ 📊 Grafana   │
        │ NOC View     │
        └──────────────┘

📡 Telemetry Data

The source data is RAN performance telemetry organized around:

Base Station + Sector + Timestamp + KPIs

A typical row contains identity and time information plus technology-specific performance counters.

5G / NR examples

5G max active users DL

5G max active users UL

5G data volume DL

5G data volume UL

5G max RRC users

5G RB utilization

5G CQI rank 1..4

5G RRC users

5G active users UL / DL

5G MIMO rank DL

4G / LTE examples

4G max active users DL / UL

4G data volume DL / UL

4G max RRC users

4G RB utilization

4G CQI rank 1..4

4G RRC users

4G active users UL / DL

4G MIMO rank DL

2G / GSM examples

2G TS available

2G TS used

2G TS utilization

⚠️ Empty values must not automatically be converted to zero: an empty KPI can mean that the metric or technology is not applicable for that record.

🐍 Telemetry Replay Producer

The source dataset is historical, so the Python producer acts as a time-aware simulator.

CSV Files
   ↓
Normalize columns
   ↓
Parse timestamps
   ↓
Sort by time
   ↓
Group records by timestamp
   ↓
Build JSON event
   ↓
Publish to Kafka
   ↓
Wait for simulated interval
   ↓
Publish next timestamp group

If the source contains:

10:00
10:15
10:30
10:45

the producer can replay it as:

T+0s   → 10:00 event group
T+5s   → 10:15 event group
T+10s  → 10:30 event group
T+15s  → 10:45 event group

This keeps the original event timestamps while accelerating the simulation.

Kafka key

The recommended producer key is:

base_station + sector

Example:

Site36-Sector1

This gives Kafka a stable key for partition assignment and helps keep records for the same sector ordered within a partition.

⚡ Kafka Ingestion

Kafka is the distributed ingestion backbone of the platform.

Current logical configuration

Property

Value

Kafka brokers

5

Topic

ran-telemetry

Replication factor

3

min.insync.replicas target

2

Partitions

5 recommended / configurable

Note: the currently tested topic in the cluster has also been created with 10 partitions. The README treats partition count as a deployment parameter rather than hard-coding the number into application logic.

Why partitions?

Partitions provide parallelism and allow Kafka to distribute the topic across brokers.

                ran-telemetry
                     │
     ┌───────┬───────┼───────┬───────┐
     ▼       ▼       ▼       ▼       ▼
    P0      P1      P2      P3      P4 ...
     │       │       │       │       │
     └───────┴───────┴───────┴───────┘
                    │
                    ▼
             Spark Streaming

A Kafka partition is not permanently bound to one Spark worker. Spark schedules tasks for Kafka partitions across available executors.

Replication

With RF=3, a partition has three copies across brokers.

Partition 0
   ├── Broker A  ← Leader
   ├── Broker B  ← Replica
   └── Broker C  ← Replica

If the leader fails and an in-sync replica remains available, Kafka can elect a new leader.

🔥 Spark Processing

The Spark cluster is used for both streaming and batch workloads.

Structured Streaming

Kafka
  ↓
Read stream
  ↓
Parse / validate
  ↓
Transform
  ↓
Feature calculations
  ↓
Anomaly detection
  ↓
ClickHouse / alert path

Batch

HDFS
  ↓
Spark Batch
  ↓
Historical transformations
  ↓
Aggregations
  ↓
Feature engineering
  ↓
Model preparation / analytics

Parallelism

The cluster contains:

2 Spark Masters
5 Spark Workers

The masters provide the control-plane/recovery architecture, while workers provide distributed execution capacity.

🐘 HDFS Data Lake

HDFS is the durable raw-data layer.

Target HA architecture:

                 🟢 Active NameNode
                        │
                        │ shared edits
                        ▼
                🟡 Standby NameNode
                        │
                   ZKFC / ZooKeeper
                        │
            ┌───────────┼───────────┐
            ▼           ▼           ▼
           JN1         JN2         JN3
                        │
                        ▼
               DataNodes × 5

The purpose of preserving raw telemetry is simple:

If our detection logic changes tomorrow, we should be able to reprocess the past without asking the network to send the data again.

🗄️ ClickHouse Serving Layer

ClickHouse provides the low-latency analytical serving layer for processed telemetry.

It is intended for:

KPI dashboards

Cell-level comparisons

Time-series aggregation

anomaly/risk views

operational analytics

Typical query pattern:

SELECT
    base_station,
    sector,
    avg(data_volume_dl) AS avg_dl_volume,
    avg(rb_utilization) AS avg_rb_utilization
FROM telemetry_events
GROUP BY
    base_station,
    sector;

The project can use ClickHouse replication for serving resilience.

🧠 Behavioral Intelligence

The intelligence layer moves beyond simple threshold rules.

From this:

RB Utilization > 80%
        ↓
      ALERT

Toward this:

Historical Behavior
       +
Current Behavior
       +
Multiple KPIs
       ↓
Behavioral Deviation
       ↓
Anomaly
       ↓
Risk Score
       ↓
Investigation Priority

A baseline detector can use statistics such as:

mean ± 3 × standard deviation

for selected metrics.

This is an MVP detector, not a production predictive model.

🛡️ Risk Scoring

The long-term goal is to reduce alert noise by prioritizing cells.

Example:

Cell

Risk Score

Status

Site 29 / Sector 2

92

🔴 Critical

Site 36 / Sector 2

76

🟠 High

Site 148 / Sector 1

24

🟢 Normal

This turns hundreds of low-value alerts into a ranked engineering queue.

📊 Observability

Grafana provides the operational view of the platform.

Network view

active users

traffic/data volume

RB utilization

CQI

RRC users

energy consumption

anomaly count

risk score

Cluster view

Kafka broker health

partition / ISR state

Spark workers

HDFS health

ClickHouse health

CPU / memory / network utilization

Prometheus provides infrastructure/application metrics that feed the dashboards.

🖥️ Cluster Topology

The project runs across five real machines connected through a Tailscale mesh and orchestrated using Docker Swarm.

<p align="center">
  <img src="docs/assets/cluster-topology.png" alt="RAN Intelligence cluster topology" width="100%" />
</p>

Machine

Swarm role

Main responsibilities

zzzz-vm

Manager / Leader

Kafka-1, ZooKeeper-1, Spark Master-1, HDFS Active NameNode, Worker-1

abdullah

Manager

Kafka-2, ZooKeeper-2, Spark Master-2, HDFS Standby NameNode, Worker-2

sharawy-vmware-virtual-platform

Manager

Kafka-3, ZooKeeper-3, JournalNode-3, Worker-3

ahmed-vmware-virtual-platform

Worker

Kafka-4, ClickHouse-1, Worker-4

victus

Worker

Kafka-5, ClickHouse-2, Grafana, Prometheus, Airflow, Worker-5

Exact service placement is controlled with Swarm placement constraints so the distributed design is intentional rather than random.

🌐 Why two network layers?

Tailscale mesh
      ↓
Host-to-host connectivity
      ↓
Docker Swarm overlay network
      ↓
Service-to-service communication

Tailscale connects the machines.
Docker Swarm provides the application network used by containers.

🔐 High Availability Strategy

The platform demonstrates HA at multiple levels.

                  HA
                   │
      ┌────────────┼────────────┐
      ▼            ▼            ▼
    Kafka         HDFS         Spark
    RF=3        NN HA       Master recovery
      │            │            │
      └────────────┼────────────┘
                   ▼
               ClickHouse
               Replication

Kafka

5 brokers

replicated partitions

leader election

acks=all for the producer

recommended min.insync.replicas=2

HDFS

Active / Standby NameNodes

JournalNode quorum

replicated DataNodes

Spark

two masters

multiple workers

recovery-oriented deployment

ClickHouse

replicated analytical tables / replicas

Docker Swarm

3 managers

2 workers

explicit service placement

🧪 Fault-Tolerance Testing

The objective is not merely to say HA.

The objective is to demonstrate it.

Kafka HA test

Inspect the topic before failure.

/opt/kafka/bin/kafka-topics.sh \
  --describe \
  --bootstrap-server kafka1:9092 \
  --topic ran-telemetry

Record:

Leader
Replicas
ISR

Stop one Kafka broker / physical node.

Describe the topic again.

Verify that affected partitions can elect an available in-sync replica.

Continue producing telemetry and verify that the pipeline recovers.

Expected behavior

          BEFORE

Leader
  │
  ├── Replica
  └── Replica

        ↓ Broker failure

          AFTER

Failed Broker ❌
       ↓
Leader Election
       ↓
Healthy ISR Replica
       ↓
New Leader ✅

Important HA boundary

With:

RF = 3
min.insync.replicas = 2
acks = all

one broker failure can still leave two replicas in-sync.

If a failure pattern leaves fewer than the required in-sync replicas, Kafka may reject new writes rather than acknowledge data with insufficient redundancy.

That is a safety property, not a failure of HA.

🚀 Getting Started

1. Prerequisites

Install Docker on all five machines:

curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
newgrp docker

Install Tailscale:

curl -fsSL https://tailscale.com/install.sh | sh
sudo tailscale up

Verify connectivity with the machines' Tailscale addresses.

2. Create the Swarm

On the first manager:

docker swarm init \
  --advertise-addr <LEADER-TAILSCALE-IP>

Join the remaining managers/workers using the generated join commands.

Verify:

docker node ls

3. Label the Nodes

The recommended deployment uses node labels for deterministic placement.

Example:

docker node update --label-add node_id=1 zzzz-vm
docker node update --label-add node_id=2 abdullah
docker node update --label-add node_id=3 sharawy-vmware-virtual-platform
docker node update --label-add node_id=4 ahmed-vmware-virtual-platform
docker node update --label-add node_id=5 victus

4. Deploy the Stack

docker stack deploy \
  -c docker-stack.yml \
  ran-platform

Inspect services:

docker stack services ran-platform
docker stack ps ran-platform --no-trunc

5. Create the Kafka Topic

Example:

/opt/kafka/bin/kafka-topics.sh \
  --create \
  --bootstrap-server kafka1:9092 \
  --topic ran-telemetry \
  --partitions 5 \
  --replication-factor 3

If you intentionally use 10 partitions in the deployed environment, change only the partition count; the application does not depend on a fixed partition number.

Set the topic-level ISR requirement:

/opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka1:9092 \
  --entity-type topics \
  --entity-name ran-telemetry \
  --alter \
  --add-config min.insync.replicas=2

Verify:

/opt/kafka/bin/kafka-topics.sh \
  --describe \
  --bootstrap-server kafka1:9092 \
  --topic ran-telemetry

6. Run the Producer

pip install -r Kafka/producer/requirements.txt

Then:

export KAFKA_BROKERS="kafka1:9092,kafka2:9092,kafka3:9092,kafka4:9092,kafka5:9092"
export DATA_DIR="./data/Raw"
python Kafka/producer/producer.py

The producer:

Read CSV
  ↓
Normalize
  ↓
Parse timestamp
  ↓
Sort
  ↓
Group by timestamp
  ↓
Create JSON event
  ↓
Publish to Kafka
  ↓
Wait / replay next timestamp

7. Run Spark Streaming

spark-submit \
  Spark/streaming/ran_streaming.py

The application consumes ran-telemetry and feeds the real-time processing path.

📁 Repository Structure

ran-intelligence-platform/
│
├── README.md
├── docker-stack.yml
├── .env.example
│
├── data/
│   └── Raw/
│       ├── LTE/
│       ├── NR/
│       └── GSM/
│
├── Kafka/
│   └── producer/
│       ├── producer.py
│       ├── Dockerfile
│       └── requirements.txt
│
├── Spark/
│   ├── streaming/
│   │   ├── ran_streaming.py
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   │
│   ├── batch/
│   │   └── jobs/
│   │
│   └── ml/
│       └── anomaly_detection.py
│
├── configs/
│   ├── kafka/
│   ├── zookeeper/
│   ├── hadoop/
│   ├── spark/
│   ├── clickhouse/
│   └── prometheus/
│
├── monitoring/
│   └── grafana/
│       └── dashboards/
│
├── scripts/
│   ├── deploy.sh
│   ├── label-nodes.sh
│   ├── create-kafka-topic.sh
│   └── init-hdfs.sh
│
└── docs/
    └── assets/
        ├── ran-intelligence-cover.png
        ├── architecture-overview.png
        ├── data-flow.png
        └── cluster-topology.png

📈 Observability & Operations

Swarm

docker node ls
docker stack services ran-platform
docker stack ps ran-platform --no-trunc

Logs

docker service logs -f ran-platform_kafka1
docker service logs -f ran-platform_grafana
docker service logs -f ran-platform_clickhouse

Kafka

/opt/kafka/bin/kafka-topics.sh \
  --list \
  --bootstrap-server kafka1:9092

/opt/kafka/bin/kafka-topics.sh \
  --describe \
  --bootstrap-server kafka1:9092 \
  --topic ran-telemetry

🔮 Future Vision

The current platform is the foundation for a stronger predictive network-operations system.

Historical Telemetry
        +
Current Streaming Telemetry
        +
Cell Behavioral Profiles
        +
Incident History
        ↓
🧠 Machine Learning
        ↓
Failure / Degradation Prediction
        ↓
🛡️ Risk Scoring
        ↓
🚨 Early Warning
        ↓
👨‍💻 NOC Action

Potential extensions:

🔮 cell failure prediction

📈 traffic forecasting

⚡ energy optimization

🧠 root-cause analysis

📡 cell health scoring

🔄 mobility and handover intelligence

🛠️ predictive maintenance

🚨 intelligent alert routing

✅ What Success Looks Like

The final demonstration should tell one continuous story:

📡 A cell produces telemetry
        ↓
🐍 The producer replays it like a live stream
        ↓
⚡ Kafka distributes and replicates it
        ↓
🔥 Spark processes it in real time
        ↓
🐘 HDFS preserves the raw history
        ↓
🧠 Analytics identifies abnormal behavior
        ↓
🛡️ Risk scoring prioritizes the cell
        ↓
🗄️ ClickHouse serves the results
        ↓
📊 Grafana shows the NOC the story
        ↓
💡 Engineers act before the problem grows

The goal is not to build a collection of tools. The goal is to build a system that turns network telemetry into a decision.

👥 Team

Role

Core responsibility

👑 Architecture & Infrastructure

Distributed architecture, Swarm, deployment, HA design

🐘 HDFS & Storage

HDFS HA, JournalNodes, DataNodes, data-lake design

⚡ Kafka & Streaming

Kafka cluster, ingestion, producer, Spark streaming

🗄️ Data Engineering

ClickHouse, analytical serving, data modeling

📊 Monitoring & Orchestration

Grafana, Prometheus, Airflow, operational observability

⚠️ Development Status

🟡 Development / Academic Prototype

The platform is being developed as a distributed RAN intelligence demonstration.

Implemented / actively integrated

✅ Five-node Docker Swarm environment

✅ Distributed Kafka cluster

✅ Timestamp-based telemetry replay

✅ Spark streaming architecture

✅ HDFS data-lake architecture

✅ ClickHouse analytical layer

✅ Grafana / Prometheus observability

✅ Baseline anomaly detection

✅ Kafka HA testing

Still requiring production hardening

⏳ production credentials and secrets

⏳ TLS / authentication

⏳ dedicated metrics exporters

⏳ fully automated HDFS HA bootstrap

⏳ complete Spark HA automation

⏳ production-grade ML models

⏳ distributed persistent storage strategy

⏳ CI/CD and automated validation

📜 License

This project is developed for educational and engineering demonstration purposes.

<p align="center">
  <strong>📡 RAN Behavioral Intelligence</strong><br/>
  <em>From Telemetry → Intelligence → Action</em>
</p>
