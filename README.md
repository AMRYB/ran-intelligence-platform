# 📡 RAN Behavioral Intelligence & Predictive Network Operations Platform

<p align="center">

  <img
    src="https://commons.wikimedia.org/wiki/Special:Redirect/file/5G%20cell%20tower.JPG"
    alt="5G Cell Tower"
    width="100%"
  />

</p>

<h3 align="center">
From Raw RAN Telemetry → Real-Time Intelligence → Proactive Network Operations
</h3>

<p align="center">

[![Kafka](https://img.shields.io/badge/Apache%20Kafka-3.x-black?style=for-the-badge&logo=apachekafka)](https://kafka.apache.org/)
[![Spark](https://img.shields.io/badge/Apache%20Spark-3.x-orange?style=for-the-badge&logo=apachespark)](https://spark.apache.org/)
[![HDFS](https://img.shields.io/badge/Hadoop-HDFS-yellow?style=for-the-badge&logo=apachehadoop)](https://hadoop.apache.org/)
[![ClickHouse](https://img.shields.io/badge/ClickHouse-Analytics-E34F26?style=for-the-badge&logo=clickhouse)](https://clickhouse.com/)
[![Docker](https://img.shields.io/badge/Docker-Swarm-2496ED?style=for-the-badge&logo=docker)](https://www.docker.com/)
[![Airflow](https://img.shields.io/badge/Airflow-Orchestration-017CEE?style=for-the-badge&logo=apacheairflow)](https://airflow.apache.org/)
[![Grafana](https://img.shields.io/badge/Grafana-Monitoring-F46800?style=for-the-badge&logo=grafana)](https://grafana.com/)
[![Python](https://img.shields.io/badge/Python-ML%20%26%20Streaming-3776AB?style=for-the-badge&logo=python)](https://www.python.org/)
[![Tailscale](https://img.shields.io/badge/Tailscale-Mesh%20VPN-black?style=for-the-badge&logo=tailscale)](https://tailscale.com/)

</p>

<p align="center">

**Analyze · Detect · Prioritize · Predict · Prevent**

</p>

---

## 🧭 Navigate

**[📖 Story](#-the-story)** •
**[⚠️ Problem](#️-the-problem)** •
**[💡 Solution](#-our-solution)** •
**[🏗️ Architecture](#️-architecture)** •
**[📡 Data Flow](#-end-to-end-data-flow)** •
**[🖥️ Cluster](#️-cluster-topology)** •
**[📊 Data Model](#-telemetry-data)** •
**[🚀 Deployment](#-getting-started)** •
**[🧪 HA Testing](#-fault-tolerance-testing)**

---

# 📖 The Story

Modern cellular networks generate enormous amounts of telemetry.

Every cell and sector continuously reports measurements such as:

- 📶 Data volume
- 👥 Active users
- 🔗 RRC users
- 📊 Resource Block utilization
- 📡 CQI
- ⚡ Radio Unit energy consumption
- 🧠 Baseband energy consumption
- 📈 MIMO performance

Our dataset represents this behavior as **periodic RAN performance measurements**.

Each cell/sector reports its performance approximately every **15 minutes**.

The problem is not collecting the data.

The problem is answering:

> **"What is happening inside the network, and which cells require attention?"**

---

# ⚠️ The Problem

Traditional network monitoring often depends heavily on fixed thresholds.

For example:

```text
IF RB Utilization > 80%
        ↓
     ALERT
But real networks are more complicated.
A KPI that is normal for one cell may be abnormal for another.
A cell may also show degradation through a combination of signals:
↓ Data Volume
+ ↑ Active Users
+ ↑ RB Utilization
+ ↓ CQI
        ↓
Potential Cell Degradation
Therefore, we need more than individual threshold alerts.
We need a platform that can:
Observe
   ↓
Understand
   ↓
Detect
   ↓
Prioritize
   ↓
Predict
   ↓
Act
💡 Our Solution
The RAN Behavioral Intelligence Platform is a distributed Big Data platform designed to transform raw RAN telemetry into actionable network intelligence.
The platform provides:
📡 Data Ingestion
Read historical RAN CSV datasets and simulate a real telemetry source.
The producer does not simply send the whole CSV immediately.
Instead:
CSV
 ↓
Parse Timestamp
 ↓
Group Records by Timestamp
 ↓
Wait / Simulate Time
 ↓
Publish Event
 ↓
Kafka
This allows historical data to behave like a real streaming source.
⚡ Real-Time Streaming
Telemetry events are published to a replicated Kafka cluster.
RAN Producer
     │
     ▼
┌───────────────────────┐
│   Kafka Cluster       │
│                       │
│ Broker 1              │
│ Broker 2              │
│ Broker 3              │
│ Broker 4              │
│ Broker 5              │
└───────────────────────┘
The topic used by the platform is:
ran-telemetry
Current target configuration:
Partitions       = 5
Replication      = 3
Brokers          = 5
🏗️ Architecture
High-Level Architecture
Mermaid
flowchart LR

    A["📡 RAN / PM Data<br/>LTE · NR · GSM"]

    B["🐍 Python Producer<br/>CSV → Events"]

    C["⚡ Apache Kafka<br/>5 Brokers<br/>5 Partitions<br/>RF = 3"]

    D["🐘 HDFS<br/>Raw Data Lake<br/>Replication = 3"]

    E["🔥 Spark Structured Streaming<br/>Real-Time Processing"]

    F["🔥 Spark Batch<br/>Historical Processing"]

    G["🗄️ ClickHouse<br/>Analytical Store"]

    H["🧠 ML / Behavioral Analytics<br/>Anomaly Detection"]

    I["🛡️ Risk Scoring<br/>Cell Prioritization"]

    J["📊 Grafana<br/>NOC Dashboard"]

    A --> B
    B --> C

    C --> D
    C --> E

    D --> F
    F --> G

    E --> G
    E --> H

    G --> H
    H --> I

    I --> J
    G --> J
🔄 End-to-End Data Flow
The complete journey of a telemetry record looks like this:
┌───────────────┐
│ 📡 RAN DATA   │
│               │
│ Cell / Sector │
│ KPI Metrics   │
└───────┬───────┘
        │
        │ Every ~15 minutes
        ▼
┌────────────────────┐
│ 🐍 Python Producer │
│                    │
│ CSV → Event        │
│ Timestamp grouping │
└────────┬───────────┘
         │
         ▼
┌────────────────────────────┐
│ ⚡ Apache Kafka             │
│                            │
│ ran-telemetry              │
│ 5 Partitions               │
│ Replication Factor = 3     │
└─────────────┬──────────────┘
              │
        ┌─────┴──────────────┐
        │                    │
        ▼                    ▼
┌───────────────┐    ┌──────────────────┐
│ 🐘 HDFS       │    │ 🔥 Spark Stream  │
│               │    │                  │
│ Raw / Durable │    │ Real-Time KPIs   │
└───────┬───────┘    └────────┬─────────┘
        │                     │
        ▼                     ▼
┌───────────────┐    ┌──────────────────┐
│ 🔥 Spark      │    │ 🧠 ML / Features │
│ Batch         │    │                  │
└───────┬───────┘    └────────┬─────────┘
        │                     │
        └──────────┬──────────┘
                   ▼
          ┌───────────────────┐
          │ 🗄️ ClickHouse     │
          │                   │
          │ Analytical Store  │
          └─────────┬─────────┘
                    │
                    ▼
          ┌───────────────────┐
          │ 🛡️ Risk Scoring   │
          │                   │
          │ Cell Priorities   │
          └─────────┬─────────┘
                    │
                    ▼
          ┌───────────────────┐
          │ 📊 Grafana        │
          │                   │
          │ NOC Dashboard     │
          └───────────────────┘
📡 Telemetry Data
The platform works with RAN-style performance measurements.
A typical record contains:
Category
Example Metrics
🏢 Identity
Base Station, Sector
🕒 Time
Timestamp
⚡ Energy
Radio Unit Energy, Baseband Energy
👥 Users
Active Users DL/UL
🔗 Connections
RRC Users
📊 Capacity
Data Volume DL/UL
📶 Radio Quality
CQI Rank 1–4
📈 Utilization
RB Utilization
🧠 Performance
MIMO Rank
The dataset contains measurements for multiple radio technologies:
2G / GSM
   │
   ├── TS Available
   ├── TS Used
   └── TS Utilization

4G / LTE
   │
   ├── Active Users
   ├── Data Volume
   ├── RRC Users
   ├── RB Utilization
   └── CQI

5G / NR
   │
   ├── Active Users
   ├── Data Volume
   ├── RRC Users
   ├── RB Utilization
   ├── CQI
   └── MIMO
🐍 Telemetry Simulation
The producer converts historical CSV measurements into simulated streaming events.
                 CSV Dataset
                     │
                     ▼
              ┌─────────────┐
              │ Read CSV    │
              └──────┬──────┘
                     │
                     ▼
              Normalize Fields
                     │
                     ▼
              Parse Timestamp
                     │
                     ▼
          Group by Timestamp
                     │
                     ▼
          ┌──────────────────┐
          │ Event Generator  │
          └────────┬─────────┘
                   │
             simulated delay
                   │
                   ▼
             Kafka Producer
                   │
                   ▼
              ran-telemetry
Example:
{
  "timestamp": "2023-10-08T06:00:00",
  "records": [
    {
      "base_station": "Site 36",
      "sector": 1,
      "network_type": "5G",
      "data_volume_dl": 4.54,
      "active_users_dl": 1
    }
  ]
}
⚡ Kafka Architecture
The Kafka layer contains five brokers distributed across the physical machines.
Mermaid
flowchart TB

    P["🐍 Python Producer"]

    T["ran-telemetry"]

    K1["Kafka Broker 1"]
    K2["Kafka Broker 2"]
    K3["Kafka Broker 3"]
    K4["Kafka Broker 4"]
    K5["Kafka Broker 5"]

    P --> T

    T --> K1
    T --> K2
    T --> K3
    T --> K4
    T --> K5
Topic Configuration
Topic:
ran-telemetry

Partitions:
5

Replication Factor:
3
Each partition has three replicas:
Partition 0
    ├── Broker 1
    ├── Broker 3
    └── Broker 5

Partition 1
    ├── Broker 2
    ├── Broker 4
    └── Broker 1

Partition 2
    ├── Broker 3
    ├── Broker 5
    └── Broker 2

...
This allows Kafka to continue serving data if a broker fails, assuming enough replicas remain in-sync.
🔥 Spark Processing
Spark is responsible for both streaming and historical processing.
Streaming
Kafka
  │
  ▼
Spark Structured Streaming
  │
  ├── Parse JSON
  ├── Validate records
  ├── Transform KPIs
  ├── Calculate features
  └── Detect anomalies
          │
          ▼
      ClickHouse
Batch
Historical data stored in HDFS can be reprocessed:
HDFS
 │
 ▼
Spark Batch
 │
 ├── Historical aggregation
 ├── Cell profiling
 ├── Feature engineering
 └── Model preparation
 │
 ▼
ClickHouse / ML
🧠 Behavioral Intelligence
The long-term objective is to understand how each cell normally behaves.
Instead of asking:
"Is KPI > threshold?"
the platform asks:
"Is this behavior unusual for THIS cell?"
For example:
Cell 36
────────────────────────

Normal:
RB Utilization      20–40%
Active Users        5–15
CQI                  8–12

Current:
RB Utilization      78%
Active Users        18
CQI                  4

                ↓

       ⚠️ Behavioral Anomaly
Multiple anomalies can then contribute to a cell risk score.
Anomaly Detection
        │
        ▼
Feature Engineering
        │
        ▼
Risk Scoring
        │
        ▼
Cell Priority
🛡️ Risk Scoring
Instead of giving engineers hundreds of independent alerts:
Cell 36 → ⚠️
Cell 29 → 🔴
Cell 148 → 🟢
The platform can rank cells by risk.
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
This transforms monitoring from:
Alert everything
into:
Investigate what matters first.
🖥️ Cluster Topology
The platform runs across five physical machines connected through a Tailscale mesh and orchestrated using Docker Swarm.
Mermaid
flowchart TB

    subgraph M1["🖥️ zzzz-vm<br/>Manager • Leader"]
        K1["Kafka-1"]
        Z1["ZooKeeper-1"]
        S1["Spark Master-1"]
        NN1["NameNode Active"]
        W1["Spark Worker-1"]
    end

    subgraph M2["🖥️ abdullah<br/>Manager"]
        K2["Kafka-2"]
        Z2["ZooKeeper-2"]
        S2["Spark Master-2"]
        NN2["NameNode Standby"]
        W2["Spark Worker-2"]
    end

    subgraph M3["🖥️ sharawy-vmware-virtual-platform<br/>Manager"]
        K3["Kafka-3"]
        Z3["ZooKeeper-3"]
        J3["JournalNode-3"]
        W3["Spark Worker-3"]
    end

    subgraph W4["🖥️ ahmed-vmware-virtual-platform<br/>Worker"]
        K4["Kafka-4"]
        CH1["ClickHouse-1"]
        W4S["Spark Worker-4"]
    end

    subgraph W5["🖥️ victus<br/>Worker"]
        K5["Kafka-5"]
        CH2["ClickHouse-2"]
        GF["Grafana"]
        PR["Prometheus"]
        AF["Airflow"]
        W5S["Spark Worker-5"]
    end

    M1 <--> M2
    M2 <--> M3
    M1 <--> M3
🖥️ Node Roles
Machine
Swarm Role
Main Services
zzzz-vm
Manager / Leader
Kafka-1, ZooKeeper-1, Spark Master-1, Active NameNode, Worker-1
abdullah
Manager
Kafka-2, ZooKeeper-2, Spark Master-2, Standby NameNode, Worker-2
sharawy-vmware-virtual-platform
Manager
Kafka-3, ZooKeeper-3, JournalNode-3, Worker-3
ahmed-vmware-virtual-platform
Worker
Kafka-4, ClickHouse-1, Worker-4
victus
Worker
Kafka-5, ClickHouse-2, Grafana, Prometheus, Airflow, Worker-5
Important: Service placement is controlled using Docker Swarm placement constraints so that replicas are intentionally distributed across physical machines.
🔐 High Availability Strategy
The platform contains multiple layers of redundancy.
                 ┌─────────────────────┐
                 │     HA Strategy     │
                 └──────────┬──────────┘
                            │
        ┌───────────────────┼───────────────────┐
        │                   │                   │
        ▼                   ▼                   ▼
     Kafka                 HDFS                Spark
     RF = 3                NN HA               2 Masters
        │                   │                   │
        ▼                   ▼                   ▼
   Broker Failure       NN Failure         Master Failure

                            │
                            ▼

                       ClickHouse
                       Replication
                            │
                            ▼
                     Data Availability
🧪 Fault Tolerance Testing
The objective is not simply to make the cluster run.
The objective is to prove that it can survive failures.
Kafka Failure
Stop one Kafka broker:
docker stop <kafka-container>
Then inspect:
kafka-topics.sh \
  --bootstrap-server kafka1:9092 \
  --describe \
  --topic ran-telemetry
Expected behavior:
Failed broker
     ↓
Leader election
     ↓
Another ISR replica becomes Leader
     ↓
Producer / Consumer continues
HDFS Failure
Stop the Active NameNode:
Active NameNode
      ↓
     FAIL
      ↓
ZKFC detects failure
      ↓
Standby NameNode
      ↓
New Active
Spark Failure
If one Spark worker fails:
Spark Worker
     ↓
   FAIL
     ↓
Other Workers
     ↓
Continue processing
ClickHouse Failure
With replicated tables:
ClickHouse-1
     │
     │ Replication
     ▼
ClickHouse-2
If one replica becomes unavailable, the remaining replica can continue serving queries depending on the query/replica configuration.
📊 Observability
Grafana provides the operational view of the platform.
The dashboard is intended to expose:
Kafka
Broker health
Partition distribution
Consumer lag
Throughput
Replication state
Spark
Streaming throughput
Processing latency
Active jobs
Worker health
HDFS
DataNode health
Storage utilization
Replication status
NameNode state
RAN
Cell activity
RB utilization
User count
Data volume
CQI
Energy consumption
Anomaly count
Risk score
🧰 Technology Stack
Layer
Technology
Purpose
📡 Data Source
RAN / PM Dataset
Network telemetry
🐍 Producer
Python
CSV → simulated events
⚡ Streaming
Apache Kafka
Distributed ingestion
🐘 Data Lake
Hadoop HDFS
Durable raw storage
🔥 Processing
Apache Spark
Streaming + Batch
🗄️ Analytics
ClickHouse
Fast analytical queries
🧠 Intelligence
Python / ML
Anomaly & behavioral analysis
📊 Monitoring
Grafana
NOC dashboards
📈 Metrics
Prometheus
Infrastructure metrics
🔄 Orchestration
Apache Airflow
Scheduled workflows
🐳 Infrastructure
Docker Swarm
Distributed deployment
🌐 Networking
Tailscale
Private node-to-node connectivity
🧭 Coordination
ZooKeeper
Kafka / HA coordination
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
        ├── architecture-overview.png
        ├── data-flow.png
        └── cluster-topology.png
🚀 Getting Started
1. Prepare the Nodes
Install Docker on all five machines.
curl -fsSL https://get.docker.com | sh

sudo usermod -aG docker $USER

newgrp docker
Install Tailscale:
curl -fsSL https://tailscale.com/install.sh | sh

sudo tailscale up
2. Create the Docker Swarm
On the leader:
docker swarm init \
  --advertise-addr <LEADER_TAILSCALE_IP>
Check:
docker node ls
Join the remaining nodes using the worker/manager tokens generated by Swarm.
3. Label the Nodes
Example:
docker node update \
  --label-add node_id=1 \
  zzzz-vm
docker node update \
  --label-add node_id=2 \
  abdullah
docker node update \
  --label-add node_id=3 \
  sharawy-vmware-virtual-platform
docker node update \
  --label-add node_id=4 \
  ahmed-vmware-virtual-platform
docker node update \
  --label-add node_id=5 \
  victus
Verify:
docker node inspect <NODE> \
  --format '{{.Spec.Labels}}'
4. Deploy the Stack
From the Swarm leader:
docker stack deploy \
  -c docker-stack.yml \
  ran-platform
Check services:
docker stack services ran-platform
Check tasks:
docker stack ps ran-platform
5. Create the Kafka Topic
The main telemetry topic is:
ran-telemetry
Create it with:
kafka-topics.sh \
  --bootstrap-server kafka1:9092 \
  --create \
  --topic ran-telemetry \
  --partitions 5 \
  --replication-factor 3
Verify:
kafka-topics.sh \
  --bootstrap-server kafka1:9092 \
  --describe \
  --topic ran-telemetry
Expected:
PartitionCount: 5
ReplicationFactor: 3
6. Start the Producer
Example:
export KAFKA_BROKERS="kafka1:9092,kafka2:9092,kafka3:9092"

export DATA_DIR="./data/Raw"

python Kafka/producer/producer.py
The producer will:
Read CSV
   ↓
Normalize columns
   ↓
Parse timestamps
   ↓
Group records
   ↓
Generate event
   ↓
Send to Kafka
   ↓
Wait
   ↓
Generate next event
7. Start Spark Streaming
spark-submit \
  Spark/streaming/ran_streaming.py
The streaming job consumes:
ran-telemetry
and processes telemetry continuously.
🔎 Useful Commands
Swarm
docker node ls
docker stack services ran-platform
docker stack ps ran-platform
Kafka
docker service ls | grep kafka
docker service logs -f ran-platform_kafka1
Check topic
kafka-topics.sh \
  --bootstrap-server kafka1:9092 \
  --describe \
  --topic ran-telemetry
Grafana
http://<node-ip>:3000
Prometheus
http://<node-ip>:9090
🧪 What We Want to Prove
This project is not just a collection of Big Data technologies.
We want to demonstrate a complete engineering lifecycle:
                 RAW DATA
                    │
                    ▼
              DATA INGESTION
                    │
                    ▼
              DISTRIBUTED BUS
                    │
                    ▼
             REAL-TIME PROCESSING
                    │
                    ▼
              DATA LAKE STORAGE
                    │
                    ▼
             ANALYTICAL STORAGE
                    │
                    ▼
             BEHAVIORAL ANALYSIS
                    │
                    ▼
               RISK SCORING
                    │
                    ▼
              NOC VISIBILITY
                    │
                    ▼
             PROACTIVE ACTION
🎯 Current MVP
The current project focuses on:
✅ RAN telemetry ingestion
✅ CSV → simulated streaming
✅ Kafka distributed ingestion
✅ Kafka replication
✅ Spark streaming
✅ HDFS raw storage
✅ ClickHouse analytical storage
✅ Anomaly detection foundation
✅ Grafana monitoring
✅ Docker Swarm deployment
✅ HA / fault-tolerance testing
Future iterations can extend this into:
Historical Incidents
        +
Telemetry History
        +
Cell Behavioral Profiles
        +
ML Models
        ↓
Failure Prediction
        ↓
Proactive Maintenance
🌟 Expected Impact
The ultimate objective is to move network operations from:
REACTIVE
   │
   ▼
Alarm
   │
   ▼
Engineer investigates
   │
   ▼
Problem already affects users
to:
PROACTIVE
   │
   ▼
Behavior changes
   │
   ▼
Anomaly detected
   │
   ▼
Risk score increases
   │
   ▼
NOC receives priority
   │
   ▼
Engineer acts
   │
   ▼
Failure potentially prevented
👥 Team
Role
Member
Responsibilities
👑 Infrastructure & Architecture
Mohamed
Architecture, Swarm, deployment, Kafka-1, Spark Master-1
🐘 HDFS & HA
Bodok
HDFS HA, NameNode Standby, JournalNodes
⚡ Kafka & Streaming
Abdullah
Kafka, ZooKeeper, streaming pipeline
🗄️ Data & ClickHouse
Sharawy
ClickHouse, Kafka, Spark Worker
📊 Monitoring & Orchestration
Amr
Grafana, Prometheus, Airflow, ClickHouse
📚 Architecture Decisions
Why Kafka?
Because telemetry is naturally event-oriented and Kafka provides:
Distributed ingestion
Partitioning
Replication
Consumer groups
Replayable events
Fault tolerance
Why HDFS?
Because raw telemetry should remain available for:
Historical analysis
Reprocessing
Feature engineering
Model training
Why Spark?
Because the platform requires both:
Real-time processing
Historical batch processing
Why ClickHouse?
Because the final telemetry layer needs fast analytical queries and dashboard workloads.
Why Docker Swarm?
Because the project is deployed across five physical machines and we need:
Service orchestration
Placement constraints
Overlay networking
Replica management
Failure testing
Why Tailscale?
Because the five physical machines are connected through a private mesh network without requiring direct public exposure.
⚠️ Development Status
Status: 🚧 Active Development
The platform currently represents a working distributed infrastructure and development architecture.
Some production-level capabilities still require additional work:
Production credential management
Persistent distributed storage strategy
Kafka security
TLS
Dedicated exporters
Production ML models
Automated deployment
Complete HDFS HA bootstrap automation
Complete Airflow deployment
Advanced alert routing
📜 License
This project is developed as an educational and engineering demonstration of a distributed RAN intelligence platform.
�

📡 RAN Behavioral Intelligence
From Telemetry → Intelligence → Action
Analyze · Detect · Prioritize · Predict · Prevent
�


⭐ Built with Apache Kafka · Spark · Hadoop · ClickHouse · Python · Docker Swarm
�
```
