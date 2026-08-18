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
