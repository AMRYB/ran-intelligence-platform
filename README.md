# Ran Intelligence Platform

A distributed data platform for ingesting, processing, monitoring, and analyzing telemetry and operational events from edge or telecom-like systems.

## Architecture

- Kafka and ZooKeeper for event streaming
- Hadoop HDFS for durable batch data storage
- Spark for batch/stream processing
- ClickHouse for analytical querying
- Prometheus + Grafana for observability
- ML anomaly detection pipeline for telemetry analysis

## Project layout

- `docker-stack.yml` - Docker Swarm stack definition
- `configs/` - cluster configuration files for Kafka, Zookeeper, Hadoop, Spark, ClickHouse, Prometheus, and Grafana
- `producer/` - telemetry producer service
- `streaming/` - streaming processing service
- `batch/` - batch jobs
- `ml/` - anomaly detection and model assets
- `monitoring/` - monitoring service configuration
- `scripts/` - operational scripts
- `data/` - sample datasets

## Quick start

1. Copy the example environment file:

   ```bash
   cp .env.example .env
   ```

2. Initialize Docker Swarm (if not already initialized):

   ```bash
   docker swarm init
   ```

3. Deploy the stack:

   ```bash
   ./scripts/deploy.sh
   ```

4. Check services:

   ```bash
   docker service ls
   ```

## Configuration locations

`docker-stack.yml` contains service placement, networks, ports, persistent volumes, and mounts. Technology-specific settings live under `configs/`:

- `configs/zookeeper/zoo.cfg` and `configs/zookeeper/myid-*` - ZooKeeper ensemble settings and node IDs
- `configs/kafka/kafka1.properties` through `kafka5.properties` - broker identity, listeners, replication, and storage
- `configs/hadoop/core-site.xml`, `hdfs-site.xml`, and `workers` - HDFS namespace, replication, and DataNode list
- `configs/spark/spark-defaults.conf` and `spark-env.sh` - Spark runtime and HDFS event-log settings
- `configs/prometheus/prometheus.yml` - metrics scrape targets

The stack declares these files under its top-level `configs:` section and mounts them into the relevant services. Named volumes hold state locally on the node selected by each placement constraint.

## Swarm node labels

Run the label script from a Swarm manager and pass the exact, case-sensitive hostnames from `docker node ls`:

```bash
./scripts/label-nodes.sh zzzz-vm Victus sharawy-VMware-Virtual-Platform
```

The current cluster has three nodes. Services constrained to `node_id` 4 and 5 will remain pending until two more nodes join the Swarm and receive those labels. The intended five-node command is:

```bash
./scripts/label-nodes.sh <node-1> <node-2> <node-3> <node-4> <node-5>
```

## Notes

- The provided configuration is a scaffold intended for local or development cluster deployments.
- Update credentials, node labels, and storage paths before production use.
- The sample telemetry CSV files are under `data/Raw/` and can be used to test the producer.
- This is a development placement/configuration scaffold. HDFS NameNode failover, Kafka topic creation, Spark HA initialization, Airflow metadata, and exporter setup still require their bootstrap jobs before production use.
