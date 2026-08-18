#!/usr/bin/env bash
set -euo pipefail

hdfs dfs -mkdir -p /raw/ran-telemetry
hdfs dfs -mkdir -p /spark-logs
hdfs dfs -ls -R /
