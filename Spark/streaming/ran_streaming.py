import os
import json
from kafka import KafkaConsumer
from clickhouse_driver import Client

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka1:9092,kafka2:9092,kafka3:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'ran-telemetry')
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'clickhouse')
CLICKHOUSE_DB = os.getenv('CLICKHOUSE_DB', 'ran_telemetry')
CLICKHOUSE_TABLE = os.getenv('CLICKHOUSE_TABLE', 'telemetry_events')

client = Client(host=CLICKHOUSE_HOST, database=CLICKHOUSE_DB)

client.execute(
    f"""
    CREATE TABLE IF NOT EXISTS {CLICKHOUSE_TABLE} (
        timestamp DateTime,
        cell_id String,
        region String,
        signal_strength Int32,
        temperature Float64,
        latency_ms Int32,
        packet_loss Float64,
        availability Float64,
        server_load_cpu Int32,
        server_load_mem Int32
    ) ENGINE = MergeTree()
    ORDER BY (timestamp, cell_id)
    """
)

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda m: m.decode('utf-8'),
)

for message in consumer:
    payload = json.loads(message.value)
    row = {
        'timestamp': payload.get('timestamp'),
        'cell_id': payload.get('cell_id'),
        'region': payload.get('region'),
        'signal_strength': int(payload.get('signal_strength', 0)),
        'temperature': float(payload.get('temperature', 0.0)),
        'latency_ms': int(payload.get('latency_ms', 0)),
        'packet_loss': float(payload.get('packet_loss', 0.0)),
        'availability': float(payload.get('availability', 0.0)),
        'server_load_cpu': int(payload.get('server_load_cpu', 0)),
        'server_load_mem': int(payload.get('server_load_mem', 0)),
    }
    client.execute(
        f"INSERT INTO {CLICKHOUSE_TABLE} VALUES",
        [tuple(row[k] for k in row)],
    )
    print(f"Inserted into ClickHouse: {row}")
