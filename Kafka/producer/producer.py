import glob
import json
import os
import time

import pandas as pd
from kafka import KafkaProducer


KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka1:9092,kafka2:9092,kafka3:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ran-telemetry")
SIMULATION_INTERVAL = float(os.getenv("SIMULATION_INTERVAL", "5"))
DATA_DIR = os.getenv("DATA_DIR", os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "Raw"))
CSV_FILE = os.getenv("CSV_FILE")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKERS.split(","),
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    key_serializer=lambda key: key.encode("utf-8"),
    acks="all",
    retries=10,
    linger_ms=10,
)


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(col).strip().lower().replace(" ", "_") for col in df.columns]
    return df


def infer_network_type(file_path: str) -> str:
    file_name = os.path.basename(file_path).upper()
    if "LTE" in file_name:
        return "LTE"
    if "NR" in file_name:
        return "NR"
    if "GSM" in file_name:
        return "GSM"
    return "UNKNOWN"


def parse_timestamp(value):
    if pd.isna(value):
        return pd.NaT
    if isinstance(value, str):
        value = value.strip()
    try:
        return pd.to_datetime(value, errors="coerce")
    except Exception:
        return pd.NaT


def collect_events_from_csv(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path, low_memory=False)
    df = normalize_columns(df)

    if "timestamp" not in df.columns:
        if "time" in df.columns:
            df = df.rename(columns={"time": "timestamp"})
        elif "timestamp_utc" in df.columns:
            df = df.rename(columns={"timestamp_utc": "timestamp"})

    if "base_station" not in df.columns and "base_station_name" in df.columns:
        df = df.rename(columns={"base_station_name": "base_station"})
    if "sector" not in df.columns and "cell" in df.columns:
        df = df.rename(columns={"cell": "sector"})

    if "timestamp" in df.columns:
        df["timestamp"] = df["timestamp"].apply(parse_timestamp)
        df = df.dropna(subset=["timestamp"]).copy()

    df["network_type"] = infer_network_type(csv_path)
    df["source_file"] = os.path.basename(csv_path)

    return df.sort_values("timestamp")


def load_all_events():
    file_paths = []

    if CSV_FILE:
        file_paths = [CSV_FILE]
    else:
        file_paths = sorted(glob.glob(os.path.join(DATA_DIR, "*.csv")))

    if not file_paths:
        raise FileNotFoundError(f"No CSV files found in {DATA_DIR}")

    frames = [collect_events_from_csv(path) for path in file_paths]
    combined = pd.concat(frames, ignore_index=True, sort=False)

    if "timestamp" not in combined.columns:
        raise ValueError("No timestamp column found in input CSV files")

    combined["timestamp"] = pd.to_datetime(combined["timestamp"], errors="coerce")
    combined = combined.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

    return combined


def send_grouped_events():
    df = load_all_events()
    if df.empty:
        raise ValueError("No valid events were loaded from the source CSV files")

    previous_timestamp = None
    for current_timestamp in df["timestamp"].drop_duplicates().tolist():
        batch = df[df["timestamp"] == current_timestamp].copy()

        if previous_timestamp is not None:
            delta = (current_timestamp - previous_timestamp).total_seconds()
            print(f"Gap from previous event: {delta} seconds")
            time.sleep(SIMULATION_INTERVAL)

        records = []
        for _, row in batch.iterrows():
            record = row.to_dict()
            record["timestamp"] = current_timestamp.isoformat()
            records.append(record)

        event = {
            "timestamp": current_timestamp.isoformat(),
            "record_count": len(records),
            "records": records,
            "network_types": sorted(batch["network_type"].dropna().unique().tolist()),
            "source_files": sorted(batch["source_file"].dropna().unique().tolist()),
        }

        key = f"{current_timestamp.strftime('%Y%m%d%H%M%S')}"
        producer.send(KAFKA_TOPIC, key=key, value=event)
        print(f"Sent grouped event for {event['timestamp']} with {event['record_count']} records")
        producer.flush()
        previous_timestamp = current_timestamp

    producer.close()
    print("Streaming simulation finished.")


if __name__ == "__main__":
    print(f"Reading raw telemetry files from: {DATA_DIR}")
    send_grouped_events()