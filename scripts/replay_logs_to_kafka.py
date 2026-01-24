import argparse
import glob
import json
import os
import time
from datetime import datetime

import pandas as pd
from kafka import KafkaProducer

TOPIC = "drone_telemetry"
BOOTSTRAP = "localhost:9092"

def to_epoch(ts):
    # Handles "YYYY-mm-dd HH:MM:SS.ssssss" (your CSV format)
    dt = datetime.fromisoformat(str(ts))
    return dt.timestamp()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--log-dir", required=True, help="Directory containing flight_*.csv logs")
    ap.add_argument("--rate-hz", type=float, default=2.0, help="Messages per second")
    args = ap.parse_args()

    files = sorted(glob.glob(os.path.join(args.log_dir, "*.csv")))
    if not files:
        raise SystemExit(f"No CSV files found in {args.log_dir}")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    delay = 1.0 / max(args.rate_hz, 0.1)
    sent = 0

    try:
        for f in files:
            df = pd.read_csv(f)
            # Expected columns: timestamp,height,pitch,roll,yaw,battery
            for _, row in df.iterrows():
                msg = {
                    "timestamp": to_epoch(row["timestamp"]),
                    "height": None if pd.isna(row["height"]) else float(row["height"]),
                    "pitch": None if pd.isna(row["pitch"]) else float(row["pitch"]),
                    "roll": None if pd.isna(row["roll"]) else float(row["roll"]),
                    "yaw": None if pd.isna(row["yaw"]) else float(row["yaw"]),
                    "battery": None if pd.isna(row["battery"]) else float(row["battery"]),
                }
                producer.send(TOPIC, msg)
                sent += 1
                time.sleep(delay)

        producer.flush()
        print(f"Replayed {sent} messages into Kafka topic '{TOPIC}'.")
    finally:
        try:
            producer.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
