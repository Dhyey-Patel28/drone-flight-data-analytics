# Drone Flight Data Analytics (Kafka + Spark on AWS)

End-to-end telemetry pipeline using **CoDrone EDU (real flight) → Kafka → Spark → S3 → analytics/plots**.

## What it does
- Connects to a **CoDrone EDU** using the provided Python library and collects live telemetry while the drone is flown with the controller.
- Publishes telemetry events (height, pitch, roll, yaw, battery, timestamp) to a Kafka topic (`drone_telemetry`).
- Runs a **Spark Structured Streaming** job on an AWS EC2 instance to aggregate/transform telemetry.
- Writes aggregated outputs to **Amazon S3** for persistence and sharing.
- Generates time-series plots in Python (matplotlib) from the aggregated output.

> This project was run in an AWS lab environment using **temporary AWS credentials** provided by the course.

## Architecture
CoDrone EDU (flight)
→ `kafka_producer.py`
→ Kafka on EC2 (`drone_telemetry`)
→ `spark_stream_aggregator.py` (Spark on EC2)
→ S3 (aggregated output)
→ `visualize_aggregates.py` (plots)

## Repo layout
- `milestone-1/` — early milestone work + batch scripts
- `milestone-2/`
  - `kafka_producer.py` — publishes live telemetry to Kafka (during real flight)
  - `spark_stream_aggregator.py` — reads from Kafka and writes aggregated output (S3 or local)
  - `visualize_aggregates.py` — plots results
  - `*.png` — sample plots (height/pitch/roll/yaw/battery vs time)
- `data/logs/` — optional sample telemetry logs (for replay demo)
- `scripts/`
  - `replay_logs_to_kafka.py` — replays CSV logs into Kafka (works without drone)

## AWS setup (EC2 + S3)
### 1) Create S3 bucket
Create a bucket (example):
- `s3://<your-bucket-name>/drone-telemetry/`

### 2) Launch EC2 and install dependencies
On the EC2 instance, install:
- Kafka
- Spark
- Python + required packages

Install Python deps:
`pip install -r requirements.txt`

### 3) Configure AWS credentials (lab temp creds)
Set AWS credentials on the EC2 instance using the lab-provided values (one common way):
`aws configure`

### 4) Start Kafka on EC2
Start Zookeeper/KRaft + broker (based on your Kafka setup) and ensure the broker is reachable at:
`<EC2_PUBLIC_DNS>:9092`

Create topic:
`kafka-topics --bootstrap-server <EC2_PUBLIC_DNS>:9092 --create --topic drone_telemetry --partitions 1 --replication-factor 1`

## Run (real drone → AWS Kafka/Spark → S3)
1) On EC2: start Spark aggregation (Terminal 1)  
`spark-submit .\milestone-2\spark_stream_aggregator.py --bootstrap <EC2_PUBLIC_DNS>:9092 --topic drone_telemetry --s3-bucket <your-bucket-name> --s3-prefix drone-telemetry/aggregated/`

2) On your laptop (connected to the drone/controller): publish live telemetry (Terminal 2)  
`python .\milestone-2\kafka_producer.py --bootstrap <EC2_PUBLIC_DNS>:9092 --topic drone_telemetry`

3) After the flight session: generate plots  
- Option A (download aggregated CSVs from S3 locally, then run):
  `python .\milestone-2\visualize_aggregates.py --input .\aggregated_telemetry\`
- Option B (if your plotting script supports reading from S3, run from EC2)

## Demo (no drone)
Replay sample logs into Kafka:
`python .\scripts\replay_logs_to_kafka.py --log-dir .\data\logs --rate-hz 2`
