# kafka_producer.py
# Part A – Send CoDrone EDU telemetry to Kafka at ~2 Hz until duration or Ctrl+C.
#
# Fields per assignment: timestamp, height, pitch, roll, yaw, battery
# Dependencies:
#   pip install codrone-edu kafka-python

import json
import signal
import sys
import time

from codrone_edu.drone import Drone
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "drone_telemetry"

SAMPLE_RATE_HZ = 2.0          # 2 readings per second
DURATION_SECONDS = 60.0       # ~1 minute flight (adjust if needed)
MIN_MESSAGES = 120            # make sure we send at least 120 messages


def read_sensor(getter, default=None):
    """Safely call a sensor getter; if it fails, return default."""
    try:
        return getter()
    except Exception:
        return default


def main():
    should_stop = {"value": False}

    def _on_sigint(sig, frame):
        should_stop["value"] = True
        print("\nCtrl+C pressed, stopping producer after current iteration...")

    signal.signal(signal.SIGINT, _on_sigint)

    print("Connecting to controller + CoDrone EDU…")
    drone = Drone()
    drone.pair()
    print("Paired. Start flying with the controller when ready.\n")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,                  # connecting to the Kafka broker.
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),   # Python dict to JSON string to UTF-8 bytes
    )

    # Amount of time we want to collect data
    start_monotonic = time.monotonic()
    sample_period = 1.0 / SAMPLE_RATE_HZ
    next_sample_time = start_monotonic
    sent = 0

    # Main data collection loop
    try:
        # Not forcing ctrl+c to stop the drone
        while not should_stop["value"]:

            # count the elapsed time
            now = time.monotonic()
            elapsed = now - start_monotonic

            # Auto-stop: after duration AND at least MIN_MESSAGES
            # for our case we set this to 60 seconds and at least 120 messages within this
            if (
                DURATION_SECONDS is not None
                and elapsed >= DURATION_SECONDS
                and sent >= MIN_MESSAGES
            ):
                break

            # --------- Read sensors ---------
            ts_epoch = time.time()  # used for windowing later

            # Use the calls that worked already:
            height_cm = read_sensor(lambda: drone.get_height(), default=None)
            roll_deg = read_sensor(lambda: drone.get_angle_x(), default=None)
            pitch_deg = read_sensor(lambda: drone.get_angle_y(), default=None)
            yaw_deg = read_sensor(lambda: drone.get_angle_z(), default=None)
            battery_percent = read_sensor(lambda: drone.get_battery(), default=None)

            # What we want the data to look like for each datapoint.
            msg = {
                "timestamp": ts_epoch,          # seconds
                "height": height_cm,            # height
                "pitch": pitch_deg,             # x angle
                "roll": roll_deg,               # y angle
                "yaw": yaw_deg,                 # z angle
                "battery": battery_percent,     # %
            }

            producer.send(KAFKA_TOPIC, msg)     # Push the message to Kafka "asynchronously"
            producer.flush()                    # wait until everything is sent.

            sent += 1
            print(f"[{sent}] {msg}")            # Print live telemetry data in terminal as [1] ... or [83] ...

            # Keep ~2 Hz cadence
            next_sample_time += sample_period
            sleep_for = next_sample_time - time.monotonic()
            if sleep_for > 0:
                time.sleep(sleep_for)
            else:
                # If we fell behind, reset schedule
                next_sample_time = time.monotonic()

    finally:
        try:
            producer.flush()
            producer.close()
        except Exception:
            pass

        try:
            drone.close()
        except Exception:
            pass

        print(f"\nkafka_producer.py finished, sent {sent} messages.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
