# telemetry_logger.py
# Logs automatic collection and transmission of data at a fixed pace
# It prints the csv data row by row, and saves CSV locally and to S3 bucket if enabled
# Dependencies: pip install codrone-edu boto3

import csv
import json
import math
import os
import signal
import time
from datetime import datetime, timezone
from pathlib import Path
import boto3

try:
    from codrone_edu.drone import Drone
except Exception:
    print("codrone-edu library not found. Install with: pip install codrone-edu")
    raise

# --------------
# Configurations
# --------------

SAMPLE_RATE_HZ = 2.0                        # samples per second; 2.0 => one row every 0.5 s
ENABLE_AUTO_TAKEOFF_ON_START = False        # keep False for controller flights
RUN_FOR_SECONDS = None                      # None will keep logging until Ctrl+C (or auto-stop)
PRINT_EVERY_N_ROWS = 1                      # print status every N rows

# Auto-stop when landed and stable
ENABLE_AUTO_STOP_WHEN_LANDED = False        # auto stop when the drone has landed
AUTO_STOP_HEIGHT_CM_THRESHOLD = 5.0         # considered "on floor" if height is less than this
AUTO_STOP_PLANAR_SPEED_CMS_THRESHOLD = 5.0  # planar XY speed threshold (cm/s) for "stationary"
AUTO_STOP_STABLE_FOR_SECONDS = 3.0          # must satisfy height+speed thresholds for this long

# Output files
OUTPUT_DIRECTORY_NAME = "logs"              # create a directory named logs locally to save outputs
OUTPUT_FILE_PREFIX = "flight"               # save the output files with prefix flight...
CSV_DIALECT = "excel"                       # or "unix" (excel has /r/n, unix has /n format)

# S3 upload at end of run
ENABLE_S3_UPLOAD_AT_END = True              # enable s3 to upload the data directly to s3
S3_BUCKET_NAME_FOR_UPLOAD = "cosc573-drone-project-974" # bucket name in s3, switch the bucket name to new name if different.
S3_KEY_PREFIX_FOR_UPLOAD = "drone-logs/"  # prefix within bucket

# Optional AWS credential sources
# Save learner labs' credentials in ~/.aws/credentials and ~/.aws/config
AWS_PROFILE_NAME_FALLBACK = "default"
AWS_REGION_FALLBACK = "us-east-1"

# -------------------------------------------------------------------------------------------

def local_iso_timestamp() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")

def read_sensor(getter, default=None):
    try:
        return getter()
    except Exception:
        return default

def _make_s3_client():
    # Use ~/.aws credentials with a chosen or env-selected profile.
    # So you can either save it in this file or at the home directory.
    profile = os.environ.get("AWS_PROFILE") or AWS_PROFILE_NAME_FALLBACK
    try:
        if profile:
            session = boto3.Session(profile_name=profile, region_name=AWS_REGION_FALLBACK)
            return session.client("s3", region_name=AWS_REGION_FALLBACK)
    except Exception:
        pass
    return boto3.client("s3", region_name=AWS_REGION_FALLBACK)

def main():
    # Prepare output CSV
    Path(OUTPUT_DIRECTORY_NAME).mkdir(parents=True, exist_ok=True)
    run_stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_file_path = os.path.join(OUTPUT_DIRECTORY_NAME, f"{OUTPUT_FILE_PREFIX}_{run_stamp}.csv")

    # Columns cover altitude/height, speed, yaw, and the commonly referenced 7-sensor set.
    column_names_for_csv = [
        "ts_iso", "elapsed_s",
        "state",
        "battery_percent",
        "height_cm",
        "bottom_range_cm", "front_range_cm",
        "flow_vx_cms", "flow_vy_cms", "speed_cms",
        "angle_roll_deg", "angle_pitch_deg", "angle_yaw_deg",
        "rel_x", "rel_y", "rel_z",
        "accel_x", "accel_y", "accel_z",
        "color_front", "color_back",
        "temp_c",
    ]

    # Check if Drone has been paired or not.
    print("Connecting to controller + CoDrone EDU…")
    drone = Drone()
    drone.pair()
    print("Paired. Press Ctrl+C to stop logging.\n")

    if ENABLE_AUTO_TAKEOFF_ON_START:
        try:
            drone.takeoff()
        except Exception:
            pass  # continue logging even if takeoff is not executed

    # CSV writer
    csv_file = open(csv_file_path, "w", newline="", encoding="utf-8")
    csv_writer = csv.DictWriter(csv_file, fieldnames=column_names_for_csv, dialect=CSV_DIALECT, extrasaction="ignore")
    csv_writer.writeheader()
    csv_file.flush()

    # When control C is pressed, the code will stop logging the values, and then save commands will run.
    should_stop = {"value": False}
    def _on_ctrl_c(sig, frame):
        should_stop["value"] = True
    signal.signal(signal.SIGINT, _on_ctrl_c)

    start_monotonic = time.monotonic()
    sample_period_seconds = 1.0 / float(SAMPLE_RATE_HZ)
    next_sample_time = start_monotonic
    landed_detected_since = None
    rows_written = 0

    try:
        while True:
            now_monotonic = time.monotonic()

            if RUN_FOR_SECONDS is not None and (now_monotonic - start_monotonic) >= RUN_FOR_SECONDS:
                break
            if should_stop["value"]:
                break

            def _get_flow_vx():
                try:
                    return drone.get_flow_velocity_x()
                except AttributeError:
                    return drone.get_flow_x()

            def _get_flow_vy():
                try:
                    return drone.get_flow_velocity_y()
                except AttributeError:
                    return drone.get_flow_y()

            # ---------------
            # Sensor readings
            # ---------------

            # Time Stamps
            ts_iso = local_iso_timestamp()

            # Flight Status
            flight_state = read_sensor(lambda: drone.get_flight_state(), default=None)
            battery_percent = read_sensor(lambda: drone.get_battery(), default=None)

            # Height the drone is flying at
            height_cm = read_sensor(lambda: drone.get_height(), default=None)

            # Optical flow velocities
            flow_vx_cms = read_sensor(_get_flow_vx, 0.0)
            flow_vy_cms = read_sensor(_get_flow_vy, 0.0)
            planar_speed_cms = math.hypot(flow_vx_cms, flow_vy_cms)

            # Orientation in degrees
            angle_roll_deg = read_sensor(lambda: drone.get_angle_x(), default=None)
            angle_pitch_deg = read_sensor(lambda: drone.get_angle_y(), default=None)
            angle_yaw_deg = read_sensor(lambda: drone.get_angle_z(), default=None)

            # Accelerometer
            accel_x = read_sensor(lambda: drone.get_accel_x(), default=None)
            accel_y = read_sensor(lambda: drone.get_accel_y(), default=None)
            accel_z = read_sensor(lambda: drone.get_accel_z(), default=None)

            # Optical flow relative position in cm
            rel_x = read_sensor(lambda: drone.get_pos_x(), default=None)
            rel_y = read_sensor(lambda: drone.get_pos_y(), default=None)
            rel_z = read_sensor(lambda: drone.get_pos_z(), default=None)

            # Ranges in cm
            bottom_range_cm = read_sensor(lambda: drone.get_bottom_range(), default=None)
            front_range_cm = read_sensor(lambda: drone.get_front_range(), default=None)

            # Color sensors (front/back)
            color_front = read_sensor(lambda: drone.get_front_color(), default=None)
            color_back = read_sensor(lambda: drone.get_back_color(), default=None)

            # Internal temperature
            temp_c = read_sensor(lambda: drone.get_drone_temperature(), default=None)

            row = {
                "ts_iso": ts_iso,
                "elapsed_s": round(now_monotonic - start_monotonic, 3),
                "state": flight_state,
                "battery_percent": battery_percent,
                "height_cm": height_cm,
                "bottom_range_cm": bottom_range_cm,
                "front_range_cm": front_range_cm,
                "flow_vx_cms": round(flow_vx_cms, 3),
                "flow_vy_cms": round(flow_vy_cms, 3),
                "speed_cms": round(planar_speed_cms, 3),
                "angle_roll_deg": angle_roll_deg,
                "angle_pitch_deg": angle_pitch_deg,
                "angle_yaw_deg": angle_yaw_deg,
                "rel_x": rel_x, "rel_y": rel_y, "rel_z": rel_z,
                "accel_x": accel_x, "accel_y": accel_y, "accel_z": accel_z,
                "color_front": color_front, "color_back": color_back,
                "temp_c": temp_c,
            }

            csv_writer.writerow(row)
            rows_written += 1
            if rows_written % PRINT_EVERY_N_ROWS == 0:
                ordered = {k: row.get(k) for k in column_names_for_csv}
                print(json.dumps(ordered, separators=(",", ":"), ensure_ascii=False, default=str))
            csv_file.flush()

            # Auto-stop when the drone has landed
            if ENABLE_AUTO_STOP_WHEN_LANDED:
                low_height = (height_cm is not None and height_cm <= AUTO_STOP_HEIGHT_CM_THRESHOLD)
                low_speed = (planar_speed_cms <= AUTO_STOP_PLANAR_SPEED_CMS_THRESHOLD)
                if low_height and low_speed:
                    if landed_detected_since is None:
                        landed_detected_since = now_monotonic
                    elif (now_monotonic - landed_detected_since) >= AUTO_STOP_STABLE_FOR_SECONDS:
                        print(f"Auto-stop: landed and stable for ~{AUTO_STOP_STABLE_FOR_SECONDS}s. Stopping.")
                        break
                else:
                    landed_detected_since = None

            # Maintain sampling speed
            next_sample_time += sample_period_seconds
            time_to_sleep = next_sample_time - time.monotonic()
            if time_to_sleep > 0:
                time.sleep(time_to_sleep)
            else:
                next_sample_time = time.monotonic()

    # Attempt clean shutdown even after repeated interrupts
    finally:
        try:
            drone.close()
        except Exception:
            pass
        csv_file.close()
        print(f"\nSaved CSV -> {csv_file_path}")

        # If enabled upload the data to S3 bucket
        if ENABLE_S3_UPLOAD_AT_END:
            try:
                s3 = _make_s3_client()
                s3_key = f"{S3_KEY_PREFIX_FOR_UPLOAD}{os.path.basename(csv_file_path)}"
                s3.upload_file(csv_file_path, S3_BUCKET_NAME_FOR_UPLOAD, s3_key)
                print(f"Uploaded to s3://{S3_BUCKET_NAME_FOR_UPLOAD}/{s3_key}")
            except Exception as e:
                print(f"S3 upload failed (not fatal): {e}")


if __name__ == "__main__":
    main()