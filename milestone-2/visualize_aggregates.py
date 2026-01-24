# visualize_aggregates.py
# Part C – Read aggregated_telemetry CSV and plot:
#   avg_height, avg_battery, avg_pitch, avg_roll, avg_yaw

import glob
import os
import sys

import matplotlib.pyplot as plt
import pandas as pd


def main():
    # Load aggregated CSV files
    files = sorted(glob.glob("aggregated_telemetry/*.csv"))
    files = [f for f in files if os.path.basename(f).startswith("part-")] # Filter out non-data files just in case

    # If no aggregated files are ready to visualize.
    if not files:
        print("No CSV data files found in aggregated_telemetry/.")
        print("Did you run spark_batch_aggregator.py successfully?")
        sys.exit(1)

    print(f"Found {len(files)} data file(s):")
    for f in files:
        print("  ", f)

    # Read each CSV into a pandas DataFrame and concat them.
    df_list = [pd.read_csv(f) for f in files]
    df = pd.concat(df_list, ignore_index=True)

    # ----- Plotting logic starts -----

    # Checking if there is start_time column in the pandas data frame.
    if "start_time" not in df.columns:
        print("Column 'start_time' not found in CSV.")
        print("Columns present:", df.columns.tolist())
        sys.exit(1)

    # Convert to datetime
    # Drop any duplicate windows meaning same start time, and sort chronologically.
    df["start_time"] = pd.to_datetime(df["start_time"])
    df = df.drop_duplicates(subset=["start_time"]).sort_values("start_time")

    # Now we will print a plot for each metric, i.e. column
    print("\nColumns:", df.columns.tolist())
    print(df.head())

    # 5 plot metrics are: avg_height, avg_battery, avg_pitch, avg_roll, avg_yaw
    metrics = [
        ("avg_height",  "Height (cm)",   "Average Drone Height per 10-Second Window"),
        ("avg_battery", "Battery (%)",   "Average Drone Battery per 10-Second Window"),
        ("avg_pitch",   "Pitch (deg)",   "Average Drone Pitch per 10-Second Window"),
        ("avg_roll",    "Roll (deg)",    "Average Drone Roll per 10-Second Window"),
        ("avg_yaw",     "Yaw (deg)",     "Average Drone Yaw per 10-Second Window"),
    ]

    # for 5 metrics, their y label on the plot, and the title of plot in metrics,
    # make a plot.
    for col, ylabel, title in metrics:
        if col not in df.columns:
            print(f"Warning: column '{col}' not found; skipping this plot.")
            continue

        # Actual plot logic
        plt.figure(figsize=(10, 5))                             # Size of plot is 10, 5
        plt.plot(df["start_time"], df[col], marker="o")   # x-axis: start time, y-axis: column from metric
        plt.title(title)                                        # write the title of plot
        plt.xlabel("Window Start Time")                         # x-label is window start time
        plt.ylabel(ylabel)                                      # y-label as per metric
        plt.grid(True)                                          # Show grids in background
        plt.tight_layout()                                      # Prevent overlap of text and ticks on plot
        # Uncomment to save figures
        # plt.savefig(f"{col}.png", dpi=150)
        plt.show()


if __name__ == "__main__":
    main()
