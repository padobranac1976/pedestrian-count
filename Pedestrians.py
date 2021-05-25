import numpy as np
import pandas as pd
import sys

BUFFER = 60


def progressbar(it, prefix="", size=30):
    count = len(it)

    def show(j):
        x = int(size*j/count)
        sys.stdout.write("%s[%s%s] %i/%i\r" % (prefix, "#"*x, "."*(size-x), j, count))
        sys.stdout.flush()
    show(0)
    for i, item in enumerate(it):
        yield item
        show(i+1)
    sys.stdout.write("\n")
    sys.stdout.flush()


def accumulate_pedestrians(df):
    dates = df["Date_Time"].unique()
    acc_ped_df = None
    for i in progressbar(range(len(dates)), "Accumulating Pedestrians: "):
        frame = df[df["Date_Time"] == dates[i]]
        pedestrians = sum(df[df["Date_Time"] == dates[i]]["Hourly_Counts"])
        new_frame = pd.DataFrame(frame.iloc[0].values).T
        new_frame.columns = frame.columns
        new_frame = new_frame.rename(columns={'Hourly_Counts': 'Cumulative_Counts'})
        new_frame["Cumulative_Counts"] = pedestrians
        if acc_ped_df is None:
            acc_ped_df = new_frame
        else:
            acc_ped_df = acc_ped_df.append(new_frame)
    print("-" * BUFFER)
    return acc_ped_df


def update_date_time(df, mode):
    print("-" * BUFFER)
    print("Converting date / time format to {}...".format(mode))
    print("-" * BUFFER)
    if mode == "day":
        dates = np.array([i[:10] for i in df["Date_Time"]])
        df["Date_Time"] = dates
        df = df.drop(columns=["ID", "Time", "Sensor_Name"])
    elif mode == "month":
        dates = np.array([i[:3]+i[6:10] for i in df["Date_Time"]])
        df["Date_Time"] = dates
        df = df.drop(columns=["ID", "Mdate", "Day", "Time", "Sensor_Name"])
    return df


def combine_df(sensors, pedestrians, mode):
    sensor_ids = list(sensors["sensor_id"])
    sensor_ids.sort()
    pedestrians = update_date_time(pedestrians, mode)
    all_sensors = None
    for s in sensor_ids:
        s_d = sensors[sensors["sensor_id"] == s]["sensor_description"].values[0]
        sensor_x = pedestrians[pedestrians["Sensor_ID"] == s]
        print("Processing sensor id {}: {}".format(s, s_d))
        if not sensor_x.empty:
            sensor_x = accumulate_pedestrians(sensor_x)
            sensor_info = sensors[sensors["sensor_id"] == s].drop(columns=["sensor_id", "direction_1", "direction_2"])
            cols = list(sensor_info.columns)
            values = list(sensor_info.values[0])
            for i in range(len(cols)):
                sensor_x[cols[i]] = values[i]
            if all_sensors is None:
                all_sensors = sensor_x
            else:
                all_sensors = all_sensors.append(sensor_x)
        else:
            print("-" * BUFFER)

    return all_sensors


def find_top_10(df, mode):
    return df


run_analysis = True

if run_analysis:
    sensor_location_df = pd.read_csv("Pedestrian_Counting_System_-_Sensor_Locations.csv")
    pedestrians_df = pd.read_csv("Pedestrian_Counting_System_-_Monthly__counts_per_hour_.csv")

    monthly_df = combine_df(sensor_location_df, pedestrians_df, "month")
    monthly_df.to_csv("./monthly.csv", index=False)
    daily_df = combine_df(sensor_location_df, pedestrians_df, "day")
    daily_df.to_csv("./daily.csv", index=False)

else:
    monthly_df = pd.read_csv("monthly.csv")
    daily_df = pd.read_csv("daily.csv")

top_10_monthly_df = find_top_10(monthly_df, "month")
top_10_daily_df = find_top_10(daily_df, "day")
