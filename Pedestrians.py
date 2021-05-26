import numpy as np
import pandas as pd
import sys
from sodapy import Socrata

BUFFER = 56


def progressbar(it, prefix="", size=50):
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


def update_date_time(df, mode):
    print("-" * BUFFER)
    print("Converting date / time format to '{}'-ly frequency...".format(mode))
    print("-" * BUFFER)
    df_c = df.copy()
    if mode == "day":
        dates = np.array([i[:10] for i in df["Date_Time"]])
        df_c["Date_Time"] = dates
        df_c = df_c.drop(columns=["ID", "Time", "Sensor_Name"])
    elif mode == "month":
        dates = np.array([i[:3]+i[6:10] for i in df["Date_Time"]])
        df_c["Date_Time"] = dates
        df_c = df_c.drop(columns=["ID", "Mdate", "Day", "Time", "Sensor_Name"])
    return df_c


def accumulate_pedestrians(df, x, s):
    sensors = df["Sensor_ID"].unique()
    top = None
    for i in range(len(sensors)):
        frame = df[df["Sensor_ID"] == sensors[i]]
        pedestrian_count = sum(df[df["Sensor_ID"] == sensors[i]]["Hourly_Counts"])
        new_frame = pd.DataFrame(frame.iloc[0].values).T
        new_frame.columns = frame.columns
        new_frame = new_frame.rename(columns={'Hourly_Counts': 'Cumulative_Counts'})
        new_frame["Cumulative_Counts"] = pedestrian_count
        sensor_info = s[s["sensor_id"] == sensors[i]].drop(columns=["sensor_id", "direction_1", "direction_2"])
        cols = list(sensor_info.columns)
        values = list(sensor_info.values[0])
        new_frame[cols] = values
        if top is None:
            top = new_frame
        else:
            top = top.append(new_frame)
    top = top.sort_values("Cumulative_Counts", ascending=False)[:x]
    return top


def combine_df(s, ped, mode, top_x):
    pedestrian_df = update_date_time(ped, mode)
    dates = pedestrian_df["Date_Time"].unique()
    acc_ped_df = None
    for i in progressbar(range(len(dates)), "Accumulating pedestrians / {}: ".format(mode)):
        frame = pedestrian_df[pedestrian_df["Date_Time"] == dates[i]]
        top = accumulate_pedestrians(frame, top_x, s)

        if acc_ped_df is None:
            acc_ped_df = top
        else:
            acc_ped_df = acc_ped_df.append(top)
    return acc_ped_df


if __name__ == "__main__":
    run_analysis = True
    open_from_web = False

    if open_from_web:
        client = Socrata("data.melbourne.vic.gov.au", None)

        sensor_locations = client.get("h57g-5234", limit=100)
        sensor_location_df = pd.DataFrame.from_records(sensor_locations)

        pedestrians = client.get("b2ak-trbp", limit=4000000)
        pedestrians_df = pd.DataFrame.from_records(pedestrians)
    else:
        sensor_location_df = pd.read_csv("Pedestrian_Counting_System_-_Sensor_Locations.csv")
        pedestrians_df = pd.read_csv("Pedestrian_Counting_System_-_Monthly__counts_per_hour.csv")

    monthly_df = combine_df(sensor_location_df, pedestrians_df, "month", 10)
    daily_df = combine_df(sensor_location_df, pedestrians_df, "day", 10)
