import numpy as np
import pandas as pd
import sys
import boto3
from sodapy import Socrata

BUFFER = 56


def progressbar(it, prefix="", size=50):
    """
    Implementation of the progress bar for better user experience as it gives feedback to the user while
    data is being processed.
    """
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
    """ Returns pandas data frame with updated "Date_Time" column

    In order to make it easier to identify the unique dates the hours have been removed from the "Date_Time" column
    when formatting for top 10 daily locations.
    Similarly when formatting for monthly locations both time as well as day has been removed.
    """
    print("-" * BUFFER)
    print("Converting date / time format to '{}'-ly frequency...".format(mode))
    df_c = df.copy()
    if mode == "day":
        dates = np.array([i[:10] for i in df["date_time"]])
        df_c["date_time"] = dates
        df_c = df_c.drop(columns=["id", "time", "direction_1", "direction_2"])
    elif mode == "month":
        dates = np.array([i[:7] for i in df["date_time"]])
        df_c["date_time"] = dates
        df_c = df_c.drop(columns=["id", "mdate", "day", "time", "direction_1", "direction_2"])
    return df_c


def accumulate_pedestrians(df, top_x, mode):
    """ Returns pandas data frame with all relevant data for top x locations

    Top 10 locations identified for a unique date (either day or month). Data that is irrelevant or cannot be
    combined had been dropped from the table
    """
    df1 = update_date_time(df, mode)
    dates = df1["date_time"].unique()
    dates.sort()
    acc_ped_df = None
    for i in progressbar(range(len(dates)), "Accumulating pedestrians / {}: ".format(mode)):
        top = None
        frame = df1[df1["date_time"] == dates[i]]
        sensors = frame["sensor_id"].unique()
        sensors.sort()
        for j in range(len(sensors)):
            sensor_info = frame[frame["sensor_id"] == sensors[j]]
            pedestrian_count = sum(map(int, sensor_info["hourly_counts"]))
            new_frame = pd.DataFrame(sensor_info.iloc[0].values).T
            new_frame.columns = sensor_info.columns
            new_frame = new_frame.rename(columns={'hourly_counts': 'cumulative_counts'})
            new_frame["cumulative_counts"] = pedestrian_count
            if top is None:
                top = new_frame
            else:
                top = top.append(new_frame)
        top = top.sort_values("cumulative_counts", ascending=False)[:top_x]
        if acc_ped_df is None:
            acc_ped_df = top
        else:
            acc_ped_df = acc_ped_df.append(top)
    return acc_ped_df


def get_aws_keys():
    """
    Manual inputs:  AWS access key
                    AWS secret access key
                    AWS bucket to store / retrieve the data to / from
    """
    bucket = input("Enter the AWS S3 bucket you would like to use:")
    acc_key = input("Enter the AWS access key for your S3 storage:")
    secret_key = input("Enter the AWS secret access key for your S3 storage:")
    return bucket, acc_key, secret_key


def upload_file_to_s3(file, b, a_key, secret):
    """Uploads a specified file to AWS S3 bucket
    Inputs: file name
            AWS access key
            AWS secret access key
            AWS bucket to store the data to
    """
    s3_client = boto3.client('s3',
                             aws_access_key_id=a_key,
                             aws_secret_access_key=secret)
    s3_client.upload_file(file, b, str(file))
    return 0


def query_data(b, a_key, secret):
    """Test query - Prints data for a given month directly from AWS S3 bucket
    Inputs: file name
            AWS access key
            AWS secret access key
            AWS bucket to store the data to
    """
    s3_client = boto3.client('s3',
                             aws_access_key_id=a_key,
                             aws_secret_access_key=secret)

    month = input("Chose a month you would like to query (YYYY-MM)")
    response = s3_client.select_object_content(Bucket=b, Key='monthly_data.csv',
                                               ExpressionType='SQL',
                                               Expression="SELECT \"sensor_name_y\" FROM s3object s where s."
                                                          "\"date_time\" = {}".format(month),
                                               InputSerialization={'CSV': {"FileHeaderInfo": "Use"},
                                                                   'CompressionType': 'NONE'},
                                               OutputSerialization={'CSV': {}},)

    for event in response['Payload']:
        if 'Records' in event:
            records = event['Records']['Payload'].decode('utf-8')
            print(records)
        elif 'Stats' in event:
            stats = event['Stats']['Details']
            print("Stats: bytesScanned/Processed - {}, bytesReturned - {} ".
                  format(stats['BytesScanned'], stats['BytesReturned']))


if __name__ == "__main__":
    arguments = sys.argv
    if len(arguments) > 1:
        if "-l" in arguments:
            open_from_web = False
        else:
            open_from_web = True
        if "-csv" in arguments:
            run_analysis = False
            monthly_file_name = input("Enter the name of the file that contains MONTHLY data")
            daily_file_name = input("Enter the name of the file that contains DAILY data")
        else:
            run_analysis = True
            monthly_file_name = "monthly_data.csv"
            daily_file_name = "daily_data.csv"
    else:
        open_from_web = True
        run_analysis = True
        monthly_file_name = "monthly_data.csv"
        daily_file_name = "daily_data.csv"

    print(["Loading data from local files...", "Loading data from the web API..."][open_from_web])

    if open_from_web:
        client = Socrata("data.melbourne.vic.gov.au", None)
        sensor_locations = client.get("h57g-5234", limit=100)
        sensor_location_df = pd.DataFrame.from_records(sensor_locations)

        pedestrians = client.get("b2ak-trbp", limit=4000000)
        pedestrians_df = pd.DataFrame.from_records(pedestrians)
        client.close()
    else:
        sensor_location_df = pd.read_csv("Pedestrian_Counting_System_-_Sensor_Locations.csv")
        pedestrians_df = pd.read_csv("Pedestrian_Counting_System_-_Monthly__counts_per_hour_.csv")

    if run_analysis:
        pedestrians_df = pedestrians_df.rename(columns={'Sensor_ID': 'sensor_id'})
        print("Merging data by sensor ID...")
        all_data_df = pd.merge(sensor_location_df, pedestrians_df, on="sensor_id")
        all_data_df.to_csv("all_data.csv", index=False)

        monthly_df = accumulate_pedestrians(all_data_df, 10, "month")
        print("Saving Monthly Data to CSV...")
        monthly_df.to_csv(monthly_file_name, index=False)

        daily_df = accumulate_pedestrians(all_data_df, 10, "day")
        print("Saving Daily Data to CSV...")
        daily_df.to_csv(daily_file_name, index=False)

    access_key, secret_access_key, s3_bucket = get_aws_keys()
    print("Uploading data to AWS S3...")
    upload_file_to_s3(monthly_file_name, access_key, secret_access_key, s3_bucket)
    upload_file_to_s3(daily_file_name, access_key, secret_access_key, s3_bucket)
    print("Querying data from AWS S3...")
    query_data(s3_bucket, access_key, secret_access_key)
    print("DONE!")
