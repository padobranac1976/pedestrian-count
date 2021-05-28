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

    upload_file_to_s3(monthly_file_name)
    upload_file_to_s3(daily_file_name)
    print("DONE!")
