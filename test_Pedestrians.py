import pandas as pd
from sodapy import Socrata
import unittest
import Pedestrians
# from s3_api import s3_bucket, access_key, secret_access_key  # Uncomment only if you have stored the info in s3_api.py


class TestSocrata(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        client = Socrata("data.melbourne.vic.gov.au", None)
        pedestrians = client.get("b2ak-trbp", limit=5)
        sensor_locations = client.get("h57g-5234", limit=10)
        self.sensor_location_df = pd.DataFrame.from_records(sensor_locations)
        self.pedestrians_df = pd.DataFrame.from_records(pedestrians)
        client.close()

    def test_pedestrian_data_loading(self):
        self.assertEqual(list(self.pedestrians_df.columns),
                         ['id', 'date_time', 'year', 'month', 'mdate', 'day', 'time', 'sensor_id', 'sensor_name',
                          'hourly_counts'])

        self.assertEqual(self.pedestrians_df.shape[0], 5, "There should be Should be {} records, {} have been found".
                         format(5, self.pedestrians_df.shape[0]))

    def test_sensor_data_loading(self):
        self.assertEqual(list(self.sensor_location_df.columns),
                         ['sensor_id', 'sensor_description', 'sensor_name', 'installation_date', 'status',
                          'direction_1', 'direction_2', 'latitude', 'longitude', 'location', 'note'])

        self.assertEqual(self.sensor_location_df.shape[0], 10, "There should be Should be {} records, {} have been "
                                                               "found".format(10, self.sensor_location_df.shape[0]))


class TestLogic(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        client = Socrata("data.melbourne.vic.gov.au", None)
        pedestrians = client.get("b2ak-trbp", limit=500)
        self.pedestrians_df = pd.DataFrame.from_records(pedestrians)
        sensor_locations = client.get("h57g-5234", limit=10)
        self.sensor_location_df = pd.DataFrame.from_records(sensor_locations)
        client.close()
        self.all_data_df = pd.merge(self.sensor_location_df, self.pedestrians_df, on="sensor_id")
        self.month_data_df = Pedestrians.update_date_time(self.all_data_df, "month")
        self.day_data_df = Pedestrians.update_date_time(self.all_data_df, "day")

    def test_data_merge(self):
        answer = ['sensor_id', 'sensor_description', 'sensor_name_x', 'installation_date', 'status', 'direction_1',
                  'direction_2', 'latitude', 'longitude', 'location', 'note', 'id', 'date_time', 'year', 'month',
                  'mdate', 'day', 'time', 'sensor_name_y', 'hourly_counts']
        self.assertEqual(list(self.all_data_df.columns), answer)

        self.assertEqual(self.all_data_df.shape[0], 64, "There should be Should be {} records, {} have been found".
                         format(13, self.all_data_df.shape[0]))

    def test_date_update_for_month(self):
        answer = ['sensor_id', 'sensor_description', 'sensor_name_x', 'installation_date', 'status', 'latitude',
                  'longitude', 'location', 'note', 'date_time', 'year', 'month', 'sensor_name_y', 'hourly_counts']
        self.assertEqual(list(self.month_data_df.columns), answer)
        self.assertEqual(self.month_data_df["date_time"][0], '2019-11')

    def test_date_update_for_day(self):
        answer = ['sensor_id', 'sensor_description', 'sensor_name_x', 'installation_date', 'status', 'latitude',
                  'longitude', 'location', 'note', 'date_time', 'year', 'month', 'mdate', 'day', 'sensor_name_y',
                  'hourly_counts']
        self.assertEqual(list(self.day_data_df.columns), answer)
        self.assertEqual(self.day_data_df["date_time"][0], '2019-11-01')

    def test_accumulate_pedestrians_for_month(self):
        new_frame = Pedestrians.accumulate_pedestrians(self.all_data_df, 10, "month")
        self.assertEqual(list(new_frame["cumulative_counts"][0:3]), [418, 12082, 11323])

    def test_accumulate_pedestrians_for_day(self):
        new_frame = Pedestrians.accumulate_pedestrians(self.all_data_df, 10, "day")
        self.assertEqual(list(new_frame["cumulative_counts"][0:3]), [418, 11284, 10610])


## uncomment once you have saved s3_bucket, access_key and secret_access_key in a file "s3_api.py"
# class TestAWS(unittest.TestCase):
#     def test_file_upload(self):
#         file = "monthly_data.csv"
#         answer = Pedestrians.upload_file_to_s3(file, s3_bucket, access_key, secret_access_key)
#         self.assertEqual(answer, 0)
#
#     def test_query(self):
#         month = '2021-01'
#         answer = Pedestrians.query_data(s3_bucket, access_key, secret_access_key, month)[:23]
#         self.assertEqual(answer, "Flinders La-Swanston St")


if __name__ == '__main__':
    unittest.main()
