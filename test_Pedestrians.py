import numpy as np
import pandas as pd
import sys
import boto3
from sodapy import Socrata
import unittest


class TestSocrata(unittest.TestCase):

    def test_pedestrian_data_columns(self):
        client = Socrata("data.melbourne.vic.gov.au", None)
        pedestrians = client.get("b2ak-trbp", limit=10)
        pedestrians_df = pd.DataFrame.from_records(pedestrians)
        self.assertEqual(list(pedestrians_df.columns),
                         ['id', 'date_time', 'year', 'month', 'mdate', 'day', 'time', 'sensor_id', 'sensor_name',
                          'hourly_counts'])
        client.close()

    def test_pedestrian_data_rows(self):
        client = Socrata("data.melbourne.vic.gov.au", None)
        pedestrians = client.get("b2ak-trbp", limit=5)
        pedestrians_df = pd.DataFrame.from_records(pedestrians)
        self.assertEqual(pedestrians_df.shape[0], 5, "There should be Should be {} records, {} have been found".
                         format(5, pedestrians_df.shape[0]))
        client.close()

    def test_sensor_data_columns(self):
        client = Socrata("data.melbourne.vic.gov.au", None)
        sensor_locations = client.get("h57g-5234", limit=10)
        sensor_location_df = pd.DataFrame.from_records(sensor_locations)
        self.assertEqual(list(sensor_location_df.columns),
                         ['sensor_id', 'sensor_description', 'sensor_name', 'installation_date', 'status',
                          'direction_1', 'direction_2', 'latitude', 'longitude', 'location', 'note'])
        client.close()

    def test_sensor_data_rows(self):
        client = Socrata("data.melbourne.vic.gov.au", None)
        sensor_locations = client.get("h57g-5234", limit=5)
        sensor_location_df = pd.DataFrame.from_records(sensor_locations)
        self.assertEqual(sensor_location_df.shape[0], 5, "There should be Should be {} records, {} have been found".
                         format(5, sensor_location_df.shape[0]))
        client.close()


if __name__ == '__main__':
    unittest.main()
