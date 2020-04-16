import unittest
from configs.start import start_spark
from etl_jobs.etl_state_dimension import *


class EtlStateDimensionTests(unittest.TestCase):

    def start(self):
        self.spark = start_spark('EtlStateDimensionTests')

    def test_tr_create_state_dim(self):
        df_source = self.spark.read.csv('us-counties.txt', header=True)
        df_result = tr_create_state_dim(df_source)

        df_expected_result = self.spark.read.parquet('StateDim.parquet')

        col_count_expected = len(df_expected_result.columns)
        col_count_result = len(df_result.columns)

        lines_count_expected = df_expected_result.count()
        lines_count_result = df_result.count()

        col_expected = df_expected_result.columns
        col_result = [df_result.columns]

        self.assertEqual(col_count_expected, col_count_result)
        self.assertEqual(lines_count_expected, lines_count_result)
        self.assertTrue(col_result == col_expected)