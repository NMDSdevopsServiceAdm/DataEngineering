import unittest
from jobs import denormalise_ascwds_dataset
from pyspark.sql import SparkSession
from mock import patch


class TestDenormaliseASCWDSDataset(unittest.TestCase):

    test_csv_path = "tests/test_data/example_csv.csv"
    test_csv_joinable_path = "tests/test_data/joinable.csv"

    def setUp(self):
        spark = SparkSession.builder \
            .appName("sfc_data_engineering_denormalise_ascwds_dataset") \
            .getOrCreate()

        self.df1 = spark.read.csv(self.test_csv_path, header=True)
        self.df2 = spark.read.csv(self.test_csv_joinable_path, header=True)

    def tearDown(self):
        pass

    @patch('jobs.denormalise_ascwds_dataset.worker_fields_required', ["col_a", "col_b", "col_c", "date_col"])
    @patch('jobs.denormalise_ascwds_dataset.workplace_fields_required', ["col_a",  "col_d", "col_date_2"])
    @patch('jobs.denormalise_ascwds_dataset.join_on_field', "col_a")
    def test_denormalise_datasets(self):

        self.assertEqual(len(self.df1.columns), 4)
        self.assertEqual(len(self.df2.columns), 3)
        self.assertEqual(self.df1.count(), 3)
        self.assertEqual(self.df2.count(), 3)

        df = denormalise_ascwds_dataset.denormalise(self.df1, self.df2)

        self.assertEqual(len(df.columns), 6)
        self.assertEqual(df.count(), 3)

    @patch('jobs.denormalise_ascwds_dataset.worker_fields_required', ["col_a", "date_col"])
    @patch('jobs.denormalise_ascwds_dataset.workplace_fields_required', ["col_a", "col_d", "col_date_2"])
    @patch('jobs.denormalise_ascwds_dataset.join_on_field', "col_a")
    def test_denormalise_datasets_returns_subset_of_columns(self):

        df = denormalise_ascwds_dataset.denormalise(self.df1, self.df2)

        self.assertEqual(len(df.columns), 4)
        self.assertEqual(
            df.columns, ["col_a", "date_col", "col_d", "col_date_2"])
        self.assertEqual(df.count(), 3)


if __name__ == '__main__':
    unittest.main()
