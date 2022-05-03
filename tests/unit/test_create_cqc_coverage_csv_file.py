import unittest
from pyspark.sql import SparkSession

from jobs import create_cqc_coverage_csv_file as job


class Estimate2021JobsTests(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("test_create_cqc_coverage_csv_file").getOrCreate()

    def test_get_cqc_locations_df(self):
        pass

    def test_get_cqc_providers_df(self):
        pass

    def test_get_ascwds_workplace_df(self):
        pass

    def test_relabel_permission_col(self):
        pass

    def test_main(self):
        pass


if __name__ == "__main__":
    unittest.main(warnings="ignore")
