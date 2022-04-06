import shutil
import unittest

from pyspark.sql import SparkSession

from jobs import ethnicity_breakdown


class EthnicityBreakdownTests(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("test_ethnicity_breakdown").getOrCreate()

    def get_ascwds_ethnicity_df(self):
        path = "tests/test_data/domain=ASCWDS/dataset=worker/version=0.0.1/format=parquet"
        import_date = "20220301"
        df = ethnicity_breakdown.get_ascwds_ethnicity_df(path, import_date, "tests/test_data/")

        self.assertEqual(df.count(), 10)

        self.assertEqual(df.columns[0], "locationid")
        self.assertEqual(df.columns[1], "mainjrid")
        self.assertEqual(df.columns[2], "ethnicity")

        df = df.collect()
        self.assertEqual(df[0]["mainjrid"], 1)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
