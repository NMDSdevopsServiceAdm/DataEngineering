import unittest

from pyspark.sql import SparkSession
from datetime import date

from jobs import stayer_vs_leaver_dataset


class CQC_Care_Directory_Tests(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("test_ingest_cqc_care_directory").getOrCreate()

    def test_updated_within_time_period(self):

        columns = [
            "establishmentid",
            "mupddate",
            "import_date",
            "other_col",
        ]

        rows = [
            ("1", date(2020, 1, 14), "20200715", "abc"),
            ("2", date(2020, 1, 15), "20200715", "abc"),
            ("3", date(2020, 1, 16), "20200715", "abc"),
        ]

        df = self.spark.createDataFrame(rows, columns)

        filtered_df = stayer_vs_leaver_dataset.updated_within_time_period(df)

        self.assertEqual(filtered_df.count(), 1)
        self.assertEqual(filtered_df.columns, ["establishmentid"])

        collected_df = filtered_df.collect()
        self.assertEqual(collected_df[0]["establishmentid"], "3")

    def test_main(self):
        pass


if __name__ == "__main__":
    unittest.main(warnings="ignore")
