import unittest
from jobs import format_fields
from jobs import prepare_locations
from pyspark.sql import SparkSession
from pathlib import Path
import shutil


class PrepareLocationsTests(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder \
            .appName("test_prepare_locations") \
            .getOrCreate()

    # def test_filter_nulls(self):

    #     columns = ["locationid", "wkrrecs", "totalstaff"]
    #     rows = [
    #         ("1-000000001", None, 20),
    #         ("1-000000002", 500, 500),
    #         ("1-000000003", 100, None),
    #         ("1-000000004", None, None),
    #         ("1-000000005", 25, 75),
    #         (None, 1, 0),
    #     ]
    #     df = self.spark.createDataFrame(rows, columns)

    #     filtered_df = prepare_locations.filter_nulls(df)
    #     self.assertEqual(filtered_df.count(), 4)
    #     self.assertEqual(filtered_df.select("locationid").rdd.flatMap(lambda x: x).collect(), [
    #                      "1-000000001", "1-000000002", "1-000000003", "1-000000005"])

    # def test_clean(self):

    #     columns = ["locationid", "wkrrecs", "totalstaff"]
    #     rows = [
    #         ("1-000000001", None, "0"),
    #         ("1-000000002", "500", "500"),
    #         ("1-000000003", "100", "-1"),
    #         ("1-000000004", None, "0"),
    #         ("1-000000005", "25", "75"),
    #         (None, "1", "0"),
    #     ]
    #     df = self.spark.createDataFrame(rows, columns)

    #     cleaned_df = prepare_locations.clean(df)
    #     cleaned_df_list = cleaned_df.collect()
    #     self.assertEqual(cleaned_df.count(), 6)
    #     self.assertEqual(cleaned_df_list[0]["totalstaff"], None)
    #     self.assertEqual(cleaned_df_list[1]["totalstaff"], 500)

    def test_calculate_jobcount(self):
        columns = ["locationid", "wkrrecs", "totalstaff", "numberofbeds"]
        rows = [
            ("1-000000001", None, 0, 0),  # Both 0: Return 0
            # Both 500: Return 500
            ("1-000000002", 500, 500, 490),
            # Only know wkrrecs: Return wkrrecs (100)
            ("1-000000003", 100, None, 10),
            # Only know totalstaff: Return totalstaf (10)
            ("1-000000004", None, 10, 12),
            # None of the rules apply: Return None
            ("1-000000005", 25, 75, 40),
            # None of the rules apply: Return None
            ("1-000000006", 30, 60, 40),
            # None of the rules apply: Return None
            ("1-000000007", 600, 900, 150),
            # Absolute difference is within 10%: Return Average
            ("1-000000008", 10, 12, None),
            # Either totalstaff or wkrrecs < 3: return max
            ("1-000000009", 1, 23, None),
            # Utilise bedcount estimate - Average
            ("1-000000010", 90, 102, 85),
            # Utilise bedcount estimate - Wkrrecs
            ("1-000000011", 90, 102, 95),
            # Utilise bedcount estimate - Totalstaff
            ("1-000000012", 90, 102, 80),

        ]
        df = self.spark.createDataFrame(rows, columns)

        jobcount_df = prepare_locations.calculate_jobcount(df)
        jobcount_df_list = jobcount_df.collect()
        jobcount_df.show()

        self.assertEqual(jobcount_df_list[0]["jobcount"], 0.0)
        self.assertEqual(jobcount_df_list[1]["jobcount"], 500.0)
        self.assertEqual(jobcount_df_list[2]["jobcount"], 100.0)
        self.assertEqual(jobcount_df_list[3]["jobcount"], 10.0)
        self.assertEqual(jobcount_df_list[4]["jobcount"], None)
        self.assertEqual(jobcount_df_list[5]["jobcount"], None)
        self.assertEqual(jobcount_df_list[6]["jobcount"], None)
        self.assertEqual(jobcount_df_list[7]["jobcount"], 11.0)
        self.assertEqual(jobcount_df_list[8]["jobcount"], 23.0)
        self.assertEqual(jobcount_df_list[9]["jobcount"], 96.0)
        self.assertEqual(jobcount_df_list[10]["jobcount"], 102.0)
        self.assertEqual(jobcount_df_list[11]["jobcount"], 90.0)


if __name__ == '__main__':
    unittest.main()
