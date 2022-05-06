import shutil
import unittest

from pyspark.sql import SparkSession

from jobs import ingest_cqc_care_directory


class CQC_Care_Directory_Tests(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("test_ingest_cqc_care_directory").getOrCreate()

    def test_get_all_job_roles_per_location_df(self):
        columns = ["providerid", "locationid", "other_cols"]

        rows = [
            ("1-000000001", "1-000000001", "other_data"),
            ("1-000000002", "1-000000002", "other_data"),
            ("1-000000002", "1-000000003", "other_data"),
            ("1-000000003", "1-000000004", "other_data"),
            ("1-000000003", "1-000000005", "other_data"),
            ("1-000000003", "1-000000006", "other_data"),
            ("1-000000004", "1-000000007", "other_data"),
            ("1-000000004", "1-000000008", "other_data"),
            ("1-000000004", "1-000000009", "other_data"),
            ("1-000000004", "1-000000010", "other_data"),
        ]

        df = self.spark.createDataFrame(rows, columns)

        locations_at_prov_df = ingest_cqc_care_directory.unique_providers_with_locations(df)

        self.assertEqual(locations_at_prov_df.count(), 4)
        self.assertEqual(locations_at_prov_df.columns, ["providerid", "locationids"])

        locations_at_prov_df = locations_at_prov_df.collect()
        self.assertEqual(sorted(locations_at_prov_df[0]["locationids"]), ["1-000000001"])
        self.assertEqual(sorted(locations_at_prov_df[1]["locationids"]), ["1-000000002", "1-000000003"])
        self.assertEqual(sorted(locations_at_prov_df[2]["locationids"]), ["1-000000004", "1-000000005", "1-000000006"])
        self.assertEqual(
            sorted(locations_at_prov_df[3]["locationids"]), ["1-000000007", "1-000000008", "1-000000009", "1-000000010"]
        )

    def test_main(self):
        pass

        # result_df = ingest_cqc_care_directory.main(
        #     self.TEST_CQC_PROV_LOC_FILE,
        # )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
