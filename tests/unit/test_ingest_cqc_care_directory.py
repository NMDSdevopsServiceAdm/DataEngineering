import shutil
import unittest

from pyspark.sql import SparkSession

from jobs import ingest_cqc_care_directory


class CQC_Care_Directory_Tests(unittest.TestCase):

    TEST_CQC_CARE_DIRECTORY_FILE = "tests/test_data/example_cqc_care_directory.csv"

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

    def test_get_distinct_provider_info(self):
        columns = [
            "providerid",
            "provider_brandid",
            "provider_brandname",
            "provider_name",
            "provider_mainphonenumber",
            "provider_website",
            "provider_postaladdressline1",
            "provider_postaladdressline2",
            "provider_postaladdresstowncity",
            "provider_postaladdresscounty",
            "provider_postalcode",
            "provider_nominated_individual_name",
        ]

        rows = [
            ("1-000000001", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1"),
            ("1-000000002", "2", "2", "2", "2", "2", "2", "2", "2", "2", "2", "2"),
            ("1-000000002", "2", "2", "2", "2", "2", "2", "2", "2", "2", "2", "2"),
            ("1-000000003", "3", "3", "3", "3", "3", "3", "3", "3", "3", "3", "3"),
            ("1-000000003", "3", "3", "3", "3", "3", "3", "3", "3", "3", "3", "3"),
            ("1-000000003", "3", "3", "3", "3", "3", "3", "3", "3", "3", "3", "3"),
            ("1-000000004", "4", "4", "4", "4", "4", "4", "4", "4", "4", "4", "4"),
            ("1-000000004", "4", "4", "4", "4", "4", "4", "4", "4", "4", "4", "4"),
            ("1-000000004", "4", "4", "4", "4", "4", "4", "4", "4", "4", "4", "4"),
            ("1-000000004", "4", "4", "4", "4", "4", "4", "4", "4", "4", "4", "4"),
        ]

        df = self.spark.createDataFrame(rows, columns)

        distinct_prov_df = ingest_cqc_care_directory.get_distinct_provider_info(df)

        self.assertEqual(distinct_prov_df.count(), 4)
        self.assertEqual(
            distinct_prov_df.columns,
            [
                "providerid",
                "brandid",
                "brandname",
                "name",
                "mainPhoneNumber",
                "website",
                "postalAddressLine1",
                "postaladdressline2",
                "postalAddressTownCity",
                "postalAddressCounty",
                "postalCode",
                "nominated_individual_name",
            ],
        )

    def test_main(self):
        datasets = ingest_cqc_care_directory.main(self.TEST_CQC_CARE_DIRECTORY_FILE)

        provider_df = datasets[0]
        location_df = datasets[1]

        self.assertEqual(provider_df.count(), 4)
        self.assertEqual(
            provider_df.columns,
            [
                "providerid",
                "locationids",
                "brandid",
                "brandname",
                "name",
                "mainPhoneNumber",
                "website",
                "postalAddressLine1",
                "postaladdressline2",
                "postalAddressTownCity",
                "postalAddressCounty",
                "postalCode",
                "nominated_individual_name",
                "organisationType",
                "registrationstatus",
            ],
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
