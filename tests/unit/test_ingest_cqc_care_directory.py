import shutil
import unittest

from pyspark.sql import SparkSession, Row

from jobs import ingest_cqc_care_directory


class CQC_Care_Directory_Tests(unittest.TestCase):

    TEST_CQC_CARE_DIRECTORY_FILE = "tests/test_data/example_cqc_care_directory.csv"

    REFORMAT_DICT = {
        "Column A": "name A",
        "Column B": ["name B", "description B"],
        "Column C": "name C",
        "Column D": ["name D", "description D"],
        "Column-E": ["name E", "description E"],
    }

    def setUp(self):
        self.spark = SparkSession.builder.appName("test_ingest_cqc_care_directory").getOrCreate()

    def test_get_all_job_roles_per_location_df(self):
        columns = ["providerid", "locationid", "other_cols"]

        rows = [
            ("1-000000001", "1-000000001", "other_data"),
            ("1-000000002", "1-000000002", "other_data"),
            ("1-000000002", "1-000000003", "other_data"),
        ]

        df = self.spark.createDataFrame(rows, columns)

        locations_at_prov_df = ingest_cqc_care_directory.unique_providers_with_locations(df)

        self.assertEqual(locations_at_prov_df.count(), 2)
        self.assertEqual(locations_at_prov_df.columns, ["providerid", "locationids"])

        provider1check_df = locations_at_prov_df.filter("providerid=='1-000000001'").select("locationids")
        self.assertEqual(provider1check_df.collect(), [Row(locationids=["1-000000001"])])

        locations_at_prov_df = locations_at_prov_df.collect()
        self.assertEqual(sorted(locations_at_prov_df[1]["locationids"]), ["1-000000002", "1-000000003"])

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
                "organisationType",
                "registrationstatus",
            ],
        )

    def test_reformat_cols(self):
        columns = [
            "locationid",
            "Column A",
            "Column B",
            "Column C",
            "Column D",
            "Column-E",
        ]

        rows = [
            ("1-000000001", "Y", "Y", "Y", "", ""),
            ("1-000000002", "", "Y", "", "Y", ""),
            ("1-000000003", "", "", "", "", "Y"),
        ]

        df = self.spark.createDataFrame(rows, columns)

        services_df = ingest_cqc_care_directory.reformat_cols(df, self.REFORMAT_DICT, "new_alias")

        self.assertEqual(services_df.count(), 3)
        self.assertEqual(services_df.columns, ["locationid", "new_alias"])

        services_df = services_df.collect()
        self.assertEqual(sorted(services_df[0]["new_alias"]), ["name A", "name B", "name C"])
        self.assertEqual(sorted(services_df[1]["new_alias"]), ["name B", "name D"])
        self.assertEqual(sorted(services_df[2]["new_alias"]), ["name E"])

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

        self.assertEqual(location_df.count(), 10)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
