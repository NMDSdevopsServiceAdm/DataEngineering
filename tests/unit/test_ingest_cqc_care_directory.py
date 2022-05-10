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
                "organisationType",
                "registrationstatus",
            ],
        )

    def test_get_specialisms(self):
        columns = [
            "locationid",
            "Service_user_band_Children_0-18_years",
            "Service_user_band_Dementia",
            "Service_user_band_Learning_disabilities_or_autistic_spectrum_disorder",
            "Service_user_band_Mental_Health",
            "Service_user_band_Older_People",
            "Service_user_band_People_detained_under_the_Mental_Health_Act",
            "Service_user_band_People_who_misuse_drugs_and_alcohol",
            "Service_user_band_People_with_an_eating_disorder",
            "Service_user_band_Physical_Disability",
            "Service_user_band_Sensory_Impairment",
            "Service_user_band_Whole_Population",
            "Service_user_band_Younger_Adults",
        ]

        rows = [
            ("1-000000001", "Y", "Y", "Y", "", "", "", "", "", "", "", "", ""),
            ("1-000000002", "", "Y", "", "Y", "", "", "", "", "", "", "", ""),
            ("1-000000003", "", "", "", "", "", "", "", "", "", "", "Y", ""),
            ("1-000000004", "Y", "", "Y", "", "Y", "", "Y", "", "Y", "", "Y", ""),
            ("1-000000005", "", "Y", "", "Y", "", "Y", "", "Y", "", "Y", "", "Y"),
        ]

        df = self.spark.createDataFrame(rows, columns)

        services_df = ingest_cqc_care_directory.get_specialisms(df)

        self.assertEqual(services_df.count(), 5)
        self.assertEqual(services_df.columns, ["locationid", "specialisms"])

        services_df = services_df.collect()
        self.assertEqual(
            sorted(services_df[0]["specialisms"]), ["Caring for children", "Dementia", "Learning disabilities"]
        )
        self.assertEqual(sorted(services_df[1]["specialisms"]), ["Dementia", "Mental health conditions"])
        self.assertEqual(sorted(services_df[2]["specialisms"]), ["Services for everyone"])
        self.assertEqual(
            sorted(services_df[3]["specialisms"]),
            [
                "Caring for adults over 65 yrs",
                "Caring for children",
                "Learning disabilities",
                "Physical disabilities",
                "Services for everyone",
                "Substance misuse problems",
            ],
        )
        self.assertEqual(
            sorted(services_df[4]["specialisms"]),
            [
                "Caring for adults under 65 yrs",
                "Caring for people whose rights are restricted under the Mental Health Act",
                "Dementia",
                "Eating disorders",
                "Mental health conditions",
                "Sensory impairment",
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
