import unittest

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructField, StringType, StructType
from jobs import ingest_cqc_care_directory


class CQC_Care_Directory_Tests(unittest.TestCase):

    TEST_CQC_CARE_DIRECTORY_FILE = "tests/test_data/example_cqc_care_directory.csv"

    REFORMAT_DICT = {
        "Column A": "name A",
        "Column B": "name B,description B",
        "Column C": "name C",
        "Column D": "name D,description D",
        "Column-E": "name E,description E",
    }

    def setUp(self):
        self.spark = SparkSession.builder.appName("test_ingest_cqc_care_directory").getOrCreate()

    def test_get_all_job_roles_per_location_df(self):
        columns = ["providerId", "locationId", "other_cols"]

        rows = [
            ("1-000000001", "1-000000001", "other_data"),
            ("1-000000002", "1-000000002", "other_data"),
            ("1-000000002", "1-000000003", "other_data"),
        ]

        df = self.spark.createDataFrame(rows, columns)

        locations_at_prov_df = ingest_cqc_care_directory.unique_providers_with_locations(df)

        self.assertEqual(locations_at_prov_df.count(), 2)
        self.assertEqual(locations_at_prov_df.columns, ["providerId", "locationIds"])

        provider1check_df = locations_at_prov_df.filter("providerId=='1-000000001'").select("locationIds")
        self.assertEqual(provider1check_df.collect(), [Row(locationIds=["1-000000001"])])

        locations_at_prov_df = locations_at_prov_df.collect()
        self.assertEqual(sorted(locations_at_prov_df[1]["locationIds"]), ["1-000000002", "1-000000003"])

    def test_get_distinct_provider_info(self):
        columns = [
            "providerId",
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
                "providerId",
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
            "locationId",
            "Column A",
            "Column B",
            "Column C",
            "Column D",
            "Column-E",
        ]

        rows = [
            ("1-000000001", "Y", "Y", "Y", None, None),
            ("1-000000002", None, "Y", None, "Y", None),
            ("1-000000003", None, None, None, None, "Y"),
        ]

        df = self.spark.createDataFrame(rows, columns)

        services_df = ingest_cqc_care_directory.reformat_cols(df, self.REFORMAT_DICT, "new_alias")

        self.assertEqual(services_df.count(), 3)
        self.assertEqual(services_df.columns, ["locationId", "new_alias"])

        services_df = services_df.collect()
        self.assertEqual(sorted(services_df[0]["new_alias"]), [["name A"], ["name B", "description B"], ["name C"]])
        self.assertEqual(
            sorted(services_df[1]["new_alias"]), [["name B", "description B"], ["name D", "description D"]]
        )
        self.assertEqual(sorted(services_df[2]["new_alias"]), [["name E", "description E"]])

    def test_reg_man_to_struct(self):
        register_manager_schema = StructType(
            fields=[
                StructField("locationId", StringType(), True),
                StructField("registered_manager_name", StringType(), True),
            ]
        )

        rows = [
            ("1-000000001", "Surname, Firstname"),
            ("1-000000002", "Surname, First Name"),
            ("1-000000003", None),
        ]

        df = self.spark.createDataFrame(data=rows, schema=register_manager_schema)

        df = ingest_cqc_care_directory.reg_man_to_struct(df)

        self.assertEqual(df.count(), 3)
        self.assertEqual(df.columns, ["locationId", "contacts"])

        collected_df = df.collect()
        self.assertEqual(
            collected_df[0]["contacts"],
            [
                Row(
                    personTitle="M",
                    personGivenName="Firstname",
                    personFamilyName="Surname",
                    personRoles="Registered Manager",
                )
            ],
        )
        self.assertEqual(
            collected_df[1]["contacts"],
            [
                Row(
                    personTitle="M",
                    personGivenName="First Name",
                    personFamilyName="Surname",
                    personRoles="Registered Manager",
                )
            ],
        )
        self.assertEqual(
            collected_df[2]["contacts"],
            [
                Row(
                    personTitle=None,
                    personGivenName=None,
                    personFamilyName=None,
                    personRoles=None,
                )
            ],
        )

    def test_gacservicetypes_to_struct(self):
        columns = [
            "locationId",
            "gacservicetypes",
        ]

        rows = [
            ("1-000000001", [["The name", "description"], ["The name 2", "description 2"]]),
            ("1-000000002", [["Another name", "Some other description"]]),
            ("1-000000003", []),
        ]

        df = self.spark.createDataFrame(rows, columns)

        df = ingest_cqc_care_directory.gacservicetypes_to_struct(df)

        self.assertEqual(df.count(), 3)
        self.assertEqual(df.columns, ["locationId", "gacservicetypes"])

        collected_df = df.collect()
        self.assertEqual(
            collected_df[0]["gacservicetypes"],
            [
                Row(name="The name", description="description"),
                Row(
                    name="The name 2",
                    description="description 2",
                ),
            ],
        )
        self.assertEqual(
            collected_df[1]["gacservicetypes"],
            [
                Row(
                    name="Another name",
                    description="Some other description",
                )
            ],
        )
        self.assertEqual(
            collected_df[2]["gacservicetypes"],
            [],
        )

    def test_specialisms_to_struct(self):
        columns = [
            "locationId",
            "specialisms",
        ]

        rows = [
            ("1-000000001", [["The name"], ["The name 2"]]),
            ("1-000000002", [["Another name"]]),
            ("1-000000003", []),
        ]

        df = self.spark.createDataFrame(rows, columns)

        df = ingest_cqc_care_directory.specialisms_to_struct(df)

        self.assertEqual(df.count(), 3)
        self.assertEqual(df.columns, ["locationId", "specialisms"])

        collected_df = df.collect()
        self.assertEqual(
            collected_df[0]["specialisms"],
            [
                Row(name="The name"),
                Row(name="The name 2"),
            ],
        )
        self.assertEqual(
            collected_df[1]["specialisms"],
            [Row(name="Another name")],
        )
        self.assertEqual(
            collected_df[2]["specialisms"],
            [],
        )

    def test_main(self):
        datasets = ingest_cqc_care_directory.main(self.TEST_CQC_CARE_DIRECTORY_FILE)

        provider_df = datasets[0]

        self.assertEqual(provider_df.count(), 4)
        self.assertEqual(
            provider_df.columns,
            [
                "providerId",
                "locationIds",
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

        location_df = datasets[1]

        self.assertEqual(location_df.count(), 10)
        self.assertEqual(
            location_df.columns,
            [
                "locationId",
                "providerId",
                "type",
                "name",
                "registrationdate",
                "numberofbeds",
                "website",
                "postaladdressline1",
                "postaladdresstowncity",
                "postaladdresscounty",
                "region",
                "postalcode",
                "carehome",
                "mainphonenumber",
                "localauthority",
                "organisationType",
                "registrationstatus",
                "regulatedactivities",
                "gacservicetypes",
                "specialisms",
            ],
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
