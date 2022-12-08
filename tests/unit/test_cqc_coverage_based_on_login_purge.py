import unittest
import shutil
from datetime import date
import warnings

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
)

from jobs import cqc_coverage_based_on_login_purge
from tests.test_file_generator import (
    generate_ascwds_workplace_file,
    generate_cqc_locations_file,
    generate_cqc_providers_file,
    generate_acswds_workplace_structure_file,
)


class PrepareLocationsTests(unittest.TestCase):

    TEST_ASCWDS_WORKPLACE_FILE = "tests/test_data/domain=ascwds/dataset=workplace"
    TEST_CQC_LOCATION_FILE = "tests/test_data/domain=cqc/dataset=location"
    TEST_CQC_PROVIDERS_FILE = "tests/test_data/domain=cqc/dataset=providers"
    TEST_ASCWDS_WORKPLACE_STRUCTURE = (
        "tests/test_data/domain=ascwds/tmp_workplace_structure"
    )
    DESTINATION = "tests/test_data/domain=data_engineering/dataset=locations_prepared/version=1.0.0"

    def setUp(self):
        self.spark = SparkSession.builder.appName(
            "test_cqc_coverage_based_on_login_purge"
        ).getOrCreate()
        generate_ascwds_workplace_file(self.TEST_ASCWDS_WORKPLACE_FILE)
        generate_cqc_locations_file(self.TEST_CQC_LOCATION_FILE)
        generate_cqc_providers_file(self.TEST_CQC_PROVIDERS_FILE)
        generate_acswds_workplace_structure_file(self.TEST_ASCWDS_WORKPLACE_STRUCTURE)

        warnings.simplefilter("ignore", ResourceWarning)

    def tearDown(self):
        try:
            shutil.rmtree(self.TEST_ASCWDS_WORKPLACE_FILE)
            shutil.rmtree(self.TEST_CQC_LOCATION_FILE)
            shutil.rmtree(self.TEST_CQC_PROVIDERS_FILE)
            shutil.rmtree(self.TEST_ASCWDS_WORKPLACE_STRUCTURE)
            shutil.rmtree(self.DESTINATION)
        except OSError:
            pass  # Ignore dir does not exist

    def test_main_successfully_runs(self):
        output_df = cqc_coverage_based_on_login_purge.main(
            self.TEST_ASCWDS_WORKPLACE_FILE,
            self.TEST_CQC_LOCATION_FILE,
            self.TEST_CQC_PROVIDERS_FILE,
            self.DESTINATION,
        )

        self.assertIsNotNone(output_df)
        self.assertEqual(output_df.count(), 35)

    #     self.assertEqual(
    #         output_df.columns,
    #         [
    #             "snapshot_date",
    #             "snapshot_year",
    #             "snapshot_month",
    #             "snapshot_day",
    #             "ascwds_workplace_import_date",
    #             "cqc_locations_import_date",
    #             "cqc_providers_import_date",
    #             "cqc_pir_import_date",
    #             "ons_import_date",
    #             "locationid",
    #             "location_type",
    #             "location_name",
    #             "organisation_type",
    #             "providerid",
    #             "provider_name",
    #             "orgid",
    #             "establishmentid",
    #             "registration_status",
    #             "registration_date",
    #             "deregistration_date",
    #             "carehome",
    #             "dormancy",
    #             "number_of_beds",
    #             "services_offered",
    #             "people_directly_employed",
    #             "job_count",
    #             "region",
    #             "postal_code",
    #             "constituency",
    #             "local_authority",
    #             "cqc_sector",
    #             "ons_region",
    #             "nhs_england_region",
    #             "country",
    #             "lsoa",
    #             "msoa",
    #             "stp",
    #             "clinical_commisioning_group",
    #             "rural_urban_indicator",
    #             "oslaua",
    #         ],
    #     )

    def test_get_ascwds_workplace_df(self):
        workplace_df = cqc_coverage_based_on_login_purge.get_ascwds_workplace_df(
            self.TEST_ASCWDS_WORKPLACE_FILE, "20200101"
        )

        self.assertEqual(workplace_df.columns[0], "locationid")
        self.assertEqual(workplace_df.columns[1], "establishmentid")
        self.assertEqual(workplace_df.columns[2], "import_date")
        self.assertEqual(workplace_df.columns[7], "ascwds_workplace_structure")
        self.assertEqual(workplace_df.count(), 10)

    def test_get_cqc_location_df(self):
        cqc_location_df = cqc_coverage_based_on_login_purge.get_cqc_location_df(
            self.TEST_CQC_LOCATION_FILE, "20200101"
        )

        self.assertEqual(cqc_location_df.columns[0], "locationid")
        self.assertEqual(cqc_location_df.columns[1], "providerid")
        self.assertEqual(cqc_location_df.columns[16], "import_date")
        self.assertEqual(cqc_location_df.count(), 14)

    def test_get_cqc_provider_df(self):
        cqc_provider_df = cqc_coverage_based_on_login_purge.get_cqc_provider_df(
            self.TEST_CQC_PROVIDERS_FILE, "20210105"
        )

        self.assertEqual(cqc_provider_df.columns[0], "providerid")
        self.assertEqual(cqc_provider_df.columns[1], "provider_name")
        self.assertEqual(cqc_provider_df.columns[2], "import_date")
        self.assertEqual(cqc_provider_df.count(), 3)

    def test_get_date_closest_to_search_date_returns_correct_date(self):
        date_search_list = [
            date(2022, 1, 1),
            date(2022, 2, 11),
            date(2022, 3, 21),
            date(2022, 3, 28),
            date(2022, 4, 3),
        ]
        test_date = date(2022, 3, 12)

        result = cqc_coverage_based_on_login_purge.get_date_closest_to_search_date(
            test_date, date_search_list
        )
        self.assertEqual(result, date(2022, 2, 11))

    def test_get_date_closest_to_search_date_returns_None_if_no_historical_dates_available(
        self,
    ):
        date_search_list = [
            date(2022, 1, 1),
            date(2022, 2, 11),
            date(2022, 3, 21),
            date(2022, 3, 28),
            date(2022, 4, 3),
        ]
        test_date = date(2019, 1, 1)

        result = cqc_coverage_based_on_login_purge.get_date_closest_to_search_date(
            test_date, date_search_list
        )
        self.assertEqual(result, None)

    def test_get_date_closest_to_search_date_with_single_valid_date_returns_date(self):
        date_search_list = [date(2020, 3, 31)]
        test_date = date(2022, 5, 1)

        result = cqc_coverage_based_on_login_purge.get_date_closest_to_search_date(
            test_date, date_search_list
        )
        self.assertEqual(result, date(2020, 3, 31))

    def test_get_unique_import_dates_from_cqc_location_dataset(self):
        cqc_location_df = cqc_coverage_based_on_login_purge.get_cqc_location_df(
            self.TEST_CQC_LOCATION_FILE, "20210101"
        )

        result = cqc_coverage_based_on_login_purge.get_unique_import_dates(
            cqc_location_df
        )
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 1)
        self.assertEqual(result, ["20220101"])

    def test_get_unique_import_dates(self):
        columns = ["import_date", "other_column"]
        rows = [
            (date(2023, 3, 19), "1"),
            (date(2023, 3, 19), "1"),
            (date(2023, 3, 19), "1"),
            (date(2023, 3, 19), "1"),
            (date(2023, 3, 19), "1"),
            (date(2023, 3, 19), "2"),
            (date(2023, 3, 19), "3"),
            (date(2023, 3, 19), "4"),
            (date(2010, 1, 1), "5"),
            (date(2011, 1, 1), "5"),
            (date(2012, 1, 1), "5"),
            (date(2013, 1, 1), "5"),
        ]
        df = self.spark.createDataFrame(rows, columns)

        result = cqc_coverage_based_on_login_purge.get_unique_import_dates(df)
        self.assertEqual(
            result,
            [
                date(2010, 1, 1),
                date(2011, 1, 1),
                date(2012, 1, 1),
                date(2013, 1, 1),
                date(2023, 3, 19),
            ],
        )

    def test_generate_closest_date_matrix(self):
        workplace_df = cqc_coverage_based_on_login_purge.get_ascwds_workplace_df(
            self.TEST_ASCWDS_WORKPLACE_FILE,
            # date(2021, 11, 29)
        )
        cqc_location_df = cqc_coverage_based_on_login_purge.get_cqc_location_df(
            self.TEST_CQC_LOCATION_FILE,
            # date(2022, 1, 4)
        )
        cqc_provider_df = cqc_coverage_based_on_login_purge.get_cqc_provider_df(
            self.TEST_CQC_PROVIDERS_FILE,
            # date(2022, 3, 29)
        )

        result = cqc_coverage_based_on_login_purge.generate_closest_date_matrix(
            workplace_df, cqc_location_df, cqc_provider_df
        )

        self.assertIsNotNone(result)
        # fmt: off
        self.assertEqual(result, [
            {"snapshot_date": "20190101", "asc_workplace_date": "20190101", "cqc_location_date": "20190101", "cqc_provider_date": None},
            {"snapshot_date": "20200101", "asc_workplace_date": "20200101", "cqc_location_date": "20200101", "cqc_provider_date": None},
            {"snapshot_date": "20210101", "asc_workplace_date": "20210101", "cqc_location_date": "20210101", "cqc_provider_date": "20200105"},
            {"snapshot_date": "20220101", "asc_workplace_date": "20220101", "cqc_location_date": "20220101", "cqc_provider_date": "20210105"},
        ])
        # fmt: on

    def test_purge_workplaces(self):
        columns = [
            "locationid",
            "orgid",
            "import_date",
            "isparent",
            "mupddate",
            "lastloggedin",
        ]
        rows = [
            ("1", "1", date(2023, 3, 19), "1", date(2018, 9, 5), date(2018, 9, 5)),
            ("2", "1", date(2023, 3, 19), "0", date(2019, 7, 10), date(2019, 7, 10)),
            ("3", "1", date(2023, 3, 19), "1", date(2020, 5, 15), date(2020, 5, 15)),
            ("4", "1", date(2023, 3, 19), "0", date(2021, 3, 20), date(2021, 3, 20)),
            ("5", "1", date(2023, 3, 19), "1", date(2022, 1, 25), date(2022, 1, 25)),
            ("6", "2", date(2023, 3, 19), "1", date(2021, 3, 18), date(2021, 3, 18)),
            ("7", "3", date(2023, 3, 19), "1", date(2021, 3, 19), date(2021, 3, 19)),
            ("8", "4", date(2023, 3, 19), "1", date(2021, 3, 20), date(2021, 3, 20)),
            ("9", "5", date(2010, 1, 1), "0", date(2010, 1, 1), date(2010, 1, 1)),
            ("9", "5", date(2011, 1, 1), "0", date(2010, 1, 1), date(2010, 1, 1)),
            ("9", "5", date(2012, 1, 1), "0", date(2010, 1, 1), date(2010, 1, 1)),
            ("9", "5", date(2013, 1, 1), "0", date(2010, 1, 1), date(2010, 1, 1)),
            ("10", "6", date(2023, 3, 19), "1", date(2021, 3, 19), date(2022, 3, 19)),
        ]
        df = self.spark.createDataFrame(rows, columns)
        df = cqc_coverage_based_on_login_purge.purge_workplaces(df)

        self.assertEqual(df.count(), 8)

        # asserts equivalent items are present in both sequences
        self.assertCountEqual(
            df.select("locationid").rdd.flatMap(lambda x: x).collect(),
            ["1", "3", "4", "5", "8", "9", "9", "10"],
        )

    def test_add_cqc_sector(self):
        columns = ["providerid", "provider_name"]
        rows = [
            ("1-000000001", "This is an MDC"),
            ("1-000000002", "Local authority council alert"),
            ("1-000000003", "The Royal Borough of Skills for Care"),
            ("1-000000004", "Not actually a borough"),
            ("1-000000005", "The Council of St Monica Trust"),
        ]
        df = self.spark.createDataFrame(rows, columns)

        df = cqc_coverage_based_on_login_purge.add_cqc_sector(df)
        self.assertEqual(df.count(), 5)

        df = df.collect()
        self.assertEqual(df[0]["cqc_sector"], "Local authority")
        self.assertEqual(df[1]["cqc_sector"], "Local authority")
        self.assertEqual(df[2]["cqc_sector"], "Local authority")
        self.assertEqual(df[3]["cqc_sector"], "Independent")
        self.assertEqual(df[4]["cqc_sector"], "Independent")

    def test_get_cqc_location_df_standardises_yorkshire_and_the_humber_region(self):
        cqc_locations = cqc_coverage_based_on_login_purge.get_cqc_location_df(
            self.TEST_CQC_LOCATION_FILE
        )

        yorks_and_the_humber_count = cqc_locations.filter(
            cqc_locations.region == "Yorkshire and The Humber"
        ).count()
        yorks_and_humberside_count = cqc_locations.filter(
            cqc_locations.region == "Yorkshire & Humberside"
        ).count()

        self.assertEqual(yorks_and_humberside_count, 0)
        self.assertEqual(yorks_and_the_humber_count, 5)

    def test_get_cqc_location_df_convert_dormancy_to_a_bool(self):
        cqc_locations = cqc_coverage_based_on_login_purge.get_cqc_location_df(
            self.TEST_CQC_LOCATION_FILE
        )

        dormancy_data_type = cqc_locations.schema["dormancy"].dataType

        self.assertEqual(dormancy_data_type, BooleanType())

        dormancy_count = cqc_locations.filter(cqc_locations.dormancy).count()

        self.assertEqual(dormancy_count, 5)

    def test_add_ascwds_workplace_structure(self):
        workplace_df = cqc_coverage_based_on_login_purge.add_ascwds_workplace_structure(
            self.TEST_ASCWDS_WORKPLACE_STRUCTURE
        )

        self.assertEqual(workplace_df.count(), 4)

        rows = workplace_df.collect()
        self.assertEqual(rows[0]["ascwds_workplace_structure"], "Parent")
        self.assertEqual(rows[1]["ascwds_workplace_structure"], "Subsidiary")
        self.assertEqual(rows[2]["ascwds_workplace_structure"], "Single")
        self.assertEqual(rows[3]["ascwds_workplace_structure"], "")


if __name__ == "__main__":
    unittest.main(warnings="ignore")
