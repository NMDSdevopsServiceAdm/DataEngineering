import unittest
import shutil
from datetime import date
import warnings

from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    BooleanType,
)

import jobs.prepare_locations as job
from utils import utils
from tests.test_file_generator import (
    generate_ascwds_workplace_file,
    generate_cqc_locations_file,
    generate_cqc_providers_file,
    generate_ons_denormalised_data,
    generate_pir_file,
)


class PrepareLocationsTests(unittest.TestCase):
    TEST_ASCWDS_WORKPLACE_FILE = "tests/test_data/domain=ascwds/dataset=workplace"
    TEST_CQC_LOCATION_FILE = "tests/test_data/domain=cqc/dataset=location"
    TEST_CQC_PROVIDERS_FILE = "tests/test_data/domain=cqc/dataset=providers"
    TEST_PIR_FILE = "tests/test_data/domain=cqc/dataset=pir"
    TEST_ONS_FILE = "tests/test_data/domain=ons/dataset=postcodes"
    DESTINATION = "tests/test_data/domain=data_engineering/dataset=locations_prepared/version=1.0.0"

    def setUp(self):
        self.spark = utils.get_spark()
        generate_ascwds_workplace_file(self.TEST_ASCWDS_WORKPLACE_FILE)
        self.cqc_loc_df = generate_cqc_locations_file(self.TEST_CQC_LOCATION_FILE)
        generate_cqc_providers_file(self.TEST_CQC_PROVIDERS_FILE)
        generate_pir_file(self.TEST_PIR_FILE)
        self.ons_df = generate_ons_denormalised_data(self.TEST_ONS_FILE)

        warnings.simplefilter("ignore", ResourceWarning)

    def tearDown(self):
        try:
            shutil.rmtree(self.TEST_ASCWDS_WORKPLACE_FILE)
            shutil.rmtree(self.TEST_CQC_LOCATION_FILE)
            shutil.rmtree(self.TEST_CQC_PROVIDERS_FILE)
            shutil.rmtree(self.TEST_PIR_FILE)
            shutil.rmtree(self.TEST_ONS_FILE)
            shutil.rmtree(self.DESTINATION)
        except OSError:
            pass  # Ignore dir does not exist

    def test_main_successfully_runs(self):
        output_df = job.main(
            self.TEST_ASCWDS_WORKPLACE_FILE,
            self.TEST_CQC_LOCATION_FILE,
            self.TEST_CQC_PROVIDERS_FILE,
            self.TEST_PIR_FILE,
            self.TEST_ONS_FILE,
            self.DESTINATION,
        )

        self.assertIsNotNone(output_df)
        self.assertEqual(output_df.count(), 28)
        self.assertEqual(
            output_df.columns,
            [
                "snapshot_date",
                "snapshot_year",
                "snapshot_month",
                "snapshot_day",
                "ascwds_workplace_import_date",
                "cqc_locations_import_date",
                "cqc_providers_import_date",
                "cqc_pir_import_date",
                "ons_import_date",
                "locationid",
                "location_type",
                "location_name",
                "organisation_type",
                "providerid",
                "provider_name",
                "orgid",
                "establishmentid",
                "cqc_coverage_in_ascwds",
                "registration_status",
                "registration_date",
                "deregistration_date",
                "carehome",
                "dormancy",
                "number_of_beds",
                "services_offered",
                "primary_service_type",
                "people_directly_employed",
                "total_staff",
                "worker_record_count",
                "region",
                "postal_code",
                "constituency",
                "local_authority",
                "cqc_sector",
                "ons_region",
                "nhs_england_region",
                "lsoa",
                "msoa",
                "clinical_commisioning_group",
                "rural_urban_indicator",
            ],
        )

    def test_get_ascwds_workplace_df(self):
        workplace_df = job.get_ascwds_workplace_df(
            self.TEST_ASCWDS_WORKPLACE_FILE, "20200101"
        )

        self.assertEqual(workplace_df.columns[0], "locationid")
        self.assertEqual(workplace_df.columns[1], "establishmentid")
        self.assertEqual(workplace_df.columns[2], "total_staff")
        self.assertEqual(workplace_df.columns[3], "worker_record_count")
        self.assertEqual(workplace_df.columns[4], "import_date")
        self.assertEqual(workplace_df.columns[8], "parentid")
        self.assertEqual(workplace_df.columns[9], "lastloggedin")
        self.assertEqual(workplace_df.count(), 10)

    def test_get_cqc_location_df(self):
        cqc_location_df = job.get_cqc_location_df(
            self.TEST_CQC_LOCATION_FILE, "20200101"
        )

        self.assertEqual(cqc_location_df.columns[0], "locationid")
        self.assertEqual(cqc_location_df.columns[1], "providerid")
        self.assertEqual(cqc_location_df.columns[16], "import_date")
        self.assertEqual(cqc_location_df.columns[17], "primary_service_type")
        self.assertEqual(cqc_location_df.count(), 14)

        rows = cqc_location_df.collect()
        self.assertEqual(rows[12]["registration_date"], date(2011, 2, 15))
        self.assertEqual(rows[12]["deregistration_date"], date(2015, 1, 1))

    def test_determine_ascwds_primary_service_type(self):
        columns = ["locationid", "services_offered"]
        # fmt: off
        rows = [
            ("1-000000001", ["Care home service with nursing", "Care home service without nursing", "Fake service"]),
            ("1-000000002", ["Care home service without nursing", "Fake service"]),
            ("1-000000003", ["Fake service"]),
            ("1-000000004", []),
        ]
        df = self.spark.createDataFrame(rows, columns)

        df = job.allocate_primary_service_type(df)
        self.assertEqual(df.count(), 4)

        df = df.collect()
        self.assertEqual(df[0]["primary_service_type"], "Care home with nursing")
        self.assertEqual(df[1]["primary_service_type"], "Care home without nursing")
        self.assertEqual(df[2]["primary_service_type"], "non-residential")
        self.assertEqual(df[3]["primary_service_type"], "non-residential")

    def test_get_cqc_provider_df(self):
        cqc_provider_df = job.get_cqc_provider_df(
            self.TEST_CQC_PROVIDERS_FILE, "20210105"
        )

        self.assertEqual(cqc_provider_df.columns[0], "providerid")
        self.assertEqual(cqc_provider_df.columns[1], "provider_name")
        self.assertEqual(cqc_provider_df.columns[2], "import_date")
        self.assertEqual(cqc_provider_df.count(), 3)

    def test_get_pir_df(self):
        pir_df = job.get_pir_df(self.TEST_PIR_FILE, "20210105")

        self.assertEqual(pir_df.count(), 8)
        self.assertEqual(len(pir_df.columns), 3)

        self.assertEqual(pir_df.columns[0], "locationid")
        self.assertEqual(pir_df.columns[1], "people_directly_employed")
        self.assertEqual(pir_df.columns[2], "import_date")

        data_type_of_column = dict(pir_df.dtypes)["people_directly_employed"]
        self.assertEqual(data_type_of_column, "int")

    def test_get_ons_df(self):
        ons_df = job.get_ons_df(self.TEST_ONS_FILE)

        self.assertEqual(ons_df.count(), 3)
        self.assertEqual(len(ons_df.columns), 11)

        self.assertIn("nhs_england_region", ons_df.columns)
        self.assertIn("ons_region", ons_df.columns)
        self.assertIn("ons_import_date", ons_df.columns)

    def test_map_illegitimate_postcodes_is_replacing_wrong_postcodes(self):
        df = job.map_illegitimate_postcodes(self.cqc_loc_df, "postalCode")

        rows = df.collect()

        self.assertEqual(rows[0]["postal_code"], "UB4 0EJ")
        self.assertEqual(rows[1]["postal_code"], "OX29 9UB")
        self.assertEqual(rows[5]["postal_code"], "YO61 3FN")
        self.assertEqual(rows[10]["postal_code"], "HU170LS")
        self.assertEqual(rows[21]["postal_code"], "TS20 2BL")
        self.assertIsNone(rows[-1]["postal_code"])

    def test_get_date_closest_to_search_date_returns_correct_date(self):
        date_search_list = [
            date(2022, 1, 1),
            date(2022, 2, 11),
            date(2022, 3, 21),
            date(2022, 3, 28),
            date(2022, 4, 3),
        ]
        test_date = date(2022, 3, 12)

        result = job.get_date_closest_to_search_date(test_date, date_search_list)
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

        result = job.get_date_closest_to_search_date(test_date, date_search_list)
        self.assertEqual(result, None)

    def test_get_date_closest_to_search_date_with_single_valid_date_returns_date(self):
        date_search_list = [date(2020, 3, 31)]
        test_date = date(2022, 5, 1)

        result = job.get_date_closest_to_search_date(test_date, date_search_list)
        self.assertEqual(result, date(2020, 3, 31))

    def test_get_unique_import_dates_from_cqc_location_dataset(self):
        cqc_location_df = job.get_cqc_location_df(
            self.TEST_CQC_LOCATION_FILE, "20210101"
        )

        result = job.get_unique_import_dates(cqc_location_df)
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

        result = job.get_unique_import_dates(df)
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
        workplace_df = job.get_ascwds_workplace_df(
            self.TEST_ASCWDS_WORKPLACE_FILE,
            # date(2021, 11, 29)
        )
        cqc_location_df = job.get_cqc_location_df(
            self.TEST_CQC_LOCATION_FILE,
            # date(2022, 1, 4)
        )
        cqc_provider_df = job.get_cqc_provider_df(
            self.TEST_CQC_PROVIDERS_FILE,
            # date(2022, 3, 29)
        )
        pir_df = job.get_pir_df(
            self.TEST_PIR_FILE,
            #  date(2020, 3, 30)
        )

        result = job.generate_closest_date_matrix(
            workplace_df, cqc_location_df, cqc_provider_df, pir_df
        )

        self.assertIsNotNone(result)
        # fmt: off
        self.assertEqual(result, [
            {"snapshot_date": "20190101", "asc_workplace_date": "20190101", "cqc_location_date": "20190101", "cqc_provider_date": None, "pir_date": None},
            {"snapshot_date": "20200101", "asc_workplace_date": "20200101", "cqc_location_date": "20200101", "cqc_provider_date": None, "pir_date": None},
            {"snapshot_date": "20210101", "asc_workplace_date": "20210101", "cqc_location_date": "20210101", "cqc_provider_date": "20200105", "pir_date": None},
            {"snapshot_date": "20220101", "asc_workplace_date": "20220101", "cqc_location_date": "20220101", "cqc_provider_date": "20210105", "pir_date": "20210105"},
        ])
        # fmt: on

    def test_clean(self):
        columns = ["locationid", "worker_record_count", "total_staff"]
        rows = [
            ("1-000000001", None, "0"),
            ("1-000000002", "500", "500"),
            ("1-000000003", "100", "-1"),
            ("1-000000004", None, "0"),
            ("1-000000005", "25", "75"),
            (None, "1", "0"),
        ]
        df = self.spark.createDataFrame(rows, columns)

        cleaned_df = job.clean(df)
        cleaned_df_list = cleaned_df.collect()
        self.assertEqual(cleaned_df.count(), 6)
        self.assertEqual(cleaned_df_list[0]["total_staff"], None)
        self.assertEqual(cleaned_df_list[1]["total_staff"], 500)

    def test_add_geographical_data(self):
        columns = ["locationid", "postal_code"]
        rows = [
            ("1-000000001", "SW100AA"),
            ("1-000000002", "SW10 0AA"),
            ("1-000000003", "sw10 0AA"),
            ("1-000000004", "SW10 0AB"),
        ]
        locations_df = self.spark.createDataFrame(rows, columns)
        ons_df = job.get_ons_df(self.TEST_ONS_FILE)

        df = job.add_geographical_data(locations_df, ons_df)

        location_one = df.where(df.locationid == "1-000000001").first()
        self.assertEqual(location_one.ons_region, "London")

        location_two = df.where(df.locationid == "1-000000002").first()
        self.assertEqual(location_two.ons_region, "London")

        location_three = df.where(df.locationid == "1-000000003").first()
        self.assertEqual(location_three.ons_region, "London")

        location_four = df.where(df.locationid == "1-000000004").first()
        self.assertEqual(location_four.nhs_england_region, "London")

    def test_purge_workplaces(self):
        columns = ["locationid", "import_date", "orgid", "isparent", "mupddate"]
        rows = [
            ("1", date(2023, 3, 19), "1", "1", date(2018, 9, 5)),
            ("2", date(2023, 3, 19), "1", "0", date(2019, 7, 10)),
            ("3", date(2023, 3, 19), "1", "1", date(2020, 5, 15)),
            ("4", date(2023, 3, 19), "1", "0", date(2021, 3, 20)),
            ("5", date(2023, 3, 19), "1", "1", date(2022, 1, 25)),
            ("6", date(2023, 3, 19), "2", "1", date(2021, 3, 18)),
            ("7", date(2023, 3, 19), "3", "1", date(2021, 3, 19)),
            ("8", date(2023, 3, 19), "4", "1", date(2021, 3, 20)),
            ("9", date(2010, 1, 1), "5", "0", date(2010, 1, 1)),
            ("9", date(2011, 1, 1), "5", "0", date(2010, 1, 1)),
            ("9", date(2012, 1, 1), "5", "0", date(2010, 1, 1)),
            ("9", date(2013, 1, 1), "5", "0", date(2010, 1, 1)),
        ]
        df = self.spark.createDataFrame(rows, columns)
        df = job.purge_workplaces(df)

        self.assertEqual(df.count(), 7)

        # asserts equivalent items are present in both sequences
        self.assertCountEqual(
            df.select("locationid").rdd.flatMap(lambda x: x).collect(),
            ["1", "3", "4", "5", "8", "9", "9"],
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

        df = job.add_cqc_sector(df)
        self.assertEqual(df.count(), 5)

        df = df.collect()
        self.assertEqual(df[0]["cqc_sector"], "Local authority")
        self.assertEqual(df[1]["cqc_sector"], "Local authority")
        self.assertEqual(df[2]["cqc_sector"], "Local authority")
        self.assertEqual(df[3]["cqc_sector"], "Independent")
        self.assertEqual(df[4]["cqc_sector"], "Independent")

    def test_get_cqc_location_df_standardises_yorkshire_and_the_humber_region(self):
        cqc_locations = job.get_cqc_location_df(self.TEST_CQC_LOCATION_FILE)

        yorks_and_the_humber_count = cqc_locations.filter(
            cqc_locations.region == "Yorkshire and The Humber"
        ).count()
        yorks_and_humberside_count = cqc_locations.filter(
            cqc_locations.region == "Yorkshire & Humberside"
        ).count()

        self.assertEqual(yorks_and_humberside_count, 0)
        self.assertEqual(yorks_and_the_humber_count, 5)

    def test_get_cqc_location_df_convert_dormancy_to_a_bool(self):
        cqc_locations = job.get_cqc_location_df(self.TEST_CQC_LOCATION_FILE)

        dormancy_data_type = cqc_locations.schema["dormancy"].dataType

        self.assertEqual(dormancy_data_type, BooleanType())

        dormancy_count = cqc_locations.filter(cqc_locations.dormancy).count()

        self.assertEqual(dormancy_count, 5)

    def test_create_coverage_df(self):
        spark = utils.get_spark()

        columns = [
            "locationid",
            "establishmentid",
            "orgid",
            "import_date",
            "isparent",
            "mupddate",
            "lastloggedin",
        ]
        # fmt: off
        rows = [
            ("1", "1", "1", date(2023, 3, 19), "1", date(2018, 9, 5), date(2018, 9, 5)),
            ("2", "2", "1", date(2023, 3, 19), "0", date(2019, 7, 10), date(2019, 7, 10)),
            ("3", "3", "1", date(2023, 3, 19), "1", date(2020, 5, 15), date(2020, 5, 15)),
            ("4", "4", "1", date(2023, 3, 19), "0", date(2021, 3, 20), date(2021, 3, 20)),
            ("5", "5", "1", date(2023, 3, 19), "1", date(2022, 1, 25), date(2022, 1, 25)),
            ("6", "6", "2", date(2023, 3, 19), "1", date(2021, 3, 18), date(2021, 3, 18)),
            ("7", "7", "3", date(2023, 3, 19), "1", date(2021, 3, 19), date(2021, 3, 19)),
            ("8", "8", "4", date(2023, 3, 19), "1", date(2021, 3, 20), date(2021, 3, 20)),
            ("9", "9", "5", date(2010, 1, 1), "0", date(2010, 1, 1), date(2010, 1, 1)),
            ("9", "9", "5", date(2011, 1, 1), "0", date(2010, 1, 1), date(2010, 1, 1)),
            ("9", "9", "5", date(2012, 1, 1), "0", date(2010, 1, 1), date(2010, 1, 1)),
            ("9", "9", "5", date(2013, 1, 1), "0", date(2010, 1, 1), date(2010, 1, 1)),
            ("10", "10", "6", date(2023, 3, 19), "1", date(2021, 3, 19), date(2022, 3, 19),),
        ]
        # fmt: on

        df = spark.createDataFrame(rows, columns)

        df = job.create_coverage_df(df)

        self.assertEqual(df.count(), 8)

        # asserts equivalent items are present in both sequences
        self.assertCountEqual(
            df.select("locationid").rdd.flatMap(lambda x: x).collect(),
            ["1", "3", "4", "5", "8", "9", "9", "10"],
        )

        self.assertEqual(df.columns, ["locationid", "establishmentid", "import_date"])

    def test_add_column_if_locationid_is_in_ascwds(self):
        spark = utils.get_spark()
        ascwds_schema = StructType(
            fields=[
                StructField("locationid", StringType(), True),
                StructField("establishmentid", StringType(), True),
            ]
        )
        rows = [
            ("1", None),
            ("2", "5"),
        ]
        df = spark.createDataFrame(data=rows, schema=ascwds_schema)

        df = job.add_column_if_locationid_is_in_ascwds(df)

        self.assertEqual(df.count(), 2)

        df_collected = df.collect()

        self.assertEqual(df_collected[0]["cqc_coverage_in_ascwds"], 0)
        self.assertEqual(df_collected[1]["cqc_coverage_in_ascwds"], 1)

        self.assertEqual(
            df.columns, ["locationid", "establishmentid", "cqc_coverage_in_ascwds"]
        )

    def test_filter_out_locations_with_no_providerid_removes_cases_with_no_provider_id(
        self,
    ):
        spark = utils.get_spark()
        cqc_locations_schema = StructType(
            fields=[
                StructField("locationid", StringType(), True),
                StructField("providerid", StringType(), True),
            ]
        )
        rows = [
            ("1", None),
            ("2", "5"),
        ]
        df = spark.createDataFrame(data=rows, schema=cqc_locations_schema)

        df = job.filter_out_locations_with_no_providerid(df)

        self.assertEqual(df.count(), 1)

        df_collected = df.collect()

        self.assertEqual(df_collected[0]["providerid"], "5")

        self.assertEqual(df.columns, ["locationid", "providerid"])


if __name__ == "__main__":
    unittest.main(warnings="ignore")
