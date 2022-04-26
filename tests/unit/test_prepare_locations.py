from cmath import pi
from datetime import datetime, date
import shutil
import unittest
import mock
from pathlib import Path
from environment import constants

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from jobs import format_fields, prepare_locations


class PrepareLocationsTests(unittest.TestCase):

    path_ascwds_workplace = "tests/test_data/domain=ASCWDS/dataset=workplace/version=0.0.1/format=parquet"
    path_cqc_locations = "tests/test_data/domain=CQC/dataset=CQC_locations/version=0.0.1/format=parquet/"
    path_cqc_providers = "tests/test_data/domain=CQC/dataset=CQC_providers/version=0.0.1/format=parquet/"
    path_pir = "tests/test_data/domain=CQC/dataset=pir/version=0.0.1/format=parquet"

    test_data_basepath = "tests/test_data"

    calculate_jobs_schema = StructType(
        [
            StructField("locationid", StringType(), False),
            StructField("total_staff", IntegerType(), True),
            StructField("worker_record_count", IntegerType(), True),
            StructField("number_of_beds", IntegerType(), True),
            StructField("job_count", DoubleType(), True),
        ]
    )

    def setUp(self):
        self.spark = SparkSession.builder.appName("test_prepare_locations").getOrCreate()
        self.mock_base_paths = mock.patch.object(
            constants, "ASCWDS_WORKPLACE_BASE_PATH", return_value="tests/test_data"
        )

    def test_main_successfully_runs(self):
        with self.mock_base_paths:
            output_df = prepare_locations.main(
                self.path_ascwds_workplace, self.path_cqc_locations, self.path_cqc_providers, self.path_pir
            )
            self.assertIsNotNone(output_df)

    # def test_get_ascwds_workplace_df(self):
    #     workplace_df = prepare_locations.get_ascwds_workplace_df(
    #         self.path_ascwds_workplace, date(2021, 11, 30), self.test_data_basepath
    #     )
    #     self.assertEqual(workplace_df.columns[0], "locationid")
    #     self.assertEqual(workplace_df.columns[1], "establishmentid")
    #     self.assertEqual(workplace_df.columns[2], "total_staff")
    #     self.assertEqual(workplace_df.columns[3], "worker_record_count")
    #     self.assertEqual(workplace_df.columns[4], "import_date")
    #     self.assertEqual(workplace_df.count(), 10)

    # def test_get_cqc_location_df(self):
    #     cqc_location_df = prepare_locations.get_cqc_location_df(
    #         self.path_cqc_locations, date(2022, 1, 5), self.test_data_basepath
    #     )

    #     self.assertEqual(cqc_location_df.columns[0], "locationid")
    #     self.assertEqual(cqc_location_df.columns[1], "providerid")
    #     self.assertEqual(cqc_location_df.columns[16], "import_date")
    #     self.assertEqual(cqc_location_df.count(), 49)

    # def test_get_cqc_provider_df(self):
    #     cqc_provider_df = prepare_locations.get_cqc_provider_df(
    #         self.path_cqc_providers, date(2022, 4, 1), self.test_data_basepath
    #     )

    #     self.assertEqual(cqc_provider_df.columns[0], "providerid")
    #     self.assertEqual(cqc_provider_df.columns[1], "provider_name")
    #     self.assertEqual(cqc_provider_df.columns[2], "import_date")
    #     self.assertEqual(cqc_provider_df.count(), 9)

    # def test_get_pir_df(self):
    #     pir_df = prepare_locations.get_pir_df(self.path_pir, date(2020, 3, 31), self.test_data_basepath)

    #     self.assertEqual(pir_df.count(), 10)
    #     self.assertEqual(len(pir_df.columns), 3)

    #     self.assertEqual(pir_df.columns[0], "locationid")
    #     self.assertEqual(pir_df.columns[1], "pir_service_users")
    #     self.assertEqual(pir_df.columns[2], "import_date")

    # def test_get_date_closest_to_search_date(self):
    #     date_search_list = [date(2022, 1, 1), date(2022, 2, 11), date(2022, 3, 21), date(2022, 3, 28), date(2022, 4, 3)]
    #     test_data_list = [
    #         (date(2022, 1, 3), None),
    #         (date(2022, 2, 11), date(2022, 2, 11)),
    #         (date(2022, 2, 14), date(2022, 2, 11)),
    #         (date(2022, 3, 27), date(2022, 3, 21)),
    #     ]

    #     for test_data in test_data_list:
    #         result = prepare_locations.get_date_closest_to_search_date(test_data[0], date_search_list)
    #         self.assertEqual(result, test_data[1])

    # def test_get_unique_import_dates(self):
    #     cqc_location_df = prepare_locations.get_cqc_location_df(
    #         self.path_cqc_locations, date(2022, 1, 5), self.test_data_basepath
    #     )

    #     result = prepare_locations.get_unique_import_dates(cqc_location_df)
    #     self.assertIsNotNone(result)
    #     self.assertEqual(len(result), 1)

    # def test_generate_closest_date_matrix(self):
    #     workplace_df = prepare_locations.get_ascwds_workplace_df(
    #         self.path_ascwds_workplace, date(2021, 11, 30), self.test_data_basepath
    #     )
    #     cqc_location_df = prepare_locations.get_cqc_location_df(
    #         self.path_cqc_locations, date(2022, 1, 5), self.test_data_basepath
    #     )
    #     cqc_provider_df = prepare_locations.get_cqc_provider_df(
    #         self.path_cqc_providers, date(2022, 4, 1), self.test_data_basepath
    #     )
    #     pir_df = prepare_locations.get_pir_df(self.path_pir, date(2022, 4, 1), self.test_data_basepath)

    #     result = prepare_locations.generate_closest_date_matrix(workplace_df, cqc_location_df, cqc_provider_df, pir_df)

    #     self.assertIsNotNone(result)

    # def test_filter_nulls(self):
    #     columns = ["locationid", "worker_record_count", "total_staff"]
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

    #     list_filtered_df = filtered_df.collect()

    #     self.assertCountEqual(
    #         filtered_df.select("locationid").rdd.flatMap(lambda x: x).collect(),
    #         ["1-000000001", "1-000000002", "1-000000003", "1-000000005"],
    #     )

    # def test_clean(self):

    #     columns = ["locationid", "worker_record_count", "total_staff"]
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
    #     self.assertEqual(cleaned_df_list[0]["total_staff"], None)
    #     self.assertEqual(cleaned_df_list[1]["total_staff"], 500)

    # def test_purge_workplaces(self):
    #     columns = ["locationid", "ascwds_workplace_import_date", "orgid", "isparent", "mupddate"]
    #     rows = [
    #         ("1", date(2023, 3, 19), "1", "1", date(2018, 9, 5)),
    #         ("2", date(2023, 3, 19), "1", "0", date(2019, 7, 10)),
    #         ("3", date(2023, 3, 19), "1", "1", date(2020, 5, 15)),
    #         ("4", date(2023, 3, 19), "1", "0", date(2021, 3, 20)),
    #         ("5", date(2023, 3, 19), "1", "1", date(2022, 1, 25)),
    #         ("6", date(2023, 3, 19), "2", "1", date(2021, 3, 18)),
    #         ("7", date(2023, 3, 19), "3", "1", date(2021, 3, 19)),
    #         ("8", date(2023, 3, 19), "4", "1", date(2021, 3, 20)),
    #         ("9", date(2010, 1, 1), "5", "0", date(2010, 1, 1)),
    #         ("9", date(2011, 1, 1), "5", "0", date(2010, 1, 1)),
    #         ("9", date(2012, 1, 1), "5", "0", date(2010, 1, 1)),
    #         ("9", date(2013, 1, 1), "5", "0", date(2010, 1, 1)),
    #     ]
    #     df = self.spark.createDataFrame(rows, columns)
    #     df = prepare_locations.purge_workplaces(df)

    #     self.assertEqual(df.count(), 7)

    #     # asserts equivalent items are present in both sequences
    #     self.assertCountEqual(
    #         df.select("locationid").rdd.flatMap(lambda x: x).collect(), ["1", "3", "4", "5", "8", "9", "9"]
    #     )

    # def test_add_cqc_sector(self):
    #     columns = ["providerid", "provider_name"]
    #     rows = [
    #         ("1-000000001", "This is an MDC"),
    #         ("1-000000002", "Local authority council alert"),
    #         ("1-000000003", "The Royal Borough of Skills for Care"),
    #         ("1-000000004", "Not actually a borough"),
    #         ("1-000000005", "The Council of St Monica Trust"),
    #     ]
    #     df = self.spark.createDataFrame(rows, columns)

    #     df = prepare_locations.add_cqc_sector(df)
    #     self.assertEqual(df.count(), 5)

    #     df = df.collect()
    #     self.assertEqual(df[0]["cqc_sector"], "Local authority")
    #     self.assertEqual(df[1]["cqc_sector"], "Local authority")
    #     self.assertEqual(df[2]["cqc_sector"], "Local authority")
    #     self.assertEqual(df[3]["cqc_sector"], "Independent")
    #     self.assertEqual(df[4]["cqc_sector"], "Independent")

    # def test_filter_out_cqc_la_data(self):
    #     columns = ["providerid", "cqc_sector"]
    #     rows = [
    #         ("1-000000001", "Local authority"),
    #         ("1-000000002", "Independent"),
    #     ]
    #     df = self.spark.createDataFrame(rows, columns)

    #     df = prepare_locations.filter_out_cqc_la_data(df)
    #     self.assertEqual(df.count(), 1)

    #     df = df.collect()
    #     self.assertEqual(df[0]["cqc_sector"], "Independent")

    # def test_calculate_jobcount_totalstaff_equal_wkrrecs(self):
    #     columns = ["locationid", "worker_record_count", "total_staff", "number_of_beds", "job_count"]
    #     rows = [
    #         ("1-000000001", 20, 20, 25, None),
    #     ]
    #     df = self.spark.createDataFrame(data=rows, schema=self.calculate_jobs_schema)

    #     df = prepare_locations.calculate_jobcount_totalstaff_equal_wkrrecs(df)
    #     self.assertEqual(df.count(), 1)

    #     df = df.collect()
    #     self.assertEqual(df[0]["job_count"], 20)

    # def test_calculate_jobcount_coalesce_totalstaff_wkrrecs(self):
    #     rows = [
    #         ("1-000000001", None, 50, 15, None),
    #     ]
    #     df = self.spark.createDataFrame(data=rows, schema=self.calculate_jobs_schema)

    #     df = prepare_locations.calculate_jobcount_coalesce_totalstaff_wkrrecs(df)
    #     self.assertEqual(df.count(), 1)

    #     df = df.collect()
    #     self.assertEqual(df[0]["job_count"], 50)

    # def test_calculate_jobcount_abs_difference_within_range(self):
    #     rows = [
    #         ("1-000000008", 10, 12, 15, None),
    #         ("1-000000001", 100, 109, 80, None),
    #     ]
    #     df = self.spark.createDataFrame(data=rows, schema=self.calculate_jobs_schema)

    #     df = prepare_locations.calculate_jobcount_abs_difference_within_range(df)
    #     self.assertEqual(df.count(), 2)

    #     df = df.collect()
    #     self.assertEqual(df[0]["job_count"], 11)
    #     self.assertEqual(df[1]["job_count"], 104.5)

    # def test_calculate_jobcount_handle_tiny_values(self):
    #     rows = [
    #         ("1-000000008", 2, 53, 26, None),
    #     ]
    #     df = self.spark.createDataFrame(data=rows, schema=self.calculate_jobs_schema)

    #     df = prepare_locations.calculate_jobcount_handle_tiny_values(df)
    #     self.assertEqual(df.count(), 1)

    #     df = df.collect()
    #     self.assertEqual(df[0]["job_count"], 53)

    # def test_calculate_jobcount(self):
    #     columns = ["locationid", "worker_record_count", "total_staff", "number_of_beds"]
    #     rows = [
    #         ("1-000000001", None, 0, 0),  # Both 0: Return 0
    #         # Both 500: Return 500
    #         ("1-000000002", 500, 500, 490),
    #         # Only know worker_record_count: Return worker_record_count (100)
    #         ("1-000000003", 100, None, 10),
    #         # Only know total_staff: Return totalstaf (10)
    #         ("1-000000004", None, 10, 12),
    #         # None of the rules apply: Return None
    #         ("1-000000005", 25, 75, 40),
    #         # None of the rules apply: Return None
    #         ("1-000000006", 30, 60, 40),
    #         # None of the rules apply: Return None
    #         ("1-000000007", 600, 900, 150),
    #         # Absolute difference is within 10%: Return Average
    #         ("1-000000008", 10, 12, None),
    #         # Either total_staff or worker_record_count < 3: return max
    #         ("1-000000009", 1, 23, None),
    #         # Utilise bedcount estimate - Average
    #         ("1-000000010", 90, 102, 85),
    #         # Utilise bedcount estimate - Wkrrecs
    #         ("1-000000011", 90, 102, 95),
    #         # Utilise bedcount estimate - Totalstaff
    #         ("1-000000012", 90, 102, 80),
    #     ]
    #     df = self.spark.createDataFrame(rows, columns)

    #     jobcount_df = prepare_locations.calculate_jobcount(df)
    #     jobcount_df_list = jobcount_df.collect()

    #     self.assertEqual(jobcount_df_list[0]["job_count"], 0.0)
    #     self.assertEqual(jobcount_df_list[1]["job_count"], 500.0)
    #     self.assertEqual(jobcount_df_list[2]["job_count"], 100.0)
    #     self.assertEqual(jobcount_df_list[3]["job_count"], 10.0)
    #     self.assertEqual(jobcount_df_list[4]["job_count"], None)
    #     self.assertEqual(jobcount_df_list[5]["job_count"], None)
    #     self.assertEqual(jobcount_df_list[6]["job_count"], None)
    #     self.assertEqual(jobcount_df_list[7]["job_count"], 11.0)
    #     self.assertEqual(jobcount_df_list[8]["job_count"], 23.0)
    #     self.assertEqual(jobcount_df_list[9]["job_count"], 96.0)
    #     self.assertEqual(jobcount_df_list[10]["job_count"], 102.0)
    #     self.assertEqual(jobcount_df_list[11]["job_count"], 90.0)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
