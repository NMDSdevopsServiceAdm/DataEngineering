import unittest
import warnings

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    DoubleType,
    IntegerType,
)

from utils.prepare_locations_utils.filter_job_count import (
    care_home_jobs_per_bed_ratio_outliers as job,
)
from tests.test_file_generator import generate_care_home_jobs_per_bed_filter_df


class FilterJobCountCareHomeJobsPerBedRatioTests(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("test_filter_job_count").getOrCreate()
        self.estimate_job_count_input_data = generate_care_home_jobs_per_bed_filter_df()
        self.filtered_output_df = job.care_home_jobs_per_bed_ratio_outliers(
            self.estimate_job_count_input_data
        )

        warnings.filterwarnings("ignore", category=ResourceWarning)

    def test_overall_output_df_has_same_number_of_rows_as_input_df(self):

        self.assertEqual(
            self.estimate_job_count_input_data.count(), self.filtered_output_df.count()
        )

    def test_relevant_data_selected(self):
        df = job.select_relevant_data(self.estimate_job_count_input_data)
        self.assertEqual(df.count(), 40)

    def test_select_data_not_in_subset_df(self):
        schema = StructType(
            [
                StructField("locationid", StringType(), True),
                StructField("other_col", StringType(), True),
            ]
        )
        rows = [
            ("1-000000001", "data"),
            ("1-000000002", "data"),
            ("1-000000003", "data"),
        ]
        subset_rows = [
            ("1-000000002", "data"),
        ]
        df = self.spark.createDataFrame(rows, schema)
        subset_df = self.spark.createDataFrame(subset_rows, schema)

        data_not_in_subset_df = job.select_data_not_in_subset_df(df, subset_df)
        self.assertEqual(
            data_not_in_subset_df.count(), (df.count() - subset_df.count())
        )

    def test_calculate_jobs_per_bed_ratio(self):
        schema = StructType(
            [
                StructField("locationid", StringType(), True),
                StructField("job_count_unfiltered", DoubleType(), True),
                StructField("number_of_beds", IntegerType(), True),
            ]
        )
        rows = [
            ("1-000000001", 5.0, 100),
            ("1-000000002", 2.0, 1),
        ]
        df = self.spark.createDataFrame(rows, schema)
        df = job.calculate_jobs_per_bed_ratio(df)

        df = df.collect()
        self.assertEqual(df[0]["jobs_per_bed_ratio"], 0.05)
        self.assertEqual(df[1]["jobs_per_bed_ratio"], 2.0)

    def test_create_banded_bed_count_column(self):
        schema = StructType(
            [
                StructField("locationid", StringType(), True),
                StructField("number_of_beds", IntegerType(), True),
            ]
        )
        rows = [
            ("1", 5),
            ("2", 24),
            ("3", 500),
        ]
        df = self.spark.createDataFrame(rows, schema)
        df = job.create_banded_bed_count_column(df)

        df = df.collect()
        self.assertEqual(df[0]["number_of_beds_banded"], "5-9 beds")
        self.assertEqual(df[1]["number_of_beds_banded"], "20-24 beds")
        self.assertEqual(df[2]["number_of_beds_banded"], "50+ beds")

    def test_calculate_average_jobs_per_banded_bed_count(self):
        schema = StructType(
            [
                StructField("locationid", StringType(), True),
                StructField("number_of_beds_banded", StringType(), True),
                StructField("jobs_per_bed_ratio", DoubleType(), True),
            ]
        )
        rows = [
            ("1", "5-9 beds", 1.1357),
            ("2", "5-9 beds", 1.3579),
            ("3", "50+ beds", 1.123456789),
        ]
        df = self.spark.createDataFrame(rows, schema)
        df = job.calculate_average_jobs_per_banded_bed_count(df)

        df = df.collect()
        self.assertAlmostEquals(df[0]["avg_jobs_per_bed_ratio"], 1.2468, places=3)
        self.assertAlmostEquals(df[1]["avg_jobs_per_bed_ratio"], 1.12346, places=3)

    def test_calculate_standardised_residuals(self):
        expected_jobs_schema = StructType(
            [
                StructField("number_of_beds_banded", StringType(), True),
                StructField("avg_jobs_per_bed_ratio", DoubleType(), True),
            ]
        )
        expected_jobs_rows = [
            ("5-9 beds", 1.4),
            ("50+ beds", 1.28),
        ]
        schema = StructType(
            [
                StructField("locationid", StringType(), True),
                StructField("number_of_beds", IntegerType(), True),
                StructField("job_count_unfiltered", DoubleType(), True),
                StructField("number_of_beds_banded", StringType(), True),
            ]
        )
        rows = [
            ("1", 10, 16.0, "5-9 beds"),
            ("2", 50, 80.0, "50+ beds"),
            ("3", 50, 10.0, "50+ beds"),
        ]
        expected_jobs_df = self.spark.createDataFrame(
            expected_jobs_rows, expected_jobs_schema
        )
        df = self.spark.createDataFrame(rows, schema)
        df = job.calculate_standardised_residuals(df, expected_jobs_df)
        self.assertEqual(df.count(), 3)
        df = df.collect()
        self.assertAlmostEquals(df[0]["standardised_residual"], 0.53452, places=2)
        self.assertAlmostEquals(df[1]["standardised_residual"], 2.0, places=2)
        self.assertAlmostEquals(df[2]["standardised_residual"], -6.75, places=2)

    def test_calculate_expected_jobs_based_on_number_of_beds(self):
        expected_jobs_schema = StructType(
            [
                StructField("number_of_beds_banded", StringType(), True),
                StructField("avg_jobs_per_bed_ratio", DoubleType(), True),
            ]
        )
        expected_jobs_rows = [
            ("5-9 beds", 1.11111),
            ("50+ beds", 1.0101),
        ]
        schema = StructType(
            [
                StructField("locationid", StringType(), True),
                StructField("number_of_beds", IntegerType(), True),
                StructField("number_of_beds_banded", StringType(), True),
            ]
        )
        rows = [
            ("1", 7, "5-9 beds"),
            ("2", 75, "50+ beds"),
        ]
        expected_jobs_df = self.spark.createDataFrame(
            expected_jobs_rows, expected_jobs_schema
        )
        df = self.spark.createDataFrame(rows, schema)
        df = job.calculate_expected_jobs_based_on_number_of_beds(df, expected_jobs_df)

        df = df.collect()
        self.assertAlmostEquals(df[0]["expected_jobs"], 7.77777, places=3)
        self.assertAlmostEquals(df[1]["expected_jobs"], 75.7575, places=3)

    def test_calculate_job_count_residuals(self):
        schema = StructType(
            [
                StructField("locationid", StringType(), True),
                StructField("job_count_unfiltered", DoubleType(), True),
                StructField("expected_jobs", DoubleType(), True),
            ]
        )
        rows = [
            ("1", 10.0, 8.76544),
            ("2", 10.0, 10.0),
            ("3", 10.0, 11.23456),
        ]
        df = self.spark.createDataFrame(rows, schema)
        df = job.calculate_job_count_residuals(df)

        df = df.sort("locationid").collect()
        self.assertAlmostEquals(df[0]["residual"], 1.23456, places=3)
        self.assertAlmostEquals(df[1]["residual"], 0.0, places=3)
        self.assertAlmostEquals(df[2]["residual"], -1.23456, places=3)

    def test_calculate_job_count_standardised_residual(self):
        schema = StructType(
            [
                StructField("locationid", StringType(), True),
                StructField("residual", DoubleType(), True),
                StructField("expected_jobs", DoubleType(), True),
            ]
        )
        rows = [
            ("1", 11.11111, 4.0),
            ("2", 17.75, 25.0),
        ]
        df = self.spark.createDataFrame(rows, schema)
        df = job.calculate_job_count_standardised_residual(df)

        df = df.sort("locationid").collect()
        self.assertAlmostEquals(df[0]["standardised_residual"], 5.55556, places=2)
        self.assertAlmostEquals(df[1]["standardised_residual"], 3.55, places=2)

    def test_calculate_standardised_residual_cutoffs(self):
        schema = StructType(
            [
                StructField("locationid", StringType(), True),
                StructField("standardised_residual", DoubleType(), True),
            ]
        )
        # fmt: off
        rows = [("1", 0.54), ("2", -3.2545), ("3", -4.25423), ("4", 2.41654), ("5", 25.0), ]
        # fmt: on
        df = self.spark.createDataFrame(rows, schema)

        df = job.calculate_standardised_residual_cutoffs(
            df, 0.4, "lower_percentile", "upper_percentile"
        )

        df = df.collect()
        self.assertAlmostEquals(df[0]["lower_percentile"], -3.45, places=2)
        self.assertAlmostEquals(df[0]["upper_percentile"], 6.93, places=2)

    def test_calculate_percentile(self):
        schema = StructType(
            [
                StructField("locationid", StringType(), True),
                StructField("standardised_residual", DoubleType(), True),
            ]
        )
        # fmt: off
        rows = [("1", 0.54), ("2", -3.2545), ("3", -4.25423), ("4", 2.41654), ("5", 25.0), ]
        # fmt: on
        df = self.spark.createDataFrame(rows, schema)

        df = job.calculate_percentile(
            df, "standardised_residual", 0.2, "lower_percentile"
        )
        df = job.calculate_percentile(
            df, "standardised_residual", 0.8, "upper_percentile"
        )
        df = df.collect()
        self.assertAlmostEquals(df[0]["lower_percentile"], -3.45, places=2)
        self.assertAlmostEquals(df[0]["upper_percentile"], 6.93, places=2)

    def test_create_filtered_job_count_df(self):
        schema = StructType(
            [
                StructField("locationid", StringType(), True),
                StructField("snapshot_date", StringType(), True),
                StructField("job_count_unfiltered", DoubleType(), True),
                StructField("standardised_residual", DoubleType(), True),
                StructField("lower_percentile", DoubleType(), True),
                StructField("upper_percentile", DoubleType(), True),
            ]
        )
        rows = [
            ("1", "2023-01-01", 1.0, 0.26545, -1.2345, 1.2345),
            ("2", "2023-01-01", 2.0, -3.2545, -1.2345, 1.2345),
            ("3", "2023-01-01", 3.0, 12.25423, -1.2345, 1.2345),
        ]
        df = self.spark.createDataFrame(rows, schema)
        df = job.create_filtered_job_count_df(df)

        self.assertEqual(df.count(), 1)
        df = df.collect()
        self.assertEqual(df[0]["job_count"], 1.0)
        self.assertEqual(df[0]["locationid"], "1")

    def test_join_filtered_col_into_care_home_df(self):
        filtered_schema = StructType(
            [
                StructField("locationid", StringType(), True),
                StructField("snapshot_date", StringType(), True),
                StructField("job_count", DoubleType(), True),
            ]
        )
        filtered_rows = [
            ("2", "2023-01-01", 2.0),
        ]
        ch_schema = StructType(
            [
                StructField("locationid", StringType(), True),
                StructField("snapshot_date", StringType(), True),
                StructField("number_of_beds", IntegerType(), True),
                StructField("job_count_unfiltered", DoubleType(), True),
            ]
        )
        ch_rows = [
            ("2", "2022-01-01", 1, 2.0),
            ("2", "2023-01-01", 1, 2.0),
            ("3", "2023-01-01", 25, 30.0),
        ]
        filtered_df = self.spark.createDataFrame(filtered_rows, filtered_schema)
        ch_df = self.spark.createDataFrame(ch_rows, ch_schema)

        df = job.join_filtered_col_into_care_home_df(ch_df, filtered_df)

        self.assertEqual(df.count(), 3)
        df = df.sort("locationid", "snapshot_date").collect()
        self.assertIsNone(df[0]["job_count"])
        self.assertEqual(df[1]["job_count"], 2.0)
        self.assertIsNone(df[2]["job_count"])

    def test_add_job_counts_without_filtering_duplicates_data_in_column(self):
        schema = StructType(
            [
                StructField("locationid", StringType(), True),
                StructField("job_count_unfiltered", StringType(), True),
            ]
        )
        rows = [
            ("1-000000002", 123),
            ("1-000000003", None),
        ]
        df = self.spark.createDataFrame(rows, schema)

        df = job.add_job_counts_without_filtering_to_data_outside_of_this_filter(df)
        df = df.collect()
        self.assertEqual(df[0]["job_count_unfiltered"], df[0]["job_count"])
        self.assertEqual(df[1]["job_count_unfiltered"], df[1]["job_count"])

    def test_combine_dataframes_keeps_all_rows_of_data(self):
        schema = StructType(
            [
                StructField("locationid", StringType(), True),
                StructField("other_col", StringType(), True),
            ]
        )
        rows_1 = [
            ("1-000000001", "data"),
        ]
        rows_2 = [
            ("1-000000002", "data"),
            ("1-000000003", "data"),
        ]
        df_1 = self.spark.createDataFrame(rows_1, schema)
        df_2 = self.spark.createDataFrame(rows_2, schema)

        df = job.combine_dataframes(df_1, df_2)
        self.assertEqual(df.count(), (df_1.count() + df_2.count()))

    def test_combine_dataframes_have_matching_column_names(self):
        schema = StructType(
            [
                StructField("locationid", StringType(), True),
                StructField("other_col", StringType(), True),
            ]
        )
        rows_1 = [
            ("1-000000001", "data"),
        ]
        rows_2 = [
            ("1-000000002", "data"),
            ("1-000000003", "data"),
        ]

        df_1 = self.spark.createDataFrame(rows_1, schema)
        df_2 = self.spark.createDataFrame(rows_2, schema)

        df = job.combine_dataframes(df_1, df_2)

        self.assertEqual(df_1.columns, df_2.columns)
        self.assertEqual(df.columns, df_1.columns)
