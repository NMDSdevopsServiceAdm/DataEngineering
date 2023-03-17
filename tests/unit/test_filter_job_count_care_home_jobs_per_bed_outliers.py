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
        self.prepared_locations_input_data = generate_care_home_jobs_per_bed_filter_df()
        self.filtered_output_df = job.care_home_jobs_per_bed_ratio_outliers(
            self.prepared_locations_input_data, "job_count_unfiltered", "job_count"
        )

        warnings.filterwarnings("ignore", category=ResourceWarning)

    def test_overall_output_df_has_same_number_of_rows_as_input_df(self):

        self.assertEqual(
            self.prepared_locations_input_data.count(), self.filtered_output_df.count()
        )

    def test_relevant_data_selected(self):
        df = job.select_relevant_data(
            self.prepared_locations_input_data, "job_count_unfiltered"
        )
        self.assertEqual(df.count(), 40)

    def test_select_data_not_in_subset_df(self):
        schema = StructType(
            [
                StructField("locationid", StringType(), True),
                StructField("other_col", StringType(), True),
            ]
        )
        # fmt: off
        rows = [("1-000000001", "data"), ("1-000000002", "data"), ("1-000000003", "data"), ]
        subset_rows = [("1-000000002", "data"), ]
        # fmt: on
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
        # fmt: off
        rows = [("1-000000001", 5.0, 100),
                ("1-000000002", 2.0, 1), ]
        # fmt: on
        df = self.spark.createDataFrame(rows, schema)
        df = job.calculate_jobs_per_bed_ratio(df, "job_count_unfiltered")

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
        # fmt: off
        rows = [("1", 5), ("2", 24), ("3", 500), ]
        # fmt: on
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
        # fmt: off
        rows = [("1", "5-9 beds", 10.1357), ("2", "5-9 beds", 10.3579), ("3", "50+ beds", 88.123456789)]
        # fmt: on
        df = self.spark.createDataFrame(rows, schema)
        df = job.calculate_average_jobs_per_banded_bed_count(df)

        df = df.collect()
        self.assertEqual(df[0]["avg_jobs_per_bed_ratio"], 10.2468)
        self.assertEqual(df[1]["avg_jobs_per_bed_ratio"], 88.12346)

    def test_calculate_standardised_residuals(self):

        pass

    def test_calculate_expected_jobs_based_on_number_of_beds(self):
        expected_jobs_schema = StructType(
            [
                StructField("number_of_beds_banded", StringType(), True),
                StructField("avg_jobs_per_bed_ratio", DoubleType(), True),
            ]
        )
        # fmt: off
        expected_jobs_rows = [("5-9 beds", 10.11111),
                              ("50+ beds", 80.0101), ]
        # fmt: on
        schema = StructType(
            [
                StructField("locationid", StringType(), True),
                StructField("number_of_beds", IntegerType(), True),
                StructField("number_of_beds_banded", StringType(), True),
            ]
        )
        # fmt: off
        rows = [("1", 7, "5-9 beds"),
                ("2", 75, "50+ beds"), ]
        # fmt: on
        expected_jobs_df = self.spark.createDataFrame(
            expected_jobs_rows, expected_jobs_schema
        )
        df = self.spark.createDataFrame(rows, schema)
        df = job.calculate_expected_jobs_based_on_number_of_beds(df, expected_jobs_df)

        df = df.collect()
        self.assertEqual(df[0]["expected_jobs"], 70.77777)
        self.assertEqual(df[1]["expected_jobs"], 6000.7575)

    def test_calculate_job_count_residuals(self):
        schema = StructType(
            [
                StructField("locationid", StringType(), True),
                StructField("job_count_unfiltered", DoubleType(), True),
                StructField("expected_jobs", DoubleType(), True),
            ]
        )
        # fmt: off
        rows = [("1", 10.0, 8.76544),
                ("2", 10.0, 10.0),
                ("3", 10.0, 11.23456), ]
        # fmt: on
        df = self.spark.createDataFrame(rows, schema)
        df = job.calculate_job_count_residuals(df, "job_count_unfiltered")

        df = df.sort("locationid").collect()
        self.assertEqual(df[0]["residual"], 1.23456)
        self.assertEqual(df[1]["residual"], 0.0)
        self.assertEqual(df[2]["residual"], -1.23456)

    def test_calculate_job_count_standardised_residual(self):
        schema = StructType(
            [
                StructField("locationid", StringType(), True),
                StructField("residual", DoubleType(), True),
                StructField("expected_jobs", DoubleType(), True),
            ]
        )
        # fmt: off
        rows = [("1", 11.11111, 4.0),
                ("2", 17.75, 25.0), ]
        # fmt: on
        df = self.spark.createDataFrame(rows, schema)
        df = job.calculate_job_count_standardised_residual(df)

        df = df.sort("locationid").collect()
        self.assertEqual(df[0]["standardised_residual"], 5.55556)
        self.assertEqual(df[1]["standardised_residual"], 3.55)

    def test_calculate_standardised_residual_cutoffs(self):

        pass

    def test_calculate_percentile(self):

        pass

    def test_create_filtered_job_count_df(self):

        pass

    def test_join_filtered_col_into_care_home_df(self):

        pass

    def test_add_job_counts_without_filtering_duplicates_data_in_column(self):
        schema = StructType(
            [
                StructField("locationid", StringType(), True),
                StructField("original_column", StringType(), True),
            ]
        )
        # fmt: off
        rows = [("1-000000002", 123), ("1-000000003", None), ]
        # fmt: on
        df = self.spark.createDataFrame(rows, schema)

        df = job.add_job_counts_without_filtering_to_data_outside_of_this_filter(
            df, "original_column", "new_column"
        )
        df = df.collect()
        self.assertEqual(df[0]["original_column"], df[0]["new_column"])
        self.assertEqual(df[1]["original_column"], df[1]["new_column"])

    def test_combine_dataframes_keeps_all_rows_of_data(self):
        schema = StructType(
            [
                StructField("locationid", StringType(), True),
                StructField("other_col", StringType(), True),
            ]
        )
        # fmt: off
        rows_1 = [("1-000000001", "data"), ]
        rows_2 = [("1-000000002", "data"), ("1-000000003", "data"), ]
        # fmt: on
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
        # fmt: off
        rows_1 = [("1-000000001", "data"), ]
        rows_2 = [("1-000000002", "data"), ("1-000000003", "data"), ]
        # fmt: on
        df_1 = self.spark.createDataFrame(rows_1, schema)
        df_2 = self.spark.createDataFrame(rows_2, schema)

        df = job.combine_dataframes(df_1, df_2)

        self.assertEqual(df_1.columns, df_2.columns)
        self.assertEqual(df.columns, df_1.columns)

    def test_round_figures_in_column(self):
        schema = StructType(
            [
                StructField("locationid", StringType(), True),
                StructField("column_with_decimals", DoubleType(), True),
            ]
        )

        rows = [
            ("1-000000001", 0.1234567890),
            ("1-000000002", 0.9876543210),
        ]
        df = self.spark.createDataFrame(rows, schema)

        df_3dp = job.round_figures_in_column(df, "column_with_decimals", 3)
        df_3dp = df_3dp.collect()
        self.assertEqual(df_3dp[0]["column_with_decimals"], 0.123)
        self.assertEqual(df_3dp[1]["column_with_decimals"], 0.988)

        df_6dp = job.round_figures_in_column(df, "column_with_decimals", 6)
        df_6dp = df_6dp.collect()
        self.assertEqual(df_6dp[0]["column_with_decimals"], 0.123457)
        self.assertEqual(df_6dp[1]["column_with_decimals"], 0.987654)
