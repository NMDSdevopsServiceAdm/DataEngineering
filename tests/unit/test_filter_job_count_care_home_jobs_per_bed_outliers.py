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

        # self.assertEqual(
        #     self.prepared_locations_input_data.count(), self.filtered_output_df.count()
        # )
        pass

    def test_relevant_data_selected(self):
        df = job.select_relevant_data(
            self.prepared_locations_input_data, "job_count_unfiltered"
        )
        self.assertEqual(df.count(), 42)

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
                StructField("job_count_unfiltered", IntegerType(), True),
                StructField("number_of_beds", IntegerType(), True),
            ]
        )
        # fmt: off
        rows = [("1-000000001", 5, 100), 
                ("1-000000002", 2, 1), ]
        # fmt: on
        df = self.spark.createDataFrame(rows, schema)
        df = job.calculate_jobs_per_bed_ratio(df, "job_count_unfiltered")

        df = df.collect()
        self.assertEqual(df[0]["jobs_per_bed_ratio"], 0.05)
        self.assertEqual(df[1]["jobs_per_bed_ratio"], 2.0)

    def test_create_banded_bed_count_column(self):

        pass

    def test_calculate_average_jobs_per_banded_bed_count(self):

        pass

    def test_calculate_standardised_residuals(self):

        pass

    def test_calculate_expected_jobs_based_on_number_of_beds(self):

        pass

    def test_calculate_job_count_residuals(self):

        pass

    def test_calculate_job_count_standardised_residual(self):

        pass

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
