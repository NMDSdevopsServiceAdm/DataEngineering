import unittest
import warnings
from datetime import date

from tests.test_file_data import (
    RemoveCareHomeFilledPostsPerBedRatioOutliersData as Data,
)
from tests.test_file_schemas import (
    RemoveCareHomeFilledPostsPerBedRatioOutliersSchema as Schemas,
)

from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    DoubleType,
    IntegerType,
    DateType,
)

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)
from utils.ind_cqc_filled_posts_utils.filter_ascwds_filled_posts import (
    remove_care_home_filled_posts_per_bed_ratio_outliers as job,
)


class FilterAscwdsFilledPostsCareHomeJobsPerBedRatioTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.care_home_filled_posts_per_bed_input_data = self.spark.createDataFrame(
            Data.care_home_filled_posts_per_bed_rows,
            Schemas.care_home_filled_posts_per_bed_schema,
        )
        self.filtered_output_df = (
            job.remove_care_home_filled_posts_per_bed_ratio_outliers(
                self.care_home_filled_posts_per_bed_input_data
            )
        )

        warnings.filterwarnings("ignore", category=ResourceWarning)


class MainTests(FilterAscwdsFilledPostsCareHomeJobsPerBedRatioTests):
    def setUp(self) -> None:
        super().setUp()

    def test_overall_output_df_has_same_number_of_rows_as_input_df(self):
        self.assertEqual(
            self.care_home_filled_posts_per_bed_input_data.count(),
            self.filtered_output_df.count(),
        )


class FilterToCareHomesWithKnownBedsAndFilledPostsTests(
    FilterAscwdsFilledPostsCareHomeJobsPerBedRatioTests
):
    def setUp(self) -> None:
        super().setUp()

    def test_relevant_data_selected(self):
        df = job.select_relevant_data(self.care_home_filled_posts_per_bed_input_data)
        self.assertEqual(df.count(), 40)


class SelectDataNotInSubsetTests(FilterAscwdsFilledPostsCareHomeJobsPerBedRatioTests):
    def setUp(self) -> None:
        super().setUp()

    def test_select_data_not_in_subset_df(self):
        schema = StructType(
            [
                StructField(IndCQC.location_id, StringType(), True),
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


class CalculateFilledPostsPerBedRatioTests(
    FilterAscwdsFilledPostsCareHomeJobsPerBedRatioTests
):
    def setUp(self) -> None:
        super().setUp()

    def test_calculate_filled_posts_per_bed_ratio(self):
        schema = StructType(
            [
                StructField(IndCQC.location_id, StringType(), True),
                StructField(IndCQC.ascwds_filled_posts, DoubleType(), True),
                StructField(IndCQC.number_of_beds, IntegerType(), True),
            ]
        )
        rows = [
            ("1-000000001", 5.0, 100),
            ("1-000000002", 2.0, 1),
        ]
        df = self.spark.createDataFrame(rows, schema)
        df = job.calculate_filled_posts_per_bed_ratio(df)

        df = df.sort(IndCQC.location_id).collect()
        self.assertEqual(df[0][job.TempColNames.filled_posts_per_bed_ratio], 0.05)
        self.assertEqual(df[1][job.TempColNames.filled_posts_per_bed_ratio], 2.0)


class CreateBandedBedCountColumnTests(
    FilterAscwdsFilledPostsCareHomeJobsPerBedRatioTests
):
    def setUp(self) -> None:
        super().setUp()

    def test_create_banded_bed_count_column(self):
        schema = StructType(
            [
                StructField(IndCQC.location_id, StringType(), True),
                StructField(IndCQC.number_of_beds, IntegerType(), True),
            ]
        )
        rows = [
            ("1", 5),
            ("2", 24),
            ("3", 500),
        ]
        df = self.spark.createDataFrame(rows, schema)
        df = job.create_banded_bed_count_column(df)

        df = df.sort(IndCQC.location_id).collect()
        self.assertEqual(df[0][job.TempColNames.number_of_beds_banded], 2.0)
        self.assertEqual(df[1][job.TempColNames.number_of_beds_banded], 5.0)
        self.assertEqual(df[2][job.TempColNames.number_of_beds_banded], 7.0)


class CalculateAverageFilledPostsPerBandedBedCount(
    FilterAscwdsFilledPostsCareHomeJobsPerBedRatioTests
):
    def setUp(self) -> None:
        super().setUp()

    def test_calculate_average_filled_posts_per_banded_bed_count(self):
        schema = StructType(
            [
                StructField(IndCQC.location_id, StringType(), True),
                StructField(job.TempColNames.number_of_beds_banded, DoubleType(), True),
                StructField(
                    job.TempColNames.filled_posts_per_bed_ratio, DoubleType(), True
                ),
            ]
        )
        rows = [
            ("1", 0.0, 1.1357),
            ("2", 0.0, 1.3579),
            ("3", 1.0, 1.123456789),
        ]
        df = self.spark.createDataFrame(rows, schema)
        df = job.calculate_average_filled_posts_per_banded_bed_count(df)

        df = df.sort(job.TempColNames.number_of_beds_banded).collect()
        self.assertAlmostEquals(
            df[0][job.TempColNames.avg_filled_posts_per_bed_ratio], 1.2468, places=3
        )
        self.assertAlmostEquals(
            df[1][job.TempColNames.avg_filled_posts_per_bed_ratio], 1.12346, places=3
        )


class CalculateStandardisedResidualsTests(
    FilterAscwdsFilledPostsCareHomeJobsPerBedRatioTests
):
    def setUp(self) -> None:
        super().setUp()

    def test_calculate_standardised_residuals(self):
        expected_filled_posts_schema = StructType(
            [
                StructField(job.TempColNames.number_of_beds_banded, DoubleType(), True),
                StructField(
                    job.TempColNames.avg_filled_posts_per_bed_ratio, DoubleType(), True
                ),
            ]
        )
        expected_filled_posts_rows = [
            (0.0, 1.4),
            (1.0, 1.28),
        ]
        schema = StructType(
            [
                StructField(IndCQC.location_id, StringType(), True),
                StructField(IndCQC.number_of_beds, IntegerType(), True),
                StructField(IndCQC.ascwds_filled_posts, DoubleType(), True),
                StructField(job.TempColNames.number_of_beds_banded, DoubleType(), True),
            ]
        )
        rows = [
            ("1", 10, 16.0, 0.0),
            ("2", 50, 80.0, 1.0),
            ("3", 50, 10.0, 1.0),
        ]
        expected_filled_posts_df = self.spark.createDataFrame(
            expected_filled_posts_rows, expected_filled_posts_schema
        )
        df = self.spark.createDataFrame(rows, schema)
        df = job.calculate_standardised_residuals(df, expected_filled_posts_df)
        self.assertEqual(df.count(), 3)
        df = df.sort(IndCQC.location_id).collect()
        self.assertAlmostEquals(
            df[0][job.TempColNames.standardised_residual], 0.53452, places=2
        )
        self.assertAlmostEquals(
            df[1][job.TempColNames.standardised_residual], 2.0, places=2
        )
        self.assertAlmostEquals(
            df[2][job.TempColNames.standardised_residual], -6.75, places=2
        )


class CalculateExpectedFilledPostsBasedOnNumberOfBedsTests(
    FilterAscwdsFilledPostsCareHomeJobsPerBedRatioTests
):
    def setUp(self) -> None:
        super().setUp()

    def test_calculate_expected_filled_posts_based_on_number_of_beds(self):
        expected_filled_posts_schema = StructType(
            [
                StructField(job.TempColNames.number_of_beds_banded, DoubleType(), True),
                StructField(
                    job.TempColNames.avg_filled_posts_per_bed_ratio, DoubleType(), True
                ),
            ]
        )
        expected_filled_posts_rows = [
            (0.0, 1.11111),
            (1.0, 1.0101),
        ]
        schema = StructType(
            [
                StructField(IndCQC.location_id, StringType(), True),
                StructField(IndCQC.number_of_beds, IntegerType(), True),
                StructField(job.TempColNames.number_of_beds_banded, DoubleType(), True),
            ]
        )
        rows = [
            ("1", 7, 0.0),
            ("2", 75, 1.0),
        ]
        expected_filled_posts_df = self.spark.createDataFrame(
            expected_filled_posts_rows, expected_filled_posts_schema
        )
        df = self.spark.createDataFrame(rows, schema)
        df = job.calculate_expected_filled_posts_based_on_number_of_beds(
            df, expected_filled_posts_df
        )

        df = df.sort(IndCQC.location_id).collect()
        self.assertAlmostEquals(
            df[0][job.TempColNames.expected_filled_posts], 7.77777, places=3
        )
        self.assertAlmostEquals(
            df[1][job.TempColNames.expected_filled_posts], 75.7575, places=3
        )


class CalculateFilledPostResidualsTests(
    FilterAscwdsFilledPostsCareHomeJobsPerBedRatioTests
):
    def setUp(self) -> None:
        super().setUp()

    def test_calculate_filled_post_residuals(self):
        schema = StructType(
            [
                StructField(IndCQC.location_id, StringType(), True),
                StructField(IndCQC.ascwds_filled_posts, DoubleType(), True),
                StructField(job.TempColNames.expected_filled_posts, DoubleType(), True),
            ]
        )
        rows = [
            ("1", 10.0, 8.76544),
            ("2", 10.0, 10.0),
            ("3", 10.0, 11.23456),
        ]
        df = self.spark.createDataFrame(rows, schema)
        df = job.calculate_filled_post_residuals(df)

        df = df.sort(IndCQC.location_id).collect()
        self.assertAlmostEquals(df[0][job.TempColNames.residual], 1.23456, places=3)
        self.assertAlmostEquals(df[1][job.TempColNames.residual], 0.0, places=3)
        self.assertAlmostEquals(df[2][job.TempColNames.residual], -1.23456, places=3)


class CalculateFilledPostStandardisedResidualsTests(
    FilterAscwdsFilledPostsCareHomeJobsPerBedRatioTests
):
    def setUp(self) -> None:
        super().setUp()

    def test_calculate_filled_post_standardised_residual(self):
        schema = StructType(
            [
                StructField(IndCQC.location_id, StringType(), True),
                StructField(job.TempColNames.residual, DoubleType(), True),
                StructField(job.TempColNames.expected_filled_posts, DoubleType(), True),
            ]
        )
        rows = [
            ("1", 11.11111, 4.0),
            ("2", 17.75, 25.0),
        ]
        df = self.spark.createDataFrame(rows, schema)
        df = job.calculate_filled_post_standardised_residual(df)

        df = df.sort(IndCQC.location_id).collect()
        self.assertAlmostEquals(
            df[0][job.TempColNames.standardised_residual], 5.55556, places=2
        )
        self.assertAlmostEquals(
            df[1][job.TempColNames.standardised_residual], 3.55, places=2
        )


class CalculateLowerAndUpperStandardisedResidualCutoffTests(
    FilterAscwdsFilledPostsCareHomeJobsPerBedRatioTests
):
    def setUp(self) -> None:
        super().setUp()

        self.standardised_residual_percentile_cutoff_df = self.spark.createDataFrame(
            Data.standardised_residual_percentile_cutoff_rows,
            Schemas.standardised_residual_percentile_cutoff_schema,
        )
        self.returned_df = (
            job.calculate_lower_and_upper_standardised_residual_percentile_cutoffs(
                self.standardised_residual_percentile_cutoff_df,
                0.4,
            )
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_standardised_residual_percentile_cutoff_with_percentiles_rows,
            Schemas.expected_standardised_residual_percentile_cutoff_with_percentiles_schema,
        )
        self.returned_data = self.returned_df.sort(IndCQC.location_id).collect()
        self.expected_data = self.expected_df.sort(IndCQC.location_id).collect()

    def test_calculate_standardised_residual_percentile_cutoffs_returns_expected_columns(
        self,
    ):
        self.assertEqual(
            sorted(self.returned_df.columns), sorted(self.expected_df.columns)
        )

    def test_calculate_standardised_residual_percentile_cutoffs_returns_expected_number_of_rows(
        self,
    ):
        self.assertEqual(self.returned_df.count(), self.expected_df.count())

    def test_calculate_standardised_residual_percentile_cutoffs_returns_expected_lower_percentile_values(
        self,
    ):
        self.assertAlmostEquals(
            self.returned_data[0][job.TempColNames.lower_percentile],
            self.expected_data[0][job.TempColNames.lower_percentile],
            places=2,
        )
        self.assertAlmostEquals(
            self.returned_data[1][job.TempColNames.lower_percentile],
            self.expected_data[1][job.TempColNames.lower_percentile],
            places=2,
        )
        self.assertAlmostEquals(
            self.returned_data[2][job.TempColNames.lower_percentile],
            self.expected_data[2][job.TempColNames.lower_percentile],
            places=2,
        )
        self.assertAlmostEquals(
            self.returned_data[3][job.TempColNames.lower_percentile],
            self.expected_data[3][job.TempColNames.lower_percentile],
            places=2,
        )
        self.assertAlmostEquals(
            self.returned_data[4][job.TempColNames.lower_percentile],
            self.expected_data[4][job.TempColNames.lower_percentile],
            places=2,
        )
        self.assertAlmostEquals(
            self.returned_data[5][job.TempColNames.lower_percentile],
            self.expected_data[5][job.TempColNames.lower_percentile],
            places=2,
        )
        self.assertAlmostEquals(
            self.returned_data[6][job.TempColNames.lower_percentile],
            self.expected_data[6][job.TempColNames.lower_percentile],
            places=2,
        )
        self.assertAlmostEquals(
            self.returned_data[7][job.TempColNames.lower_percentile],
            self.expected_data[7][job.TempColNames.lower_percentile],
            places=2,
        )

    def test_calculate_standardised_residual_percentile_cutoffs_returns_expected_upper_percentile_values(
        self,
    ):
        self.assertAlmostEquals(
            self.returned_data[0][job.TempColNames.upper_percentile],
            self.expected_data[0][job.TempColNames.upper_percentile],
            places=2,
        )
        self.assertAlmostEquals(
            self.returned_data[1][job.TempColNames.upper_percentile],
            self.expected_data[1][job.TempColNames.upper_percentile],
            places=2,
        )
        self.assertAlmostEquals(
            self.returned_data[2][job.TempColNames.upper_percentile],
            self.expected_data[2][job.TempColNames.upper_percentile],
            places=2,
        )
        self.assertAlmostEquals(
            self.returned_data[3][job.TempColNames.upper_percentile],
            self.expected_data[3][job.TempColNames.upper_percentile],
            places=2,
        )
        self.assertAlmostEquals(
            self.returned_data[4][job.TempColNames.upper_percentile],
            self.expected_data[4][job.TempColNames.upper_percentile],
            places=2,
        )
        self.assertAlmostEquals(
            self.returned_data[5][job.TempColNames.upper_percentile],
            self.expected_data[5][job.TempColNames.upper_percentile],
            places=2,
        )
        self.assertAlmostEquals(
            self.returned_data[6][job.TempColNames.upper_percentile],
            self.expected_data[6][job.TempColNames.upper_percentile],
            places=2,
        )
        self.assertAlmostEquals(
            self.returned_data[7][job.TempColNames.upper_percentile],
            self.expected_data[7][job.TempColNames.upper_percentile],
            places=2,
        )

    def test_raise_error_if_percentage_of_data_to_filter_out_equal_to_one(self):
        with self.assertRaises(ValueError) as context:
            job.calculate_lower_and_upper_standardised_residual_percentile_cutoffs(
                self.standardised_residual_percentile_cutoff_df,
                1.0,
            )

        self.assertTrue(
            "Percentage of data to filter out must be less than 1 (equivalent to 100%)"
            in str(context.exception)
        )

    def test_raise_error_if_percentage_of_data_to_filter_out_greater_than_one(self):
        with self.assertRaises(ValueError) as context:
            job.calculate_lower_and_upper_standardised_residual_percentile_cutoffs(
                self.standardised_residual_percentile_cutoff_df,
                1.1,
            )

        self.assertTrue(
            "Percentage of data to filter out must be less than 1 (equivalent to 100%)"
            in str(context.exception)
        )


class CreateFilledPostsCleanColInFilteredDfTests(
    FilterAscwdsFilledPostsCareHomeJobsPerBedRatioTests
):
    def setUp(self) -> None:
        super().setUp()

    def test_create_filled_posts_clean_col_in_filtered_df(self):
        schema = StructType(
            [
                StructField(IndCQC.location_id, StringType(), True),
                StructField(IndCQC.cqc_location_import_date, DateType(), True),
                StructField(IndCQC.ascwds_filled_posts, DoubleType(), True),
                StructField(job.TempColNames.standardised_residual, DoubleType(), True),
                StructField(job.TempColNames.lower_percentile, DoubleType(), True),
                StructField(job.TempColNames.upper_percentile, DoubleType(), True),
            ]
        )
        rows = [
            ("1", date(2023, 1, 1), 1.0, 0.26545, -1.2345, 1.2345),
            ("2", date(2023, 1, 1), 2.0, -3.2545, -1.2345, 1.2345),
            ("3", date(2023, 1, 1), 3.0, 12.25423, -1.2345, 1.2345),
        ]
        df = self.spark.createDataFrame(rows, schema)
        df = job.create_filled_posts_clean_col_in_filtered_df(df)

        self.assertEqual(df.count(), 1)
        df = df.sort(IndCQC.location_id).collect()
        self.assertEqual(df[0][IndCQC.ascwds_filled_posts_clean], 1.0)
        self.assertEqual(df[0][IndCQC.location_id], "1")


class JoinFilteredColIntoCareHomeDfTests(
    FilterAscwdsFilledPostsCareHomeJobsPerBedRatioTests
):
    def setUp(self) -> None:
        super().setUp()

    def test_join_filtered_col_into_care_home_df(self):
        filtered_schema = StructType(
            [
                StructField(IndCQC.location_id, StringType(), True),
                StructField(IndCQC.cqc_location_import_date, DateType(), True),
                StructField(IndCQC.ascwds_filled_posts_clean, DoubleType(), True),
            ]
        )
        filtered_rows = [
            ("2", date(2023, 1, 1), 2.0),
        ]
        ch_schema = StructType(
            [
                StructField(IndCQC.location_id, StringType(), True),
                StructField(IndCQC.cqc_location_import_date, DateType(), True),
                StructField(IndCQC.number_of_beds, IntegerType(), True),
                StructField(IndCQC.ascwds_filled_posts, DoubleType(), True),
            ]
        )
        ch_rows = [
            ("2", date(2022, 1, 1), 1, 2.0),
            ("2", date(2023, 1, 1), 1, 2.0),
            ("3", date(2023, 1, 1), 25, 30.0),
        ]
        filtered_df = self.spark.createDataFrame(filtered_rows, filtered_schema)
        ch_df = self.spark.createDataFrame(ch_rows, ch_schema)

        df = job.join_filtered_col_into_care_home_df(ch_df, filtered_df)

        self.assertEqual(df.count(), 3)
        df = df.sort(IndCQC.location_id, IndCQC.cqc_location_import_date).collect()
        self.assertIsNone(df[0][IndCQC.ascwds_filled_posts_clean])
        self.assertEqual(df[1][IndCQC.ascwds_filled_posts_clean], 2.0)
        self.assertIsNone(df[2][IndCQC.ascwds_filled_posts_clean])


class AddFilledPostsCleanWithoutFilteringDuplicatesDataInColumnTests(
    FilterAscwdsFilledPostsCareHomeJobsPerBedRatioTests
):
    def setUp(self) -> None:
        super().setUp()

    def test_add_filled_posts_clean_without_filtering_duplicates_data_in_column(self):
        schema = StructType(
            [
                StructField(IndCQC.location_id, StringType(), True),
                StructField(IndCQC.ascwds_filled_posts, StringType(), True),
            ]
        )
        rows = [
            ("1-000000002", 123),
            ("1-000000003", None),
        ]
        df = self.spark.createDataFrame(rows, schema)

        df = (
            job.add_filled_posts_clean_without_filtering_to_data_outside_of_this_filter(
                df
            )
        )
        df = df.sort(IndCQC.location_id).collect()
        self.assertEqual(
            df[0][IndCQC.ascwds_filled_posts], df[0][IndCQC.ascwds_filled_posts_clean]
        )
        self.assertEqual(
            df[1][IndCQC.ascwds_filled_posts], df[1][IndCQC.ascwds_filled_posts_clean]
        )


class CombineDataframeTests(FilterAscwdsFilledPostsCareHomeJobsPerBedRatioTests):
    def setUp(self) -> None:
        super().setUp()

    def test_combine_dataframes_keeps_all_rows_of_data(self):
        schema = StructType(
            [
                StructField(IndCQC.location_id, StringType(), True),
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
                StructField(IndCQC.location_id, StringType(), True),
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
