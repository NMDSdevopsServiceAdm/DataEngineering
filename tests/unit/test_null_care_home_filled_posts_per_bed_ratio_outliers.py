import unittest
import warnings

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
)

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)
from utils.column_names.null_outlier_columns import NullOutlierColumns
from utils.ind_cqc_filled_posts_utils.null_ascwds_filled_post_outliers import (
    null_care_home_filled_posts_per_bed_ratio_outliers as job,
)


class NullAscwdsFilledPostsCareHomeJobsPerBedRatioOutlierTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.unfiltered_ind_cqc_df = self.spark.createDataFrame(
            Data.unfiltered_ind_cqc_rows,
            Schemas.ind_cqc_schema,
        )

        warnings.filterwarnings("ignore", category=ResourceWarning)


class MainTests(NullAscwdsFilledPostsCareHomeJobsPerBedRatioOutlierTests):
    def setUp(self) -> None:
        super().setUp()

        self.returned_filtered_df = (
            job.null_care_home_filled_posts_per_bed_ratio_outliers(
                self.unfiltered_ind_cqc_df
            )
        )

    def test_returned_filtered_df_has_same_number_of_rows_as_initial_unfiltered_df(
        self,
    ):
        self.assertEqual(
            self.unfiltered_ind_cqc_df.count(),
            self.returned_filtered_df.count(),
        )

    def test_returned_filtered_df_has_same_schema_as_initial_unfiltered_df(
        self,
    ):
        self.assertEqual(
            self.unfiltered_ind_cqc_df.schema,
            self.returned_filtered_df.schema,
        )

    def test_returned_df_matches_expected_df(self):
        expected_filtered_df = self.spark.createDataFrame(
            Data.expected_care_home_jobs_per_bed_ratio_filtered_rows,
            Schemas.ind_cqc_schema,
        )

        returned_data = self.returned_filtered_df.sort(IndCQC.location_id).collect()
        expected_data = expected_filtered_df.sort(IndCQC.location_id).collect()

        self.assertEqual(expected_data, returned_data)


class FilterToCareHomesWithKnownBedsAndFilledPostsTests(
    NullAscwdsFilledPostsCareHomeJobsPerBedRatioOutlierTests
):
    def setUp(self) -> None:
        super().setUp()

    def test_relevant_data_selected(self):
        # TODO - replace test data with own test data and improve tests
        df = job.select_relevant_data(self.unfiltered_ind_cqc_df)
        self.assertEqual(df.count(), 40)


class SelectDataNotInSubsetTests(
    NullAscwdsFilledPostsCareHomeJobsPerBedRatioOutlierTests
):
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
    NullAscwdsFilledPostsCareHomeJobsPerBedRatioOutlierTests
):
    def setUp(self) -> None:
        super().setUp()

    def test_calculate_filled_posts_per_bed_ratio(self):
        schema = StructType(
            [
                StructField(IndCQC.location_id, StringType(), True),
                StructField(IndCQC.ascwds_filled_posts_dedup_clean, DoubleType(), True),
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
        self.assertEqual(df[0][NullOutlierColumns.filled_posts_per_bed_ratio], 0.05)
        self.assertEqual(df[1][NullOutlierColumns.filled_posts_per_bed_ratio], 2.0)


class CreateBandedBedCountColumnTests(
    NullAscwdsFilledPostsCareHomeJobsPerBedRatioOutlierTests
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
        self.assertEqual(df[0][NullOutlierColumns.number_of_beds_banded], 2.0)
        self.assertEqual(df[1][NullOutlierColumns.number_of_beds_banded], 5.0)
        self.assertEqual(df[2][NullOutlierColumns.number_of_beds_banded], 7.0)


class CalculateAverageFilledPostsPerBandedBedCount(
    NullAscwdsFilledPostsCareHomeJobsPerBedRatioOutlierTests
):
    def setUp(self) -> None:
        super().setUp()

    def test_calculate_average_filled_posts_per_banded_bed_count(self):
        schema = StructType(
            [
                StructField(IndCQC.location_id, StringType(), True),
                StructField(
                    NullOutlierColumns.number_of_beds_banded, DoubleType(), True
                ),
                StructField(
                    NullOutlierColumns.filled_posts_per_bed_ratio, DoubleType(), True
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

        df = df.sort(NullOutlierColumns.number_of_beds_banded).collect()
        self.assertAlmostEquals(
            df[0][NullOutlierColumns.avg_filled_posts_per_bed_ratio], 1.2468, places=3
        )
        self.assertAlmostEquals(
            df[1][NullOutlierColumns.avg_filled_posts_per_bed_ratio], 1.12346, places=3
        )


class CalculateStandardisedResidualsTests(
    NullAscwdsFilledPostsCareHomeJobsPerBedRatioOutlierTests
):
    def setUp(self) -> None:
        super().setUp()

    def test_calculate_standardised_residuals(self):
        expected_filled_posts_schema = StructType(
            [
                StructField(
                    NullOutlierColumns.number_of_beds_banded, DoubleType(), True
                ),
                StructField(
                    NullOutlierColumns.avg_filled_posts_per_bed_ratio,
                    DoubleType(),
                    True,
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
                StructField(IndCQC.ascwds_filled_posts_dedup_clean, DoubleType(), True),
                StructField(
                    NullOutlierColumns.number_of_beds_banded, DoubleType(), True
                ),
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
            df[0][NullOutlierColumns.standardised_residual], 0.53452, places=2
        )
        self.assertAlmostEquals(
            df[1][NullOutlierColumns.standardised_residual], 2.0, places=2
        )
        self.assertAlmostEquals(
            df[2][NullOutlierColumns.standardised_residual], -6.75, places=2
        )


class CalculateExpectedFilledPostsBasedOnNumberOfBedsTests(
    NullAscwdsFilledPostsCareHomeJobsPerBedRatioOutlierTests
):
    def setUp(self) -> None:
        super().setUp()

    def test_calculate_expected_filled_posts_based_on_number_of_beds(self):
        expected_filled_posts_schema = StructType(
            [
                StructField(
                    NullOutlierColumns.number_of_beds_banded, DoubleType(), True
                ),
                StructField(
                    NullOutlierColumns.avg_filled_posts_per_bed_ratio,
                    DoubleType(),
                    True,
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
                StructField(
                    NullOutlierColumns.number_of_beds_banded, DoubleType(), True
                ),
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
            df[0][NullOutlierColumns.expected_filled_posts], 7.77777, places=3
        )
        self.assertAlmostEquals(
            df[1][NullOutlierColumns.expected_filled_posts], 75.7575, places=3
        )


class CalculateFilledPostResidualsTests(
    NullAscwdsFilledPostsCareHomeJobsPerBedRatioOutlierTests
):
    def setUp(self) -> None:
        super().setUp()

    def test_calculate_filled_post_residuals(self):
        schema = StructType(
            [
                StructField(IndCQC.location_id, StringType(), True),
                StructField(IndCQC.ascwds_filled_posts_dedup_clean, DoubleType(), True),
                StructField(
                    NullOutlierColumns.expected_filled_posts, DoubleType(), True
                ),
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
        self.assertAlmostEquals(df[0][NullOutlierColumns.residual], 1.23456, places=3)
        self.assertAlmostEquals(df[1][NullOutlierColumns.residual], 0.0, places=3)
        self.assertAlmostEquals(df[2][NullOutlierColumns.residual], -1.23456, places=3)


class CalculateFilledPostStandardisedResidualsTests(
    NullAscwdsFilledPostsCareHomeJobsPerBedRatioOutlierTests
):
    def setUp(self) -> None:
        super().setUp()

    def test_calculate_filled_post_standardised_residual(self):
        schema = StructType(
            [
                StructField(IndCQC.location_id, StringType(), True),
                StructField(NullOutlierColumns.residual, DoubleType(), True),
                StructField(
                    NullOutlierColumns.expected_filled_posts, DoubleType(), True
                ),
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
            df[0][NullOutlierColumns.standardised_residual], 5.55556, places=2
        )
        self.assertAlmostEquals(
            df[1][NullOutlierColumns.standardised_residual], 3.55, places=2
        )


class CalculateLowerAndUpperStandardisedResidualCutoffTests(
    NullAscwdsFilledPostsCareHomeJobsPerBedRatioOutlierTests
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
            self.returned_data[0][NullOutlierColumns.lower_percentile],
            self.expected_data[0][NullOutlierColumns.lower_percentile],
            places=2,
        )
        self.assertAlmostEquals(
            self.returned_data[1][NullOutlierColumns.lower_percentile],
            self.expected_data[1][NullOutlierColumns.lower_percentile],
            places=2,
        )
        self.assertAlmostEquals(
            self.returned_data[2][NullOutlierColumns.lower_percentile],
            self.expected_data[2][NullOutlierColumns.lower_percentile],
            places=2,
        )
        self.assertAlmostEquals(
            self.returned_data[3][NullOutlierColumns.lower_percentile],
            self.expected_data[3][NullOutlierColumns.lower_percentile],
            places=2,
        )
        self.assertAlmostEquals(
            self.returned_data[4][NullOutlierColumns.lower_percentile],
            self.expected_data[4][NullOutlierColumns.lower_percentile],
            places=2,
        )
        self.assertAlmostEquals(
            self.returned_data[5][NullOutlierColumns.lower_percentile],
            self.expected_data[5][NullOutlierColumns.lower_percentile],
            places=2,
        )
        self.assertAlmostEquals(
            self.returned_data[6][NullOutlierColumns.lower_percentile],
            self.expected_data[6][NullOutlierColumns.lower_percentile],
            places=2,
        )
        self.assertAlmostEquals(
            self.returned_data[7][NullOutlierColumns.lower_percentile],
            self.expected_data[7][NullOutlierColumns.lower_percentile],
            places=2,
        )

    def test_calculate_standardised_residual_percentile_cutoffs_returns_expected_upper_percentile_values(
        self,
    ):
        self.assertAlmostEquals(
            self.returned_data[0][NullOutlierColumns.upper_percentile],
            self.expected_data[0][NullOutlierColumns.upper_percentile],
            places=2,
        )
        self.assertAlmostEquals(
            self.returned_data[1][NullOutlierColumns.upper_percentile],
            self.expected_data[1][NullOutlierColumns.upper_percentile],
            places=2,
        )
        self.assertAlmostEquals(
            self.returned_data[2][NullOutlierColumns.upper_percentile],
            self.expected_data[2][NullOutlierColumns.upper_percentile],
            places=2,
        )
        self.assertAlmostEquals(
            self.returned_data[3][NullOutlierColumns.upper_percentile],
            self.expected_data[3][NullOutlierColumns.upper_percentile],
            places=2,
        )
        self.assertAlmostEquals(
            self.returned_data[4][NullOutlierColumns.upper_percentile],
            self.expected_data[4][NullOutlierColumns.upper_percentile],
            places=2,
        )
        self.assertAlmostEquals(
            self.returned_data[5][NullOutlierColumns.upper_percentile],
            self.expected_data[5][NullOutlierColumns.upper_percentile],
            places=2,
        )
        self.assertAlmostEquals(
            self.returned_data[6][NullOutlierColumns.upper_percentile],
            self.expected_data[6][NullOutlierColumns.upper_percentile],
            places=2,
        )
        self.assertAlmostEquals(
            self.returned_data[7][NullOutlierColumns.upper_percentile],
            self.expected_data[7][NullOutlierColumns.upper_percentile],
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


class NullValuesOutsideOfStandardisedResidualCutoffsTests(
    NullAscwdsFilledPostsCareHomeJobsPerBedRatioOutlierTests
):
    def setUp(self) -> None:
        super().setUp()

        df = self.spark.createDataFrame(
            Data.null_values_outside_of_standardised_residual_cutoff_rows,
            Schemas.null_values_outside_of_standardised_residual_cutoff_schema,
        )
        returned_df = job.null_values_outside_of_standardised_residual_cutoffs(df)

        expected_df = self.spark.createDataFrame(
            Data.expected_null_values_outside_of_standardised_residual_cutoff_rows,
            Schemas.null_values_outside_of_standardised_residual_cutoff_schema,
        )

        self.returned_data = returned_df.sort(IndCQC.location_id).collect()
        self.expected_data = expected_df.sort(IndCQC.location_id).collect()

    def test_value_nulled_when_below_lower_cutoff(self):
        self.assertEqual(
            self.returned_data[0][IndCQC.ascwds_filled_posts_dedup_clean],
            self.expected_data[0][IndCQC.ascwds_filled_posts_dedup_clean],
        )

    def test_value_not_nulled_when_equal_to_lower_cutoff(self):
        self.assertEqual(
            self.returned_data[1][IndCQC.ascwds_filled_posts_dedup_clean],
            self.expected_data[1][IndCQC.ascwds_filled_posts_dedup_clean],
        )

    def test_value_not_nulled_when_in_between_cutoffs(self):
        self.assertEqual(
            self.returned_data[2][IndCQC.ascwds_filled_posts_dedup_clean],
            self.expected_data[2][IndCQC.ascwds_filled_posts_dedup_clean],
        )

    def test_value_not_nulled_when_equal_to_upper_cutoff(self):
        self.assertEqual(
            self.returned_data[3][IndCQC.ascwds_filled_posts_dedup_clean],
            self.expected_data[3][IndCQC.ascwds_filled_posts_dedup_clean],
        )

    def test_value_nulled_when_above_upper_cutoff(self):
        self.assertEqual(
            self.returned_data[4][IndCQC.ascwds_filled_posts_dedup_clean],
            self.expected_data[4][IndCQC.ascwds_filled_posts_dedup_clean],
        )


class CombineDataframeTests(NullAscwdsFilledPostsCareHomeJobsPerBedRatioOutlierTests):
    def setUp(self) -> None:
        super().setUp()

        self.care_home_df = self.spark.createDataFrame(
            Data.combine_dataframes_care_home_rows,
            Schemas.combine_dataframes_care_home_schema,
        )
        self.non_care_home_df = self.spark.createDataFrame(
            Data.combine_dataframes_non_care_home_rows,
            Schemas.combine_dataframes_non_care_home_schema,
        )
        self.returned_combined_df = job.combine_dataframes(
            self.care_home_df, self.non_care_home_df
        )

    def test_combine_dataframes_keeps_all_rows_of_data(self):
        self.assertEqual(
            self.returned_combined_df.count(),
            (self.care_home_df.count() + self.non_care_home_df.count()),
        )

    def test_combine_dataframes_has_same_schema_as_original_non_care_home_df(self):
        self.assertEqual(self.returned_combined_df.schema, self.non_care_home_df.schema)

    def test_returned_combined_dataframe_matches_expected_df(self):
        expected_df = self.spark.createDataFrame(
            Data.expected_combined_dataframes_rows,
            Schemas.expected_combined_dataframes_schema,
        )

        returned_data = self.returned_combined_df.sort(IndCQC.location_id).collect()
        expected_data = expected_df.sort(IndCQC.location_id).collect()

        self.assertEqual(returned_data, expected_data)
