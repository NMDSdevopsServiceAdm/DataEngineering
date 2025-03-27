import unittest
import warnings

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
from utils.ind_cqc_filled_posts_utils.clean_ascwds_filled_post_outliers import (
    winsorize_care_home_filled_posts_per_bed_ratio_outliers as job,
)
from tests.test_file_data import (
    WinsorizeCareHomeFilledPostsPerBedRatioOutliersData as Data,
)
from tests.test_file_schemas import (
    WinsorizeCareHomeFilledPostsPerBedRatioOutliersSchema as Schemas,
)


class WinsorizeAscwdsFilledPostsCareHomeJobsPerBedRatioOutlierTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.unfiltered_ind_cqc_df = self.spark.createDataFrame(
            Data.unfiltered_ind_cqc_rows,
            Schemas.ind_cqc_schema,
        )

        warnings.filterwarnings("ignore", category=ResourceWarning)


class MainTests(WinsorizeAscwdsFilledPostsCareHomeJobsPerBedRatioOutlierTests):
    def setUp(self) -> None:
        super().setUp()

        self.returned_filtered_df = (
            job.winsorize_care_home_filled_posts_per_bed_ratio_outliers(
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


class SetValuesForWinsorizationTests(
    WinsorizeAscwdsFilledPostsCareHomeJobsPerBedRatioOutlierTests
):
    def setUp(self) -> None:
        super().setUp()

    def test_percentage_of_data_to_remove_as_outliers_value(self):
        self.assertEqual(
            job.SetValuesForWinsorization.PERCENTAGE_OF_DATA_TO_REMOVE_AS_OUTLIERS, 0.05
        )

    def test_minimum_permitted_lower_ratio_cutoff_value(self):
        self.assertEqual(
            job.SetValuesForWinsorization.MINIMUM_PERMITTED_LOWER_RATIO_CUTOFF, 0.75
        )

    def test_minimum_permitted_upper_ratio_cutoff_value(self):
        self.assertEqual(
            job.SetValuesForWinsorization.MINIMUM_PERMITTED_UPPER_RATIO_CUTOFF, 5.0
        )


class FilterToCareHomesWithKnownBedsAndFilledPostsTests(
    WinsorizeAscwdsFilledPostsCareHomeJobsPerBedRatioOutlierTests
):
    def setUp(self) -> None:
        super().setUp()

        unfiltered_df = self.spark.createDataFrame(
            Data.filter_df_to_care_homes_with_known_beds_and_filled_posts_rows,
            Schemas.filter_df_to_care_homes_with_known_beds_and_filled_posts_schema,
        )
        self.returned_df = job.filter_df_to_care_homes_with_known_beds_and_filled_posts(
            unfiltered_df
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_filtered_df_to_care_homes_with_known_beds_and_filled_posts_rows,
            Schemas.filter_df_to_care_homes_with_known_beds_and_filled_posts_schema,
        )
        self.returned_data = self.returned_df.collect()
        self.expected_data = self.expected_df.collect()

    def test_filtered_dataframe_has_expected_number_of_rows(self):
        self.assertEqual(self.returned_df.count(), self.expected_df.count())

    def test_filtered_dataframe_matches_expected_dataframe(self):
        self.assertEqual(self.returned_data, self.expected_data)


class SelectDataNotInSubsetTests(
    WinsorizeAscwdsFilledPostsCareHomeJobsPerBedRatioOutlierTests
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


class CalculateAverageFilledPostsPerBandedBedCount(
    WinsorizeAscwdsFilledPostsCareHomeJobsPerBedRatioOutlierTests
):
    def setUp(self) -> None:
        super().setUp()

    def test_calculate_average_filled_posts_per_banded_bed_count(self):
        schema = StructType(
            [
                StructField(IndCQC.location_id, StringType(), True),
                StructField(IndCQC.number_of_beds_banded, DoubleType(), True),
                StructField(IndCQC.filled_posts_per_bed_ratio, DoubleType(), True),
            ]
        )
        rows = [
            ("1", 0.0, 1.1357),
            ("2", 0.0, 1.3579),
            ("3", 1.0, 1.123456789),
        ]
        df = self.spark.createDataFrame(rows, schema)
        df = job.calculate_average_filled_posts_per_banded_bed_count(df)

        df = df.sort(IndCQC.number_of_beds_banded).collect()
        self.assertAlmostEquals(
            df[0][IndCQC.avg_filled_posts_per_bed_ratio], 1.2468, places=3
        )
        self.assertAlmostEquals(
            df[1][IndCQC.avg_filled_posts_per_bed_ratio], 1.12346, places=3
        )


class CalculateExpectedFilledPostsBasedOnNumberOfBedsTests(
    WinsorizeAscwdsFilledPostsCareHomeJobsPerBedRatioOutlierTests
):
    def setUp(self) -> None:
        super().setUp()

    def test_calculate_expected_filled_posts_based_on_number_of_beds(self):
        expected_filled_posts_schema = StructType(
            [
                StructField(IndCQC.number_of_beds_banded, DoubleType(), True),
                StructField(
                    IndCQC.avg_filled_posts_per_bed_ratio,
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
                StructField(IndCQC.number_of_beds_banded, DoubleType(), True),
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
        self.assertAlmostEquals(df[0][IndCQC.expected_filled_posts], 7.77777, places=3)
        self.assertAlmostEquals(df[1][IndCQC.expected_filled_posts], 75.7575, places=3)


class CalculateFilledPostStandardisedResidualsTests(
    WinsorizeAscwdsFilledPostsCareHomeJobsPerBedRatioOutlierTests
):
    def setUp(self) -> None:
        super().setUp()

        calculate_standardised_residuals_df = self.spark.createDataFrame(
            Data.calculate_standardised_residuals_rows,
            Schemas.calculate_standardised_residuals_schema,
        )
        self.returned_df = job.calculate_filled_post_standardised_residual(
            calculate_standardised_residuals_df
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_calculate_standardised_residuals_rows,
            Schemas.expected_calculate_standardised_residuals_schema,
        )
        self.returned_data = self.returned_df.sort(IndCQC.location_id).collect()
        self.expected_data = self.expected_df.collect()

    def test_calculate_filled_post_standardised_residual_matches_expected_dataframe(
        self,
    ):
        self.assertEqual(self.returned_data, self.expected_data)


class CalculateLowerAndUpperStandardisedResidualCutoffTests(
    WinsorizeAscwdsFilledPostsCareHomeJobsPerBedRatioOutlierTests
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
            self.returned_data[0][IndCQC.lower_percentile],
            self.expected_data[0][IndCQC.lower_percentile],
            places=2,
        )
        self.assertAlmostEquals(
            self.returned_data[1][IndCQC.lower_percentile],
            self.expected_data[1][IndCQC.lower_percentile],
            places=2,
        )
        self.assertAlmostEquals(
            self.returned_data[2][IndCQC.lower_percentile],
            self.expected_data[2][IndCQC.lower_percentile],
            places=2,
        )
        self.assertAlmostEquals(
            self.returned_data[3][IndCQC.lower_percentile],
            self.expected_data[3][IndCQC.lower_percentile],
            places=2,
        )
        self.assertAlmostEquals(
            self.returned_data[4][IndCQC.lower_percentile],
            self.expected_data[4][IndCQC.lower_percentile],
            places=2,
        )
        self.assertAlmostEquals(
            self.returned_data[5][IndCQC.lower_percentile],
            self.expected_data[5][IndCQC.lower_percentile],
            places=2,
        )
        self.assertAlmostEquals(
            self.returned_data[6][IndCQC.lower_percentile],
            self.expected_data[6][IndCQC.lower_percentile],
            places=2,
        )
        self.assertAlmostEquals(
            self.returned_data[7][IndCQC.lower_percentile],
            self.expected_data[7][IndCQC.lower_percentile],
            places=2,
        )

    def test_calculate_standardised_residual_percentile_cutoffs_returns_expected_upper_percentile_values(
        self,
    ):
        self.assertAlmostEquals(
            self.returned_data[0][IndCQC.upper_percentile],
            self.expected_data[0][IndCQC.upper_percentile],
            places=2,
        )
        self.assertAlmostEquals(
            self.returned_data[1][IndCQC.upper_percentile],
            self.expected_data[1][IndCQC.upper_percentile],
            places=2,
        )
        self.assertAlmostEquals(
            self.returned_data[2][IndCQC.upper_percentile],
            self.expected_data[2][IndCQC.upper_percentile],
            places=2,
        )
        self.assertAlmostEquals(
            self.returned_data[3][IndCQC.upper_percentile],
            self.expected_data[3][IndCQC.upper_percentile],
            places=2,
        )
        self.assertAlmostEquals(
            self.returned_data[4][IndCQC.upper_percentile],
            self.expected_data[4][IndCQC.upper_percentile],
            places=2,
        )
        self.assertAlmostEquals(
            self.returned_data[5][IndCQC.upper_percentile],
            self.expected_data[5][IndCQC.upper_percentile],
            places=2,
        )
        self.assertAlmostEquals(
            self.returned_data[6][IndCQC.upper_percentile],
            self.expected_data[6][IndCQC.upper_percentile],
            places=2,
        )
        self.assertAlmostEquals(
            self.returned_data[7][IndCQC.upper_percentile],
            self.expected_data[7][IndCQC.upper_percentile],
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


class DuplicateRatiosWithinStandardisedResidualCutoffsTests(
    WinsorizeAscwdsFilledPostsCareHomeJobsPerBedRatioOutlierTests
):
    def setUp(self) -> None:
        super().setUp()

        df = self.spark.createDataFrame(
            Data.duplicate_ratios_within_standardised_residual_cutoff_rows,
            Schemas.duplicate_ratios_within_standardised_residual_cutoff_schema,
        )
        returned_df = job.duplicate_ratios_within_standardised_residual_cutoffs(df)

        expected_df = self.spark.createDataFrame(
            Data.expected_duplicate_ratios_within_standardised_residual_cutoff_rows,
            Schemas.expected_duplicate_ratios_within_standardised_residual_cutoff_schema,
        )

        self.returned_data = returned_df.sort(IndCQC.location_id).collect()
        self.expected_data = expected_df.sort(IndCQC.location_id).collect()

    def test_ratio_not_duplicated_when_below_lower_cutoff(self):
        self.assertEqual(
            self.returned_data[0][IndCQC.filled_posts_per_bed_ratio_within_std_resids],
            self.expected_data[0][IndCQC.filled_posts_per_bed_ratio_within_std_resids],
        )

    def test_ratio_duplicated_when_equal_to_lower_cutoff(self):
        self.assertEqual(
            self.returned_data[1][IndCQC.filled_posts_per_bed_ratio_within_std_resids],
            self.expected_data[1][IndCQC.filled_posts_per_bed_ratio_within_std_resids],
        )

    def test_ratio_duplicated_when_in_between_cutoffs(self):
        self.assertEqual(
            self.returned_data[2][IndCQC.filled_posts_per_bed_ratio_within_std_resids],
            self.expected_data[2][IndCQC.filled_posts_per_bed_ratio_within_std_resids],
        )

    def test_ratio_duplicated_when_equal_to_upper_cutoff(self):
        self.assertEqual(
            self.returned_data[3][IndCQC.filled_posts_per_bed_ratio_within_std_resids],
            self.expected_data[3][IndCQC.filled_posts_per_bed_ratio_within_std_resids],
        )

    def test_ratio_not_duplicated_when_above_upper_cutoff(self):
        self.assertEqual(
            self.returned_data[4][IndCQC.filled_posts_per_bed_ratio_within_std_resids],
            self.expected_data[4][IndCQC.filled_posts_per_bed_ratio_within_std_resids],
        )


class CalculateMinAndMaxPermittedFilledPostPerBedRatiosTests(
    WinsorizeAscwdsFilledPostsCareHomeJobsPerBedRatioOutlierTests
):
    def setUp(self) -> None:
        super().setUp()

        self.care_home_df = self.spark.createDataFrame(
            Data.min_and_max_permitted_ratios_rows,
            Schemas.min_and_max_permitted_ratios_schema,
        )
        self.returned_df = (
            job.calculate_min_and_max_permitted_filled_posts_per_bed_ratios(
                self.care_home_df
            )
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_min_and_max_permitted_ratios_rows,
            Schemas.expected_min_and_max_permitted_ratios_schema,
        )

        self.returned_data = self.returned_df.sort(IndCQC.location_id).collect()
        self.expected_data = self.expected_df.sort(IndCQC.location_id).collect()

    def test_returned_data_has_same_number_of_rows_as_original_df(self):
        self.assertEqual(self.returned_df.count(), self.care_home_df.count())

    def test_returned_data_has_expected_columns(self):
        self.assertEqual(
            sorted(self.returned_df.columns), sorted(self.expected_df.columns)
        )

    def test_returned_min_values_match_expected(self):
        for i in range(len(self.returned_data)):
            self.assertEqual(
                self.returned_data[i][IndCQC.min_filled_posts_per_bed_ratio],
                self.expected_data[i][IndCQC.min_filled_posts_per_bed_ratio],
                f"Returned row {i} does not match expected",
            )

    def test_returned_max_values_match_expected(self):
        for i in range(len(self.returned_data)):
            self.assertEqual(
                self.returned_data[i][IndCQC.max_filled_posts_per_bed_ratio],
                self.expected_data[i][IndCQC.max_filled_posts_per_bed_ratio],
                f"Returned row {i} does not match expected",
            )


class SetMinimumPermittedRatioTests(
    WinsorizeAscwdsFilledPostsCareHomeJobsPerBedRatioOutlierTests
):
    def setUp(self) -> None:
        super().setUp()

    def test_returned_data_matches_expected_data(self):
        TEST_MIN_VALUE: float = 0.75

        care_home_df = self.spark.createDataFrame(
            Data.set_minimum_permitted_ratio_rows,
            Schemas.set_minimum_permitted_ratio_schema,
        )
        returned_df = job.set_minimum_permitted_ratio(
            care_home_df, IndCQC.filled_posts_per_bed_ratio, TEST_MIN_VALUE
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_set_minimum_permitted_ratio_rows,
            Schemas.set_minimum_permitted_ratio_schema,
        )

        returned_data = returned_df.sort(IndCQC.location_id).collect()
        expected_data = expected_df.sort(IndCQC.location_id).collect()

        self.assertEqual(returned_data, expected_data)


class WinsorizeOutliersTests(
    WinsorizeAscwdsFilledPostsCareHomeJobsPerBedRatioOutlierTests
):
    def setUp(self) -> None:
        super().setUp()

        self.care_home_df = self.spark.createDataFrame(
            Data.winsorize_outliers_rows,
            Schemas.winsorize_outliers_schema,
        )
        self.returned_df = job.winsorize_outliers(self.care_home_df)

        self.expected_df = self.spark.createDataFrame(
            Data.expected_winsorize_outliers_rows,
            Schemas.winsorize_outliers_schema,
        )

        self.returned_data = self.returned_df.sort(IndCQC.location_id).collect()
        self.expected_data = self.expected_df.sort(IndCQC.location_id).collect()

    def test_returned_filled_post_values_match_expected(self):
        for i in range(len(self.returned_data)):
            self.assertEqual(
                self.returned_data[i][IndCQC.ascwds_filled_posts_dedup_clean],
                self.expected_data[i][IndCQC.ascwds_filled_posts_dedup_clean],
                f"Returned row {i} does not match expected",
            )

    def test_returned_filled_posts_per_bed_ratio_values_match_expected(self):
        for i in range(len(self.returned_data)):
            self.assertEqual(
                self.returned_data[i][IndCQC.filled_posts_per_bed_ratio],
                self.expected_data[i][IndCQC.filled_posts_per_bed_ratio],
                f"Returned row {i} does not match expected",
            )


class CombineDataframeTests(
    WinsorizeAscwdsFilledPostsCareHomeJobsPerBedRatioOutlierTests
):
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
