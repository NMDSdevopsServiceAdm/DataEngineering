import unittest
from unittest.mock import Mock, patch

import polars as pl
import polars.testing as pl_testing

from projects._03_independent_cqc._02_clean.fargate.utils.clean_ascwds_filled_post_outliers import (
    winsorize_care_home_filled_posts_per_bed_ratio_outliers as job,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    WinsorizeCareHomeFilledPostsPerBedRatioOutliersData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    WinsorizeCareHomeFilledPostsPerBedRatioOutliersSchema as Schemas,
)

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


class WinsorizeCHFilledPostsPerBedRatioOutliersTests(unittest.TestCase):
    def setUp(self) -> None:
        self.unfiltered_ind_cqc_lf = pl.LazyFrame(
            Data.unfiltered_ind_cqc_rows, Schemas.ind_cqc_schema, orient="row"
        )

        self.returned_filtered_lf = (
            job.winsorize_care_home_filled_posts_per_bed_ratio_outliers(
                self.unfiltered_ind_cqc_lf
            )
        )

    def test_returned_filtered_lf_has_same_number_of_rows_as_initial_unfiltered_lf(
        self,
    ):
        self.assertEqual(
            self.unfiltered_ind_cqc_lf.collect().height,
            self.returned_filtered_lf.collect().height,
        )

    def test_returned_filtered_lf_has_same_schema_as_initial_unfiltered_lf(
        self,
    ):
        self.assertEqual(
            self.unfiltered_ind_cqc_lf.collect_schema(),
            self.returned_filtered_lf.collect_schema(),
        )

    def test_function_returns_expected_values(self):
        expected_filtered_lf = pl.LazyFrame(
            Data.expected_care_home_jobs_per_bed_ratio_filtered_rows,
            Schemas.ind_cqc_schema,
            orient="row",
        )
        returned_data = self.returned_filtered_lf.sort(IndCQC.location_id)
        expected_data = expected_filtered_lf.sort(IndCQC.location_id)

        pl_testing.assert_frame_equal(expected_data, returned_data)


class SetValuesForWinsorizationTests(unittest.TestCase):
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


class FilterToCareHomesWithKnownBedsAndFilledPostsTests(unittest.TestCase):
    def setUp(self) -> None:

        unfiltered_lf = pl.LazyFrame(
            Data.filter_df_to_care_homes_with_known_beds_and_filled_posts_rows,
            Schemas.filter_df_to_care_homes_with_known_beds_and_filled_posts_schema,
            orient="row",
        )
        self.returned_lf = job.filter_lf_to_care_homes_with_known_beds_and_filled_posts(
            unfiltered_lf
        )
        self.expected_lf = pl.LazyFrame(
            Data.expected_filtered_df_to_care_homes_with_known_beds_and_filled_posts_rows,
            Schemas.filter_df_to_care_homes_with_known_beds_and_filled_posts_schema,
            orient="row",
        )

    def test_filtered_lf_matches_expected_dataframe(self):
        pl_testing.assert_frame_equal(self.returned_lf, self.expected_lf)


class SelectDataNotInSubsetTests(unittest.TestCase):
    def test_function_returns_expected_values(self):
        lf = pl.LazyFrame(
            data=Data.select_data_not_in_subset_rows,
            schema=Schemas.select_data_not_in_subset_schema,
            orient="row",
        )
        subset_lf = pl.LazyFrame(
            data=Data.subset_rows,
            schema=Schemas.select_data_not_in_subset_schema,
            orient="row",
        )

        returned_lf = job.select_data_not_in_subset(lf, subset_lf)
        expected_lf = pl.LazyFrame(
            data=Data.expected_select_data_not_in_subset_rows,
            schema=Schemas.select_data_not_in_subset_schema,
            orient="row",
        )

        pl_testing.assert_frame_equal(
            returned_lf,
            expected_lf,
        )


class CalculateAverageFilledPostsPerBandedBedCount(unittest.TestCase):
    def test_function_returns_expected_values(self):
        test_lf = pl.LazyFrame(
            data=Data.calculate_average_filled_posts_rows,
            schema=Schemas.calculate_average_filled_posts_schema,
            orient="row",
        )
        returned_lf = job.calculate_average_filled_posts_per_banded_bed_count(test_lf)
        expected_lf = pl.LazyFrame(
            data=Data.expected_calculate_average_filled_posts_rows,
            schema=Schemas.expected_calculate_average_filled_posts_schema,
            orient="row",
        )
        pl_testing.assert_frame_equal(
            returned_lf.sort(IndCQC.number_of_beds_banded), expected_lf
        )


class CalculateExpectedFilledPostsBasedOnNumberOfBedsTests(unittest.TestCase):
    def test_function_returns_expected_values(self):
        test_base_lf = pl.LazyFrame(
            data=Data.base_filled_posts_rows,
            schema=Schemas.calculate_expected_filled_posts_base_schema,
            orient="row",
        )
        test_join_lf = pl.LazyFrame(
            data=Data.join_filled_posts_rows,
            schema=Schemas.calculate_expected_filled_posts_join_schema,
            orient="row",
        )

        returned_lf = job.calculate_expected_filled_posts_based_on_number_of_beds(
            test_base_lf, test_join_lf
        )

        expected_lf = pl.LazyFrame(
            data=Data.expected_filled_posts_rows,
            schema=Schemas.expected_filled_posts_schema,
            orient="row",
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class CalculateFilledPostStandardisedResidualsTests(unittest.TestCase):
    def setUp(self) -> None:
        test_lf = pl.LazyFrame(
            Data.calculate_standardised_residuals_rows,
            Schemas.calculate_standardised_residuals_schema,
            orient="row",
        )
        self.returned_lf = job.calculate_filled_post_standardised_residual(test_lf)
        self.expected_lf = pl.LazyFrame(
            Data.expected_calculate_standardised_residuals_rows,
            Schemas.expected_calculate_standardised_residuals_schema,
            orient="row",
        )

    def test_function_returns_expected_values(
        self,
    ):
        pl_testing.assert_frame_equal(self.returned_lf, self.expected_lf)


class CalculateLowerAndUpperStandardisedResidualCutoffTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_lf = pl.LazyFrame(
            Data.standardised_residual_percentile_cutoff_rows,
            Schemas.standardised_residual_percentile_cutoff_schema,
            orient="row",
        )
        self.returned_lf = (
            job.calculate_lower_and_upper_standardised_residual_percentile_cutoffs(
                self.test_lf,
                0.4,
            )
        )
        self.expected_lf = pl.LazyFrame(
            Data.expected_standardised_residual_percentile_cutoff_with_percentiles_rows,
            Schemas.expected_standardised_residual_percentile_cutoff_with_percentiles_schema,
            orient="row",
        )

    def test_function_returns_expected_percentile_values(
        self,
    ):
        pl_testing.assert_frame_equal(self.returned_lf, self.expected_lf)

    def test_raise_error_if_percentage_of_data_to_filter_out_equal_to_one(self):
        with self.assertRaises(ValueError) as context:
            job.calculate_lower_and_upper_standardised_residual_percentile_cutoffs(
                self.test_lf,
                1.0,
            )

        self.assertTrue(
            "Percentage of data to filter out must be less than 1 (equivalent to 100%)"
            in str(context.exception)
        )

    def test_function_raises_error_if_percentage_of_data_to_filter_out_greater_than_one(
        self,
    ):
        with self.assertRaises(ValueError) as context:
            job.calculate_lower_and_upper_standardised_residual_percentile_cutoffs(
                self.test_lf,
                1.1,
            )

        self.assertTrue(
            "Percentage of data to filter out must be less than 1 (equivalent to 100%)"
            in str(context.exception)
        )


class DuplicateRatiosWithinStandardisedResidualCutoffsTests(unittest.TestCase):
    def setUp(self) -> None:
        test_lf = pl.LazyFrame(
            Data.duplicate_ratios_within_standardised_residual_cutoff_rows,
            Schemas.duplicate_ratios_within_standardised_residual_cutoff_schema,
            orient="row",
        )
        self.returned_lf = job.duplicate_ratios_within_standardised_residual_cutoffs(
            test_lf
        )

        self.expected_lf = pl.LazyFrame(
            Data.expected_duplicate_ratios_within_standardised_residual_cutoff_rows,
            Schemas.expected_duplicate_ratios_within_standardised_residual_cutoff_schema,
            orient="row",
        )

    def test_function_returns_expected_values(self):
        pl_testing.assert_frame_equal(self.returned_lf, self.expected_lf)


class CalculateMinAndMaxPermittedFilledPostPerBedRatiosTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_lf = pl.LazyFrame(
            Data.min_and_max_permitted_ratios_rows,
            Schemas.min_and_max_permitted_ratios_schema,
            orient="row",
        )
        self.returned_lf = (
            job.calculate_min_and_max_permitted_filled_posts_per_bed_ratios(
                self.test_lf
            )
        )
        self.expected_lf = pl.LazyFrame(
            Data.expected_min_and_max_permitted_ratios_rows,
            Schemas.expected_min_and_max_permitted_ratios_schema,
            orient="row",
        )

    def test_function_returns_expected_values(self):
        pl_testing.assert_frame_equal(self.returned_lf, self.expected_lf)


class SetMinimumPermittedRatioTests(unittest.TestCase):
    def test_function_returns_expected_values(self):
        TEST_MIN_VALUE: float = 0.75

        test_lf = pl.LazyFrame(
            Data.set_minimum_permitted_ratio_rows,
            Schemas.set_minimum_permitted_ratio_schema,
            orient="row",
        )
        returned_lf = job.set_minimum_permitted_ratio(
            test_lf, IndCQC.filled_posts_per_bed_ratio, TEST_MIN_VALUE
        )
        expected_lf = pl.LazyFrame(
            Data.expected_set_minimum_permitted_ratio_rows,
            Schemas.set_minimum_permitted_ratio_schema,
            orient="row",
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class WinsorizeOutliersTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_lf = pl.LazyFrame(
            Data.winsorize_outliers_rows,
            Schemas.winsorize_outliers_schema,
            orient="row",
        )
        self.returned_lf = job.winsorize_outliers(self.test_lf)

        self.expected_lf = pl.LazyFrame(
            Data.expected_winsorize_outliers_rows,
            Schemas.winsorize_outliers_schema,
            orient="row",
        )

    def test_function_returns_expected_values(self):
        pl_testing.assert_frame_equal(self.returned_lf, self.expected_lf)


class CombineDataframeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.care_home_lf = pl.LazyFrame(
            Data.combine_dataframes_care_home_rows,
            Schemas.combine_dataframes_care_home_schema,
            orient="row",
        )
        self.non_care_home_lf = pl.LazyFrame(
            Data.combine_dataframes_non_care_home_rows,
            Schemas.combine_dataframes_non_care_home_schema,
            orient="row",
        )
        self.returned_combined_lf = job.combine_data(
            self.care_home_lf, self.non_care_home_lf
        )

    def test_function_returns_expected_values(self):
        expected_lf = pl.LazyFrame(
            Data.expected_combined_dataframes_rows,
            Schemas.expected_combined_dataframes_schema,
            orient="row",
        )
        pl_testing.assert_frame_equal(
            self.returned_combined_lf.sort(IndCQC.location_id), expected_lf
        )
