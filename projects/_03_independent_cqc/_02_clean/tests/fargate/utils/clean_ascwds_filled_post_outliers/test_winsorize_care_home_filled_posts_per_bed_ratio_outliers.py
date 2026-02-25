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

    def test_returned_filtered_df_has_same_number_of_rows_as_initial_unfiltered_df(
        self,
    ):
        self.assertEqual(
            self.unfiltered_ind_cqc_lf.collect().height,
            self.returned_filtered_lf.collect().height,
        )

    def test_returned_filtered_df_has_same_schema_as_initial_unfiltered_df(
        self,
    ):
        self.assertEqual(
            self.unfiltered_ind_cqc_lf.collect_schema(),
            self.returned_filtered_lf.collect_schema(),
        )

    def test_returned_df_matches_expected_df(self):
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

    def test_filtered_lazyframe_has_expected_number_of_rows(self):
        self.assertEqual(
            self.returned_lf.collect().height, self.expected_lf.collect().height
        )

    def test_filtered_lazyframe_matches_expected_dataframe(self):
        pl_testing.assert_frame_equal(self.returned_lf, self.expected_lf)


class SelectDataNotInSubsetTests(WinsorizeCHFilledPostsPerBedRatioOutliersTests):
    def test_select_data_not_in_subset_lf(self):
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

        data_not_in_subset_lf = job.select_data_not_in_subset(lf, subset_lf)
        self.assertEqual(
            data_not_in_subset_lf.collect().height,
            (lf.collect().height - subset_lf.collect().height),
        )
        expected_lf = pl.LazyFrame(
            data=Data.expected_select_data_not_in_subset_rows,
            schema=Schemas.select_data_not_in_subset_schema,
            orient="row",
        )

        pl_testing.assert_frame_equal(
            data_not_in_subset_lf,
            expected_lf,
        )


class CalculateAverageFilledPostsPerBandedBedCount(
    WinsorizeCHFilledPostsPerBedRatioOutliersTests
):
    def test_calculate_average_filled_posts_per_banded_bed_count(self):
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
