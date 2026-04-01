import unittest
from unittest.mock import Mock, patch

import polars as pl
import polars.testing as pl_testing

import projects._03_independent_cqc._06_estimate_filled_posts.fargate.utils.models.estimate_non_res_ct_filled_posts as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    EstimateNonResCapacityTrackerFilledPostsData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    EstimateNonResCapacityTrackerFilledPostsSchemas as Schemas,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

PATCH_PATH: str = (
    "projects._03_independent_cqc._06_estimate_filled_posts.fargate.utils.models.estimate_non_res_ct_filled_posts"
)


class EstimateNonResCapacityTrackerFilledPostsTests(unittest.TestCase):
    def setUp(self):
        self.expected_lf = pl.LazyFrame(
            data=Data.expected_estimate_non_res_capacity_tracker_filled_posts_rows,
            schema=Schemas.expected_estimate_non_res_capacity_tracker_filled_posts_schema,
            orient="row",
        )
        self.test_lf = self.expected_lf.drop(
            [
                IndCQC.ct_non_res_all_posts,
                IndCQC.ct_non_res_filled_post_estimate,
                IndCQC.ct_non_res_filled_post_estimate_source,
            ]
        )

    @patch(f"{PATCH_PATH}.utils.nullify_ct_values_previous_to_first_submission")
    @patch(f"{PATCH_PATH}.utils.coalesce_with_source_labels")
    @patch(f"{PATCH_PATH}.calculate_care_worker_ratio")
    def test_function_calls_expected_functions(
        self,
        calculate_care_worker_ratio_mock: Mock,
        coalesce_with_source_labels_mock: Mock,
        nullify_ct_values_previous_to_first_submission_mock: Mock,
    ):
        job.estimate_non_res_capacity_tracker_filled_posts(self.test_lf)

        calculate_care_worker_ratio_mock.assert_called_once()
        coalesce_with_source_labels_mock.assert_called_once()
        nullify_ct_values_previous_to_first_submission_mock.assert_called_once()

    def test_function_returns_expected_values(self):
        returned_lf = job.estimate_non_res_capacity_tracker_filled_posts(self.test_lf)

        pl_testing.assert_frame_equal(returned_lf, self.expected_lf)


class CalculateCareWorkerRatioTest(EstimateNonResCapacityTrackerFilledPostsTests):
    def setUp(self):
        super().setUp()

    def test_function_returns_expected_value(self):
        returned_ratio = "returned_ratio"
        expected_lf = pl.LazyFrame({returned_ratio: [0.8]})
        returned_ratio = (
            self.test_lf.with_columns(
                job.calculate_care_worker_ratio().alias("returned_ratio")
            )
            .select(returned_ratio)
            .unique()
        )
        pl_testing.assert_frame_equal(returned_ratio, expected_lf)
