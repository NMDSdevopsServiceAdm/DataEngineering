import unittest
from unittest.mock import Mock, patch

from projects._03_independent_cqc._06_estimate_filled_posts.utils.models import (
    estimate_non_res_ct_filled_posts as job,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_data import (
    EstimateNonResCTFilledPostsData as Data,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_schemas import (
    EstimateNonResCTFilledPostsSchemas as Schemas,
)
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc

PATCH_PATH: str = (
    "projects._03_independent_cqc._06_estimate_filled_posts.utils.models.estimate_non_res_ct_filled_posts"
)


class EstimateNonResCTFilledPostsTests(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()
        self.estimates_df = self.spark.createDataFrame(
            Data.estimates_rows,
            Schemas.estimates_schema,
        )


class MainTests(EstimateNonResCTFilledPostsTests):
    def setUp(self) -> None:
        super().setUp()

    @patch(f"{PATCH_PATH}.merge_columns_in_order")
    @patch(f"{PATCH_PATH}.convert_to_all_posts_using_ratio")
    @patch(f"{PATCH_PATH}.calculate_care_worker_ratio")
    def test_estimate_non_res_capacity_tracker_filled_posts_runs(
        self,
        calculate_care_worker_ratio_mock: Mock,
        convert_to_all_posts_using_ratio_mock: Mock,
        merge_columns_in_order_mock: Mock,
    ):
        job.estimate_non_res_capacity_tracker_filled_posts(self.estimates_df)

        calculate_care_worker_ratio_mock.assert_called_once()
        convert_to_all_posts_using_ratio_mock.assert_called_once()
        merge_columns_in_order_mock.assert_called_once()


class CalculateCareWorkerRatioTests(EstimateNonResCTFilledPostsTests):
    def setUp(self) -> None:
        super().setUp()

    def test_calculate_care_worker_ratio_returns_correct_ratio(self):
        returned_ratio = job.calculate_care_worker_ratio(self.estimates_df)
        expected_ratio = Data.expected_care_worker_ratio

        self.assertEqual(returned_ratio, expected_ratio)


class ConvertToAllPostsUsingRatioTest(EstimateNonResCTFilledPostsTests):
    def setUp(self) -> None:
        super().setUp()

    def test_convert_to_all_posts_using_ratio_returns_correct_values(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.convert_to_all_posts_using_ratio_rows,
            Schemas.convert_to_all_posts_using_ratio_schema,
        )
        test_ratio = Data.expected_care_worker_ratio
        expected_df = self.spark.createDataFrame(
            Data.expected_convert_to_all_posts_using_ratio_rows,
            Schemas.expected_convert_to_all_posts_using_ratio_schema,
        )
        returned_df = job.convert_to_all_posts_using_ratio(test_df, test_ratio)

        self.assertEqual(
            returned_df.sort(IndCqc.location_id).collect(),
            expected_df.collect(),
        )
