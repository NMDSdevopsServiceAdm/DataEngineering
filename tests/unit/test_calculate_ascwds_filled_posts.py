import unittest
import warnings

from tests.test_file_data import CalculateAscwdsFilledPostsData as Data
from tests.test_file_schemas import CalculateAscwdsFilledPostsSchemas as Schemas

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)

import utils.ind_cqc_filled_posts_utils.ascwds_filled_posts_calculator.ascwds_filled_posts_calculator as job


class TestAscwdsFilledPostsCalculator(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()
        self.ascwds_total_staff_and_worker_record_df = self.spark.createDataFrame(
            Data.calculate_ascwds_filled_posts_rows,
            Schemas.calculate_ascwds_filled_posts_schema,
        )
        warnings.filterwarnings("ignore", category=ResourceWarning)

    def test_calculate_ascwds_filled_posts_returns_expected_data(
        self,
    ):
        returned_df = job.calculate_ascwds_filled_posts(
            self.ascwds_total_staff_and_worker_record_df,
            IndCQC.total_staff_bounded,
            IndCQC.worker_records_bounded,
            IndCQC.ascwds_filled_posts,
            IndCQC.ascwds_filled_posts_source,
        )

        expected_df = self.spark.createDataFrame(
            Data.expected_ascwds_filled_posts_rows,
            Schemas.calculate_ascwds_filled_posts_schema,
        )

        returned_data = returned_df.sort(IndCQC.location_id).collect()
        expected_data = expected_df.sort(IndCQC.location_id).collect()
        self.assertEqual(returned_data, expected_data)
