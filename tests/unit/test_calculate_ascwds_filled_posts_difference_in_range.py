import unittest
import warnings

from tests.test_file_data import CleanIndCQCData as Data
from tests.test_file_schemas import CleanIndCQCData as Schemas

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)

import utils.ind_cqc_filled_posts_utils.ascwds_filled_posts_calculator.calculate_ascwds_filled_posts_difference_within_range as job


class TestAscwdsFilledPostsAbsoluteDiffInRange(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()
        self.ascwds_total_staff_and_worker_record_df = self.spark.createDataFrame(
            Data.calculate_ascwds_filled_posts_rows,
            Schemas.calculate_ascwds_filled_posts_schema,
        )
        self.df = job.calculate_ascwds_filled_posts_difference_within_range(
            self.ascwds_total_staff_and_worker_record_df,
            IndCQC.total_staff_bounded,
            IndCQC.worker_records_bounded,
            IndCQC.ascwds_filled_posts,
            IndCQC.ascwds_filled_posts_source,
        )
        warnings.filterwarnings("ignore", category=ResourceWarning)

    def test_calculate_ascwds_filled_posts_row_count_correct(
        self,
    ):
        self.assertEqual(self.df.count(), 9)

    def test_calculate_ascwds_filled_posts_difference_within_range_values(
        self,
    ):
        df = self.df.sort(IndCQC.location_id).collect()
        self.assertEqual(df[0][IndCQC.ascwds_filled_posts], None)
        self.assertEqual(df[1][IndCQC.ascwds_filled_posts], 500.0)
        self.assertEqual(df[2][IndCQC.ascwds_filled_posts], None)
        self.assertEqual(df[3][IndCQC.ascwds_filled_posts], None)
        self.assertEqual(df[4][IndCQC.ascwds_filled_posts], None)
        self.assertEqual(df[5][IndCQC.ascwds_filled_posts], None)
        self.assertEqual(df[6][IndCQC.ascwds_filled_posts], 11.5)
        self.assertEqual(df[7][IndCQC.ascwds_filled_posts], 487.5)
        self.assertEqual(df[8][IndCQC.ascwds_filled_posts], 8.0)

    def test_calculate_ascwds_filled_posts_difference_within_range_sources(
        self,
    ):
        df = self.df.sort(IndCQC.location_id).collect()
        self.assertEqual(df[0][IndCQC.ascwds_filled_posts_source], None)
        self.assertEqual(
            df[1][IndCQC.ascwds_filled_posts_source],
            job.ascwds_filled_posts_difference_within_range_source_description,
        )
        self.assertEqual(df[2][IndCQC.ascwds_filled_posts_source], None)
        self.assertEqual(df[3][IndCQC.ascwds_filled_posts_source], None)
        self.assertEqual(df[4][IndCQC.ascwds_filled_posts_source], None)
        self.assertEqual(df[5][IndCQC.ascwds_filled_posts_source], None)
        self.assertEqual(
            df[6][IndCQC.ascwds_filled_posts_source],
            job.ascwds_filled_posts_difference_within_range_source_description,
        )
        self.assertEqual(
            df[7][IndCQC.ascwds_filled_posts_source],
            job.ascwds_filled_posts_difference_within_range_source_description,
        )
        self.assertEqual(df[8][IndCQC.ascwds_filled_posts_source], "already populated")
