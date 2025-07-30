import unittest
import warnings

import projects._03_independent_cqc._02_clean.utils.ascwds_filled_posts_calculator.total_staff_equals_worker_records as job
from tests.test_file_data import (
    CalculateAscwdsFilledPostsTotalStaffEqualWorkerRecordsData as Data,
)
from tests.test_file_schemas import (
    CalculateAscwdsFilledPostsTotalStaffEqualWorkerRecordsSchemas as Schemas,
)
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


class TestAscwdsFilledPostsWorkerRecordsEqualsTotalStaff(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()
        self.ascwds_total_staff_and_worker_record_df = self.spark.createDataFrame(
            Data.calculate_ascwds_filled_posts_rows,
            Schemas.calculate_ascwds_filled_posts_schema,
        )
        self.df = job.calculate_ascwds_filled_posts_totalstaff_equal_wkrrecs(
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

    def test_calculate_ascwds_filled_posts_totalstaff_equal_wkrrecs_values(
        self,
    ):
        df = self.df.sort(IndCQC.location_id).collect()
        self.assertEqual(df[0][IndCQC.ascwds_filled_posts], None)
        self.assertEqual(df[1][IndCQC.ascwds_filled_posts], 500.0)
        self.assertEqual(df[2][IndCQC.ascwds_filled_posts], None)
        self.assertEqual(df[3][IndCQC.ascwds_filled_posts], None)
        self.assertEqual(df[4][IndCQC.ascwds_filled_posts], None)
        self.assertEqual(df[5][IndCQC.ascwds_filled_posts], None)
        self.assertEqual(df[6][IndCQC.ascwds_filled_posts], None)
        self.assertEqual(df[7][IndCQC.ascwds_filled_posts], None)
        self.assertEqual(df[8][IndCQC.ascwds_filled_posts], 8.0)

    def test_calculate_ascwds_filled_posts_totalstaff_equal_wkrrecs_sources(
        self,
    ):
        df = self.df.sort(IndCQC.location_id).collect()
        self.assertEqual(df[0][IndCQC.ascwds_filled_posts_source], None)
        self.assertEqual(
            df[1][IndCQC.ascwds_filled_posts_source],
            job.ascwds_filled_posts_totalstaff_equal_wkrrecs_source_description,
        )
        self.assertEqual(df[2][IndCQC.ascwds_filled_posts_source], None)
        self.assertEqual(df[3][IndCQC.ascwds_filled_posts_source], None)
        self.assertEqual(df[4][IndCQC.ascwds_filled_posts_source], None)
        self.assertEqual(df[5][IndCQC.ascwds_filled_posts_source], None)
        self.assertEqual(df[6][IndCQC.ascwds_filled_posts_source], None)
        self.assertEqual(
            df[7][IndCQC.ascwds_filled_posts_source],
            None,
        )
        self.assertEqual(df[8][IndCQC.ascwds_filled_posts_source], "already populated")
