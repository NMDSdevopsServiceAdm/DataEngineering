import polars as pl
import polars.testing as pl_testing
import unittest

import projects._03_independent_cqc._02_clean.fargate.utils.ascwds_filled_posts_calculator.total_staff_equals_worker_records as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    CalculateAscwdsFilledPostsTotalStaffEqualWorkerRecordsData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    CalculateAscwdsFilledPostsTotalStaffEqualWorkerRecordsSchemas as Schemas,
)
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


class TestAscwdsFilledPostsWorkerRecordsEqualsTotalStaff(unittest.TestCase):
    def test_calculate_ascwds_filled_posts_totalstaff_equal_wkrrecs(
        self,
    ):
        ascwds_total_staff_and_worker_record_lf = pl.LazyFrame(
            Data.calculate_ascwds_filled_posts_rows,
            Schemas.calculate_ascwds_filled_posts_schema,
            orient="row",
        )

        result_lf = job.calculate_ascwds_filled_posts_totalstaff_equal_wkrrecs(
            ascwds_total_staff_and_worker_record_lf,
            total_staff_column=IndCQC.total_staff_bounded,
            worker_records_column=IndCQC.worker_records_bounded,
            output_column_name=IndCQC.ascwds_filled_posts,
            source_output_column_name=IndCQC.ascwds_filled_posts_source,
        )

        result_df = result_lf.collect().sort(IndCQC.location_id)

        self.assertEqual(result_df.height, 9)
        expected_df = (
            pl.LazyFrame(
                Data.expected_ascwds_filled_posts_rows,
                Schemas.calculate_ascwds_filled_posts_schema,
                orient="row",
            )
            .collect()
            .sort(IndCQC.location_id)
        )

        # Compare actual vs expected
        pl_testing.assert_frame_equal(result_df, expected_df)
