import polars as pl
import polars.testing as pl_testing
import unittest

import projects._03_independent_cqc._02_clean.fargate.utils.ascwds_filled_posts_calculator.ascwds_filled_posts_calculator as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    CalculateAscwdsFilledPostsData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    CalculateAscwdsFilledPostsSchemas as Schemas,
)

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


class AscwdsFilledPostsCalculatorTests(unittest.TestCase):
    def test_calculate_ascwds_filled_posts_returns_expected_data(
        self,
    ):
        ascwds_total_staff_and_worker_record_lf = pl.LazyFrame(
            Data.calculate_ascwds_filled_posts_rows,
            Schemas.calculate_ascwds_filled_posts_schema,
            orient="row",
        )

        returned_lf = job.calculate_ascwds_filled_posts(
            ascwds_total_staff_and_worker_record_lf,
            IndCQC.total_staff_bounded,
            IndCQC.worker_records_bounded,
            IndCQC.ascwds_filled_posts,
            IndCQC.ascwds_filled_posts_source,
        )

        expected_lf = pl.LazyFrame(
            Data.expected_ascwds_filled_posts_rows,
            Schemas.calculate_ascwds_filled_posts_schema,
            orient="row",
        )

        returned_data = returned_lf.sort(IndCQC.location_id).collect()
        expected_data = expected_lf.sort(IndCQC.location_id).collect()
        pl_testing.assert_frame_equal(returned_data, expected_data)
