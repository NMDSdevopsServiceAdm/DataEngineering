import unittest

import polars as pl
import polars.testing as pl_testing

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate._04_estimate as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    EstimateFilledPostsByJobRole04EstimateData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    EstimateFilledPostsByJobRole04EstimateSchemas as Schemas,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


class TestCalculateEstimatedFilledPostsByJobRole(unittest.TestCase):
    def test_function_returns_expected_values(self):
        expected_lf = pl.LazyFrame(
            data=Data.calculate_estimated_filled_posts_by_job_role_rows,
            schema=Schemas.calculate_estimated_filled_posts_by_job_role_schema,
            orient="row",
        )
        test_lf = expected_lf.drop(
            [
                IndCQC.ascwds_job_role_ratios_merged,
                IndCQC.estimate_filled_posts_by_job_role,
            ]
        )
        returned_lf = job.calculate_estimated_filled_posts_by_job_role(test_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf)
