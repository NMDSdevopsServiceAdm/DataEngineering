from datetime import date
import unittest
import polars as pl
import polars.testing as pl_testing

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.validate_utils as job
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import (
    MainJobRoleLabels,
    JobGroupLabels,
)


class TestCreateJobRoleEstimatesDataValidationColumns(unittest.TestCase):
    def test_function_returns_expected_values(self):
        expected_schema = {
            IndCQC.cqc_location_import_date: pl.Date,
            IndCQC.main_job_role_clean_labelled: pl.String,
            IndCQC.estimate_filled_posts_by_job_role_manager_adjusted: pl.Float32,
            IndCQC.estimate_filled_posts_from_all_job_roles: pl.Float32,
            IndCQC.national_percentage_care_worker_filled_posts: pl.Float32,
            IndCQC.national_percentage_direct_care_filled_posts: pl.Float32,
            IndCQC.national_percentage_managers_filled_posts: pl.Float32,
            IndCQC.national_percentage_regulated_professions_filled_posts: pl.Float32,
            IndCQC.national_percentage_other_filled_posts: pl.Float32,
        }
        expected_rows = [
            # date,         job_role,               filled_posts, est_all,  %cw, %dc, %mgr, %reg, %other
            (date(2026, 1, 1), "care_worker",        40.0, 100.0, 0.4, 0.7, 0.2, 0.1, 0.0),
            (date(2026, 1, 1), "support_worker",     30.0, 100.0, 0.4, 0.7, 0.2, 0.1, 0.0),
            (date(2026, 1, 1), "registered_manager", 20.0, 100.0, 0.4, 0.7, 0.2, 0.1, 0.0),
            (date(2026, 1, 1), "registered_nurse",   10.0, 100.0, 0.4, 0.7, 0.2, 0.1, 0.0),
            (date(2026, 2, 1), "care_worker",        60.0, 200.0, 0.3, 0.5, 0.3, 0.2, 0.0),
            (date(2026, 2, 1), "support_worker",     40.0, 200.0, 0.3, 0.5, 0.3, 0.2, 0.0),
            (date(2026, 2, 1), "registered_manager", 60.0, 200.0, 0.3, 0.5, 0.3, 0.2, 0.0),
            (date(2026, 2, 1), "registered_nurse",   40.0, 200.0, 0.3, 0.5, 0.3, 0.2, 0.0),
        ] # fmt: skip
        expected_lf = pl.LazyFrame(expected_rows, schema=expected_schema, orient="row")
        test_lf = expected_lf.drop(
            IndCQC.national_percentage_care_worker_filled_posts,
            IndCQC.national_percentage_direct_care_filled_posts,
            IndCQC.national_percentage_managers_filled_posts,
            IndCQC.national_percentage_regulated_professions_filled_posts,
            IndCQC.national_percentage_other_filled_posts,
        )
        returned_lf = job.create_job_role_estimates_data_validation_columns(test_lf)
        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)
