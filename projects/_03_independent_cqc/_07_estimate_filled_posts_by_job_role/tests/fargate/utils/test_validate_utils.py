from datetime import date
import unittest
import polars as pl
import polars.testing as pl_testing

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.validate_utils as job
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import MainJobRoleLabels
from utils.column_names.validation_table_columns import Validation as validationColumns


class TestCreateJobRoleEstimatesDataValidationColumns(unittest.TestCase):
    def test_function_returns_expected_values(self):
        input_schema = {
            IndCQC.cqc_location_import_date: pl.Date,
            IndCQC.main_job_role_clean_labelled: pl.String,
            IndCQC.estimate_filled_posts_by_job_role_manager_adjusted: pl.Float32,
            IndCQC.estimate_filled_posts_from_all_job_roles: pl.Float32,
        }
        input_rows = [
            (date(2026, 1, 1), MainJobRoleLabels.care_worker, 40.0, 100.0),
            (date(2026, 1, 1), MainJobRoleLabels.support_worker, 30.0, 100.0),
            (date(2026, 1, 1), MainJobRoleLabels.registered_manager, 20.0, 100.0),
            (date(2026, 1, 1), MainJobRoleLabels.registered_nurse, 10.0, 100.0),
            (date(2026, 2, 1), MainJobRoleLabels.care_worker, 60.0, 200.0),
            (date(2026, 2, 1), MainJobRoleLabels.support_worker, 40.0, 200.0),
            (date(2026, 2, 1), MainJobRoleLabels.registered_manager, 60.0, 200.0),
            (date(2026, 2, 1), MainJobRoleLabels.software_developer, 40.0, 200.0),
        ]

        expected_schema = {
            IndCQC.cqc_location_import_date: pl.Date,
            IndCQC.national_percentage_care_worker_filled_posts: pl.Float32,
            IndCQC.national_percentage_direct_care_filled_posts: pl.Float32,
            IndCQC.national_percentage_managers_filled_posts: pl.Float32,
            IndCQC.national_percentage_regulated_professions_filled_posts: pl.Float32,
            IndCQC.national_percentage_other_filled_posts: pl.Float32,
            validationColumns.total_job_role_records: pl.Int32,
        }
        expected_rows = [
            (date(2026, 1, 1), 0.4, 0.7, 0.2, 0.1, 0.0, 8),
            (date(2026, 2, 1), 0.3, 0.5, 0.3, 0.0, 0.2, 8),
        ]

        test_lf = pl.LazyFrame(input_rows, input_schema, orient="row")
        expected_lf = pl.LazyFrame(expected_rows, expected_schema, orient="row")

        returned_lf = job.create_job_role_estimates_data_validation_columns(test_lf)
        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)
