import unittest

import polars as pl
import polars.testing as pl_testing

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.utils as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    EstimateIndCQCFilledPostsByJobRoleUtilsData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    EstimateIndCQCFilledPostsByJobRoleUtilsSchemas as Schemas,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import MainJobRoleLabels


class AggregateAscwdsWorkerJobRolesPerEstablishmentTests(unittest.TestCase):
    def test_aggregate_ascwds_worker_job_roles_per_establishment(self):
        list_of_job_roles_for_tests = [
            MainJobRoleLabels.care_worker,
            MainJobRoleLabels.senior_care_worker,
            MainJobRoleLabels.registered_nurse,
        ]

        test_lf = pl.LazyFrame(
            data=Data.aggregate_ascwds_worker_job_roles_per_establishment_rows,
            schema=Schemas.aggregate_ascwds_worker_job_roles_per_establishment_schema,
        )
        returned_lf = job.aggregate_ascwds_worker_job_roles_per_establishment(
            test_lf, list_of_job_roles_for_tests
        )
        expected_lf = pl.LazyFrame(
            data=Data.expected_aggregate_ascwds_worker_job_roles_per_establishment_rows,
            schema=Schemas.expected_aggregate_ascwds_worker_job_roles_per_establishment_schema,
        )

        pl_testing.assert_frame_equal(
            returned_lf.sort(
                [IndCQC.establishment_id, IndCQC.ascwds_worker_import_date]
            ),
            expected_lf.sort(
                [IndCQC.establishment_id, IndCQC.ascwds_worker_import_date]
            ),
        )


class JoinWorkerToEstimatesDataframeTests(unittest.TestCase):
    def test_join_worker_to_estimates_dataframe_returns_expected_df(self):
        estimates_lf = pl.LazyFrame(
            data=Data.estimates_df_before_join_rows,
            schema=Schemas.estimates_df_before_join_schema,
        )
        worker_lf = pl.LazyFrame(
            data=Data.worker_df_before_join_rows,
            schema=Schemas.worker_df_before_join_schema,
        )
        returned_lf = job.join_worker_to_estimates_dataframe(estimates_lf, worker_lf)
        expected_lf = pl.LazyFrame(
            data=Data.expected_join_worker_to_estimates_dataframe_rows,
            schema=Schemas.expected_join_worker_to_estimates_dataframe_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)
