import unittest

import polars as pl
import polars.testing as pl_testing

import projects._03_independent_cqc._01_merge.fargate.utils.utils as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    PrepareJobRoleCountsUtilsData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    PrepareJobRoleCountsUtilsSchemas as Schemas,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_values.categorical_column_values import MainJobRoleLabels


class AggregateAscwdsWorkerJobRolesPerEstablishmentTests(unittest.TestCase):
    def setUp(self):
        self.list_of_job_roles_for_tests = [
            MainJobRoleLabels.care_worker,
            MainJobRoleLabels.senior_care_worker,
        ]
        self.columns_to_aggregate_on = [
            IndCQC.establishment_id,
            IndCQC.ascwds_worker_import_date,
            IndCQC.main_job_role_clean_labelled,
            Keys.year,
            Keys.month,
            Keys.day,
            Keys.import_date,
        ]
        self.columns_to_sort_outputs_on = [
            IndCQC.establishment_id,
            IndCQC.ascwds_worker_import_date,
        ]

    def test_aggregate_ascwds_worker_job_roles_returns_expected_data_when_estab_has_many_in_same_role(
        self,
    ):
        test_lf = pl.LazyFrame(
            data=Data.aggregate_ascwds_worker_job_roles_per_establishment_when_estab_has_many_in_same_role_rows,
            schema=Schemas.aggregate_ascwds_worker_job_roles_per_establishment_schema,
        )
        returned_lf = job.aggregate_ascwds_worker_job_roles_per_establishment(
            test_lf, self.list_of_job_roles_for_tests, self.columns_to_aggregate_on
        )
        expected_lf = pl.LazyFrame(
            data=Data.expected_aggregate_ascwds_worker_job_roles_per_establishment_when_estab_has_many_in_same_role_rows,
            schema=Schemas.expected_aggregate_ascwds_worker_job_roles_per_establishment_schema,
        )

        pl_testing.assert_frame_equal(
            returned_lf.sort(self.columns_to_sort_outputs_on),
            expected_lf.sort(self.columns_to_sort_outputs_on),
        )

    def test_aggregate_ascwds_worker_job_roles_returns_expected_data_when_estab_has_multiple_import_dates(
        self,
    ):
        test_lf = pl.LazyFrame(
            data=Data.aggregate_ascwds_worker_job_roles_per_establishment_when_estab_has_multiple_import_dates_rows,
            schema=Schemas.aggregate_ascwds_worker_job_roles_per_establishment_schema,
        )
        returned_lf = job.aggregate_ascwds_worker_job_roles_per_establishment(
            test_lf, self.list_of_job_roles_for_tests, self.columns_to_aggregate_on
        )
        expected_lf = pl.LazyFrame(
            data=Data.expected_aggregate_ascwds_worker_job_roles_per_establishment_when_estab_has_multiple_import_dates_rows,
            schema=Schemas.expected_aggregate_ascwds_worker_job_roles_per_establishment_schema,
        )

        pl_testing.assert_frame_equal(
            returned_lf.sort(self.columns_to_sort_outputs_on),
            expected_lf.sort(self.columns_to_sort_outputs_on),
        )

    def test_aggregate_ascwds_worker_job_roles_returns_expected_data_when_multiple_estabs_have_same_import_date(
        self,
    ):
        test_lf = pl.LazyFrame(
            data=Data.aggregate_ascwds_worker_job_roles_per_establishment_when_multiple_estabs_have_same_import_date_rows,
            schema=Schemas.aggregate_ascwds_worker_job_roles_per_establishment_schema,
        )
        returned_lf = job.aggregate_ascwds_worker_job_roles_per_establishment(
            test_lf, self.list_of_job_roles_for_tests, self.columns_to_aggregate_on
        )
        expected_lf = pl.LazyFrame(
            data=Data.expected_aggregate_ascwds_worker_job_roles_per_establishment_when_multiple_estabs_have_same_import_date_rows,
            schema=Schemas.expected_aggregate_ascwds_worker_job_roles_per_establishment_schema,
        )

        pl_testing.assert_frame_equal(
            returned_lf.sort(self.columns_to_sort_outputs_on),
            expected_lf.sort(self.columns_to_sort_outputs_on),
        )
