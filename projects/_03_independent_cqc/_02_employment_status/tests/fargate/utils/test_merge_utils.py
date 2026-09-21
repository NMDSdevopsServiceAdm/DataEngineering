import polars as pl
import polars.testing as pl_testing
import pytest

import projects._03_independent_cqc._02_employment_status.fargate.utils.merge_utils as job
from polars_utils.column_types import CategoricalColumnTypes as CatColType
from projects._03_independent_cqc._02_employment_status.unittest_data.polars_employment_status_test_data import (
    TestMergeUtilsData as Data,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.slv_job_role_columns import SLVJobRoleColumns as SLVCols
from utils.column_values.categorical_columns_by_dataset import (
    EstimatedIndCQCFilledPostsByJobRoleCategoricalValues as CatVals,
)
from utils.column_values.categorical_columns_by_dataset import (
    SLVPrepareCategoricalValues,
)

METRIC = IndCQC.estimate_filled_posts_by_job_role_historically_reallocated


class TestRolesSharedByBothJobRoleTaxonomies:
    all_job_roles = set(CatVals.main_job_role_labels_column_values.categorical_values)
    published_roles = set(
        SLVPrepareCategoricalValues.published_job_role_labels_column_values.categorical_values
    )

    def test_contains_every_role_common_to_both_taxonomies_and_nothing_else(self):
        for role in self.all_job_roles | self.published_roles:
            is_in_both = role in self.all_job_roles and role in self.published_roles
            assert (role in job.ROLES_SHARED_BY_BOTH_JOB_ROLE_TAXONOMIES) == is_in_both

    def test_length_is_less_than_all_job_roles(self):
        assert len(job.ROLES_SHARED_BY_BOTH_JOB_ROLE_TAXONOMIES) < len(
            self.all_job_roles
        )

    def test_length_is_less_than_published_roles(self):
        assert len(job.ROLES_SHARED_BY_BOTH_JOB_ROLE_TAXONOMIES) < len(
            self.published_roles
        )


class TestJobRoleLabelToPublishedLabel:
    all_job_roles = set(CatVals.main_job_role_labels_column_values.categorical_values)
    published_roles = set(
        SLVPrepareCategoricalValues.published_job_role_labels_column_values.categorical_values
    )

    def test_covers_every_raw_job_role(self):
        assert set(job.JOB_ROLE_LABEL_TO_PUBLISHED_LABEL.keys()) == self.all_job_roles

    def test_every_mapped_value_is_a_published_label(self):
        mapped_values = set(job.JOB_ROLE_LABEL_TO_PUBLISHED_LABEL.values())
        assert mapped_values <= self.published_roles

    def test_shared_roles_map_to_themselves(self):
        for role in job.ROLES_SHARED_BY_BOTH_JOB_ROLE_TAXONOMIES:
            assert job.JOB_ROLE_LABEL_TO_PUBLISHED_LABEL[role] == role


class TestCollapseJobRoleEstimatesToPublishedLabels:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.collapse_job_role_estimates_to_published_labels_test_cases
        ],
    )
    def test_collapses_job_roles_as_expected(self, case):
        test_lf = pl.LazyFrame(case.input_data, schema_overrides={METRIC: pl.Float64})
        expected_lf = pl.LazyFrame(
            case.expected_data, schema_overrides={METRIC: pl.Float64}
        ).with_columns(
            pl.col(SLVCols.published_job_role_label).cast(
                CatColType.PublishedJobRoleLabelCatType
            )
        )

        returned_lf = job.collapse_job_role_estimates_to_published_labels(test_lf)

        pl_testing.assert_frame_equal(
            returned_lf,
            expected_lf,
            check_row_order=False,
            check_column_order=False,
        )


JOB_ROLE_ESTIMATES_SCHEMA_OVERRIDES = {
    IndCQC.primary_service_type: CatColType.PrimaryServiceEnumType,
    SLVCols.published_job_role_label: CatColType.PublishedJobRoleLabelCatType,
    METRIC: pl.Float64,
}

EXPECTED_SCHEMA_OVERRIDES = {
    **JOB_ROLE_ESTIMATES_SCHEMA_OVERRIDES,
    SLVCols.estimated_emp_stat_perm: pl.Float64,
    SLVCols.estimated_emp_stat_temp: pl.Float64,
    SLVCols.estimated_emp_stat_bank_or_pool: pl.Float64,
    SLVCols.estimated_emp_stat_agency: pl.Float64,
    SLVCols.estimated_emp_stat_other: pl.Float64,
    SLVCols.estimated_employees: pl.Float64,
}


class TestApplyEmploymentStatusMagicNumbers:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.apply_employment_status_magic_numbers_test_cases
        ],
    )
    def test_splits_metric_and_computes_estimated_employees_column_as_expected(
        self, case
    ):
        job_role_estimates_lf = pl.LazyFrame(
            case.job_role_estimates_data,
            schema_overrides=JOB_ROLE_ESTIMATES_SCHEMA_OVERRIDES,
            orient="row",
        )
        employment_status_rates_lf = pl.LazyFrame(case.employment_status_rates_data)
        expected_lf = pl.LazyFrame(
            case.expected_data, schema_overrides=EXPECTED_SCHEMA_OVERRIDES, orient="row"
        )

        returned_lf = job.apply_employment_status_magic_numbers(
            job_role_estimates_lf, employment_status_rates_lf
        )

        pl_testing.assert_frame_equal(
            returned_lf,
            expected_lf,
            check_row_order=False,
            check_column_order=False,
        )
