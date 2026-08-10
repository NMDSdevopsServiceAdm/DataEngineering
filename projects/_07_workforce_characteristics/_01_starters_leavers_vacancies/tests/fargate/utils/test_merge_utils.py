import polars as pl
import polars.testing as pl_testing
import pytest

import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.utils.merge_utils as job
from polars_utils.column_types import CategoricalColumnTypes as CatColType
from projects._07_workforce_characteristics.unittest_data.polars_slv_test_data import (
    TestMergeUtilsData as Data,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.slv_job_role_columns import SLVJobRoleColumns as SLVCols

PATCH_PATH = "projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.utils.merge_utils"

METRIC = IndCQC.estimate_filled_posts_by_job_role_historically_reallocated

SLV_METRIC_COLUMNS = [
    SLVCols.employees,
    SLVCols.starters,
    SLVCols.leavers,
    SLVCols.vacancies,
]


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
            pl.col(SLVCols.job_role_label).cast(
                CatColType.PublishedJobRoleLabelEnumType
            )
        )

        returned_lf = job.collapse_job_role_estimates_to_published_labels(test_lf)

        pl_testing.assert_frame_equal(
            returned_lf,
            expected_lf,
            check_row_order=False,
            check_column_order=False,
        )


class TestApplyEmploymentStatusMagicNumbers:
    def test_apply_employment_status_magic_numbers(self):
        pass
