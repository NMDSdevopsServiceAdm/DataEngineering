from dataclasses import dataclass

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.utils.prepare_utils as job
from projects._07_workforce_characteristics.unittest_data.polars_slv_test_data import (
    TestPrepareUtilsData as Data,
)
from utils.column_values.categorical_column_values import PublishedJobRoleLabels

PATCH_PATH = "projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.utils.prepare_utils"


class TestReduceToPublishedRoles:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.reduce_to_published_roles_test_cases
        ],
    )
    def test_reduce_to_published_roles(self, case):
        test_lf = pl.LazyFrame(case.input_data)
        expected_lf = pl.LazyFrame(case.expected_data)
        returned_lf = job.reduce_to_published_roles(test_lf, case.mapping)

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestPivotJobRoleColsToRows:
    def test_pivot_job_role_cols_to_rows(self):
        pass


@dataclass
class RelabelJobRoleColumnsTestCase:
    id: str
    input_columns: list[str]
    expected_columns: list[str]

    def as_pytest_param(self):
        return pytest.param(self.input_columns, self.expected_columns, id=self.id)


relabel_job_role_columns_cases = [
    RelabelJobRoleColumnsTestCase(
        id="known_code_renames_to_published_label_and_suffix",
        input_columns=["jr01emp"],
        expected_columns=["senior_management_emp"],
    ),
    RelabelJobRoleColumnsTestCase(
        id="suffix_is_derived_per_column_not_a_fixed_list",
        input_columns=["jr04strt", "jr06stop", "jr07vacy"],
        expected_columns=["registered_manager_strt", "social_worker_stop", "senior_care_worker_vacy"],
    ),
    RelabelJobRoleColumnsTestCase(
        id="synthetic_merged_codes_rename_via_synthetic_dict",
        input_columns=["jr1001emp", "jr1002strt", "jr1003stop", "jr1004vacy"],
        expected_columns=[
            "other_managers_emp",
            "other_regulated_professions_strt",
            "other_direct_care_stop",
            "other_vacy",
        ],
    ),
    RelabelJobRoleColumnsTestCase(
        id="non_jr_prefixed_columns_are_left_untouched",
        input_columns=["establishment_id", "jr08emp"],
        expected_columns=["establishment_id", "care_worker_emp"],
    ),
    RelabelJobRoleColumnsTestCase(
        id="no_job_role_columns_present_is_a_no_op",
        input_columns=["establishment_id", "ascwds_workplace_import_date"],
        expected_columns=["establishment_id", "ascwds_workplace_import_date"],
    ),
]  # fmt: skip


class TestRelabelJobRoleColumns:
    @pytest.mark.parametrize(
        "input_columns,expected_columns",
        [case.as_pytest_param() for case in relabel_job_role_columns_cases],
    )
    def test_function_renames_columns_as_expected(
        self, input_columns: list[str], expected_columns: list[str]
    ):
        test_data = [tuple(range(len(input_columns)))]
        test_lf = pl.LazyFrame(
            test_data,
            schema={col: pl.Int64 for col in input_columns},
            orient="row",
        )

        returned_lf = job.relabel_job_role_columns(test_lf)

        expected_lf = pl.LazyFrame(
            test_data,
            schema={col: pl.Int64 for col in expected_columns},
            orient="row",
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_unmapped_code_raises_error(self):
        test_lf = pl.LazyFrame(schema={"jr99emp": pl.Int64})

        with pytest.raises(ValueError):
            job.relabel_job_role_columns(test_lf)

    def test_no_duplicate_columns_when_renaming_full_set_of_published_codes(self):
        codes = [
            "01",
            "04",
            "06",
            "07",
            "08",
            "09",
            "15",
            "16",
            "17",
            "43",
            "1001",
            "1002",
            "1003",
            "1004",
        ]
        input_columns = [f"jr{code}{suffix}" for code in codes for suffix in ("emp", "strt", "stop", "vacy")]  # fmt: skip
        test_lf = pl.LazyFrame(schema={col: pl.Int64 for col in input_columns})

        returned_columns = (
            job.relabel_job_role_columns(test_lf).collect_schema().names()
        )

        assert len(returned_columns) == len(set(returned_columns))


class TestSyntheticJobRoleLabels:
    def test_pins_the_synthetic_job_role_labels(self):
        expected_labels = {
            "1001": PublishedJobRoleLabels.other_managers,
            "1002": PublishedJobRoleLabels.other_regulated_professions,
            "1003": PublishedJobRoleLabels.other_direct_care,
            "1004": PublishedJobRoleLabels.other,
        }

        assert job.SYNTHETIC_JOB_ROLE_LABELS == expected_labels
