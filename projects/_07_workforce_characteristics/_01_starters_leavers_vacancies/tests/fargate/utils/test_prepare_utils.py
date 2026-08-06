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
        returned_lf = job.reduce_to_published_roles(test_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_raises_value_error_for_uncatalogued_job_role_code(self):
        test_lf = pl.LazyFrame(
            {"jr98emp": 1}
        )  # not an AscwdsWorkerValueLabelsMainjrid code

        with pytest.raises(ValueError, match="98"):
            job.reduce_to_published_roles(test_lf)

    def test_raises_value_error_for_technician_and_care_navigator_codes(self):
        # 22 (technician) and 41 (care_navigator) have no label/job-group entry,
        # so they're treated the same as any other uncatalogued code. They're
        # already merged upstream before reaching this function in practice.
        test_lf = pl.LazyFrame({"jr22emp": 1, "jr41emp": 2})

        with pytest.raises(ValueError, match="22"):
            job.reduce_to_published_roles(test_lf)


class TestJobRoleCodeDerivation:
    def test_published_role_code_is_in_published_job_role_codes(self):
        assert "01" in job.published_job_role_codes_and_labels.keys()

    def test_unpublished_role_code_is_not_in_published_job_role_codes(self):
        assert "02" not in job.published_job_role_codes_and_labels.keys()

    def test_unpublished_role_code_maps_to_expected_other_role_code(self):
        assert job.job_role_code_to_other_bucket_code["02"] == "1001"

    def test_every_catalogued_code_is_published_or_bucketed_exactly_once(self):
        published_codes = set(job.published_job_role_codes_and_labels)
        bucketed_codes = set(job.job_role_code_to_other_bucket_code)

        assert not (published_codes & bucketed_codes)
        assert published_codes | bucketed_codes == set(
            job.all_zero_filled_job_role_codes_and_labels
        )


class TestPivotJobRoleColsToRows:
    def test_pivot_job_role_cols_to_rows(self):
        pass


class TestRelabelJobRoleColumns:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.relabel_job_role_columns_test_cases
        ],
    )
    def test_function_renames_columns_as_expected(self, case):
        test_data = [tuple(range(len(case.input_columns)))]
        test_lf = pl.LazyFrame(
            test_data,
            schema={col: pl.Int64 for col in case.input_columns},
            orient="row",
        )

        returned_lf = job.relabel_job_role_columns(test_lf)

        expected_lf = pl.LazyFrame(
            test_data,
            schema={col: pl.Int64 for col in case.expected_columns},
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
            "52",
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
