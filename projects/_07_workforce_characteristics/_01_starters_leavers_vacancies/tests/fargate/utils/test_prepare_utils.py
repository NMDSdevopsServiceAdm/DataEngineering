import polars as pl
import polars.testing as pl_testing
import pytest

import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.utils.prepare_utils as job
from projects._07_workforce_characteristics.unittest_data.polars_slv_test_data import (
    TestPrepareUtilsData as Data,
)

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
        test_lf = pl.LazyFrame({"jr98emp": 1})  # 98 isn't a MainJobRoleID code

        with pytest.raises(ValueError, match="98"):
            job.reduce_to_published_roles(test_lf)


class TestJobRoleCodeDerivation:
    def test_published_role_code_is_in_published_job_role_codes(self):
        assert "01" in job.PUBLISHED_JOB_ROLE_CODES

    def test_unpublished_role_code_is_not_in_published_job_role_codes(self):
        assert "02" not in job.PUBLISHED_JOB_ROLE_CODES

    def test_unpublished_role_code_maps_to_expected_other_role_code(self):
        assert job.CODE_TO_OTHER_ROLE_CODE["02"] == "1001"


class TestPivotJobRoleColsToRows:
    def test_pivot_job_role_cols_to_rows(self):
        pass


class TestConvertJobRoleStringsToNumberOnly:
    def test_convert_job_role_strings_to_number_only(self):
        pass
