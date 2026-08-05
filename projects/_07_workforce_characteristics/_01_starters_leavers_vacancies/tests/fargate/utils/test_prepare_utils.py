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
        returned_lf = job.reduce_to_published_roles(test_lf, case.mapping)

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestPivotJobRoleColsToRows:
    def test_pivot_job_role_cols_to_rows(self):
        pass


class TestConvertJobRoleStringsToNumberOnly:
    def test_convert_job_role_strings_to_number_only(self):
        pass
