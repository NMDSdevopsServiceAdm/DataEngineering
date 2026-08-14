import polars as pl
import polars.testing as pl_testing
import pytest

import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.utils.clean_utils as job
from projects._07_workforce_characteristics.unittest_data.polars_slv_test_data import (
    TestCleanUtilsData as Data,
)

PATCH_PATH = "projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.utils.clean_utils"


class TestCombineJobRoleRows:
    def test_combine_job_role_rows(self):
        pass


class TestDeduplicateSLVOverTime:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.deduplicate_slv_over_time_test_cases
        ],
    )
    def test_deduplicate_slv_over_time(self, case):
        test_lf = pl.LazyFrame(case.input_data)

        returned_lf = job.deduplicate_slv_over_time(test_lf)

        expected_lf = pl.LazyFrame(case.expected_data)

        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)
