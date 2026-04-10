import polars as pl
import polars.testing as pl_testing
import pytest

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.utils as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    TestJoinEstimatesToAscwds as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    TestJoinEstimatesToAscwds as Schemas,
)


@pytest.fixture(autouse=True)
def mock_roles(monkeypatch):
    roles = ["role_a", "role_b"]

    monkeypatch.setattr(
        job.AscwdsWorkerValueLabelsJobGroup,
        "all_roles",
        lambda: roles,
    )

    monkeypatch.setattr(
        job,
        "JobRoleEnumType",
        pl.Enum(roles),
    )


class TestJoinEstimatesToAscwds:
    @pytest.mark.parametrize(
        "case",
        [pytest.param(case, id=case.id) for case in Data.join_estimates_test_cases],
    )
    def test_function_returns_expected_values(self, case):
        estimates_lf = pl.LazyFrame(
            case.estimates_data,
            schema=Schemas.estimates_schema,
            orient="row",
        )

        ascwds_lf = pl.LazyFrame(
            case.ascwds_data,
            schema=Schemas.ascwds_schema,
            orient="row",
        )

        expected_lf = pl.LazyFrame(
            case.expected_data,
            schema=Schemas.expected_schema,
            orient="row",
        )

        result_lf = job.join_estimates_to_ascwds(estimates_lf, ascwds_lf)

        pl_testing.assert_frame_equal(
            result_lf,
            expected_lf,
            check_row_order=False,
            categorical_as_str=True,
        )

        # Sanity check: correct expansion
        expected_rows = len(case.estimates_data) * 2  # mocked roles
        assert result_lf.collect().height == expected_rows
