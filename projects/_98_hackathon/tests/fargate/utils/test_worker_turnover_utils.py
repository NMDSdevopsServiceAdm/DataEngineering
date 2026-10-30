import polars as pl
import polars.testing as pl_testing
import pytest

import projects._98_hackathon.fargate.utils.worker_turnover_utils as job
from projects._98_hackathon.unittest_data.polars_hackathon_test_file_data import (
    WorkerTurnoverData as Data,
)
from projects._98_hackathon.unittest_data.polars_hackathon_test_file_schemas import (
    WorkerTurnoverSchemas as Schemas,
)


class TestFilterWorkerTurnoverData:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.filter_worker_turnover_data_cases
        ],
    )
    def test_returns_expected_rows(self, case):
        input_lf = pl.LazyFrame(
            case.input_rows, schema=Schemas.cleaned_worker_schema, orient="row"
        )
        expected_lf = pl.LazyFrame(
            case.expected_rows, schema=Schemas.cleaned_worker_schema, orient="row"
        )

        returned_lf = job.filter_worker_turnover_data(input_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf)
