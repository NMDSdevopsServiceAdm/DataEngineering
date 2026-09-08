import polars as pl
import polars.testing as pl_testing

import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.utils.prepare_worker_utils as job


class TestAggregateEmploymentStatusData:
    def test_returns_input_unchanged(self):
        test_lf = pl.LazyFrame({"employment_status_clean": ["permanent", "temporary"]})

        returned_lf = job.aggregate_employment_status_data(test_lf)

        pl_testing.assert_frame_equal(returned_lf, test_lf)


class TestReshapeEmploymentStatusData:
    def test_returns_input_unchanged(self):
        test_lf = pl.LazyFrame({"employment_status_clean": ["permanent", "temporary"]})

        returned_lf = job.reshape_employment_status_data(test_lf)

        pl_testing.assert_frame_equal(returned_lf, test_lf)
