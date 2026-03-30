import unittest
from unittest.mock import ANY, Mock, call, patch
from dataclasses import dataclass
import polars as pl
import polars.testing as pl_testing
import pytest

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.estimate_ind_cqc_filled_posts_by_job_role as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    TestJoinEstimatesToAscwds as JoinEstimatesToAscwdsData,
    TestImputeRatios as ImputeRatiosData,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    TestJoinEstimatesToAscwds as JoinEstimatesToAscwdsSchemas,
    TestImputeRatios as ImputeRatiosSchemas,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

PATCH_PATH = "projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.estimate_ind_cqc_filled_posts_by_job_role"


@pytest.mark.skip
class MainTests(unittest.TestCase):
    ESTIMATE_SOURCE = "some/source"
    PREPARED_JOB_ROLE_COUNTS_SOURCE = "some/other/source"
    ESTIMATES_DESTINATION = "some/destination"

    mock_estimate_lf = pl.LazyFrame(schema=job.transformation_columns)
    mock_prepared_job_role_counts_lf = pl.LazyFrame(schema=job.ascwds_columns_to_import)

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.JRUtils.ManagerialFilledPostAdjustmentExpr")
    @patch(f"{PATCH_PATH}.utils.coalesce_with_source_labels")
    @patch(f"{PATCH_PATH}.JRUtils.rolling_sum_of_job_role_counts")
    @patch(f"{PATCH_PATH}.JRUtils.impute_full_time_series")
    @patch(f"{PATCH_PATH}.JRUtils.percentage_share")
    @patch(f"{PATCH_PATH}.JRUtils.nullify_job_role_count_when_source_not_ascwds")
    @patch(
        f"{PATCH_PATH}.utils.scan_parquet",
        side_effect=[mock_estimate_lf, mock_prepared_job_role_counts_lf],
    )
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        nullify_job_role_count_when_source_not_ascwds_mock: Mock,
        percentage_share_mock: Mock,
        impute_full_time_series_mock: Mock,
        rolling_sum_mock: Mock,
        coalesce_with_source_labels_mock: Mock,
        ManagerialFilledPostAdjustmentExprMock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        with patch(f"{PATCH_PATH}.log_polars_plan", return_value="Mocked Plan"):
            job.main(
                self.ESTIMATE_SOURCE,
                self.PREPARED_JOB_ROLE_COUNTS_SOURCE,
                self.ESTIMATES_DESTINATION,
            )

        self.assertEqual(scan_parquet_mock.call_count, 2)
        scan_parquet_mock.assert_has_calls(
            [
                call(
                    source=self.ESTIMATE_SOURCE,
                    selected_columns=list(job.transformation_columns),
                ),
                call(
                    source=self.PREPARED_JOB_ROLE_COUNTS_SOURCE,
                    selected_columns=list(job.ascwds_columns_to_import),
                ),
            ]
        )

        nullify_job_role_count_when_source_not_ascwds_mock.assert_called_once()

        calls = [
            call(IndCQC.ascwds_job_role_counts),
            call(IndCQC.ascwds_job_role_rolling_sum),
        ]
        percentage_share_mock.assert_has_calls(calls, any_order=True)
        pct_share_groups = [IndCQC.location_id, IndCQC.cqc_location_import_date]
        # Assert that we're getting the percentage_share over the required groups.
        percentage_share_mock.return_value.over.assert_has_calls(
            [call(pct_share_groups)] * 2,
            any_order=True,
        )

        impute_full_time_series_mock.assert_called_once_with(
            IndCQC.ascwds_job_role_ratios
        )
        rolling_sum_mock.assert_called_once_with(period="6mo")
        coalesce_with_source_labels_mock.assert_called_once()

        ManagerialFilledPostAdjustmentExprMock.build.assert_called_once()
        ManagerialFilledPostAdjustmentExprMock.build.return_value.over.assert_called_once_with(
            pct_share_groups
        )

        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=ANY,
            output_path=self.ESTIMATES_DESTINATION,
            partition_cols=job.partition_keys,
            append=False,
        )


@pytest.fixture(autouse=True)
def mock_roles(monkeypatch):
    monkeypatch.setattr(
        job.AscwdsWorkerValueLabelsJobGroup,
        "all_roles",
        lambda: ["role_a", "role_b"],
    )


class TestJoinEstimatesToAscwds:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in JoinEstimatesToAscwdsData.join_estimates_test_cases
        ],
    )
    def test_join_estimates(self, case):
        estimates_lf = pl.LazyFrame(
            case.estimates_data,
            schema=JoinEstimatesToAscwdsSchemas.estimates_schema,
            orient="row",
        )

        ascwds_lf = pl.LazyFrame(
            case.ascwds_data,
            schema=JoinEstimatesToAscwdsSchemas.ascwds_schema,
            orient="row",
        )

        expected_lf = pl.LazyFrame(
            case.expected_data,
            schema=JoinEstimatesToAscwdsSchemas.expected_schema,
            orient="row",
        )

        result_lf = job.join_estimates_to_ascwds(estimates_lf, ascwds_lf)

        # Only compare relevant columns
        result_lf = result_lf.select(expected_lf.collect_schema().names())

        pl_testing.assert_frame_equal(
            result_lf,
            expected_lf,
            check_row_order=False,
        )

        # Sanity check: correct expansion
        expected_rows = len(case.estimates_data) * 2  # mocked roles
        assert result_lf.collect().height == expected_rows


class TestImputeRatios:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in ImputeRatiosData.impute_ratios_test_cases
        ],
    )
    def test_impute_ratios(self, case):
        expected_lf = pl.LazyFrame(
            case.test_data,
            schema=ImputeRatiosSchemas.impute_ratios_schema,
            orient="row",
        )

        input_lf = expected_lf.drop(IndCQC.imputed_ascwds_job_role_ratios)

        result_lf = job.impute_ratios(input_lf)

        pl_testing.assert_frame_equal(
            result_lf,
            expected_lf,
            check_row_order=False,
        )
