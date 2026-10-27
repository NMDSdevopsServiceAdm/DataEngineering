from datetime import date
from unittest.mock import Mock, patch

import polars as pl
import pytest

import projects._03_independent_cqc._03_starters_leavers_vacancies.fargate._02_clean as job
from utils.column_names.ind_cqc_pipeline_columns import (
    EmploymentStatusColumns as EmpStatus,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import (
    StartersLeaversVacanciesColumns as SLVCols,
)
from utils.column_values.categorical_column_values import (
    LeaversFilteringRule,
    StartersFilteringRule,
    VacanciesFilteringRule,
)

PATCH_PATH = (
    "projects._03_independent_cqc._03_starters_leavers_vacancies.fargate._02_clean"
)

METRICS = [
    (
        SLVCols.ascwds_starters,
        SLVCols.ascwds_starters_clean,
        SLVCols.ascwds_starters_filtering_rule,
    ),
    (
        SLVCols.ascwds_leavers,
        SLVCols.ascwds_leavers_clean,
        SLVCols.ascwds_leavers_filtering_rule,
    ),
    (
        SLVCols.ascwds_vacancies,
        SLVCols.ascwds_vacancies_clean,
        SLVCols.ascwds_vacancies_filtering_rule,
    ),
]


class TestMain:
    MERGED_DATA_SOURCE = "some/source"
    CLEANED_DATA_DESTINATION = "some/destination"

    @pytest.fixture
    def result_df(self) -> pl.DataFrame:
        """Runs main on three dates: 1st has values, 2nd repeats them, 3rd is null."""
        merged_lf = pl.LazyFrame(
            {
                IndCQC.location_id: ["1-001"] * 3,
                IndCQC.published_job_role_label: ["role"] * 3,
                IndCQC.cqc_location_import_date: [
                    date(2025, 1, 1),
                    date(2025, 2, 1),
                    date(2025, 3, 1),
                ],
                EmpStatus.estimated_employees: [10.0, 10.0, 10.0],
                SLVCols.ascwds_starters: [3, 3, None],
                SLVCols.ascwds_leavers: [4, 4, None],
                SLVCols.ascwds_vacancies: [5, 5, None],
            }
        )
        with (
            patch(f"{PATCH_PATH}.utils.scan_parquet", return_value=merged_lf),
            patch(f"{PATCH_PATH}.utils.sink_to_parquet") as sink_mock,
        ):
            job.main(self.MERGED_DATA_SOURCE, self.CLEANED_DATA_DESTINATION)
        return sink_mock.call_args.kwargs["lazy_df"].collect()

    @pytest.mark.parametrize("raw, cleaned", [(m[0], m[1]) for m in METRICS])
    def test_main_removes_repeated_values_from_raw_columns(
        self, result_df, raw, cleaned
    ):
        assert result_df[cleaned].to_list() == [result_df[raw][0], None, None]

    @pytest.mark.parametrize(
        "raw, cleaned, rule, rule_class",
        [
            (*METRICS[0], StartersFilteringRule),
            (*METRICS[1], LeaversFilteringRule),
            (*METRICS[2], VacanciesFilteringRule),
        ],
    )
    def test_main_creates_cleaned_column_and_rule_per_metric(
        self, result_df, raw, cleaned, rule, rule_class
    ):
        assert cleaned in result_df.columns
        assert result_df[rule].dtype == pl.Categorical
        assert result_df[rule].cast(pl.String).to_list() == [
            rule_class.populated,
            rule_class.missing_data,
            rule_class.missing_data,
        ]

    def test_main_drops_dedup_columns(self, result_df):
        for dedup in (
            SLVCols.ascwds_starters_dedup,
            SLVCols.ascwds_leavers_dedup,
            SLVCols.ascwds_vacancies_dedup,
        ):
            assert dedup not in result_df.columns

    def test_main_creates_rate_columns_from_cleaned_columns(self, result_df):
        assert result_df[SLVCols.ascwds_turnover_rate].to_list() == [
            pytest.approx(0.4),
            None,
            None,
        ]

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.cleanUtils.create_slv_rate_columns")
    @patch(f"{PATCH_PATH}.add_filtering_rule_column")
    @patch(f"{PATCH_PATH}.cleaningUtils.remove_repeated_values_over_time_as_group")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_call_order(
        self,
        scan_parquet_mock: Mock,
        dedup_mock: Mock,
        add_rule_mock: Mock,
        rate_mock: Mock,
        sink_mock: Mock,
    ):
        starters_lf, leavers_lf, vacancies_lf = Mock(), Mock(), Mock()
        add_rule_mock.side_effect = [starters_lf, leavers_lf, vacancies_lf]
        renamed_lf = dedup_mock.return_value.rename.return_value

        job.main(self.MERGED_DATA_SOURCE, self.CLEANED_DATA_DESTINATION)

        scan_parquet_mock.assert_called_once_with(self.MERGED_DATA_SOURCE)
        assert dedup_mock.call_args.args[0] is scan_parquet_mock.return_value
        assert dedup_mock.call_args.kwargs["columns_to_clean"] == [
            SLVCols.ascwds_starters,
            SLVCols.ascwds_leavers,
            SLVCols.ascwds_vacancies,
        ]
        helper_inputs = [c.args[0] for c in add_rule_mock.call_args_list]
        assert helper_inputs == [renamed_lf, starters_lf, leavers_lf]
        dedup_mock.return_value.rename.assert_called_once_with(
            {
                SLVCols.ascwds_starters_dedup: SLVCols.ascwds_starters_clean,
                SLVCols.ascwds_leavers_dedup: SLVCols.ascwds_leavers_clean,
                SLVCols.ascwds_vacancies_dedup: SLVCols.ascwds_vacancies_clean,
            }
        )
        assert [c.args[2] for c in add_rule_mock.call_args_list] == [
            SLVCols.ascwds_starters_clean,
            SLVCols.ascwds_leavers_clean,
            SLVCols.ascwds_vacancies_clean,
        ]
        rate_mock.assert_called_once_with(vacancies_lf)
        sink_mock.assert_called_once_with(
            lazy_df=rate_mock.return_value,
            output_path=self.CLEANED_DATA_DESTINATION,
        )
