from unittest.mock import Mock, patch

import polars as pl
import polars.testing as pl_testing

import projects._03_independent_cqc._02_employment_status.fargate.utils.clean_utils as job
from polars_utils.column_types import CategoricalColumnTypes as CatColType
from utils.column_names.ind_cqc_pipeline_columns import (
    EmploymentStatusColumns as EmpStatus,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import EmploymentStatusFilteringRule

PATCH_PATH = (
    "projects._03_independent_cqc._02_employment_status.fargate.utils.clean_utils"
)


class TestDeduplicateEmploymentStatusCounts:
    @patch(f"{PATCH_PATH}.cleaningUtils.remove_repeated_values_over_time_as_group")
    def test_calls_remove_repeated_values_with_expected_args(
        self, remove_repeated_values_over_time_as_group_mock: Mock
    ):
        input_lf = Mock()

        returned_lf = job.deduplicate_employment_status_counts(input_lf)

        remove_repeated_values_over_time_as_group_mock.assert_called_once_with(
            input_lf,
            columns_to_clean=[
                EmpStatus.permanent_count,
                EmpStatus.temporary_count,
                EmpStatus.bank_or_pool_count,
                EmpStatus.agency_count,
                EmpStatus.other_count,
            ],
            partition_by_columns=[
                IndCQC.location_id,
                IndCQC.published_job_role_label,
            ],
            date_column=IndCQC.cqc_location_import_date,
            workplace_columns=[IndCQC.location_id, IndCQC.cqc_location_import_date],
        )
        assert (
            returned_lf == remove_repeated_values_over_time_as_group_mock.return_value
        )


class TestCreateCleanCountColumns:
    def test_copies_dedup_counts_and_sets_filtering_rule(self):
        input_lf = pl.LazyFrame(
            {
                EmpStatus.permanent_count_dedup: [3, None],
                EmpStatus.temporary_count_dedup: [1, None],
                EmpStatus.bank_or_pool_count_dedup: [0, None],
                EmpStatus.agency_count_dedup: [2, None],
                EmpStatus.other_count_dedup: [4, None],
            },
            schema_overrides={
                column: pl.Int64 for column in job.DEDUP_TO_CLEAN_COUNT_COLUMNS
            },
        )
        expected_lf = input_lf.with_columns(
            [
                pl.col(dedup).alias(clean)
                for dedup, clean in job.DEDUP_TO_CLEAN_COUNT_COLUMNS.items()
            ]
        ).with_columns(
            pl.Series(
                EmpStatus.filtering_rule,
                [
                    EmploymentStatusFilteringRule.populated,
                    EmploymentStatusFilteringRule.missing_data,
                ],
            ).cast(CatColType.EmploymentStatusFilteringRuleCatType)
        )

        returned_lf = job.create_clean_count_columns(input_lf)

        pl_testing.assert_frame_equal(
            returned_lf,
            expected_lf,
            check_column_order=False,
        )


class TestCreateEmploymentStatusPercentageColumns:
    @patch(f"{PATCH_PATH}.cleaningUtils.percentage_share_horizontal")
    def test_calls_percentage_share_with_expected_args(
        self,
        percentage_share_horizontal_mock: Mock,
    ):
        input_lf = Mock()

        returned_lf = job.create_employment_status_percentage_columns(input_lf)

        percentage_share_horizontal_mock.assert_called_once_with(
            input_lf,
            columns=[
                EmpStatus.permanent_count_dedup,
                EmpStatus.temporary_count_dedup,
                EmpStatus.bank_or_pool_count_dedup,
                EmpStatus.agency_count_dedup,
                EmpStatus.other_count_dedup,
            ],
            output_columns=[
                EmpStatus.permanent_percentage,
                EmpStatus.temporary_percentage,
                EmpStatus.bank_or_pool_percentage,
                EmpStatus.agency_percentage,
                EmpStatus.other_percentage,
            ],
        )
        assert returned_lf == percentage_share_horizontal_mock.return_value
