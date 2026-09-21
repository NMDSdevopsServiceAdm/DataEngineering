import polars as pl
import polars.testing as pl_testing
import pytest

import projects._03_independent_cqc.utils.cleaning_utils as job
from projects._03_independent_cqc.unittest_data.polars_independent_cqc_test_data import (
    TestCleaningUtilsData as Data,
)


class TestRemoveRepeatedValuesOverTimeAsGroup:
    @pytest.mark.parametrize(
        "case",
        [
            c.as_pytest_param()
            for c in Data.remove_repeated_values_over_time_as_group_test_cases
        ],
    )
    def test_returns_expected_dedup_values(self, case):
        schema_overrides = {column: pl.Int64 for column in case.columns_to_clean}
        input_lf = pl.LazyFrame(case.input_data, schema_overrides=schema_overrides)
        expected_schema_overrides = {
            **schema_overrides,
            **{f"{column}_dedup": pl.Int64 for column in case.columns_to_clean},
        }
        expected_lf = pl.LazyFrame(
            case.expected_data, schema_overrides=expected_schema_overrides
        )

        returned_lf = job.remove_repeated_values_over_time_as_group(
            input_lf,
            columns_to_clean=case.columns_to_clean,
            partition_by_columns=case.partition_by_columns,
            date_column=case.date_column,
        )

        pl_testing.assert_frame_equal(
            returned_lf,
            expected_lf,
            check_row_order=False,
            check_column_order=False,
        )


class TestPercentageShareHorizontal:
    @pytest.mark.parametrize(
        "case",
        [c.as_pytest_param() for c in Data.percentage_share_horizontal_test_cases],
    )
    def test_returns_expected_percentage_values(self, case):
        schema_overrides = {column: pl.Int64 for column in case.columns}
        input_lf = pl.LazyFrame(case.input_data, schema_overrides=schema_overrides)
        expected_schema_overrides = {
            **schema_overrides,
            **{column: pl.Float32 for column in case.output_columns},
        }
        expected_lf = pl.LazyFrame(
            {**case.input_data, **case.expected_data},
            schema_overrides=expected_schema_overrides,
        )

        returned_lf = job.percentage_share_horizontal(
            input_lf,
            columns=case.columns,
            output_columns=case.output_columns,
        )

        pl_testing.assert_frame_equal(
            returned_lf,
            expected_lf,
            check_row_order=False,
            check_column_order=False,
        )
