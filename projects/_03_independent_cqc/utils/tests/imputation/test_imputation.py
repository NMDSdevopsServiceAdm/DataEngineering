from dataclasses import dataclass
from typing import Any
from unittest.mock import ANY, Mock, patch

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._03_independent_cqc.utils.imputation.imputation as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    ModelImputation as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    ModelImputation as Schemas,
)
from utils.column_names.ind_cqc_pipeline_columns import Imputation
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc
from utils.column_values.categorical_column_values import CareHome

PATCH_PATH = "projects._03_independent_cqc.utils.imputation.imputation"


class TestModelImputationFunctionality:
    @patch(f"{PATCH_PATH}.model_interpolation")
    @patch(f"{PATCH_PATH}.model_extrapolation")
    @patch(f"{PATCH_PATH}.flag_rows_eligible_for_imputation")
    def test_function_has_expected_calls(
        self,
        flag_rows_eligible_for_imputation_mock: Mock,
        model_extrapolation_mock: Mock,
        model_interpolation_mock: Mock,
    ):
        flagged_lf = pl.LazyFrame(
            [],
            Schemas.expected_model_imputation_schema,
            orient="row",
        )
        flag_rows_eligible_for_imputation_mock.return_value = flagged_lf
        model_extrapolation_mock.return_value = flagged_lf
        model_interpolation_mock.return_value = flagged_lf

        job.model_imputation(
            Mock(name="input_lf"),
            Data.column_with_null_values_name,
            Data.model_column_name,
            Data.imputed_values_column_name,
            care_home=False,
            extrapolation_method="nominal",
        )

        flag_rows_eligible_for_imputation_mock.assert_called_once()

        expected_group_columns = [IndCqc.location_id, IndCqc.care_home]
        model_extrapolation_mock.assert_called_once()
        assert (
            model_extrapolation_mock.call_args.kwargs["group_columns"]
            == expected_group_columns
        )
        model_interpolation_mock.assert_called_once()
        assert (
            model_interpolation_mock.call_args.kwargs["group_columns"]
            == expected_group_columns
        )


class TestModelImputationResults:
    @pytest.mark.parametrize(
        "model_imputation_data",
        [case.as_pytest_param() for case in Data.expected_model_imputation_test_cases],
    )
    def test_function_returns_expected_data(self, model_imputation_data):
        expected_lf = pl.LazyFrame(
            model_imputation_data,
            Schemas.expected_model_imputation_schema,
            orient="row",
        )
        input_lf = expected_lf.drop(Data.imputed_values_column_name)
        returned_lf = job.model_imputation(
            input_lf,
            Data.column_with_null_values_name,
            Data.model_column_name,
            Data.imputed_values_column_name,
            care_home=False,
            extrapolation_method="nominal",
        )

        pl_testing.assert_frame_equal(
            returned_lf,
            expected_lf,
            check_row_order=False,
        )


class TestFlagRowsEligibleForImputation:
    input_lf = pl.LazyFrame(
        data=[
            (1, "1-001", CareHome.care_home, 10.0),
            (2, "1-001", CareHome.care_home, None),
            (3, "1-002", CareHome.care_home, 10.0),
            (4, "1-003", CareHome.care_home, None),
            (5, "1-003", CareHome.care_home, None),
            (6, "1-004", CareHome.not_care_home, 10.0),
            (7, "1-004", CareHome.not_care_home, None),
            (8, "1-005", CareHome.not_care_home, 10.0),
            (9, "1-006", CareHome.not_care_home, None),
            (10, "1-006", CareHome.not_care_home, None),
        ],
        schema=Schemas.input_flag_rows_eligible_for_imputation_schema,
        orient="row",
    )

    def test_function_flags_expected_rows(self):
        returned_when_care_home_lf = job.flag_rows_eligible_for_imputation(
            self.input_lf,
            Data.column_with_null_values_name,
            care_home=True,
        )
        returned_when_not_care_home_lf = job.flag_rows_eligible_for_imputation(
            self.input_lf,
            Data.column_with_null_values_name,
            care_home=False,
        )

        expected_rowid_when_care_home_returned_lf = [1, 2, 3]
        expected_rowid_when_not_care_home_returned_lf = [6, 7, 8]

        assert (
            returned_when_care_home_lf.filter(
                pl.col(Imputation.eligible_for_imputation)
            )
            .select(Schemas.row_id)
            .collect()
            .to_series()
            .to_list()
            == expected_rowid_when_care_home_returned_lf
        )
        assert (
            returned_when_not_care_home_lf.filter(
                pl.col(Imputation.eligible_for_imputation)
            )
            .select(Schemas.row_id)
            .collect()
            .to_series()
            .to_list()
            == expected_rowid_when_not_care_home_returned_lf
        )
