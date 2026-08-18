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
    def test_function_has_expected_calls(
        self,
        model_extrapolation_mock: Mock,
        model_interpolation_mock: Mock,
    ):
        flagged_lf = pl.LazyFrame(
            [],
            Schemas.expected_model_imputation_schema,
            orient="row",
        )
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
