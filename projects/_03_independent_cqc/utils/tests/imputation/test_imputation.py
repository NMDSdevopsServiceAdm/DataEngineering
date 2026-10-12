from dataclasses import dataclass
from typing import Any
from unittest.mock import Mock, patch

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._03_independent_cqc.utils.imputation.imputation as job
from projects._03_independent_cqc.unittest_data.polars_independent_cqc_test_data import (
    ModelImputation as Data,
)
from projects._03_independent_cqc.unittest_data.polars_independent_cqc_test_schema import (
    ModelImputation as Schemas,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc

PATCH_PATH = "projects._03_independent_cqc.utils.imputation.imputation"


@dataclass
class GroupColumnsTestCase:
    id: str
    kwargs: dict[str, Any]
    expected_group_columns: list[str]

    def as_pytest_param(self):
        return pytest.param(self.kwargs, self.expected_group_columns, id=self.id)


group_columns_cases = [
    GroupColumnsTestCase(
        id="defaults_to_location_and_care_home",
        kwargs={"care_home": False},
        expected_group_columns=[IndCqc.location_id, IndCqc.care_home],
    ),
    GroupColumnsTestCase(
        id="uses_given_group_columns",
        kwargs={"care_home": None, "group_columns": [IndCqc.location_id]},
        expected_group_columns=[IndCqc.location_id],
    ),
]


class TestModelImputationFunctionality:
    @pytest.mark.parametrize(
        "kwargs, expected_group_columns",
        [case.as_pytest_param() for case in group_columns_cases],
    )
    @patch(f"{PATCH_PATH}.model_interpolation")
    @patch(f"{PATCH_PATH}.model_extrapolation")
    def test_function_passes_group_columns_to_models(
        self,
        model_extrapolation_mock: Mock,
        model_interpolation_mock: Mock,
        kwargs,
        expected_group_columns,
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
            extrapolation_method="nominal",
            **kwargs,
        )

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


class TestModelImputationWithArguments:
    @pytest.mark.parametrize(
        "model_imputation_data, kwargs",
        [
            case.as_pytest_param()
            for case in Data.expected_model_imputation_with_arguments_test_cases
        ],
    )
    def test_function_returns_expected_data(self, model_imputation_data, kwargs):
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
            extrapolation_method="nominal",
            **kwargs,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)
