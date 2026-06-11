import unittest
from unittest.mock import Mock, patch

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
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import CareHome

PATCH_PATH = "projects._03_independent_cqc.utils.imputation.imputation"


class TestModelImputationFunctionality(unittest.TestCase):
    def setUp(self):
        self.imputed_lf = pl.LazyFrame(
            Data.imputed_rows,
            Schemas.expected_model_imputation_schema,
            orient="row",
        )
        self.non_imputed_lf = pl.LazyFrame(
            Data.non_imputed_rows,
            Schemas.expected_model_imputation_schema,
            orient="row",
        )

    @patch(f"{PATCH_PATH}.model_interpolation")
    @patch(f"{PATCH_PATH}.model_extrapolation")
    @patch(f"{PATCH_PATH}.split_dataset_for_imputation")
    def test_function_has_expected_calls(
        self,
        split_dataset_for_imputation_mock: Mock,
        model_extrapolation_mock: Mock,
        model_interpolation_mock: Mock,
    ):
        split_dataset_for_imputation_mock.return_value = (
            self.imputed_lf,
            self.non_imputed_lf,
        )
        model_extrapolation_mock.return_value = self.imputed_lf
        model_interpolation_mock.return_value = self.imputed_lf
        job.model_imputation(
            Mock(name="input_lf"),
            Data.column_with_null_values_name,
            Data.model_column_name,
            Data.imputed_values_column_name,
            care_home=False,
            extrapolation_method="nominal",
        )

        split_dataset_for_imputation_mock.assert_called_once()
        model_extrapolation_mock.assert_called_once()
        model_interpolation_mock.assert_called_once()


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


class SplitDatasetForImputationTests(unittest.TestCase):
    def test_function_returns_expected_data(self):

        input_lf = pl.LazyFrame(
            data=[
                ("1-001", CareHome.care_home, 10.0),
                ("1-001", CareHome.care_home, None),
                ("1-002", CareHome.care_home, 10.0),
                ("1-003", CareHome.care_home, None),
                ("1-003", CareHome.care_home, None),
                ("1-004", CareHome.not_care_home, 10.0),
                ("1-004", CareHome.not_care_home, None),
                ("1-005", CareHome.not_care_home, 10.0),
                ("1-006", CareHome.not_care_home, None),
                ("1-006", CareHome.not_care_home, None),
            ],
            schema=Schemas.input_split_dataset_for_imputation_schema,
            orient="row",
        )
        returned_inputed_when_care_home, returned_non_inputed_when_care_home = (
            job.split_dataset_for_imputation(
                input_lf,
                Data.column_with_null_values_name,
                care_home=True,
            )
        )
        returned_inputed_when_not_care_home, returned_non_inputed_when_not_care_home = (
            job.split_dataset_for_imputation(
                input_lf,
                Data.column_with_null_values_name,
                care_home=False,
            )
        )

        self.assertEqual(
            returned_inputed_when_care_home.select(IndCQC.location_id)
            .collect()
            .to_series()
            .to_list(),
            ["1-001", "1-001", "1-002"],
        )
        self.assertEqual(
            returned_non_inputed_when_care_home.select(IndCQC.location_id)
            .collect()
            .to_series()
            .to_list(),
            ["1-003", "1-003", "1-004", "1-004", "1-005", "1-006", "1-006"],
        )
        self.assertEqual(
            returned_inputed_when_not_care_home.select(IndCQC.location_id)
            .collect()
            .to_series()
            .to_list(),
            ["1-004", "1-004", "1-005"],
        )
        self.assertEqual(
            returned_non_inputed_when_not_care_home.select(IndCQC.location_id)
            .collect()
            .to_series()
            .to_list(),
            ["1-001", "1-001", "1-002", "1-003", "1-003", "1-006", "1-006"],
        )
