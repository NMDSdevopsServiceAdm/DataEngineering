import unittest
from unittest.mock import Mock, patch
import warnings

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc
import utils.estimate_filled_posts.models.imputation_with_extrapolation_and_interpolation as job
from utils import utils
from tests.test_file_data import (
    ModelImputationWithExtrapolationAndInterpolationData as Data,
)
from tests.test_file_schemas import (
    ModelImputationWithExtrapolationAndInterpolationSchemas as Schemas,
)


class ModelImputationWithExtrapolationAndInterpolationTests(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()

        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)


class MainTests(ModelImputationWithExtrapolationAndInterpolationTests):
    def setUp(self) -> None:
        super().setUp()

        self.imputation_with_extrapolation_and_interpolation_df = (
            self.spark.createDataFrame(
                Data.imputation_with_extrapolation_and_interpolation_rows,
                Schemas.imputation_with_extrapolation_and_interpolation_schema,
            )
        )
        self.returned_df = job.model_imputation_with_extrapolation_and_interpolation(
            self.imputation_with_extrapolation_and_interpolation_df,
            Data.column_with_null_values_name,
            Data.model_column_name,
        )

    @patch(
        "utils.estimate_filled_posts.models.imputation_with_extrapolation_and_interpolation.model_interpolation"
    )
    @patch(
        "utils.estimate_filled_posts.models.imputation_with_extrapolation_and_interpolation.model_extrapolation"
    )
    def test_model_imputation_with_extrapolation_and_interpolation_runs(
        self,
        model_extrapolation_mock: Mock,
        model_interpolation_mock: Mock,
    ):
        job.model_imputation_with_extrapolation_and_interpolation(
            self.imputation_with_extrapolation_and_interpolation_df,
            Data.column_with_null_values_name,
            Data.model_column_name,
        )

        model_extrapolation_mock.assert_called_once()
        model_interpolation_mock.assert_called_once()

    def test_model_imputation_with_extrapolation_and_interpolation_returns_same_number_of_rows(
        self,
    ):
        self.assertEqual(
            self.imputation_with_extrapolation_and_interpolation_df.count(),
            self.returned_df.count(),
        )

    def test_model_imputation_with_extrapolation_and_interpolation_returns_new_column(
        self,
    ):
        self.assertIn(Data.imputation_model_column_name, self.returned_df.columns)


class CreateImputationModelNameTests(
    ModelImputationWithExtrapolationAndInterpolationTests
):
    def setUp(self) -> None:
        super().setUp()

    def test_create_imputation_model_name_returns_expected_column_name(self):
        self.assertEqual(
            job.create_imputation_model_name(
                Data.column_with_null_values_name, Data.model_column_name
            ),
            Data.expected_column_name,
        )


class ModelImputationTests(ModelImputationWithExtrapolationAndInterpolationTests):
    def setUp(self) -> None:
        super().setUp()

    def test_imputation_model_returns_correct_values(self):
        null_value_column: str = "null_values"
        imputation_model: str = "imputation_model"
        test_df = self.spark.createDataFrame(
            Data.imputation_model_rows, Schemas.imputation_model_schema
        )
        returned_df = job.model_imputation(test_df, null_value_column, imputation_model)
        expected_df = self.spark.createDataFrame(
            Data.expected_imputation_model_rows,
            Schemas.expected_imputation_model_schema,
        )
        returned_data = returned_df.sort(IndCqc.location_id).collect()
        expected_data = expected_df.collect()
        for i in range(len(returned_data)):
            self.assertEqual(
                returned_data[i][imputation_model],
                expected_data[i][imputation_model],
                f"Returned row {i} does not match expected",
            )
