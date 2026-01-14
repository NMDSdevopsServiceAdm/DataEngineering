import unittest
import warnings
from datetime import date
from unittest.mock import ANY, Mock, patch

from projects._03_independent_cqc._06_estimate_filled_posts.utils.models import (
    utils as job,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_data import (
    EstimateFilledPostsModelsUtils as Data,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_schemas import (
    EstimateFilledPostsModelsUtils as Schemas,
)
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc

PATCH_PATH: str = (
    "projects._03_independent_cqc._06_estimate_filled_posts.utils.models.utils"
)


class EstimateFilledPostsModelsUtilsTests(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()


class InsertPredictionsIntoPipelineTest(EstimateFilledPostsModelsUtilsTests):
    def setUp(self) -> None:
        super().setUp()

        self.cleaned_cqc_ind_df = self.spark.createDataFrame(
            Data.cleaned_cqc_rows, Schemas.cleaned_cqc_schema
        )
        self.predictions_df = self.spark.createDataFrame(
            Data.predictions_rows, Schemas.predictions_schema
        )
        self.returned_df = job.insert_predictions_into_pipeline(
            self.cleaned_cqc_ind_df,
            self.predictions_df,
            IndCqc.care_home_model,
        )

        warnings.filterwarnings("ignore", category=ResourceWarning)

    def test_insert_predictions_into_pipeline_adds_extra_column(self):
        self.assertTrue(IndCqc.care_home_model in self.returned_df.columns)

    def test_insert_predictions_into_pipeline_does_so_when_join_matches(self):
        df = self.returned_df

        expected_df = df.where(
            (df[IndCqc.location_id] == "1-000000001")
            & (df[IndCqc.cqc_location_import_date] == date(2022, 3, 29))
        ).collect()[0]

        self.assertAlmostEqual(expected_df[IndCqc.care_home_model], 56.89, places=2)

    def test_insert_predictions_into_pipeline_returns_null_if_no_match(self):
        df = self.returned_df

        expected_df = df.where(
            (df[IndCqc.location_id] == "1-000000001")
            & (df[IndCqc.cqc_location_import_date] == date(2022, 2, 20))
        ).collect()[0]

        self.assertIsNone(expected_df[IndCqc.estimate_filled_posts])


class JoinModelPredictionsTest(EstimateFilledPostsModelsUtilsTests):
    def setUp(self) -> None:
        super().setUp()

        self.test_bucket = "test_bucket"

        self.mock_ind_cqc_df = Mock(name="ind_cqc_df")
        self.mock_predictions_df = Mock(name="predictions_df")

        self.ind_cqc_df = self.spark.createDataFrame(
            Data.join_model_ind_cqc_rows, Schemas.join_model_ind_cqc_schema
        )

        self.care_home_model = Schemas.test_care_home_model_name
        self.care_home_pred_df = self.spark.createDataFrame(
            Data.join_model_predictions_care_home_rows,
            Schemas.join_model_predictions_care_home_schema,
        )
        self.expected_joined_care_home_df = self.spark.createDataFrame(
            Data.expected_join_model_ind_cqc_care_home_rows,
            Schemas.expected_join_model_ind_cqc_care_home_schema,
        )

        self.non_res_model = Schemas.test_non_res_model_name
        self.non_res_pred_df = self.spark.createDataFrame(
            Data.join_model_predictions_non_res_rows,
            Schemas.join_model_predictions_non_res_schema,
        )
        self.expected_joined_non_res_df = self.spark.createDataFrame(
            Data.expected_join_model_ind_cqc_non_res_rows,
            Schemas.expected_join_model_ind_cqc_non_res_schema,
        )

    @patch(f"{PATCH_PATH}.prepare_predictions_for_join")
    @patch(f"{PATCH_PATH}.set_min_value")
    @patch(f"{PATCH_PATH}.calculate_filled_posts_from_beds_and_ratio")
    @patch(f"{PATCH_PATH}.utils.read_from_parquet")
    @patch(f"{PATCH_PATH}.generate_predictions_path")
    def test_function_calls_all_necessary_functions_when_care_home_model(
        self,
        generate_predictions_path_mock: Mock,
        read_from_parquet_mock: Mock,
        calculate_filled_posts_mock: Mock,
        set_min_value_mock: Mock,
        prepare_predictions_for_join_mock: Mock,
    ):
        read_from_parquet_mock.return_value = self.mock_predictions_df

        job.join_model_predictions(
            self.mock_ind_cqc_df, self.test_bucket, self.care_home_model
        )

        generate_predictions_path_mock.assert_called_once_with(
            self.test_bucket, self.care_home_model
        )
        read_from_parquet_mock.assert_called_once_with(
            generate_predictions_path_mock.return_value
        )
        calculate_filled_posts_mock.assert_called_once()
        set_min_value_mock.assert_called_once_with(ANY, IndCqc.prediction, 1.0)
        prepare_predictions_for_join_mock.assert_called_once_with(
            ANY, self.care_home_model
        )
        self.mock_ind_cqc_df.join.assert_called_once_with(
            ANY,
            [IndCqc.location_id, IndCqc.cqc_location_import_date],
            "left",
        )

    @patch(f"{PATCH_PATH}.prepare_predictions_for_join")
    @patch(f"{PATCH_PATH}.set_min_value")
    @patch(f"{PATCH_PATH}.calculate_filled_posts_from_beds_and_ratio")
    @patch(f"{PATCH_PATH}.utils.read_from_parquet")
    @patch(f"{PATCH_PATH}.generate_predictions_path")
    def test_function_does_not_call_ratio_conversion_for_non_care_home_model(
        self,
        generate_predictions_path_mock: Mock,
        read_from_parquet_mock: Mock,
        calculate_filled_posts_mock: Mock,
        set_min_value_mock: Mock,
        prepare_predictions_for_join_mock: Mock,
    ):
        read_from_parquet_mock.return_value = self.mock_predictions_df

        job.join_model_predictions(
            self.mock_ind_cqc_df, self.test_bucket, self.non_res_model
        )

        generate_predictions_path_mock.assert_called_once_with(
            self.test_bucket, self.non_res_model
        )
        read_from_parquet_mock.assert_called_once_with(
            generate_predictions_path_mock.return_value
        )
        calculate_filled_posts_mock.assert_not_called()
        set_min_value_mock.assert_called_once_with(ANY, IndCqc.prediction, 1.0)
        prepare_predictions_for_join_mock.assert_called_once_with(
            ANY, self.non_res_model
        )
        self.mock_ind_cqc_df.join.assert_called_once_with(
            ANY,
            [IndCqc.location_id, IndCqc.cqc_location_import_date],
            "left",
        )

    @patch(f"{PATCH_PATH}.utils.read_from_parquet")
    @patch(f"{PATCH_PATH}.generate_predictions_path")
    def test_function_returns_expected_data_for_care_home_model(
        self,
        generate_predictions_path_mock: Mock,
        read_from_parquet_mock: Mock,
    ):
        read_from_parquet_mock.return_value = self.care_home_pred_df

        returned_df = job.join_model_predictions(
            self.ind_cqc_df, self.test_bucket, self.care_home_model
        )

        self.assertEqual(returned_df.columns, self.expected_joined_care_home_df.columns)
        self.assertEqual(
            returned_df.sort(IndCqc.location_id).collect(),
            self.expected_joined_care_home_df.collect(),
        )

    @patch(f"{PATCH_PATH}.utils.read_from_parquet")
    @patch(f"{PATCH_PATH}.generate_predictions_path")
    def test_function_returns_expected_data_for_non_care_home_model(
        self,
        generate_predictions_path_mock: Mock,
        read_from_parquet_mock: Mock,
    ):
        read_from_parquet_mock.return_value = self.non_res_pred_df

        returned_df = job.join_model_predictions(
            self.ind_cqc_df, self.test_bucket, self.non_res_model
        )

        self.assertEqual(returned_df.columns, self.expected_joined_non_res_df.columns)
        self.assertEqual(
            returned_df.sort(IndCqc.location_id).collect(),
            self.expected_joined_non_res_df.collect(),
        )


class SetMinimumValueTests(EstimateFilledPostsModelsUtilsTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_df = self.spark.createDataFrame(
            Data.set_min_value_when_below_minimum_rows,
            Schemas.set_min_value_schema,
        )

    def test_set_min_value_replaces_values_below_min_value(self):
        returned_df = job.set_min_value(self.test_df, IndCqc.prediction, 2.0)

        expected_df = self.spark.createDataFrame(
            Data.expected_set_min_value_when_below_min_value_rows,
            Schemas.set_min_value_schema,
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_set_min_value_replaces_value_with_the_default_when_below_min_value_and_value_not_set(
        self,
    ):
        returned_df = job.set_min_value(self.test_df, IndCqc.prediction)

        expected_df = self.spark.createDataFrame(
            Data.expected_set_min_value_when_below_minimum_and_default_not_set_rows,
            Schemas.set_min_value_schema,
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_set_min_value_replaces_value_with_the_greatest_value_when_both_are_negative(
        self,
    ):
        returned_df = job.set_min_value(self.test_df, IndCqc.prediction, -5.0)

        expected_df = self.spark.createDataFrame(
            Data.expected_set_min_value_when_below_minimum_and_min_value_is_negative_rows,
            Schemas.set_min_value_schema,
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_set_min_value_does_not_replace_value_when_min_value_is_none(
        self,
    ):
        returned_df = job.set_min_value(self.test_df, IndCqc.prediction, None)

        self.assertEqual(returned_df.collect(), self.test_df.collect())

    def test_set_min_value_does_not_replace_predictions_above_minimum_value(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.set_min_value_when_above_minimum_rows,
            Schemas.set_min_value_schema,
        )
        returned_df = job.set_min_value(test_df, IndCqc.prediction, 1.0)
        expected_df = test_df

        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_set_min_value_does_not_replace_null_predictions(self):
        test_df = self.spark.createDataFrame(
            Data.set_min_value_when_null_rows,
            Schemas.set_min_value_schema,
        )
        returned_df = job.set_min_value(test_df, IndCqc.prediction, 1.0)
        expected_df = test_df

        self.assertEqual(returned_df.collect(), expected_df.collect())


class PreparePredictionsForJoinTests(EstimateFilledPostsModelsUtilsTests):
    def setUp(self) -> None:
        super().setUp()

        self.model_name = IndCqc.care_home_model

        self.test_df = self.spark.createDataFrame(
            Data.prepare_predictions_for_join_rows,
            Schemas.prepare_predictions_for_join_schema,
        )
        self.returned_df = job.prepare_predictions_for_join(
            self.test_df, self.model_name
        )

        self.expected_df = self.spark.createDataFrame(
            Data.expected_prepare_predictions_for_join_rows,
            Schemas.expected_prepare_predictions_for_join_schema,
        )

    def test_function_renames_and_selects_columns_correctly(self):
        self.assertEqual(self.returned_df.columns, self.expected_df.columns)

    def test_function_preserves_row_count(self):
        self.assertEqual(self.returned_df.count(), self.test_df.count())
