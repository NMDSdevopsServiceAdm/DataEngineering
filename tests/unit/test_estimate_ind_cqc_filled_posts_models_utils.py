import unittest
import warnings
from unittest.mock import ANY, MagicMock, Mock, patch

from datetime import date
from pyspark.sql import functions as F
from pyspark.ml.linalg import Vectors
from pyspark.ml.regression import LinearRegressionModel

from utils.estimate_filled_posts.models import utils as job
from tests.test_file_data import EstimateFilledPostsModelsUtils as Data
from tests.test_file_schemas import EstimateFilledPostsModelsUtils as Schemas
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc
from utils.column_values.categorical_column_values import CareHome

PATCH_PATH: str = "utils.estimate_filled_posts.models.utils"


class EstimateFilledPostsModelsUtilsTests(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()

        self.model_source: str = "s3://pipeline-resources/models/model_name/1.0.0/"
        self.branch_name = "test_branch"
        self.model_name = "test_model"
        self.model_version = "1.0.0"


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


class CombineCareHomeRatiosAndNonResPostsTests(EstimateFilledPostsModelsUtilsTests):
    def setUp(self) -> None:
        super().setUp()

        test_df = self.spark.createDataFrame(
            Data.combine_care_home_ratios_and_non_res_posts_rows,
            Schemas.combine_care_home_ratios_and_non_res_posts_schema,
        )
        self.returned_df = job.combine_care_home_ratios_and_non_res_posts(
            test_df,
            IndCqc.filled_posts_per_bed_ratio,
            IndCqc.ascwds_filled_posts_dedup_clean,
            IndCqc.combined_ratio_and_filled_posts,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_combine_care_home_ratios_and_non_res_posts_rows,
            Schemas.expected_combine_care_home_ratios_and_non_res_posts_schema,
        )

    def test_create_combine_care_home_ratios_and_non_res_posts_returns_expected_columns(
        self,
    ):
        self.assertEqual(
            sorted(self.returned_df.columns),
            sorted(self.expected_df.columns),
        )

    def test_returned_column_values_match_expected_when_care_home(self):
        returned_care_home_data = (
            self.returned_df.where(F.col(IndCqc.care_home) == CareHome.care_home)
            .sort(IndCqc.location_id)
            .collect()
        )
        expected_care_home_data = self.expected_df.where(
            F.col(IndCqc.care_home) == CareHome.care_home
        ).collect()

        for i in range(len(returned_care_home_data)):
            self.assertEqual(
                returned_care_home_data[i][IndCqc.combined_ratio_and_filled_posts],
                expected_care_home_data[i][IndCqc.combined_ratio_and_filled_posts],
                f"Returned row {i} does not match expected",
            )

    def test_returned_column_values_match_expected_when_not_care_home(self):
        returned_not_care_home_data = (
            self.returned_df.where(F.col(IndCqc.care_home) != CareHome.care_home)
            .sort(IndCqc.location_id)
            .collect()
        )
        expected_not_care_home_data = self.expected_df.where(
            F.col(IndCqc.care_home) != CareHome.care_home
        ).collect()

        for i in range(len(returned_not_care_home_data)):
            self.assertEqual(
                returned_not_care_home_data[i][IndCqc.combined_ratio_and_filled_posts],
                expected_not_care_home_data[i][IndCqc.combined_ratio_and_filled_posts],
                f"Returned row {i} does not match expected",
            )


class CleanNumberOfBedsBandedTests(EstimateFilledPostsModelsUtilsTests):
    def setUp(self) -> None:
        super().setUp()

        test_df = self.spark.createDataFrame(
            Data.clean_number_of_beds_banded_rows,
            Schemas.clean_number_of_beds_banded_schema,
        )
        self.returned_df = job.clean_number_of_beds_banded(test_df)
        self.expected_df = self.spark.createDataFrame(
            Data.expected_clean_number_of_beds_banded_rows,
            Schemas.expected_clean_number_of_beds_banded_schema,
        )

    def test_clean_number_of_beds_banded_returns_expected_columns(
        self,
    ):
        self.assertEqual(
            sorted(self.returned_df.columns),
            sorted(self.expected_df.columns),
        )

    def test_clean_number_of_beds_banded_returns_expected_data(self):
        returned_data = self.returned_df.sort(IndCqc.location_id).collect()
        expected_data = self.expected_df.collect()

        for i in range(len(returned_data)):
            self.assertEqual(
                returned_data[i],
                expected_data[i],
                f"Returned row {i} does not match expected",
            )


class ConvertCareHomeRatiosToFilledPostsAndMergeWithFilledPostValuesTests(
    EstimateFilledPostsModelsUtilsTests
):
    def setUp(self) -> None:
        super().setUp()

        self.test_df = self.spark.createDataFrame(
            Data.convert_care_home_ratios_to_filled_posts_and_merge_with_filled_post_values_rows,
            Schemas.convert_care_home_ratios_to_filled_posts_and_merge_with_filled_post_values_schema,
        )
        self.returned_df = job.convert_care_home_ratios_to_filled_posts_and_merge_with_filled_post_values(
            self.test_df,
            IndCqc.banded_bed_ratio_rolling_average_model,
            IndCqc.posts_rolling_average_model,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_convert_care_home_ratios_to_filled_posts_and_merge_with_filled_post_values_rows,
            Schemas.convert_care_home_ratios_to_filled_posts_and_merge_with_filled_post_values_schema,
        )

    def test_returned_columns_match_original_data_columns(self):
        self.assertEqual(self.returned_df.columns, self.test_df.columns)

    def test_returned_column_values_match_expected_when_not_care_home(self):
        returned_data = self.returned_df.sort(IndCqc.location_id).collect()
        expected_data = self.expected_df.collect()

        for i in range(len(returned_data)):
            self.assertEqual(
                returned_data[i],
                expected_data[i],
                f"Returned row {i} does not match expected",
            )


class TrainLassoRegressionModelTests(EstimateFilledPostsModelsUtilsTests):
    def setUp(self) -> None:
        super().setUp()

        self.features_df = self.spark.createDataFrame(
            Data.train_lasso_regression_model_rows,
            Schemas.train_lasso_regression_model_schema,
        )

    def test_train_lasso_regression_model_returns_model_with_non_null_coefficients(
        self,
    ):
        trained_model = job.train_lasso_regression_model(
            self.features_df, IndCqc.imputed_filled_post_model, self.model_name
        )

        self.assertIsInstance(trained_model, LinearRegressionModel)
        self.assertIsNotNone(trained_model.coefficients)

    @patch(f"{PATCH_PATH}.LinearRegression")
    def test_train_lasso_model_called_with_expected_parameters(
        self, LinearRegressionModel_mock: Mock
    ):
        mock_model = MagicMock()
        mock_lr_instance = MagicMock()
        mock_lr_instance.fit.return_value = mock_model
        LinearRegressionModel_mock.return_value = mock_lr_instance

        job.train_lasso_regression_model(
            self.features_df, IndCqc.imputed_filled_post_model, self.model_name
        )

        LinearRegressionModel_mock.assert_called_once_with(
            featuresCol=IndCqc.features,
            labelCol=IndCqc.imputed_filled_post_model,
            predictionCol=self.model_name,
            elasticNetParam=1,
            regParam=0.001,
        )


class GetExistingRunNumbersTests(EstimateFilledPostsModelsUtilsTests):
    def setUp(self) -> None:
        super().setUp()

    @patch(f"{PATCH_PATH}.boto3.client")
    @patch(
        f"{PATCH_PATH}.utils.split_s3_uri",
        return_value=("test-bucket", "models/prediction/1.0.0/"),
    )
    def test_get_existing_run_numbers_returns_expected_list(
        self, split_s3_uri_mock: Mock, boto_client_mock: Mock
    ):
        mock_s3 = boto_client_mock.return_value
        mock_s3.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "models/prediction/1.0.0/run=1/"},
                {"Key": "models/prediction/1.0.0/run=2/"},
                {"Key": "models/prediction/1.0.0/run=3/"},
            ]
        }

        returned_list = job.get_existing_run_numbers(self.model_source)
        expected_list = [1, 2, 3]

        self.assertEqual(returned_list, expected_list)


class GenerateRunNumberTests(EstimateFilledPostsModelsUtilsTests):
    def setUp(self) -> None:
        super().setUp()

    @patch(f"{PATCH_PATH}.get_existing_run_numbers")
    def test_generate_run_number_returns_max_run_number_when_mode_is_load_and_previous_runs_exist(
        self, get_existing_run_numbers_mock: Mock
    ):
        get_existing_run_numbers_mock.return_value = [1, 2, 3]

        returned_number = job.generate_run_number(self.model_source, mode="load")
        expected_number = 3

        self.assertEqual(returned_number, expected_number)

    @patch(f"{PATCH_PATH}.get_existing_run_numbers")
    def test_generate_run_number_raises_error_when_mode_is_load_and_no_previous_runs_exist(
        self, get_existing_run_numbers_mock: Mock
    ):
        get_existing_run_numbers_mock.return_value = []

        with self.assertRaises(FileNotFoundError):
            job.generate_run_number(self.model_source, mode="load")

    @patch(f"{PATCH_PATH}.get_existing_run_numbers")
    def test_generate_run_number_next_run_number_when_mode_is_save_and_previous_runs_exist(
        self, get_existing_run_numbers_mock: Mock
    ):
        get_existing_run_numbers_mock.return_value = [1, 2]

        returned_number = job.generate_run_number(self.model_source, mode="save")
        expected_number = 3

        self.assertEqual(returned_number, expected_number)

    @patch(f"{PATCH_PATH}.get_existing_run_numbers")
    def test_generate_run_number_returns_run_1_when_mode_is_save_and_no_previous_runs_exist(
        self, get_existing_run_numbers_mock: Mock
    ):
        get_existing_run_numbers_mock.return_value = []

        returned_number = job.generate_run_number(self.model_source, mode="save")
        expected_number = 1

        self.assertEqual(returned_number, expected_number)

    @patch(f"{PATCH_PATH}.get_existing_run_numbers")
    def test_generate_run_number_raises_value_error_when_mode_is_invalid(
        self, get_existing_run_numbers_mock: Mock
    ):
        get_existing_run_numbers_mock.return_value = []

        with self.assertRaises(ValueError) as context:
            job.generate_run_number(self.model_source, mode="invalid_mode")

        self.assertTrue("mode must be 'load' or 'save'" in str(context.exception))


class SaveModelToS3Tests(EstimateFilledPostsModelsUtilsTests):
    def setUp(self) -> None:
        super().setUp()

        self.mock_model = MagicMock()
        self.run_number: int = 4
        self.model_s3_location = f"{self.model_source}run={self.run_number}/"

    @patch(f"{PATCH_PATH}.generate_run_number")
    def test_save_model_to_s3_has_correct_calls(self, generate_run_number_mock: Mock):
        generate_run_number_mock.return_value = self.run_number

        job.save_model_to_s3(self.mock_model, self.model_source)

        generate_run_number_mock.assert_called_once_with(self.model_source, mode="save")
        self.mock_model.save.assert_called_once_with(self.model_s3_location)


class LoadLatestModelTests(EstimateFilledPostsModelsUtilsTests):
    def setUp(self) -> None:
        super().setUp()

    @patch(f"{PATCH_PATH}.generate_run_number")
    @patch(f"{PATCH_PATH}.LinearRegressionModel.load")
    def test_load_latest_model_loads_expected_model(
        self, mock_model_load: Mock, generate_run_number_mock: Mock
    ):
        generate_run_number_mock.return_value = 4
        mock_model = MagicMock()
        mock_model_load.return_value = mock_model
        expected_load_path = (
            f"{self.model_source}run={generate_run_number_mock.return_value}/"
        )

        result = job.load_latest_model_from_s3(self.model_source)

        mock_model_load.assert_called_once_with(expected_load_path)
        self.assertEqual(result, mock_model)


class GenerateFeaturesS3PathTests(EstimateFilledPostsModelsUtilsTests):
    def setUp(self) -> None:
        super().setUp()

    def test_generate_model_features_s3_path_returns_expected_path(self):
        returned_path = job.generate_model_features_s3_path(
            self.branch_name, self.model_name
        )
        expected_path = "s3://sfc-test_branch-datasets/domain=ind_cqc_filled_posts/dataset=ind_cqc_model_features/model_name=test_model/"

        self.assertEqual(returned_path, expected_path)


class GenerateModelS3PathTests(EstimateFilledPostsModelsUtilsTests):
    def setUp(self) -> None:
        super().setUp()

    def test_generate_model_s3_path_returns_expected_path(self):
        returned_path = job.generate_model_s3_path(
            self.branch_name, self.model_name, self.model_version
        )
        expected_path = (
            "s3://sfc-test_branch-pipeline-resources/models/test_model/1.0.0/"
        )

        self.assertEqual(returned_path, expected_path)


class GenerateModelPredictionsS3PathTests(EstimateFilledPostsModelsUtilsTests):
    def setUp(self) -> None:
        super().setUp()

    def test_generate_model_predictions_s3_path_returns_expected_path(self):
        returned_path = job.generate_model_predictions_s3_path(
            self.branch_name, self.model_name
        )
        expected_path = "s3://sfc-test_branch-datasets/domain=ind_cqc_filled_posts/dataset=ind_cqc_model_predictions/model_name=test_model/"

        self.assertEqual(returned_path, expected_path)
