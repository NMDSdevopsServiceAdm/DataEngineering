import unittest
from unittest.mock import ANY, Mock, patch

import projects._03_independent_cqc._04_model.fargate.model_01_features_non_res_with_dormancy as job
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

PATCH_PATH = "projects._03_independent_cqc._04_model.fargate.model_01_features_non_res_with_dormancy"


class ModelFeaturesNonResWithoutDormancyTests(unittest.TestCase):
    TEST_BUCKET_NAME = "some_bucket"
    TEST_MODEL_NAME = "some_model"
    TEST_MODEL_REGISTRY = {
        "my_model": {"dependent": "dependent_col_test", "features": ["feat1", "feat2"]}
    }
    partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

    mock_ind_cqc_data = Mock(name="ind_cqc_data")

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.fUtils.select_and_filter_features_data")
    @patch(f"{PATCH_PATH}.fUtils.expand_encode_and_extract_features")
    @patch(f"{PATCH_PATH}.fUtils.cap_integer_at_max_value")
    @patch(f"{PATCH_PATH}.fUtils.add_array_column_count")
    @patch(f"{PATCH_PATH}.fUtils.add_squared_column")
    @patch(f"{PATCH_PATH}.fUtils.add_date_index_column")
    @patch(f"{PATCH_PATH}.utils.scan_parquet", return_value=mock_ind_cqc_data)
    @patch(f"{PATCH_PATH}.vUtils.validate_model_definition")
    @patch(f"{PATCH_PATH}.pUtils.generate_features_path")
    @patch(f"{PATCH_PATH}.pUtils.generate_ind_cqc_path")
    @patch(f"{PATCH_PATH}.model_registry", return_value=TEST_MODEL_REGISTRY)
    def test_main_runs_successfully(
        self,
        model_registry_mock: Mock,
        generate_ind_cqc_path_mock: Mock,
        generate_features_path_mock: Mock,
        validate_model_definition_mock: Mock,
        scan_parquet_mock: Mock,
        add_date_index_column_mock: Mock,
        add_squared_column_mock: Mock,
        add_array_column_count_mock: Mock,
        cap_integer_at_max_value_mock: Mock,
        expand_encode_and_extract_features_mock: Mock,
        select_and_filter_features_data_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(self.TEST_BUCKET_NAME, self.TEST_MODEL_NAME)

        generate_ind_cqc_path_mock.assert_called_once()
        generate_features_path_mock.assert_called_once()
        validate_model_definition_mock.assert_called_once()
        scan_parquet_mock.assert_called_once()
        add_date_index_column_mock.assert_called_once()
        add_squared_column_mock.assert_called_once()
        self.assertEqual(add_array_column_count_mock.call_count, 2)
        self.assertEqual(cap_integer_at_max_value_mock.call_count, 2)
        self.assertEqual(expand_encode_and_extract_features_mock.call_count, 5)
        select_and_filter_features_data_mock.assert_called_once()
        sink_to_parquet_mock.assert_called_once_with(
            ANY, ANY, partition_cols=self.partition_keys, append=False
        )
