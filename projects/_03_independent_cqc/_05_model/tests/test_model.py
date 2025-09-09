import unittest
from unittest.mock import patch

from polars import scan_parquet, DataFrame
from sklearn.linear_model import LinearRegression

from projects._03_independent_cqc._05_model.utils.model import (
    Model,
    ModelType,
    ModelNotTrainedError,
)

PATCH_PATH = "projects._03_independent_cqc._05_model.utils.model"


class TestModel(unittest.TestCase):
    standard_model = Model(
        model_type=ModelType.SIMPLE_LINEAR,
        model_identifier="test_linear_model",
        model_params={},
        version_parameter_location="/some/location",
        data_source_prefix="some/prefix",
        target_columns=["target"],
        feature_columns=["column1", "column2"],
    )
    ice_cream_model = Model(
        model_type=ModelType.SIMPLE_LINEAR,
        model_identifier="test_linear_model_ice_cream",
        model_params={},
        version_parameter_location="/some/location",
        data_source_prefix="some/prefix",
        target_columns=["IceCreamSales"],
        feature_columns=["MeanDailyTemperature"],
    )

    def setUp(self):
        self.lf = scan_parquet("tests/test_data/sample_parquet/sales1.parquet")
        self.lf = self.lf.with_row_index()

    def test_model_linear_regression_instantiates(self):
        self.assertEqual(self.standard_model.model_type, ModelType.SIMPLE_LINEAR)
        self.assertEqual(self.standard_model.model_identifier, "test_linear_model")
        self.assertEqual(self.standard_model.model_params, {})
        self.assertEqual(
            self.standard_model.version_parameter_location, "/some/location"
        )
        self.assertEqual(self.standard_model.data_source_prefix, "some/prefix")
        self.assertEqual(self.standard_model.target_columns, ["target"])
        self.assertEqual(self.standard_model.feature_columns, ["column1", "column2"])
        self.assertIsInstance(self.standard_model.model, LinearRegression)
        self.assertEqual(self.standard_model.training_score, None)
        self.assertEqual(self.standard_model.testing_score, None)

    @patch(f"{PATCH_PATH}.ModelType", autospec=True)
    def test_model_linear_regression_raises_value_error_if_invalid_model_type(
        self, mock_model_type
    ):
        mock_model_type.SILLY_MODEL = "silly_model"
        with self.assertRaises(ValueError):
            model = Model(
                model_type=mock_model_type.SILLY_MODEL,
                model_identifier="test_linear_model",
                model_params={},
                version_parameter_location="/some/location",
                data_source_prefix="some/prefix",
                target_columns=["target"],
                feature_columns=["column1", "column2"],
            )

    @patch(f"{PATCH_PATH}.pl.scan_parquet")
    def test_get_raw_data_uses_correct_s3_uri(self, mock_scan_parquet):
        data = self.standard_model.get_raw_data("test_bucket")
        expected_s3_uri = "s3://test_bucket/some/prefix"
        mock_scan_parquet.assert_called_once_with(expected_s3_uri)

    def test_get_test_train_from_dataframe(self):
        data = self.lf.collect()
        train, test = self.standard_model.create_train_and_test_datasets(
            data=data, seed=123
        )
        self.assertAlmostEqual(train.shape[0], 14000, delta=3)
        self.assertAlmostEqual(test.shape[0], 6000, delta=3)
        self.assertIsInstance(train, DataFrame)
        self.assertIsInstance(test, DataFrame)
        self.assertEqual(data.columns, train.columns)
        self.assertEqual(data.columns, test.columns)
        self.assertEqual(train["index"].n_unique(), 14000)
        self.assertEqual(test["index"].n_unique(), 6000)

        check_df = train.join(test, on="index", how="inner")
        self.assertEqual(check_df.shape[0], 0)

    def test_get_test_train_from_lazyframe(self):
        data = self.lf
        train, test = self.standard_model.create_train_and_test_datasets(
            data=data, seed=123
        )

        self.assertAlmostEqual(train.shape[0], 14000, delta=3)
        self.assertAlmostEqual(test.shape[0], 6000, delta=3)
        self.assertIsInstance(train, DataFrame)
        self.assertIsInstance(test, DataFrame)
        self.assertEqual(data.columns, train.columns)
        self.assertEqual(data.columns, test.columns)
        self.assertEqual(train["index"].n_unique(), 14000)
        self.assertEqual(test["index"].n_unique(), 6000)

        check_df = train.join(test, on="index", how="inner")
        self.assertEqual(check_df.shape[0], 0)

    def test_model_fit(self):
        data = self.lf
        train, _ = self.ice_cream_model.create_train_and_test_datasets(
            data=data, seed=123
        )
        fitted_model = self.ice_cream_model.fit(train)
        self.assertIsInstance(fitted_model, LinearRegression)
        self.assertAlmostEqual(fitted_model.coef_[0][0], 248274.01134039, places=3)
        self.assertAlmostEqual(fitted_model.intercept_[0], 38307.77491212, places=3)
        self.assertEqual(len(fitted_model.coef_), 1)
        self.assertEqual(len(fitted_model.intercept_), 1)
        self.assertAlmostEqual(
            self.ice_cream_model.training_score, 0.8354809377928092, places=3
        )

    def test_model_validate(self):
        data = self.lf
        train, test = self.ice_cream_model.create_train_and_test_datasets(
            data=data, seed=123
        )
        self.ice_cream_model.fit(train)
        validation_score = self.ice_cream_model.validate(test)
        self.assertAlmostEqual(validation_score, 0.0, places=2)
        self.assertAlmostEqual(
            self.ice_cream_model.testing_score, 0.8350624481353132, places=3
        )

    def test_model_validate_fails_if_not_trained(self):
        model = self.standard_model
        data = self.lf
        _, test = model.create_train_and_test_datasets(data=data, seed=123)
        with self.assertRaises(ModelNotTrainedError):
            model.validate(test)

    def test_model_predict_returns_polars_dataframe(self):
        data = self.lf
        train, test = self.ice_cream_model.create_train_and_test_datasets(
            data=data, seed=123
        )
        self.ice_cream_model.fit(train)
        result = self.ice_cream_model.predict(test)
        self.assertIsInstance(result, DataFrame)
