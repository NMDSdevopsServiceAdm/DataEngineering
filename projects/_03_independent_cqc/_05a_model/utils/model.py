from enum import Enum
from typing import Dict, Any, Union
import polars as pl
import sklearn

from sklearn.linear_model import LinearRegression, Lasso, Ridge


class ModelType(Enum):
    SIMPLE_LINEAR = "simple_linear"
    LASSO = "lasso"
    RIDGE = "ridge"


class ModelNotTrainedError(Exception):
    pass


class Model:
    """
    Manages training and testing of linear models using Scikit-learn.
    """

    def __init__(
        self,
        model_type: ModelType,
        model_identifier: str,
        model_params: Dict[str, Any],
        version_parameter_location: str,
        data_source_prefix: str,
        target_columns: list[str],
        feature_columns: list[str],
    ) -> None:
        self.model_type = model_type
        self.model_identifier = model_identifier
        self.model_params = model_params
        self.version_parameter_location = version_parameter_location
        self.data_source_prefix = data_source_prefix
        self.target_columns = target_columns
        self.feature_columns = feature_columns
        self.training_score: float | None = None
        self.testing_score: float | None = None
        match self.model_type:
            case ModelType.SIMPLE_LINEAR:
                self.model = LinearRegression(**model_params)
            case ModelType.LASSO:
                self.model = Lasso(**model_params)
            case ModelType.RIDGE:
                self.model = Ridge(**model_params)
            case _:
                raise ValueError("Unknown model type")

    def get_raw_data(self, bucket_name: str) -> pl.LazyFrame:
        """
        Retrieves raw data from S3 bucket.
        Args:
            bucket_name (str): Name of the S3 bucket where the raw data is located.

        Returns:
            pl.LazyFrame: Raw data from S3 bucket.

        """
        s3_uri = f"s3://{bucket_name}/{self.data_source_prefix}"
        return pl.scan_parquet(s3_uri)

    @classmethod
    def create_train_and_test_datasets(
        cls,
        data: pl.DataFrame | pl.LazyFrame,
        split_size: float = 0.7,
        seed: int = None,
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        """
        Creates training and testing datasets.
        Args:
            data (pl.DataFrame | pl.LazyFrame): Data to be split
            split_size (float): Proportion of dataset that should be training data (eg 0.7 will mean 70% of the data is training data)
            seed (int): Seed for sampling data

        Returns:
            tuple[pl.DataFrame, pl.DataFrame]: Train dataset, Test dataset
        """
        if isinstance(data, pl.LazyFrame):
            data = data.collect()

        df_train = data.sample(
            fraction=split_size, with_replacement=False, shuffle=True, seed=seed
        )
        df_test = data.join(df_train, on=data.columns, how="anti")

        return df_train, df_test

    def fit(self, train_df: pl.DataFrame) -> LinearRegression | Lasso | Ridge:
        """
        Fits the model to training data and calculates R^2 value for the training data.
        Args:
            train_df (pl.DataFrame): Training data to fit the model to

        Returns:
            LinearRegression | Lasso | Ridge: Fitted model
        """
        x1 = train_df.select(self.feature_columns)
        y1 = train_df.select(self.target_columns)
        self.model.fit(x1, y1)
        self.training_score = self.model.score(x1, y1)
        return self.model

    def validate(self, test_df: pl.DataFrame) -> float:
        """
        Calculates the absolute difference between the R^2 score for the training data and the test data.
        Args:
            test_df (pl.DataFrame): Test data

        Returns:
            float: Absolute difference between the R^2 score for the training data and the test data

        Raises:
            ModelNotTrainedError: If the fit function has not been run yet
        """
        if self.training_score is None:
            raise ModelNotTrainedError("Model has not been trained yet.")
        x2 = test_df.select(self.feature_columns)
        y2 = test_df.select(self.target_columns)
        self.testing_score = self.model.score(x2, y2)
        score_difference = abs(self.testing_score - self.training_score)
        return score_difference

    def predict(self, input_df: pl.DataFrame) -> pl.DataFrame:
        """
        Predicts the target column values from the model, given defined features.
        Args:
            input_df (pl.DataFrame): Dataframe that includes all the feature columns

        Returns:
            pl.DataFrame: Predicted target column values
        """
        feature_df = input_df.select(self.feature_columns)
        predictions = self.model.predict(feature_df)
        return pl.from_numpy(predictions, schema=self.target_columns, orient="col")

    # model_registry = {


#     "nonres_pir_linear": Model(
#         model_type=ModelType.SIMPLE_LINEAR,
#         model_identifier ="nonres_pir_linear",
#         model_params={},
#         data_source_prefix="domain=ind_cqc_filled_posts/dataset=ind_cqc_estimated_missing_ascwds_filled_posts/",
#         target_column
#     ),
# }
