from enum import Enum
from typing import Dict, Any, Union
import polars as pl


from sklearn.linear_model import LinearRegression, Lasso, Ridge


class ModelType(Enum):
    SIMPLE_LINEAR = "simple_linear"
    LASSO = "lasso"
    RIDGE = "ridge"


class Model:

    def __init__(
        self,
        model_type: ModelType,
        model_identifier: str,
        model_params: Dict[str, Any],
        version_parameter_location: str,
        data_source_prefix: str,
        target_column: str | list[str],
        feature_columns: list[str],
    ) -> None:
        self.model_type = model_type
        self.model_identifier = model_identifier
        self.model_params = model_params
        self.version_parameter_location = version_parameter_location
        self.data_source_prefix = data_source_prefix
        self.target_column = target_column
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
        s3_uri = f"s3://{bucket_name}/{self.data_source_prefix}"
        return pl.scan_parquet(s3_uri)

    def create_train_and_test_datasets(self, data: Union[pl.DataFrame, pl.LazyFrame], split_size=0.7) -> tuple[pl.DataFrame, pl.DataFrame]:
        pass




# model_registry = {
#     "nonres_pir_linear": Model(
#         model_type=ModelType.SIMPLE_LINEAR,
#         model_identifier ="nonres_pir_linear",
#         model_params={},
#         data_source_prefix="domain=ind_cqc_filled_posts/dataset=ind_cqc_estimated_missing_ascwds_filled_posts/",
#         target_column
#     ),
# }
