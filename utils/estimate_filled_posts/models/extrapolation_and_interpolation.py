from pyspark.sql import DataFrame, functions as F

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)

# from utils.estimate_filled_posts.models.extrapolation import model_extrapolation
# from utils.estimate_filled_posts.models.interpolation import model_interpolation


def model_extrapolation_and_interpolation(
    df: DataFrame, model_column_name: str
) -> DataFrame:
    # create cols used for both

    # df = model_extrapolation(df, model_column_name)

    # df = model_interpolation(df, model_column_name)

    return df
