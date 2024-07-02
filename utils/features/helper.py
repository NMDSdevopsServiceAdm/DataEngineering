from typing import List, Dict

from pyspark.sql import DataFrame, functions as F
from pyspark.ml.feature import VectorAssembler

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)


def vectorise_dataframe(df: DataFrame, list_for_vectorisation: List[str]) -> DataFrame:
    loc_df = VectorAssembler(
        inputCols=list_for_vectorisation,
        outputCol=IndCQC.features,
        handleInvalid="skip",
    ).transform(df)
    return loc_df


def column_expansion_with_dict(
    df: DataFrame, col_name: str, lookup_dict: Dict[str, str]
) -> DataFrame:
    for key in lookup_dict.keys():
        df = df.withColumn(
            f"{key}",
            F.array_contains(df[f"{col_name}"], lookup_dict[key]).cast("integer"),
        )
    return df


def convert_categorical_variable_to_binary_variables_based_on_a_dictionary(
    df: DataFrame,
    categorical_col_name: str,
    lookup_dict: Dict[str, str],
) -> DataFrame:
    for key in lookup_dict.keys():
        df = df.withColumn(
            key, (F.col(categorical_col_name) == lookup_dict[key]).cast("integer")
        )
    return df


def add_service_count_to_data(
    df: DataFrame, new_col_name: str, col_to_check: str
) -> DataFrame:
    return df.withColumn(new_col_name, F.size(F.col(col_to_check)))


def add_date_diff_into_df(
    df: DataFrame, new_col_name: str, import_date_col: str
) -> DataFrame:
    max_d = df.agg(F.max(import_date_col)).first()[0]

    loc_df = df.withColumn(
        new_col_name, F.datediff(F.lit(max_d), F.col(import_date_col))
    )
    return loc_df


def add_time_open_into_df(df: DataFrame, new_col_name: str) -> DataFrame:
    loc_df = df.withColumn(
        new_col_name,
        F.datediff(
            F.col(IndCQC.cqc_location_import_date),
            F.col(IndCQC.imputed_registration_date),
        ),
    )
    return loc_df
