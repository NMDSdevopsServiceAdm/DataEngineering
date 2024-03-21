import re
from typing import List, Dict, Tuple, Set

from pyspark.sql import DataFrame, functions as F
from pyspark.ml.feature import VectorAssembler


def vectorise_dataframe(df: DataFrame, list_for_vectorisation: List[str]) -> DataFrame:
    loc_df = VectorAssembler(
        inputCols=list_for_vectorisation, outputCol="features", handleInvalid="skip"
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


def add_rui_data_data_frame(
    df: DataFrame,
    rui_col_name: str,
    lookup_dict: Dict[str, str],
) -> DataFrame:
    for key in lookup_dict.keys():
        df = df.withColumn(
            key, (F.col(rui_col_name) == lookup_dict[key]).cast("integer")
        )
    return df


def add_service_count_to_data(
    df: DataFrame, new_col_name: str, col_to_check: str
) -> DataFrame:
    return df.withColumn(new_col_name, F.size(F.col(col_to_check)))


def format_strings(string: str) -> str:
    no_space = string.replace(" ", "_").lower()
    return re.sub("[^a-z_]", "", no_space)


def explode_column_from_distinct_values(
    df: DataFrame, column_name: str, col_prefix: str, col_list_set: Set[str]
) -> Tuple[DataFrame, List[str]]:
    col_names = []

    for col in col_list_set:
        clean = col_prefix + format_strings(col)
        col_names.append(clean)

        df = df.withColumn(f"{clean}", (F.col(f"{column_name}") == col).cast("integer"))
    return df, col_names


def add_date_diff_into_df(
    df: DataFrame, new_col_name: str, snapshot_date_col: str
) -> DataFrame:
    max_d = df.agg(F.max(snapshot_date_col)).first()[0]

    loc_df = df.withColumn(
        new_col_name, F.datediff(F.lit(max_d), F.col(snapshot_date_col))
    )
    return loc_df
