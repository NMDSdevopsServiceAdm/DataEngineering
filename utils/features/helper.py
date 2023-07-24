import re
from typing import List, Dict, Tuple, Set

import pyspark
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler


def filter_records_since_snapshot_date(locations_df, max_snapshot):
    if not max_snapshot:
        return locations_df
    max_snapshot_date = f"{max_snapshot[0]}{max_snapshot[1]:0>2}{max_snapshot[2]:0>2}"
    print(f"max snapshot previously processed: {max_snapshot_date}")
    locations_df = locations_df.withColumn(
        "snapshot_full_date",
        F.concat(
            locations_df.snapshot_year.cast("string"),
            F.lpad(locations_df.snapshot_month.cast("string"), 2, "0"),
            F.lpad(locations_df.snapshot_day.cast("string"), 2, "0"),
        ),
    )
    locations_df = locations_df.where(
        locations_df["snapshot_full_date"] > max_snapshot_date
    )
    locations_df.drop("snapshot_full_date")
    return locations_df


def vectorise_dataframe(
    df: pyspark.sql.DataFrame, list_for_vectorisation: List[str]
) -> pyspark.sql.DataFrame:
    loc_df = VectorAssembler(
        inputCols=list_for_vectorisation, outputCol="features", handleInvalid="skip"
    ).transform(df)
    return loc_df


def get_list_of_distinct_ons_regions(
    df: pyspark.sql.DataFrame, col_name: str
) -> List[str]:
    distinct_regions = df.select(col_name).distinct().dropna().collect()
    dis_regions_list = [str(row.ons_region) for row in distinct_regions]
    return dis_regions_list


def column_expansion_with_dict(
    df: pyspark.sql.DataFrame, col_name: str, lookup_dict: Dict[str, str]
) -> pyspark.sql.DataFrame:
    for key in lookup_dict.keys():
        df = df.withColumn(
            f"{key}",
            F.array_contains(df[f"{col_name}"], lookup_dict[key]).cast("integer"),
        )
    return df


def add_rui_data_data_frame(
    df: pyspark.sql.DataFrame,
    rui_col_name: str,
    lookup_dict: Dict[str, str],
) -> pyspark.sql.DataFrame:
    for key in lookup_dict.keys():
        df = df.withColumn(
            key, (F.col(rui_col_name) == lookup_dict[key]).cast("integer")
        )
    return df


def add_service_count_to_data(
    df: pyspark.sql.DataFrame, new_col_name: str, col_to_check: str
) -> pyspark.sql.DataFrame:
    return df.withColumn(new_col_name, F.size(F.col(col_to_check)))


def format_strings(string: str) -> str:
    no_space = string.replace(" ", "_").lower()
    return re.sub("[^a-z_]", "", no_space)


def explode_column_from_distinct_values(
    df: pyspark.sql.DataFrame, column_name: str, col_prefix: str, col_list_set: Set[str]
) -> Tuple[pyspark.sql.DataFrame, List[str]]:
    col_names = []

    for col in col_list_set:
        clean = col_prefix + format_strings(col)
        col_names.append(clean)

        df = df.withColumn(f"{clean}", (F.col(f"{column_name}") == col).cast("integer"))
    return df, col_names


def add_date_diff_into_df(
    df: pyspark.sql.DataFrame, new_col_name: str, snapshot_date_col: str
) -> pyspark.sql.DataFrame:
    max_d = df.agg(F.max(snapshot_date_col)).first()[0]

    loc_df = df.withColumn(
        new_col_name, F.datediff(F.lit(max_d), F.col(snapshot_date_col))
    )
    return loc_df
