from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import LongType
from typing import List

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import (
    EstimateFilledPostsSource,
    MainJobRoleLabels,
)
from utils.value_labels.ascwds_worker.ascwds_worker_mainjrid import (
    AscwdsWorkerValueLabelsMainjrid as AscwdsJobRoles,
)
from utils.value_labels.ascwds_worker.ascwds_worker_jobgroup_dictionary import (
    AscwdsWorkerValueLabelsJobGroup,
)

list_of_job_roles_sorted = sorted(list(AscwdsJobRoles.labels_dict.values()))


def aggregate_ascwds_worker_job_roles_per_establishment(
    df: DataFrame, list_of_job_roles: list
) -> DataFrame:
    """
    Aggregates the worker dataset by establishment_id and import date and transforms them into a mapped structure.

    This function aggregates the worker dataset by establishment_id and import date and creates a pivot table that has one column per job role counts.
    Null values are populated with zero before creating a map column that contains job role names and counts.
    The individual job role count columns are then dropped.

    Args:
        df (DataFrame): A dataframe containing cleaned ASC-WDS worker data.
        list_of_job_roles (list): A list containing the ASC-WDS job role.

    Returns:
        DataFrame: A dataframe with unique establishmentid and import date.
    """
    df = (
        df.groupBy(
            F.col(IndCQC.establishment_id), F.col(IndCQC.ascwds_worker_import_date)
        )
        .pivot(IndCQC.main_job_role_clean_labelled, list_of_job_roles)
        .count()
    )
    df = df.na.fill(0, subset=list_of_job_roles)

    df = create_map_column(df, list_of_job_roles, IndCQC.ascwds_job_role_counts)

    return df


def create_map_column(
    df: DataFrame,
    column_list: List[str],
    new_col_name: str,
    drop_original_columns: bool = True,
) -> DataFrame:
    """
    Creates a new map column in a DataFrame by mapping the specified columns to their values.

    This function generates a map column where the keys are the column names and the values are the corresponding column values.
    The column list is sorted alphabetically before creating the map.
    The original columns can be optionally dropped after the map column is created.

    Args:
        df (DataFrame): DataFrame containing the list of columns to be mapped.
        column_list (List[str]): List of column names to be mapped.
        new_col_name (str): Name of the new mapped column to be added.
        drop_original_columns (bool, optional): If True, drops the original columns after creating
            the map column. Defaults to True.

    Returns:
        DataFrame: A DataFrame with the new map column added and optionally the original columns dropped.
    """
    sorted_column_list = sorted(column_list)

    map_columns = F.create_map(
        *[x for col in sorted_column_list for x in (F.lit(col), F.col(col))]
    )
    df = df.withColumn(new_col_name, map_columns)

    if drop_original_columns:
        df = df.drop(*sorted_column_list)

    return df


def merge_dataframes(
    estimated_filled_posts_df: DataFrame,
    aggregated_job_roles_per_establishment_df: DataFrame,
) -> DataFrame:
    """
    Join the ASC-WDS job role count column from the aggregated worker file into the estimated filled post DataFrame, matched on establishment_id and import_date.

    Args:
        estimated_filled_posts_df (DataFrame): A dataframe containing estimated filled posts at workplace level.
        aggregated_job_roles_per_establishment_df (DataFrame): ASC-WDS job role breakdown dataframe aggregated at workplace level.

    Returns:
        DataFrame: The estimated filled post DataFrame with the job role count map column joined in.
    """

    merged_df = (
        estimated_filled_posts_df.join(
            aggregated_job_roles_per_establishment_df,
            (
                estimated_filled_posts_df[IndCQC.establishment_id]
                == aggregated_job_roles_per_establishment_df[IndCQC.establishment_id]
            )
            & (
                estimated_filled_posts_df[IndCQC.ascwds_workplace_import_date]
                == aggregated_job_roles_per_establishment_df[
                    IndCQC.ascwds_worker_import_date
                ]
            ),
            "left",
        )
        .drop(aggregated_job_roles_per_establishment_df[IndCQC.establishment_id])
        .drop(
            aggregated_job_roles_per_establishment_df[IndCQC.ascwds_worker_import_date]
        )
    )

    return merged_df


def remove_ascwds_job_role_count_when_estimate_filled_posts_source_not_ascwds(
    df: DataFrame,
) -> DataFrame:
    """
    Changes ascwds job role counts column to null in the following cases.

    When estimate filled posts source is not 'ascwds_pir_merged' and
    estimate filled posts is not equal to ascwds filled posts dedup clean.
    This is to ensure that we're only using ascwds job role data when ascwds data has
    been used for estimated filled posts.

    Args:
        df (DataFrame): The estimated filled post by job role DataFrame.

    Returns:
        DataFrame: The estimated filled post by job role DataFrame with the ascwds job role count map column filtered.
    """

    return df.withColumn(
        IndCQC.ascwds_job_role_counts,
        F.when(
            (
                F.col(IndCQC.estimate_filled_posts_source)
                == F.lit(EstimateFilledPostsSource.ascwds_pir_merged)
            )
            & (
                F.col(IndCQC.estimate_filled_posts)
                == F.col(IndCQC.ascwds_filled_posts_dedup_clean)
            ),
            F.col(IndCQC.ascwds_job_role_counts),
        ).otherwise(F.lit(None)),
    )


def transform_job_role_count_map_to_ratios_map(
    df: DataFrame,
    count_map_column_name: str,
    ratio_map_column_name: str,
) -> DataFrame:
    """
    Transform a count map column into a ratio map column.

    Adds a column to hold the total of values from the given count map.
    Adds a column with the ratio map.
    Drops the column with the total of values from given count map.

    Args:
        df (DataFrame): A dataframe containing a job role count map at workplace level.
        count_map_column_name (str): A map column of type any:long.
        ratio_map_column_name (str): The name to give to the ratio map column.

    Returns:
        DataFrame: The estimated filled post by job role DataFrame with the job role ratio map column joined in.
    """

    temp_total_count_of_worker_records = "temp_total_count_of_worker_records"
    df = calculate_total_sum_of_values_in_a_map_column(
        df,
        count_map_column_name,
        temp_total_count_of_worker_records,
    )

    df = create_ratios_map_from_count_map_and_total(
        df,
        count_map_column_name,
        temp_total_count_of_worker_records,
        ratio_map_column_name,
    )

    return df.drop(temp_total_count_of_worker_records)


def calculate_total_sum_of_values_in_a_map_column(
    df: DataFrame,
    map_column_name: str,
    total_sum_column_name: str,
) -> DataFrame:
    """
    Adds a column which contains the total of values from a given map column of type any:long.

    Args:
        df (DataFrame): A dataframe containing a count map.
        map_column_name (str): A map column of type any:long.
        total_sum_column_name (str): The name to give to the total column being added.

    Returns:
        DataFrame: The estimated filled post by job role DataFrame with a column for total of map values added.
    """

    df = df.withColumn(
        total_sum_column_name,
        F.aggregate(
            F.map_values(F.col(map_column_name)),
            F.lit(0).cast(LongType()),
            lambda a, b: a + b,
        ),
    )

    return df


def create_ratios_map_from_count_map_and_total(
    df: DataFrame,
    count_map_column_name: str,
    total_sum_column_name: str,
    ratio_map_column_name: str,
) -> DataFrame:
    """
    Adds a column which contains a ratio map.

    Takes a map column and a column with the total of values from that map column.
    Makes another map with the same keys as the given map but values as each value in the given map
    as a percentage of total column.

    Args:
        df (DataFrame): A dataframe containing a job role count map at workplace level.
        count_map_column_name (str): A map column.
        total_sum_column_name (str): A column with the total of values from the count_map_column.
        ratio_map_column_name (str): The name to give to the new ratio map column.

    Returns:
        DataFrame: The estimated filled post by job role DataFrame with the job role ratio map column joined in.
    """

    df = df.withColumn(
        ratio_map_column_name,
        F.map_from_arrays(
            F.map_keys(F.col(count_map_column_name)),
            F.transform(
                F.map_values(F.col(count_map_column_name)),
                lambda v: v / F.col(total_sum_column_name),
            ),
        ),
    )

    return df


def count_registered_manager_names(df: DataFrame) -> DataFrame:
    """
    Adds a column with a count of elements within list of registered manager names.

    This function uses the size method to count elements in list of strings. This method
    returns the count, including null elements within a list, when list partially populated.
    It returns 0 when list is empty. It returns -1 when row is null.
    Therefore, after the counting, this function recodes values of -1 to 0.

    Args:
        df (DataFrame): A dataframe containing list of registered manager names.

    Returns:
        DataFrame: A dataframe with count of elements in list of registered manager names.
    """

    df = df.withColumn(
        IndCQC.registered_manager_count, F.size(F.col(IndCQC.registered_manager_names))
    )

    df = df.withColumn(
        IndCQC.registered_manager_count,
        F.when(F.col(IndCQC.registered_manager_count) == -1, F.lit(0)).otherwise(
            F.col(IndCQC.registered_manager_count)
        ),
    )

    return df


def sum_job_role_count_split_by_service(
    df: DataFrame, list_of_job_roles: list
) -> DataFrame:
    """
    Takes the mapped column of job counts from the dataframes and does a sum for each
    job role for each partition of service type. This is done through a combination of
    explode, group by and left join

    Args:
        df (DataFrame): A dataframe containing the estimated CQC filled posts data with job role counts.
        list_of_job_roles (list): A list containing the ASC-WDS job role.

    Returns:
        DataFrame: A dataframe with unique establishmentid and import date.
    """
    df_explode = df.select(
        IndCQC.primary_service_type, F.explode(IndCQC.ascwds_job_role_counts)
    )

    df_explode_grouped = (
        df_explode.groupBy(IndCQC.primary_service_type)
        .pivot("key", list_of_job_roles)
        .sum("value")
    )

    df_explode_grouped_with_map_column = create_map_column(
        df_explode_grouped,
        list_of_job_roles,
        IndCQC.ascwds_job_role_counts_by_primary_service,
    )

    df_result = df.join(
        df_explode_grouped_with_map_column,
        IndCQC.primary_service_type,
        "left",
    )

    return df_result


def unpack_mapped_column(df: DataFrame, column_name: str) -> DataFrame:
    """
    Unpacks a MapType column in a DataFrame into separate columns (sorted alphabetically), with keys as column names and values as row values.

    Args:
        df (DataFrame): A PySpark DataFrame containing a MapType column.
        column_name (str): The name of the MapType column to unpack.

    Returns:
        DataFrame: A DataFrame with the map column expanded into multiple columns, sorted alphabetically by key.
    """

    df_keys = df.select(F.explode(F.map_keys(F.col(column_name)))).distinct()

    list_keys = sorted([row[0] for row in df_keys.collect()])

    column_of_keys = [
        F.col(column_name).getItem(key).alias(str(key)) for key in list_keys
    ]

    result_df = df.select("*", *column_of_keys)

    return result_df


def create_estimate_filled_posts_by_job_role_map_column(
    df: DataFrame,
) -> DataFrame:
    """
    Creates a map column of estimated filled posts by job role.

    Takes the ascwds_job_role_ratios_merged column and multiplies each ratio by estimate_filled_posts.
    The results are mapped to a dictionary with the same keys as ascwds_job_role_ratios_merged.

    Args:
        df (DataFrame): A dataframe which contains a job role ratio map column and an estimated filled post column.

    Returns:
        DataFrame: A dataframe with an additional map column of estimated filled posts by job role.

    """
    df = df.withColumn(
        IndCQC.estimate_filled_posts_by_job_role,
        F.map_from_arrays(
            F.map_keys(F.col(IndCQC.ascwds_job_role_ratios_merged)),
            F.transform(
                F.map_values(F.col(IndCQC.ascwds_job_role_ratios_merged)),
                lambda v: v * F.col(IndCQC.estimate_filled_posts),
            ),
        ),
    )

    return df


def pivot_interpolated_job_role_ratios(
    df: DataFrame,
) -> DataFrame:
    """
    Pivots the job role ratio interpolated mapped column so that the key are column names

    Args:
        df (DataFrame): A dataframe which contains ascwds_job_role_ratios_interpolated mapped column.

    Returns:
        DataFrame: A dataframe with the mapped column pivot when grouped by location id and unix time
    """
    df_result = (
        df.groupBy(IndCQC.location_id, IndCQC.unix_time)
        .pivot(IndCQC.main_job_role_clean_labelled)
        .agg(F.first(IndCQC.ascwds_job_role_ratios_interpolated, ignorenulls=False))
    )

    return df_result


def convert_map_with_all_null_values_to_null(df: DataFrame) -> DataFrame:
    """
    convert a map with only null values to be just a null not in map format

    Args:
        df (DataFrame): A dataframe which contains ascwds_job_role_ratios_interpolated mapped column.

    Returns:
        DataFrame: A dataframe with the aascwds_job_role_ratios_interpolated with null values instead of map records with only null values.
    """

    df_result = df.withColumn(
        IndCQC.ascwds_job_role_ratios_interpolated,
        F.when(
            F.size(
                F.filter(
                    F.map_values(F.col(IndCQC.ascwds_job_role_ratios_interpolated)),
                    lambda x: ~F.isnull(x),
                )
            )
            == 0,
            F.lit(None),
        ).otherwise(F.col(IndCQC.ascwds_job_role_ratios_interpolated)),
    )

    return df_result


def calculate_difference_between_estimate_and_cqc_registered_managers(
    df: DataFrame,
) -> DataFrame:
    """
    Calculates count of CQC registered managers minus our estimate of registered managers.

    A positive value is when CQC have recorded more registered managers than we have estimated.
    A negative value is when we have estimated more registered managers than CQC have recorded.
    CQC have the official count of registered managers. Our estimate is based on records in ASC-WDS.

    Args:
        df (DataFrame): A dataframe which contains filled post estimates by job role and a count of registered managers from CQC.

    Returns:
        DataFrame: A dataframe with an additional column showing count from CQC minus our estimate of registered managers.
    """
    df = df.withColumn(
        IndCQC.difference_between_estimate_and_cqc_registered_managers,
        F.col(IndCQC.registered_manager_count)
        - F.col(MainJobRoleLabels.registered_manager),
    )

    return df


def calculate_job_group_sum_from_job_role_map_column(
    df: DataFrame, job_role_level_map_column: str, new_job_group_map_column_name: str
) -> DataFrame:
    """
    Sums the values from a job role map column up to job group level.

    This function takes a job role level map column, explodes the key/value pairs into rows as two columns,
    adds a new column to show the job group each job role belongs to,
    sums the value column by job group and pivots it into job group columns,
    then packages those columns into a new map column.

    Args:
        df (DataFrame): A dataframe with a job role map column.
        job_role_level_map_column (str): The name of the job role map column you want to sum.
        new_job_group_map_column_name (str): The name to give the job group map column.

    Returns:
        DataFrame: A dataframe with an additional column showing values summed to job group.

    """

    df_exploded = df.select(
        IndCQC.location_id,
        IndCQC.unix_time,
        F.explode(job_role_level_map_column).alias("job_role", "value"),
    )

    df_exploded = df_exploded.withColumn("job_group", F.col("job_role"))
    df_exploded = df_exploded.replace(
        AscwdsWorkerValueLabelsJobGroup.job_role_to_job_group_dict,
        subset="job_group",
    )

    list_of_job_groups = list(
        set(AscwdsWorkerValueLabelsJobGroup.job_role_to_job_group_dict.values())
    )
    df_exploded = (
        df_exploded.groupBy(IndCQC.location_id, IndCQC.unix_time)
        .pivot("job_group")
        .agg(F.sum("value"))
        .na.fill(0, subset=list_of_job_groups)
    )

    df_exploded = df_exploded.withColumn(
        new_job_group_map_column_name,
        create_map_column(list_of_job_groups),
    )

    df_exploded = df_exploded.drop(*list_of_job_groups)

    df = df.join(df_exploded, on=[IndCQC.location_id, IndCQC.unix_time], how="left")

    df.show()

    return df
