from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import MapType, FloatType
from typing import List

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import (
    EstimateFilledPostsSource,
    JobGroupLabels,
    MainJobRoleLabels,
)
from utils.value_labels.ascwds_worker.ascwds_worker_mainjrid import (
    AscwdsWorkerValueLabelsMainjrid as AscwdsJobRoles,
)
from utils.value_labels.ascwds_worker.ascwds_worker_jobgroup_dictionary import (
    AscwdsWorkerValueLabelsJobGroup,
)

from utils.value_labels.ascwds_worker.ascwds_worker_jobgroup_dictionary import (
    AscwdsWorkerValueLabelsJobGroup,
)
from utils.column_values.categorical_column_values import (
    JobGroupLabels,
    MainJobRoleLabels,
)


list_of_job_roles_sorted = sorted(list(AscwdsJobRoles.labels_dict.values()))
list_of_job_groups_sorted = sorted(
    list(set(AscwdsWorkerValueLabelsJobGroup.job_role_to_job_group_dict.values()))
)
list_of_non_rm_managers = sorted(
    [
        job_role
        for job_role, job_group in AscwdsWorkerValueLabelsJobGroup.job_role_to_job_group_dict.items()
        if job_group == JobGroupLabels.managers
        and job_role != MainJobRoleLabels.registered_manager
    ]
)


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
        list_of_job_roles (list): A list containing the ASC-WDS job roles.

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
        count_map_column_name (str): A map column of type string:number.
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
    Adds a column which contains the total of values from a given map column of type string:number.

    Args:
        df (DataFrame): A dataframe containing a count map.
        map_column_name (str): A map column of type string:number.
        total_sum_column_name (str): The name to give to the total column being added.

    Returns:
        DataFrame: The estimated filled post by job role DataFrame with a column for total of map values added.
    """

    map_type: MapType = df.schema[map_column_name].dataType
    df = df.withColumn(
        total_sum_column_name,
        F.aggregate(
            F.map_values(F.col(map_column_name)),
            F.lit(0).cast(map_type.valueType),
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
    Updates the 'registered_manager_count' column with a binary indicator of whether
    a location has at least one registered manager name in the 'registered_manager_names' column.

    This MVP logic sets the count to:
      - 1 if the 'registered_manager_names' array is non-empty (i.e., has >= 1 name).
      - 0 if 'registered_manager_names' is None or an empty list.

    This approach aligns with historical Excel structures where each location
    was effectively recorded with at most one registered manager.

    Args:
        df (DataFrame): A Spark DataFrame containing the column 'registered_manager_names'
            (ArrayType(StringType())). Each row represents a location's registered manager name data.

    Returns:
        DataFrame: A Spark DataFrame with an additional or updated 'registered_manager_count' column
            set to 1 if there is at least one name, or 0 otherwise.
    """
    df = df.withColumn(
        IndCQC.registered_manager_count,
        F.when(
            (F.col(IndCQC.registered_manager_names).isNull())
            | (F.size(F.col(IndCQC.registered_manager_names)) == 0),
            F.lit(0),
        ).otherwise(F.lit(1)),
    )
    return df


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


def recalculate_managerial_filled_posts(
    df: DataFrame, non_rm_managers: List
) -> DataFrame:
    """
    Adjusts estimated filled posts for non-Registered Manager (non-RM) managerial roles.

    Adjusts estimated filled posts for non-Registered Manager (non-RM) managerial roles based on
    the discrepancy between previously estimated and known Registered Manager values.

    In a prior step, the difference between estimated and actual Registered Managers was calculated
    ('difference_between_estimate_and_cqc_registered_managers'). This function proportionally
    redistributes that difference across all non-RM managerial roles, using the existing estimated
    proportions in 'proportion_of_non_rm_managerial_estimated_filled_posts_by_role'.

    This ensures the total number of estimated managerial filled posts remains consistent after
    correcting for RM discrepancies, except where doing so would push a job role's count below zero,
    in which case it is set at a minimim of 0.

    Args:
        df (DataFrame): A Spark DataFrame containing:
            - Initial estimates for each non-RM managerial role
            - 'IndCQC.proportion_of_non_rm_managerial_estimated_filled_posts_by_role' (Map of role proportions)
            - 'IndCQC.difference_between_estimate_and_cqc_registered_managers' (Float)
        non_rm_managers (List): A list of column names representing non-RM managerial roles to adjust.

    Returns:
        DataFrame: A new DataFrame with updated estimates for non-RM managerial roles.
    """
    updated_columns = {
        role: F.greatest(
            F.lit(0.0),
            F.col(role)
            + (
                F.col(
                    IndCQC.proportion_of_non_rm_managerial_estimated_filled_posts_by_role
                ).getItem(role)
                * F.col(IndCQC.difference_between_estimate_and_cqc_registered_managers)
            ),
        ).alias(role)
        for role in non_rm_managers
    }

    final_columns = [updated_columns.get(col, F.col(col)) for col in df.columns]

    df = df.select(*final_columns)

    return df


def calculate_sum_and_proportion_split_of_non_rm_managerial_estimate_posts(
    df: DataFrame,
) -> DataFrame:
    """
    A function to caclulate both the total number of estimated non rm managerial filled posts, and also to calculate the proportion of non rm managerial
    estimated filled posts per location.

    Args:
        df (DataFrame): A dataframe which contains estimates of filled posts per job role.
    Returns:
        DataFrame: A dataframe with an additional column for the sum of non registered manager estimated filled posts
        and a map column of non registered manager estimated post proportions split per role.
    """

    non_rm_managers = sorted(
        [
            job_role
            for job_role, job_group in AscwdsWorkerValueLabelsJobGroup.job_role_to_job_group_dict.items()
            if job_group == JobGroupLabels.managers
            and job_role != MainJobRoleLabels.registered_manager
        ]
    )

    temp_suffix: str = "_temp"
    non_rm_managers_temporary = sorted(
        [job_role + temp_suffix for job_role in non_rm_managers]
    )

    df = df.select(
        "*",
        *[
            F.col(role).alias(temp)
            for role, temp in zip(non_rm_managers, non_rm_managers_temporary)
        ]
    )

    df = df.withColumn(
        IndCQC.sum_non_rm_managerial_estimated_filled_posts,
        sum(F.col(new_col) for new_col in non_rm_managers_temporary),
    )

    total = F.col(IndCQC.sum_non_rm_managerial_estimated_filled_posts)
    proportions = [
        F.when(total == 0.0, F.lit(1.0 / len(non_rm_managers_temporary)))
        .when(F.col(col) == 0.0, F.lit(0.0))
        .otherwise(F.col(col) / total)
        for col in non_rm_managers_temporary
    ]

    df = df.withColumn(
        IndCQC.proportion_of_non_rm_managerial_estimated_filled_posts_by_role,
        F.map_from_arrays(
            F.array(*[F.lit(k) for k in non_rm_managers]), F.array(*proportions)
        ),
    )

    df = df.drop(*non_rm_managers_temporary)

    return df


def pivot_job_role_column(
    df: DataFrame,
    grouping_columns: List[str],
    aggregation_column: str,
) -> DataFrame:
    """
    Transforms the input DataFrame by pivoting unique job role labels into separate columns,
    aggregating values from the specified column.

    This function groups the data by the provided 'grouping_columns', then performs a pivot operation
    on the 'main_job_role_clean_labelled' column—creating one column per unique job role. For each
    group, it selects the first non-null value of the specified 'aggregation_column' for each job role.

    In this context, "pivoting" means turning distinct values from one column (the job role labels)
    into separate columns, with each column containing values from another column (the
    'aggregation_column') for those specific roles.

    Args:
        df (DataFrame): The DataFrame containing the job role column, grouping columns and aggregation column.
        grouping_columns (List[str]): Columns to group by before pivoting.
        aggregation_column (str): The column from which to extract values  during aggregation.

    Returns:
        DataFrame: A pivoted DataFrame with one column per job role label and values
        aggregated from the specified column.
    """
    df_result = (
        df.groupBy(grouping_columns)
        .pivot(IndCQC.main_job_role_clean_labelled)
        .agg(F.first(aggregation_column, ignorenulls=False))
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
        F.col(MainJobRoleLabels.registered_manager)
        - F.col(IndCQC.registered_manager_count),
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
    temp_value_column = "value"
    df_exploded = df.select(
        IndCQC.location_id,
        IndCQC.unix_time,
        F.explode(job_role_level_map_column).alias(
            IndCQC.main_job_role_clean_labelled, temp_value_column
        ),
    )

    df_exploded = df_exploded.withColumnRenamed(
        IndCQC.main_job_role_clean_labelled, IndCQC.main_job_group_labelled
    ).replace(
        AscwdsWorkerValueLabelsJobGroup.job_role_to_job_group_dict,
        subset=IndCQC.main_job_group_labelled,
    )

    df_exploded = (
        df_exploded.groupBy(IndCQC.location_id, IndCQC.unix_time)
        .pivot(IndCQC.main_job_group_labelled)
        .agg(F.sum(temp_value_column))
        .na.fill(0, subset=list_of_job_groups_sorted)
    )

    df_exploded = create_map_column(
        df_exploded, list_of_job_groups_sorted, new_job_group_map_column_name
    )

    df = df.join(df_exploded, on=[IndCQC.location_id, IndCQC.unix_time], how="left")

    return df


def apply_quality_filters_to_ascwds_job_role_data(
    df: DataFrame,
) -> DataFrame:
    """
    This function calls each of the asc-wds job role filtering functions.

    The first filter copies ascwds_job_role_counts into ascwds_job_role_counts_filtered when passed the quality check.
    The second filter sets values in the ascwds_job_role_counts_filtered column to null if they fail the quality check.

    Args:
        df (DataFrame): A dataframe with a job role map column and job group map column.

    Returns:
        DataFrame: A dataframe with an additional column of filtered job role counts.
    """

    df = filter_ascwds_job_role_map_when_direct_care_or_managers_plus_regulated_professions_greater_or_equal_to_one(
        df
    )

    df = filter_ascwds_job_role_count_map_when_job_group_ratios_outside_percentile_boundaries(
        df,
        lower_percentile_limit=0.001,
        upper_percentile_limit=0.999,
    )

    return df


def filter_ascwds_job_role_map_when_direct_care_or_managers_plus_regulated_professions_greater_or_equal_to_one(
    df: DataFrame,
) -> DataFrame:
    """
    Copies ascwds_job_role_counts into new column ascwds_job_role_counts_filtered when criteria is met.

    The criteria is:
        worker_records_bounded must be >= 1 AND
        (Job groups direct_care must be >= 1 OR managers + regulated_professions must be >= 1)

    Args:
        df (DataFrame): A dataframe with a job role map column and job group map column.

    Returns:
        DataFrame: A dataframe with an additional column of filtered job role counts.
    """

    df = df.withColumn(
        IndCQC.ascwds_job_role_counts_filtered,
        F.when(
            (F.col(IndCQC.worker_records_bounded) >= 1)
            & (
                (F.col(IndCQC.ascwds_job_group_counts)[JobGroupLabels.direct_care] >= 1)
                | (
                    (
                        F.col(IndCQC.ascwds_job_group_counts)[JobGroupLabels.managers]
                        + F.col(IndCQC.ascwds_job_group_counts)[
                            JobGroupLabels.regulated_professions
                        ]
                    )
                    >= 1
                )
            ),
            F.col(IndCQC.ascwds_job_role_counts),
        ).otherwise(None),
    )

    return df


def filter_ascwds_job_role_count_map_when_job_group_ratios_outside_percentile_boundaries(
    df: DataFrame,
    lower_percentile_limit: float,
    upper_percentile_limit: float,
) -> DataFrame:
    """
    Sets ascwds_job_role_counts_filtered to null when ascwds_job_group_ratios outside of given boundaries.

    The boundaries are given as percentiles at which the job group ratio value at that percentile becomes the limit.

    The boundaries are:
        direct_care ratio > lower_percentile_limit and < upper_percentile_limit
        managers ratio < upper_percentile_limit
        regulated_professions ratio < upper_percentile_limit
        other ratio < upper_percentile_limit

    Args:
        df (DataFrame): A dataframe with a job role count map column and job group ratio map column.
        lower_percentile_limit (float): The lower percentile limit.
        upper_percentile_limit (float): The upper percentile limit.

    Returns:
        DataFrame: A dataframe with an additional column of filtered job role counts.
    """

    original_column_ordering = df.columns

    temp_job_group = "temp_job_group"
    temp_job_group_ratio = "temp_job_group_ratio"
    df_exploded = df.select(
        IndCQC.primary_service_type,
        F.explode(IndCQC.ascwds_job_group_ratios).alias(
            temp_job_group, temp_job_group_ratio
        ),
    )

    percentile_boundaries = "percentile_boundaries"
    df_exploded = df_exploded.groupBy(IndCQC.primary_service_type, temp_job_group).agg(
        F.percentile_approx(
            temp_job_group_ratio,
            (
                lower_percentile_limit,
                upper_percentile_limit,
            ),
        ).alias(percentile_boundaries)
    )

    df_exploded = (
        df_exploded.groupBy(IndCQC.primary_service_type)
        .pivot(temp_job_group)
        .agg(F.first(F.col(percentile_boundaries)))
    )

    df_exploded = create_map_column(
        df_exploded, list_of_job_groups_sorted, percentile_boundaries
    )

    df = df.join(df_exploded, on=IndCQC.primary_service_type, how="left")

    job_group_ratios_column = F.col(IndCQC.ascwds_job_group_ratios)
    percentile_boundary_column = F.col(percentile_boundaries)
    direct_care_ratio = job_group_ratios_column[JobGroupLabels.direct_care]
    direct_care_lower_limit = percentile_boundary_column[JobGroupLabels.direct_care][0]
    direct_care_upper_limit = percentile_boundary_column[JobGroupLabels.direct_care][1]
    managers_ratio = job_group_ratios_column[JobGroupLabels.managers]
    managers_upper_limit = percentile_boundary_column[JobGroupLabels.managers][1]
    regulated_professions_ratio = job_group_ratios_column[
        JobGroupLabels.regulated_professions
    ]
    regulated_professions_upper_limit = percentile_boundary_column[
        JobGroupLabels.regulated_professions
    ][1]
    other_ratio = job_group_ratios_column[JobGroupLabels.other]
    other_upper_limit = percentile_boundary_column[JobGroupLabels.other][1]
    df = df.withColumn(
        IndCQC.ascwds_job_role_counts_filtered,
        F.when(
            (direct_care_ratio > direct_care_lower_limit)
            & (direct_care_ratio < direct_care_upper_limit)
            & (managers_ratio < managers_upper_limit)
            & (regulated_professions_ratio < regulated_professions_upper_limit)
            & (other_ratio < other_upper_limit),
            F.col(IndCQC.ascwds_job_role_counts_filtered),
        ).otherwise(None),
    )

    df = df.select(original_column_ordering)

    return df


def transform_imputed_job_role_ratios_to_counts(
    df: DataFrame,
) -> DataFrame:
    """
    Multiplies values in imputed_ascwds_job_role_ratios dict by estimated filled posts.

    This function transforms the values in imputed_ascwds_job_role_ratios dict by multiplying
    each value by estimated filled posts. The results are copied into a new dict column.

    Args:
        df (DataFrame): A dataframe with an imputed job role ratios column.

    Returns:
        DataFrame: A dataframe with an additional column of imputed job role counts.
    """

    df = df.withColumn(
        IndCQC.imputed_ascwds_job_role_counts,
        F.map_from_arrays(
            F.map_keys(F.col(IndCQC.imputed_ascwds_job_role_ratios)),
            F.transform(
                F.map_values(F.col(IndCQC.imputed_ascwds_job_role_ratios)),
                lambda v: v * F.col(IndCQC.estimate_filled_posts),
            ),
        ),
    )

    return df


def overwrite_registered_manager_estimate_with_cqc_count(df: DataFrame) -> DataFrame:
    """
    This function overwrites our estimate of registered managers with the count from cqc data.

    Args:
        df (DataFrame): A dataframe with registered manager estimates from asc-wds and counts from cqc data.

    Returns:
        DataFrame: A dataframe with registered manager estimates overwritten by cqc counts.
    """

    df = df.withColumn(
        MainJobRoleLabels.registered_manager,
        F.col(IndCQC.registered_manager_count).cast(FloatType()),
    )

    return df


def recalculate_total_filled_posts(df: DataFrame, list_of_job_roles: list) -> DataFrame:
    """
    Created a filled_posts column which represents the total number of filled posts per location_id based on job role breakdown.

    Args:
        df (DataFrame): A dataframe with individual job role breakdown.
        list_of_job_roles (list): A list containing the ASC-WDS job roles.

    Returns:
        DataFrame: A dataframe with a filled_posts column which is the sum of every individual job role per location_id.

    """

    df_result = df.withColumn(
        IndCQC.estimate_filled_posts_from_all_job_roles,
        sum([F.col(job_role) for job_role in list_of_job_roles]),
    )

    return df_result


def combine_interpolated_and_extrapolated_job_role_ratios(df: DataFrame) -> DataFrame:
    """
    Coalesce the filtered, interpolated and extrapolated asc-wds job role ratio columns into one new column.

    Args:
        df (DataFrame): A dataframe with filtered, interpolated and extrapolated asc-wds job role ratio columns.

    Returns:
        DataFrame: A dataframe with a new column called ascwds_job_role_ratios_interpolated_and_extrapolated.
    """

    df = df.withColumn(
        IndCQC.imputed_ascwds_job_role_ratios,
        F.coalesce(
            IndCQC.ascwds_job_role_ratios_filtered,
            IndCQC.ascwds_job_role_ratios_interpolated,
            IndCQC.ascwds_job_role_ratios_extrapolated,
        ),
    )

    return df


def calculate_difference_between_estimate_filled_posts_and_estimate_filled_posts_from_all_job_roles(
    df: DataFrame,
) -> DataFrame:
    """
    Calculates the difference between IndCQC.estimate_filled_posts and IndCQC.estimate_filled_posts_from_all_job_roles.

    IndCQC.estimate_filled_posts is before the breaking down the estimate into job roles.
    IndCQC.estimate_filled_posts_from_all_job_roles is the sum of estimates by job role.
    These are not expected to be equal as we overwrite our estimate of registered managers with the count from cqc data.
    E.g. if we estimate there are no mangers of any kind at a location, then we add the count from cqc, the sum of all job roles will be
    that much higher than the original overall estimate of filled posts at that location.

    Args:
        df (DataFrame): A dataframe with IndCQC.estimate_filled_posts and IndCQC.estimate_filled_posts_from_all_job_roles columns.

    Returns:
        DataFrame: A dataframe with a new column IndCQC.difference_between_estimate_filled_posts_and_estimate_filled_posts_from_all_job_roles.
    """

    df = df.withColumn(
        IndCQC.difference_between_estimate_filled_posts_and_estimate_filled_posts_from_all_job_roles,
        F.round(
            (
                F.col(IndCQC.estimate_filled_posts_from_all_job_roles)
                - F.col(IndCQC.estimate_filled_posts)
            ),
            4,
        ),
    )

    return df


def create_estimate_filled_posts_job_group_columns(
    df: DataFrame, job_role_to_job_group_dict: dict
) -> DataFrame:
    """
    Creates estimate filled posts job group columns by summing job roles.

    Args:
        df (DataFrame): A dataframe with job role estimate filled posts columns.
        job_role_to_job_group_dict (dict): A dictitonary with keys as job roles and values as list of job roles.

    Returns:
        DataFrame: A dataframe with job group estimate filled posts columns.
    """

    job_group_to_job_role_dict: dict = {}
    for job_role, job_group in job_role_to_job_group_dict.items():
        if job_group not in job_group_to_job_role_dict:
            job_group_to_job_role_dict[job_group] = []
        job_group_to_job_role_dict[job_group].append(job_role)

    for (
        job_group,
        job_roles,
    ) in job_group_to_job_role_dict.items():
        df = df.withColumn(
            job_group,
            sum(F.col(role) for role in job_roles),
        )

    return df


def create_job_role_estimates_data_validation_columns(df: DataFrame) -> DataFrame:
    """
    Creates new columns for checking the job role estimates data. These are:
        - National job group percentage split of estimated filled posts.
        - National care worker role as percentage of overall estimated filled posts.

    Args:
        df (DataFrame): A dataframe with job role and job group estimate filled posts columns.

    Returns:
        DataFrame: A dataframe with additional columns for checking the job role estimates data.
    """
    return df
