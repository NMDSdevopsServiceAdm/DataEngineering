from pyspark.sql import DataFrame, functions as F

from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)


def count_job_role_per_establishment(df: DataFrame) -> DataFrame:
    """
    Counts the number of rows per establishmentid, importdate and main job role.

    This function groups the ASC-WDS worker dataset by establishmentid, importdate and main job role
    and adds a column with the count of rows per group.
    Duplicate rows by establishmentid, importdate and main job role are removed.

    Args:
        df (DataFrame): A dataframe containing cleaned ASC-WDS worker data.

    Returns:
        DataFrame: A dataframe with unique establishmentid, importdate and main job role and row count.
    """

    df = df.groupBy(
        F.col(AWKClean.establishment_id),
        F.col(AWKClean.ascwds_worker_import_date),
        F.col(AWKClean.main_job_role_clean_labelled),
    ).agg(
        F.count(F.col(AWKClean.main_job_role_clean_labelled)).alias(
            AWKClean.ascwds_main_job_role_counts
        )
    )
    return df


def convert_job_role_count_to_job_role_map(df: DataFrame) -> DataFrame:
    """
    Adds a column with a dictionary created from main job role and main job role count then
    removes main job role and main job role count columns.

    Args:
        df (DataFrame): A dataframe containing cleaned ASC-WDS worker data with a count per main job role.

    Returns:
        DataFrame: A dataframe with unique establishmentid, importdate and dictionary where key = main job role value = main job role count.
    """

    df_struct = df.withColumn(
        "struct_column",
        F.struct(
            F.col(AWKClean.main_job_role_clean_labelled),
            F.col(AWKClean.ascwds_main_job_role_counts),
        ),
    )
    df_mapped = df_struct.groupBy(
        F.col(AWKClean.establishment_id), F.col(AWKClean.ascwds_worker_import_date)
    ).agg(
        F.map_from_entries(F.collect_list("struct_column")).alias(
            AWKClean.ascwds_main_job_role_counts
        )
    )
    return df_mapped
