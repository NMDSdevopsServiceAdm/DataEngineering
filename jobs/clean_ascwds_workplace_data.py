import sys

from pyspark.sql import DataFrame, Window, functions as F
from typing import Tuple

from utils import utils
import utils.cleaning_utils as cUtils
from utils.column_names.raw_data_files.ascwds_workplace_columns import (
    PartitionKeys as Keys,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.scale_variable_limits import AscwdsScaleVariableLimits
from utils.value_labels.ascwds_workplace.workplace_label_dictionary import (
    ascwds_workplace_labels_dict,
)

partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

DATE_COLUMN_IDENTIFIER = "date"
COLUMNS_TO_BOUND = [AWPClean.total_staff, AWPClean.worker_records]
MONTHS_BEFORE_COMPARISON_DATE_TO_PURGE = 24

cols_required_for_reconciliation_df = [
    AWPClean.ascwds_workplace_import_date,
    AWPClean.establishment_id,
    AWPClean.nmds_id,
    AWPClean.master_update_date,
    AWPClean.master_update_date_org,
    AWPClean.establishment_created_date,
    AWPClean.is_parent,
    AWPClean.parent_id,
    AWPClean.organisation_id,
    AWPClean.parent_permission,
    AWPClean.establishment_type,
    AWPClean.registration_type,
    AWPClean.location_id,
    AWPClean.main_service_id,
    AWPClean.establishment_name,
    AWPClean.region_id,
    AWPClean.total_staff,
    AWPClean.worker_records,
    AWPClean.last_logged_in_date,
    Keys.year,
    Keys.month,
    Keys.day,
    Keys.import_date,
]


def main(
    ascwds_workplace_source: str,
    cleaned_ascwds_workplace_destination: str,
    workplace_for_reconciliation_destination: str,
):
    ascwds_workplace_df = utils.read_from_parquet(ascwds_workplace_source)

    ascwds_workplace_df = filter_test_accounts(ascwds_workplace_df)
    ascwds_workplace_df = remove_white_space_from_nmdsid(ascwds_workplace_df)

    ascwds_workplace_df = ascwds_workplace_df.withColumnRenamed(
        AWPClean.last_logged_in, AWPClean.last_logged_in_date
    )

    ascwds_workplace_df = utils.format_date_fields(
        ascwds_workplace_df,
        date_column_identifier=DATE_COLUMN_IDENTIFIER,
        raw_date_format="dd/MM/yyyy",
    )

    ascwds_workplace_df = cUtils.column_to_date(
        ascwds_workplace_df,
        Keys.import_date,
        AWPClean.ascwds_workplace_import_date,
    )

    ascwds_workplace_df = cUtils.apply_categorical_labels(
        ascwds_workplace_df,
        ascwds_workplace_labels_dict,
        ascwds_workplace_labels_dict.keys(),
        add_as_new_column=False,
    )

    (
        ascwds_workplace_df,
        reconciliation_df,
    ) = create_purged_dfs_for_reconciliation_and_data(ascwds_workplace_df)

    ascwds_workplace_df = remove_workplaces_with_duplicate_location_ids(
        ascwds_workplace_df
    )

    ascwds_workplace_df = cUtils.cast_to_int(ascwds_workplace_df, COLUMNS_TO_BOUND)

    ascwds_workplace_df = cUtils.set_column_bounds(
        ascwds_workplace_df,
        AWPClean.total_staff,
        AWPClean.total_staff_bounded,
        AscwdsScaleVariableLimits.total_staff_lower_limit,
    )

    ascwds_workplace_df = cUtils.set_column_bounds(
        ascwds_workplace_df,
        AWPClean.worker_records,
        AWPClean.worker_records_bounded,
        AscwdsScaleVariableLimits.worker_records_lower_limit,
    )

    reconciliation_df = select_columns_required_for_reconciliation_df(reconciliation_df)

    print(
        f"Exporting ascwds workplace reconciliation data as parquet to {workplace_for_reconciliation_destination}"
    )
    utils.write_to_parquet(
        reconciliation_df,
        workplace_for_reconciliation_destination,
        mode="overwrite",
        partitionKeys=partition_keys,
    )

    print(
        f"Exporting clean ascwds workplace data as parquet to {cleaned_ascwds_workplace_destination}"
    )
    utils.write_to_parquet(
        ascwds_workplace_df,
        cleaned_ascwds_workplace_destination,
        mode="overwrite",
        partitionKeys=partition_keys,
    )


def filter_test_accounts(df: DataFrame) -> DataFrame:
    """
    Filters out test accounts (accounts used by internal Skills for Care staff) from the DataFrame.

    Args:
        df (DataFrame): The DataFrame to filter test accounts from.

    Returns:
        DataFrame: The DataFrame with test accounts removed.
    """
    test_accounts = [
        "305",
        "307",
        "308",
        "309",
        "310",
        "2452",
        "28470",
        "26792",
        "31657",
        "31138",
    ]

    if AWPClean.organisation_id in df.columns:
        df = df.filter(~df[AWPClean.organisation_id].isin(test_accounts))

    return df


def remove_white_space_from_nmdsid(df: DataFrame) -> DataFrame:
    """
    Removes white space from the nmds_id column in the DataFrame.

    Args:
        df (DataFrame): The DataFrame to remove white space from the nmds_id column.

    Returns:
        DataFrame: The DataFrame with white space removed from the nmds_id column
    """
    df = df.withColumn(AWPClean.nmds_id, F.trim(F.col(AWPClean.nmds_id)))
    return df


def remove_workplaces_with_duplicate_location_ids(df: DataFrame) -> DataFrame:
    location_id_count: str = "location_id_count"

    locations_without_location_id_df = df.where(F.col(AWPClean.location_id).isNull())
    locations_with_location_id_df = df.where(F.col(AWPClean.location_id).isNotNull())

    loc_id_import_date_window = Window.partitionBy(
        AWPClean.location_id, Keys.import_date
    )
    count_of_location_id_df = locations_with_location_id_df.withColumn(
        location_id_count, F.count(AWPClean.location_id).over(loc_id_import_date_window)
    )
    duplicate_location_ids_removed_df = count_of_location_id_df.filter(
        F.col(location_id_count) == 1
    ).drop(location_id_count)

    return locations_without_location_id_df.unionByName(
        duplicate_location_ids_removed_df
    )


def create_purged_dfs_for_reconciliation_and_data(
    df: DataFrame,
) -> Tuple[DataFrame, DataFrame]:
    """
    This process is designed to purge/remove data which is deemed too old to exist.

    If the worplace is a parent account, the mupddate used to purge is the maximum of any account within that organisation.

    The purge rules for workplace_last_active_date also takes last_logged_in date into account.

    Args:
        df (DataFrame): The ascwds_workplace_df to be purged

    Returns:
        Tuple[DataFrame, DataFrame]: A tuple of two dataframes: ascwds_workplace_df where old data has been removed based on
        mupddate date and reconciliation_df where old data has been removed based on the maximum of mupddate and lastloggedin date

    """
    df = calculate_maximum_master_update_date_for_organisation(df)
    df = create_data_last_amended_date_column(df)
    df = create_workplace_last_active_date_column(df)
    df = create_date_column_for_purging_data(df)

    ascwds_workplace_df = keep_workplaces_active_on_or_after_purge_date(
        df, AWPClean.data_last_amended_date, AWPClean.purge_date
    )
    reconciliation_df = keep_workplaces_active_on_or_after_purge_date(
        df, AWPClean.workplace_last_active_date, AWPClean.purge_date
    )

    return ascwds_workplace_df, reconciliation_df


def calculate_maximum_master_update_date_for_organisation(df: DataFrame) -> DataFrame:
    org_df_with_maximum_update_date = df.groupBy(
        AWPClean.organisation_id, AWPClean.ascwds_workplace_import_date
    ).agg(F.max(AWPClean.master_update_date).alias(AWPClean.master_update_date_org))

    df = df.join(
        org_df_with_maximum_update_date,
        [AWPClean.organisation_id, AWPClean.ascwds_workplace_import_date],
        "left",
    )
    return df


def create_data_last_amended_date_column(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        AWPClean.data_last_amended_date,
        F.when(
            (F.col(AWPClean.is_parent) == "Yes"), F.col(AWPClean.master_update_date_org)
        ).otherwise(F.col(AWPClean.master_update_date)),
    )
    return df


def create_workplace_last_active_date_column(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        AWPClean.workplace_last_active_date,
        F.greatest(
            F.col(AWPClean.data_last_amended_date), F.col(AWPClean.last_logged_in_date)
        ),
    )
    return df


def create_date_column_for_purging_data(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        AWPClean.purge_date,
        F.add_months(
            F.col(AWPClean.ascwds_workplace_import_date),
            -MONTHS_BEFORE_COMPARISON_DATE_TO_PURGE,
        ),
    )
    return df


def keep_workplaces_active_on_or_after_purge_date(
    df: DataFrame, last_active_date_col: str, purge_date_col: str
) -> DataFrame:
    return df.where(F.col(last_active_date_col) >= F.col(purge_date_col))


def select_columns_required_for_reconciliation_df(df: DataFrame) -> DataFrame:
    return df.select(cols_required_for_reconciliation_df)


if __name__ == "__main__":
    print("Spark job 'clean_ascwds_workplace_data' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        ascwds_workplace_source,
        cleaned_ascwds_workplace_destination,
        workplace_for_reconciliation_destination,
    ) = utils.collect_arguments(
        (
            "--ascwds_workplace_source",
            "Source s3 directory for parquet ascwds workplace dataset",
        ),
        (
            "--cleaned_ascwds_workplace_destination",
            "Destination s3 directory for cleaned parquet ascwds workplace dataset",
        ),
        (
            "--workplace_for_reconciliation_destination",
            "Destination s3 directory for ascwds reconciliation dataset",
        ),
    )
    main(
        ascwds_workplace_source,
        cleaned_ascwds_workplace_destination,
        workplace_for_reconciliation_destination,
    )

    print("Spark job 'clean_ascwds_workplace_data' complete")
