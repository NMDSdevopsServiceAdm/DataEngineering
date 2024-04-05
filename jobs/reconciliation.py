import sys
from pyspark.sql import DataFrame, functions as F
from typing import Tuple
from datetime import date

from utils import utils
import utils.cleaning_utils as cUtils
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    CqcLocationApiColumns as CQCL,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned_values import (
    CqcLocationCleanedColumns as CQCLClean,
    CqcLocationCleanedValues as CQCLValues,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned_values import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)
from utils.reconciliation_utils.reconciliation_values import (
    ReconciliationColumns as ReconColumn,
    ReconciliationValues as ReconValues,
    ReconciliationDict as ReconDict,
)

cqc_locations_columns_to_import = [
    Keys.import_date,
    CQCL.location_id,
    CQCL.registration_status,
    CQCL.deregistration_date,
]
cleaned_ascwds_workplace_columns_to_import = [
    AWPClean.ascwds_workplace_import_date,
    AWPClean.establishment_id,
    AWPClean.nmds_id,
    AWPClean.is_parent,
    AWPClean.parent_id,
    AWPClean.organisation_id,
    AWPClean.parent_permission,
    AWPClean.establishment_type_labelled,
    AWPClean.registration_type,
    AWPClean.location_id,
    AWPClean.main_service_id,
    AWPClean.establishment_name,
    AWPClean.region_id,
]


def main(
    cqc_location_api_source: str,
    cleaned_ascwds_workplace_source: str,
    reconciliation_single_and_subs_destination: str,
    reconciliation_parents_destination: str,
):
    cqc_location_df = utils.read_from_parquet(
        cqc_location_api_source, cqc_locations_columns_to_import
    )
    ascwds_workplace_df = utils.read_from_parquet(
        cleaned_ascwds_workplace_source,
        selected_columns=cleaned_ascwds_workplace_columns_to_import,
    )

    cqc_location_df = prepare_most_recent_cqc_location_df(cqc_location_df)
    (
        first_of_most_recent_month,
        first_of_previous_month,
    ) = collect_dates_to_use(cqc_location_df)

    (
        latest_ascwds_workplace_df,
        ascwds_parent_accounts_df,
    ) = prepare_latest_cleaned_ascwds_workforce_data(ascwds_workplace_df)

    merged_ascwds_cqc_df = join_cqc_location_data_into_ascwds_workplace_df(
        latest_ascwds_workplace_df, cqc_location_df
    )


def prepare_most_recent_cqc_location_df(cqc_location_df: DataFrame) -> DataFrame:
    cqc_location_df = cUtils.column_to_date(
        cqc_location_df, Keys.import_date, CQCLClean.cqc_location_import_date
    ).drop(Keys.import_date)
    cqc_location_df = utils.filter_df_to_maximum_value_in_column(
        cqc_location_df, CQCLClean.cqc_location_import_date
    )
    cqc_location_df = utils.format_date_fields(
        cqc_location_df,
        date_column_identifier=CQCL.deregistration_date,
        raw_date_format="yyyy-MM-dd",
    )
    return cqc_location_df


def collect_dates_to_use(df: DataFrame) -> Tuple[date, date]:
    most_recent: str = "most_recent"
    start_of_month: str = "start_of_month"
    start_of_previous_month: str = "start_of_previous_month"

    dates_df = df.select(F.max(CQCLClean.cqc_location_import_date).alias(most_recent))
    dates_df = dates_df.withColumn(start_of_month, F.trunc(F.col(most_recent), "MM"))
    dates_df = dates_df.withColumn(
        start_of_previous_month, F.add_months(F.col(start_of_month), -1)
    )
    dates_collected = dates_df.collect()
    first_of_most_recent_month = dates_collected[0][start_of_month]
    first_of_previous_month = dates_collected[0][start_of_previous_month]

    return (
        first_of_most_recent_month,
        first_of_previous_month,
    )


def prepare_latest_cleaned_ascwds_workforce_data(
    ascwds_workplace_df: str,
) -> Tuple[DataFrame, DataFrame]:
    df = utils.filter_df_to_maximum_value_in_column(
        ascwds_workplace_df, AWPClean.ascwds_workplace_import_date
    )

    df = add_region_id_labels_for_reconciliation(df)
    df = add_parent_sub_or_single_col_to_df(df)
    df = add_ownership_col_to_df(df)
    df = add_potentials_col_to_df(df)
    cqc_registered_accounts_df = filter_to_cqc_registration_type_only(df)
    parent_accounts_df = get_ascwds_parent_accounts(df)

    return cqc_registered_accounts_df, parent_accounts_df


def add_region_id_labels_for_reconciliation(df: DataFrame) -> DataFrame:
    return df.replace(ReconDict.region_id_dict, subset=[AWPClean.region_id])


def add_parent_sub_or_single_col_to_df(df: DataFrame) -> DataFrame:
    return df.withColumn(
        ReconColumn.parent_sub_or_single,
        F.when(
            (F.col(AWPClean.is_parent) == 1),
            F.lit(ReconValues.parent),
        )
        .when(
            ((F.col(AWPClean.is_parent) == 0) & (F.col(AWPClean.parent_id) > 0)),
            F.lit(ReconValues.subsidiary),
        )
        .otherwise(F.lit(ReconValues.single)),
    ).drop(AWPClean.parent_id)


def add_ownership_col_to_df(df: DataFrame) -> DataFrame:
    return df.withColumn(
        ReconColumn.ownership,
        F.when(
            (F.col(AWPClean.parent_permission) == 1),
            F.lit(ReconValues.parent),
        ).otherwise(F.lit(ReconValues.workplace)),
    ).drop(AWPClean.parent_permission)


def add_potentials_col_to_df(df: DataFrame) -> DataFrame:
    return df.withColumn(
        ReconColumn.potentials,
        F.when(
            (
                (
                    (F.col(ReconColumn.parent_sub_or_single) == ReconValues.single)
                    | (
                        F.col(ReconColumn.parent_sub_or_single)
                        == ReconValues.subsidiary
                    )
                )
                & (F.col(ReconColumn.ownership) == ReconValues.workplace)
            ),
            F.lit(ReconValues.singles_and_subs),
        ).otherwise(F.lit(ReconValues.parents)),
    ).drop(ReconColumn.parent_sub_or_single, ReconColumn.ownership)


def filter_to_cqc_registration_type_only(df: DataFrame) -> DataFrame:
    return df.filter(F.col(AWPClean.registration_type) == "2")


def get_ascwds_parent_accounts(df: DataFrame) -> DataFrame:
    return df.filter(F.col(AWPClean.is_parent) == "1").select(
        AWPClean.nmds_id,
        AWPClean.establishment_id,
        AWPClean.establishment_name,
        AWPClean.organisation_id,
        AWPClean.establishment_type,
        AWPClean.region_id,
    )


def join_cqc_location_data_into_ascwds_workplace_df(
    ascwds_workplace_df: DataFrame,
    cqc_location_df: DataFrame,
) -> DataFrame:
    ascwds_workplace_df = ascwds_workplace_df.withColumnRenamed(
        AWPClean.location_id, CQCL.location_id
    )
    return ascwds_workplace_df.join(cqc_location_df, CQCL.location_id, "left")


if __name__ == "__main__":
    print("Spark job 'reconciliation' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        cqc_location_api_source,
        cleaned_ascwds_workplace_source,
        reconciliation_single_and_subs_destination,
        reconciliation_parents_destination,
    ) = utils.collect_arguments(
        (
            "--cqc_location_api_source",
            "Source s3 directory for initial CQC location api dataset",
        ),
        (
            "--cleaned_ascwds_workplace_source",
            "Source s3 directory for cleaned parquet ASCWDS workplace dataset",
        ),
        (
            "--reconciliation_single_and_subs_destination",
            "Destination s3 directory for singles and subs reconciliation CSV dataset",
        ),
        (
            "--reconciliation_parents_destination",
            "Destination s3 directory for parents reconciliation CSV dataset",
        ),
    )
    main(
        cqc_location_api_source,
        cleaned_ascwds_workplace_source,
        reconciliation_single_and_subs_destination,
        reconciliation_parents_destination,
    )

    print("Spark job 'reconciliation' complete")
