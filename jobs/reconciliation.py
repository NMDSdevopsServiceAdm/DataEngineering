import sys

from pyspark.sql import DataFrame, functions as F
from typing import Tuple
from datetime import date

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned_values import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned_values import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.reconciliation_utils.reconciliation_values import (
    ReconciliationColumns as ReconColumn,
    ReconciliationValues as ReconValues,
    ReconciliationDict as ReconDict,
)

from utils.reconciliation_utils.utils import (
    create_missing_columns_required_for_output_and_reorder_for_saving,
    write_to_csv,
)

PartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

cleaned_cqc_locations_columns_to_import = [
    CQCLClean.cqc_location_import_date,
    CQCLClean.location_id,
    CQCLClean.deregistration_date,
]
cleaned_ascwds_workplace_columns_to_import = [
    AWPClean.ascwds_workplace_import_date,
    AWPClean.establishment_id,
    AWPClean.nmds_id,
    AWPClean.is_parent,
    AWPClean.parent_id,
    AWPClean.parent_permission,
    AWPClean.establishment_type,
    AWPClean.registration_type,
    AWPClean.location_id,
    AWPClean.main_service_id,
    AWPClean.establishment_name,
    AWPClean.master_update_date,
    AWPClean.region_id,
    Keys.year,
    Keys.month,
    Keys.day,
    Keys.import_date,
]


def main(
    cqc_location_api_source: str,
    deregistered_cqc_location_source: str,
    cleaned_ascwds_workplace_source: str,
    reconciliation_single_and_subs_destination: str,
    reconciliation_parents_destination: str,
) -> DataFrame:
    deregistered_locations_df = utils.read_from_parquet(
        deregistered_cqc_location_source,
        selected_columns=cleaned_cqc_locations_columns_to_import,
    )
    ascwds_workplace_df = utils.read_from_parquet(
        cleaned_ascwds_workplace_source,
        selected_columns=cleaned_ascwds_workplace_columns_to_import,
    )
    all_location_ids_df = utils.read_from_parquet(
        cqc_location_api_source, CQCLClean.location_id
    )

    first_of_most_recent_month, first_of_previous_month = collect_dates_to_use(
        deregistered_locations_df
    )

    latest_ascwds_workplace_df = prepare_latest_cleaned_ascwds_workforce_data(
        ascwds_workplace_df
    )

    latest_ascwds_workplace_df = identify_if_location_ids_in_ascwds_have_ever_existed(
        latest_ascwds_workplace_df, all_location_ids_df
    )

    locations_deregistered_before_most_recent_month_df = (
        filter_to_locations_deregistered_before_most_recent_month(
            deregistered_locations_df, first_of_most_recent_month
        )
    )

    reconciliation_df = join_deregistered_cqc_locations_into_ascwds(
        latest_ascwds_workplace_df, locations_deregistered_before_most_recent_month_df
    )

    incorrect_or_missing_locationid_df = (
        filter_to_locations_with_incorrect_or_missing_locationids(reconciliation_df)
    )

    create_reconciliation_outputs_for_ascwds_singles_and_subsidiary_accounts(
        reconciliation_df,
        incorrect_or_missing_locationid_df,
        first_of_previous_month,
        reconciliation_single_and_subs_destination,
    )

    create_reconciliation_outputs_for_ascwds_parent_accounts(
        reconciliation_df,
        incorrect_or_missing_locationid_df,
        first_of_previous_month,
        reconciliation_parents_destination,
    )


def prepare_latest_cleaned_ascwds_workforce_data(
    ascwds_workplace_df: str,
) -> DataFrame:
    df = filter_df_to_maximum_value_in_column(
        ascwds_workplace_df, AWPClean.ascwds_workplace_import_date
    )
    df = df.replace(ReconDict.region_id_dict, subset=[AWPClean.region_id])
    df = df.replace(
        ReconDict.establishment_type_dict, subset=[AWPClean.establishment_type]
    )
    df = add_parent_sub_or_single_col_to_df(df)
    df = add_ownership_col_to_df(df)
    df = add_potentials_col_to_df(df)

    return df.withColumnRenamed(AWPClean.location_id, CQCLClean.location_id)


def filter_df_to_maximum_value_in_column(
    df: DataFrame, column_to_filter_on: str
) -> DataFrame:
    max_date = df.agg(F.max(column_to_filter_on)).collect()[0][0]

    return df.filter(F.col(column_to_filter_on) == max_date)


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
    )


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
    )


def collect_dates_to_use(
    deregistered_df: DataFrame,
) -> Tuple[date, date, date]:
    dates_df = deregistered_df.select(
        F.max(CQCLClean.cqc_location_import_date).alias("most_recent")
    )
    dates_df = dates_df.withColumn(
        "start_of_month", F.trunc(F.col("most_recent"), "mon")
    )
    dates_df = dates_df.withColumn(
        "start_of_previous_month", F.add_months(F.col("start_of_month"), -1)
    )

    dates_collected = dates_df.collect()
    most_recent_cqc_location_import_date = dates_collected[0]["most_recent"]
    first_of_most_recent_month = dates_collected[0]["start_of_month"]
    first_of_previous_month = dates_collected[0]["start_of_previous_month"]

    return (
        most_recent_cqc_location_import_date,
        first_of_most_recent_month,
        first_of_previous_month,
    )


def identify_if_location_ids_in_ascwds_have_ever_existed(
    ascwds_df: DataFrame,
    cqc_location_id_df: DataFrame,
) -> DataFrame:
    distinct_location_id_df = get_all_location_ids_which_have_ever_existed(
        cqc_location_id_df
    )

    return ascwds_df.join(distinct_location_id_df, CQCLClean.location_id, "left")


def get_all_location_ids_which_have_ever_existed(
    all_location_ids_df: DataFrame,
) -> DataFrame:
    all_location_ids_df = all_location_ids_df.dropDuplicates()

    return all_location_ids_df.withColumn(ReconColumn.ever_existed, F.lit("yes"))


def filter_to_locations_deregistered_before_most_recent_month(
    deregistered_locations_df: DataFrame, first_of_most_recent_month: date
) -> DataFrame:
    return deregistered_locations_df.filter(
        F.col(CQCLClean.deregistration_date) < first_of_most_recent_month
    )


def join_deregistered_cqc_locations_into_ascwds(
    latest_ascwds_workplace_df: DataFrame,
    locations_deregistered_before_most_recent_month_df: DataFrame,
) -> DataFrame:
    return latest_ascwds_workplace_df.join(
        locations_deregistered_before_most_recent_month_df,
        CQCLClean.location_id,
        "left",
    )


def filter_to_locations_with_incorrect_or_missing_locationids(
    df: DataFrame,
) -> DataFrame:
    missing_locationid_df = filter_to_ascwds_locations_with_missing_locationids(df)
    incorrect_locationid_df = filter_to_ascwds_locations_with_incorrect_locationids(df)
    missing_and_incorrect_locationid_combined_df = missing_locationid_df.unionByName(
        incorrect_locationid_df
    )
    return remove_ascwds_head_office_accounts(
        missing_and_incorrect_locationid_combined_df
    )


def filter_to_ascwds_locations_with_missing_locationids(df: DataFrame) -> DataFrame:
    return df.where(
        (F.col(AWPClean.registration_type) == 2)
        & (F.col(AWPClean.location_id).isNull())
    )


def filter_to_ascwds_locations_with_incorrect_locationids(df: DataFrame) -> DataFrame:
    return df.where(
        (F.col(AWPClean.location_id).isNotNull())
        & (F.col(ReconColumn.ever_existed).isNull())
    )


def remove_ascwds_head_office_accounts(df: DataFrame) -> DataFrame:
    return df.where(F.col(AWPClean.main_service_id) != "72")


def create_reconciliation_outputs_for_ascwds_singles_and_subsidiary_accounts(
    reconciliation_df: DataFrame,
    incorrect_or_missing_locationid_df: DataFrame,
    first_of_previous_month: date,
    destination: str,
):
    single_sub_deregistered_df = (
        create_singles_and_subsidiary_deregistered_df_with_description_col(
            reconciliation_df, first_of_previous_month
        )
    )
    single_sub_incorrect_or_missing_locationid_df = create_singles_and_subsidiary_incorrect_or_missing_locationid_with_description_col(
        incorrect_or_missing_locationid_df
    )

    singles_and_subs_df = single_sub_deregistered_df.unionByName(
        single_sub_incorrect_or_missing_locationid_df
    )

    singles_and_subs_df = singles_and_subs_df.withColumn(
        ReconColumn.subject, F.lit("CQC Reconcilliation Work")
    )

    singles_and_subs_output_df = (
        create_missing_columns_required_for_output_and_reorder_for_saving(
            singles_and_subs_df
        )
    )
    write_to_csv(singles_and_subs_output_df, destination)


def create_singles_and_subsidiary_deregistered_df_with_description_col(
    df: DataFrame, first_of_previous_month: date
) -> DataFrame:
    df = df.where(F.col(CQCLClean.deregistration_date) >= first_of_previous_month)
    df = df.where(F.col(ReconColumn.potentials) == ReconValues.singles_and_subs)
    return df.withColumn(
        ReconColumn.description, F.lit("Potential (new): Deregistered ID")
    )


def create_singles_and_subsidiary_incorrect_or_missing_locationid_with_description_col(
    df: DataFrame,
) -> DataFrame:
    df = df.where(F.col(ReconColumn.potentials) == ReconValues.singles_and_subs)
    return df.withColumn(ReconColumn.description, F.lit("Potential (new): Regtype"))


def create_reconciliation_outputs_for_ascwds_parent_accounts(
    reconciliation_df: DataFrame,
    incorrect_or_missing_locationid_df: DataFrame,
    first_of_previous_month: date,
    destination: str,
):
    # write_to_csv(parents_output_df, destination)
    return None


if __name__ == "__main__":
    print("Spark job 'reconciliation' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        cqc_location_api_source,
        deregistered_cqc_location_source,
        cleaned_ascwds_workplace_source,
        reconciliation_single_and_subs_destination,
        reconciliation_parents_destination,
    ) = utils.collect_arguments(
        (
            "--cqc_location_api_source",
            "Source s3 directory for initial CQC location api dataset",
        ),
        (
            "--deregistered_cqc_location_source",
            "Source s3 directory for deregistered CQC locations dataset",
        ),
        (
            "--cleaned_ascwds_workplace_source",
            "Source s3 directory for cleaned parquet ASCWDS workplace dataset",
        ),
        (
            "--reconciliation_single_and_subs_destination",
            "Destination s3 directory for reconciliation parquet singles and subs dataset",
        ),
        (
            "--reconciliation_parents_destination",
            "Destination s3 directory for reconciliation parquet parents dataset",
        ),
    )
    main(
        cqc_location_api_source,
        deregistered_cqc_location_source,
        cleaned_ascwds_workplace_source,
        reconciliation_single_and_subs_destination,
        reconciliation_parents_destination,
    )

    print("Spark job 'reconciliation' complete")
