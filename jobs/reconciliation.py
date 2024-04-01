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
    AscwdsWorkplaceCleanedValues as AWPValues,
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
):

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

    latest_ascwds_workplace_df = prepare_latest_cleaned_ascwds_workforce_data(
        ascwds_workplace_df
    )

    # get all locationids that have ever existed
    entire_location_id_history_df = get_all_location_ids_which_have_ever_existed(
        all_location_ids_df
    )

    (
        most_recent_cqc_location_import_date,
        first_of_most_recent_month,
        first_of_previous_month,
    ) = collect_dates_to_use(deregistered_locations_df)

    # deregistered_locations_in_previous_month_df = (
    #     filter_to_locations_deregistered_in_latest_month(deregistered_locations_df)
    # )

    # join in ASCWDS data (import_date and locationid)

    # all the formatting steps for Support team

    # save single and subs file
    # save parent file


def prepare_latest_cleaned_ascwds_workforce_data(
    ascwds_workplace_df: str,
) -> DataFrame:
    latest_ascwds_workplace_df = filter_df_to_maximum_value_in_column(
        ascwds_workplace_df, AWPClean.ascwds_workplace_import_date
    )

    # TODO - make better! genuine dict for all ascwds data
    esttype_dict = {
        "1": "Local authority (adult services)",
        "3": "Local authority (generic/other)",
        "6": "Private sector",
        "7": "Voluntary/Charity",
        "8": "Other",
    }
    latest_ascwds_workplace_df = latest_ascwds_workplace_df.replace(
        esttype_dict, subset=[AWPClean.establishment_type]
    )
    # TODO - make better! dict specific to this job
    region_dict = {
        "4": "B - North East",
        "2": "C - East Midlands",
        "7": "D - South West",
        "8": "E - West Midlands",
        "5": "F - North West",
        "3": "G - London",
        "6": "H - South East",
        "1": "I - Eastern",
        "9": "J - Yorkshire Humber",
    }
    latest_ascwds_workplace_df = latest_ascwds_workplace_df.replace(
        region_dict, subset=[AWPClean.region_id]
    )

    latest_ascwds_workplace_df = latest_ascwds_workplace_df.withColumn(
        AWPClean.parent_sub_or_single,
        F.when(
            (F.col(AWPClean.is_parent) == 1),
            F.lit(AWPValues.parent),
        )
        .when(
            ((F.col(AWPClean.is_parent) == 0) & (F.col(AWPClean.parent_id) > 0)),
            F.lit(AWPValues.subsidiary),
        )
        .otherwise(F.lit(AWPValues.single)),
    )

    latest_ascwds_workplace_df = latest_ascwds_workplace_df.withColumn(
        AWPClean.ownership,
        F.when(
            (F.col(AWPClean.parent_permission) == 1),
            F.lit(AWPValues.parent),
        ).otherwise(F.lit(AWPValues.workplace)),
    ).drop(AWPClean.parent_permission)

    latest_ascwds_workplace_df = latest_ascwds_workplace_df.withColumn(
        AWPValues.potentials,
        F.when(
            (
                (
                    (F.col(AWPClean.parent_sub_or_single) == AWPValues.single)
                    | (F.col(AWPClean.parent_sub_or_single) == AWPValues.subsidiary)
                )
                & (F.col(AWPClean.ownership) == AWPValues.workplace)
            ),
            F.lit(AWPValues.singles_and_subs),
        ).otherwise(F.lit(AWPValues.parents)),
    )

    latest_ascwds_workplace_df = latest_ascwds_workplace_df.withColumnRenamed(
        AWPClean.location_id, CQCLClean.location_id
    )

    return latest_ascwds_workplace_df


# TODO - make utils function?
def filter_df_to_maximum_value_in_column(
    df: DataFrame, column_to_filter_on: str
) -> DataFrame:
    max_date = df.agg(F.max(column_to_filter_on)).collect()[0][0]

    return df.filter(F.col(column_to_filter_on) == max_date)


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


def get_all_location_ids_which_have_ever_existed(
    all_location_ids_df: DataFrame,
) -> DataFrame:
    all_location_ids_df = all_location_ids_df.dropDuplicates()

    return all_location_ids_df.withColumn("ever_existed", F.lit("yes"))


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
