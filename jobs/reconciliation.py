import sys
import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

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

cqcPartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

cleaned_cqc_locations_columns_to_import = [
    CQCLClean.cqc_location_import_date,
    CQCLClean.location_id,
    CQCLClean.deregistration_date,
    Keys.year,
    Keys.month,
    Keys.day,
    Keys.import_date,
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

    ascwds_workplace_df = utils.read_from_parquet(cleaned_ascwds_workplace_source)

    # Filter CQC reg file to latest date?

    # get latest? ascwds data
    latest_ascwds_workplace_df = prepare_latest_cleaned_ascwds_workforce_data(
        ascwds_workplace_df
    )

    # add in all ascwds formatting

    # join in ASCWDS data (import_date and locationid)

    # get all locationids that have ever existed
    entire_location_id_history_df = get_all_location_ids(cqc_location_api_source)

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
        "ParentSubSingle",
        F.when(
            (F.col(AWPClean.is_parent) == 1),
            F.lit("Parent"),
        )
        .when(
            ((F.col(AWPClean.is_parent) == 0) & (F.col(AWPClean.parent_id) > 0)),
            F.lit("Subsidiary"),
        )
        .otherwise(F.lit("Single")),
    )

    latest_ascwds_workplace_df = latest_ascwds_workplace_df.withColumn(
        "ownership",
        F.when(
            (F.col(AWPClean.parent_permission) == 1),
            F.lit("parent"),
        ).otherwise(F.lit("workplace")),
    ).drop(AWPClean.parent_permission)

    return latest_ascwds_workplace_df


# TODO - make utils function?
def filter_df_to_maximum_value_in_column(
    df: DataFrame, column_to_filter_on: str
) -> DataFrame:
    max_date = df.agg(F.max(column_to_filter_on)).collect()[0][0]

    return df.filter(F.col(column_to_filter_on) == max_date)


def get_all_location_ids(cqc_location_source: str) -> DataFrame:
    all_location_ids_df = (
        utils.read_from_parquet(cqc_location_source)
        .select(CQCLClean.location_id)
        .distinct()
    )
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
