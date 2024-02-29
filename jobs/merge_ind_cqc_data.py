import sys
import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

from utils import utils
import utils.cleaning_utils as cUtils

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

PartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

cqc_columns_to_import = [
    CQCLClean.cqc_location_import_date,
    CQCLClean.location_id,
    CQCLClean.name,
    CQCLClean.provider_id,
    CQCLClean.provider_name,
    CQCLClean.cqc_sector,
    CQCLClean.registration_status,
    CQCLClean.registration_date,
    CQCLClean.dormancy,
    CQCLClean.care_home,
    CQCLClean.number_of_beds,
    CQCLClean.regulated_activities,
    CQCLClean.gac_service_types,
    CQCLClean.specialisms,
    CQCLClean.primary_service_type,
    Keys.year,
    Keys.month,
    Keys.day,
    Keys.import_date,
]
ascwds_columns_to_import = [
    AWPClean.ascwds_workplace_import_date,
    AWPClean.location_id,
    AWPClean.establishment_id,
    AWPClean.organisation_id,
    AWPClean.total_staff,
    AWPClean.total_staff_bounded,
    AWPClean.total_staff_deduplicated,
    AWPClean.worker_records,
    AWPClean.worker_records_bounded,
    AWPClean.worker_records_deduplicated,
]


def main(
    cleaned_cqc_location_source: str,
    cleaned_cqc_pir_source: str,
    cleaned_ascwds_workplace_source: str,
    destination: str,
):
    cqc_location_df = utils.read_from_parquet(
        cleaned_cqc_location_source, selected_columns=cqc_columns_to_import
    )

    ascwds_workplace_df = utils.read_from_parquet(
        cleaned_ascwds_workplace_source,
        selected_columns=ascwds_columns_to_import,
    )

    cqc_pir_df = utils.read_from_parquet(cleaned_cqc_pir_source)

    ind_cqc_location_df = filter_df_to_independent_sector_only(cqc_location_df)

    ind_cqc_location_df = join_ascwds_data_into_merged_df(
        ind_cqc_location_df,
        ascwds_workplace_df,
        CQCLClean.cqc_location_import_date,
        AWPClean.ascwds_workplace_import_date,
    )

    utils.write_to_parquet(
        ind_cqc_location_df,
        destination,
        mode="overwrite",
        partitionKeys=PartitionKeys,
    )


def filter_df_to_independent_sector_only(df: DataFrame) -> DataFrame:
    return df.where(F.col(CQCLClean.cqc_sector) == CQCLValues.independent)


def join_ascwds_data_into_merged_df(
    primary_df: DataFrame,
    secondary_df: DataFrame,
    primary_import_date_column: str,
    secondary_import_date_column: str,
) -> DataFrame:
    primary_df_with_secondary_import_date = cUtils.add_aligned_date_column(
        primary_df,
        secondary_df,
        primary_import_date_column,
        secondary_import_date_column,
    )

    secondary_import_date_column_to_drop: str = (
        secondary_import_date_column + "_to_drop"
    )
    secondary_location_id_to_drop: str = AWPClean.location_id + "_to_drop"

    secondary_df = secondary_df.withColumnRenamed(
        secondary_import_date_column, secondary_import_date_column_to_drop
    ).withColumnRenamed(AWPClean.location_id, secondary_location_id_to_drop)

    merged_df = primary_df_with_secondary_import_date.join(
        secondary_df,
        (
            primary_df_with_secondary_import_date[secondary_import_date_column]
            == secondary_df[secondary_import_date_column_to_drop]
        )
        & (
            primary_df_with_secondary_import_date[CQCLClean.location_id]
            == secondary_df[secondary_location_id_to_drop]
        ),
        how="left",
    ).drop(secondary_import_date_column_to_drop, secondary_location_id_to_drop)

    return merged_df


if __name__ == "__main__":
    print("Spark job 'merge_ind_cqc_data' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        cleaned_cqc_location_source,
        cleaned_cqc_pir_source,
        cleaned_ascwds_workplace_source,
        destination,
    ) = utils.collect_arguments(
        (
            "--cleaned_cqc_location_source",
            "Source s3 directory for parquet CQC locations cleaned dataset",
        ),
        (
            "--cleaned_cqc_pir_source",
            "Source s3 directory for parquet CQC pir cleaned dataset",
        ),
        (
            "--cleaned_ascwds_workplace_source",
            "Source s3 directory for parquet ASCWDS workplace cleaned dataset",
        ),
        (
            "--destination",
            "Destination s3 directory for parquet",
        ),
    )
    main(
        cleaned_cqc_location_source,
        cleaned_cqc_pir_source,
        cleaned_ascwds_workplace_source,
        destination,
    )

    print("Spark job 'merge_ind_cqc_data' complete")
