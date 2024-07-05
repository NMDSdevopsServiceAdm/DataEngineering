import sys

from pyspark.sql import DataFrame, functions as F

from utils import utils
import utils.cleaning_utils as cUtils
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.cleaned_data_files.ons_cleaned import (
    OnsCleanedColumns as ONSClean,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)
from utils.column_names.merge_coverage_data_columns import (
    MergeCoverageDataColumns as MergeColumns,
)

PartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

cleaned_cqc_locations_columns_to_import = [
    CQCLClean.cqc_location_import_date,
    CQCLClean.location_id,
    CQCLClean.name,
    CQCLClean.provider_id,
    CQCLClean.provider_name,
    CQCLClean.cqc_sector,
    CQCLClean.registration_status,
    CQCLClean.imputed_registration_date,
    CQCLClean.dormancy,
    CQCLClean.care_home,
    CQCLClean.number_of_beds,
    CQCLClean.regulated_activities,
    CQCLClean.gac_service_types,
    CQCLClean.services_offered,
    CQCLClean.specialisms,
    CQCLClean.primary_service_type,
    ONSClean.current_ons_import_date,
    ONSClean.current_cssr,
    ONSClean.current_icb,
    ONSClean.current_region,
    ONSClean.current_rural_urban_ind_11,
    Keys.year,
    Keys.month,
    Keys.day,
    Keys.import_date,
]
cleaned_ascwds_workplace_columns_to_import = [
    AWPClean.ascwds_workplace_import_date,
    AWPClean.location_id,
    AWPClean.establishment_id,
    AWPClean.organisation_id,
    AWPClean.total_staff,
    AWPClean.worker_records,
]


def main(
    cleaned_cqc_location_source: str,
    workplace_for_reconciliation_source: str,
    merged_coverage_destination: str,
):
    spark = utils.get_spark()
    spark.sql(
        "set spark.sql.broadcastTimeout = 2000"
    )  # TODO: Test if this is needed still

    cqc_location_df = utils.read_from_parquet(
        cleaned_cqc_location_source,
        selected_columns=cleaned_cqc_locations_columns_to_import,
    )

    ascwds_workplace_df = utils.read_from_parquet(
        workplace_for_reconciliation_source,
        selected_columns=cleaned_ascwds_workplace_columns_to_import,
    )

    merged_coverage_df = join_ascwds_data_into_cqc_location_df(
        cqc_location_df,
        ascwds_workplace_df,
        CQCLClean.cqc_location_import_date,
        AWPClean.ascwds_workplace_import_date,
    )

    merged_coverage_df = add_flag_for_in_ascwds(merged_coverage_df)

    utils.write_to_parquet(
        merged_coverage_df,
        merged_coverage_destination,
        mode="overwrite",
        partitionKeys=PartitionKeys,
    )


def join_ascwds_data_into_cqc_location_df(
    cqc_location_df: DataFrame,
    ascwds_workplace_df: DataFrame,
    cqc_location_import_date_column: str,
    ascwds_workplace_import_date_column: str,
) -> DataFrame:
    merged_coverage_ascwds_df_with_ascwds_workplace_import_date = (
        cUtils.add_aligned_date_column(
            cqc_location_df,
            ascwds_workplace_df,
            cqc_location_import_date_column,
            ascwds_workplace_import_date_column,
        )
    )

    formatted_ascwds_workplace_df = ascwds_workplace_df.withColumnRenamed(
        AWPClean.location_id, CQCLClean.location_id
    )

    merged_coverage_df = (
        merged_coverage_ascwds_df_with_ascwds_workplace_import_date.join(
            formatted_ascwds_workplace_df,
            [CQCLClean.location_id, AWPClean.ascwds_workplace_import_date],
            how="left",
        )
    )

    return merged_coverage_df


def add_flag_for_in_ascwds(
    merged_coverage_df: DataFrame,
) -> DataFrame:
    merged_coverage_df = merged_coverage_df.withColumn(
        MergeColumns.in_ascwds,
        F.when(F.isnull(AWPClean.establishment_id), 0).otherwise(1),
    )


if __name__ == "__main__":
    print("Spark job 'merge_coverage_data' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        cleaned_cqc_location_source,
        workplace_for_reconciliation_source,
        merged_coverage_destination,
    ) = utils.collect_arguments(
        (
            "--cleaned_cqc_location_source",
            "Source s3 directory for parquet CQC locations cleaned dataset",
        ),
        (
            "--workplace_for_reconciliation_source",
            "Source s3 directory for parquet ASCWDS workplace for reconciliation dataset",
        ),
        (
            "--merged_coverage_destination",
            "Destination s3 directory for parquet",
        ),
    )
    main(
        cleaned_cqc_location_source,
        workplace_for_reconciliation_source,
        merged_coverage_destination,
    )

    print("Spark job 'merge_coverage_data' complete")
