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
    AWPClean.establishment_type,
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
