import sys

from utils import utils
from utils.reconciliation import reconciliation_utils as rUtils
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
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


def main(
    cqc_location_api_source: str,
    ascwds_reconciliation_source: str,
    reconciliation_single_and_subs_destination: str,
    reconciliation_parents_destination: str,
):
    spark = utils.get_spark()
    spark.sql("set spark.sql.broadcastTimeout = 1000")

    cqc_location_df = utils.read_from_parquet(
        cqc_location_api_source, cqc_locations_columns_to_import
    )
    ascwds_workplace_df = utils.read_from_parquet(ascwds_reconciliation_source)

    cqc_location_df = rUtils.prepare_most_recent_cqc_location_df(cqc_location_df)
    (
        first_of_most_recent_month,
        first_of_previous_month,
    ) = rUtils.collect_dates_to_use(cqc_location_df)

    (
        latest_ascwds_workplace_df,
        ascwds_parent_accounts_df,
    ) = rUtils.prepare_latest_cleaned_ascwds_workforce_data(ascwds_workplace_df)

    merged_ascwds_cqc_df = rUtils.join_cqc_location_data_into_ascwds_workplace_df(
        latest_ascwds_workplace_df, cqc_location_df
    )

    latest_ascwds_workplace_df.unpersist()
    cqc_location_df.unpersist()

    reconciliation_df = rUtils.filter_to_locations_relevant_to_reconcilition_process(
        merged_ascwds_cqc_df, first_of_most_recent_month, first_of_previous_month
    )
    merged_ascwds_cqc_df.unpersist()

    single_and_sub_df = (
        rUtils.create_reconciliation_output_for_ascwds_single_and_sub_accounts(
            reconciliation_df
        )
    )
    parents_df = rUtils.create_reconciliation_output_for_ascwds_parent_accounts(
        ascwds_parent_accounts_df, reconciliation_df, first_of_previous_month
    )
    reconciliation_df.unpersist()

    utils.write_to_parquet(
        single_and_sub_df,
        reconciliation_single_and_subs_destination,
        mode="overwrite",
    )

    utils.write_to_parquet(
        parents_df,
        reconciliation_parents_destination,
        mode="overwrite",
    )


if __name__ == "__main__":
    print("Spark job 'reconciliation' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        cqc_location_api_source,
        ascwds_reconciliation_source,
        reconciliation_single_and_subs_destination,
        reconciliation_parents_destination,
    ) = utils.collect_arguments(
        (
            "--cqc_location_api_source",
            "Source s3 directory for initial CQC location api dataset",
        ),
        (
            "--ascwds_reconciliation_source",
            "Source s3 directory for ASCWDS reconciliation parquet dataset",
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
        ascwds_reconciliation_source,
        reconciliation_single_and_subs_destination,
        reconciliation_parents_destination,
    )

    print("Spark job 'reconciliation' complete")
