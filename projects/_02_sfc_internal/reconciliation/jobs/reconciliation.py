import os
import sys

os.environ["SPARK_VERSION"] = "3.5"

from projects._02_sfc_internal.reconciliation.utils import (
    reconciliation_utils as rUtils,
)
from utils import utils


def main(
    cqc_deregistered_locations_source: str,
    ascwds_reconciliation_source: str,
    reconciliation_single_and_subs_destination: str,
    reconciliation_parents_destination: str,
):
    spark = utils.get_spark()
    spark.sql("set spark.sql.broadcastTimeout = 1000")

    cqc_location_df = utils.read_from_parquet(cqc_deregistered_locations_source)
    ascwds_workplace_df = utils.read_from_parquet(ascwds_reconciliation_source)

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
        cqc_deregistered_locations_source,
        ascwds_reconciliation_source,
        reconciliation_single_and_subs_destination,
        reconciliation_parents_destination,
    ) = utils.collect_arguments(
        (
            "--cqc_deregistered_locations_source",
            "Source s3 directory for CQC deregistered locations dataset",
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
        cqc_deregistered_locations_source,
        ascwds_reconciliation_source,
        reconciliation_single_and_subs_destination,
        reconciliation_parents_destination,
    )

    print("Spark job 'reconciliation' complete")
