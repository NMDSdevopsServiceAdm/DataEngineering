import sys

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)

cqcPartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]


def main(
    deregistered_cqc_location_source: str,
    cleaned_ascwds_workplace_source: str,
    reconciliation_single_and_subs_destination: str,
    reconciliation_parents_destination: str,
):

    # read required CQC cols

    # Filter CQC reg file to latest date

    # add in all ascwds formatting

    # join in ASCWDS data (import_date and locationid)

    # all the formatting steps for Support team

    # save single and subs file
    # save parent file


if __name__ == "__main__":
    print("Spark job 'reconciliation' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        deregistered_cqc_location_source,
        cleaned_ascwds_workplace_source,
        reconciliation_single_and_subs_destination,
        reconciliation_parents_destination,
    ) = utils.collect_arguments(
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
        deregistered_cqc_location_source,
        cleaned_ascwds_workplace_source,
        reconciliation_single_and_subs_destination,
        reconciliation_parents_destination,
    )

    print("Spark job 'reconciliation' complete")
