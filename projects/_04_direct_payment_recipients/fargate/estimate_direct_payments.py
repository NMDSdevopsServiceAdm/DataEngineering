from polars_utils import utils
from projects._04_direct_payment_recipients.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)

direct_payments_columns = [
    DP.LA_AREA,
    DP.YEAR,
    DP.YEAR_AS_INTEGER,
    DP.SERVICE_USER_DPRS_DURING_YEAR,
    DP.CARER_DPRS_DURING_YEAR,
    DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
    DP.HISTORIC_SERVICE_USERS_EMPLOYING_STAFF_ESTIMATE,
    DP.TOTAL_DPRS_DURING_YEAR,
    DP.FILLED_POSTS_PER_EMPLOYER,
]


def main(
    direct_payments_merged_source: str,
    destination: str,
    summary_destination: str,
) -> None:
    lf = utils.scan_parquet(
        source=direct_payments_merged_source,
        selected_columns=direct_payments_columns,
    )

    utils.sink_to_parquet(
        lf,
        destination,
        append=False,
    )


if __name__ == "__main__":
    print("Running estimate direct payments job")

    args = utils.get_args(
        (
            "--direct_payments_merged_source",
            "The bucket (name only) in which to source the model predictions dataset from",
        ),
        (
            "--destination",
            "S3 URI to read imputed ASC-WDS and PIR data from",
        ),
        (
            "--summary_destination",
            "S3 URI to save estimated filled posts data to",
        ),
    )

    main(
        direct_payments_merged_source=args.direct_payments_merged_source,
        destination=args.destination,
        summary_destination=args.summary_destination,
    )

    print("Finished estimate direct payments job")
