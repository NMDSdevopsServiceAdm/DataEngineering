import polars as pl

from polars_utils import utils
from projects._04_direct_payment_recipients.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)
from projects._04_direct_payment_recipients.direct_payments_config_polars import (
    DirectPaymentsMisspelledLaNames as LANameCorrections,
)
from projects._04_direct_payment_recipients.fargate.utils.estimate_direct_payments_utils.calculate_remaining_variables import (
    calculate_remaining_variables,
)
from projects._04_direct_payment_recipients.fargate.utils.estimate_direct_payments_utils.create_summary_table import (
    create_summary_table,
)
from projects._04_direct_payment_recipients.fargate.utils.estimate_direct_payments_utils.estimate_service_users_employing_staff import (
    calculate_estimated_service_users_employing_staff,
)
from projects._04_direct_payment_recipients.fargate.utils.estimate_direct_payments_utils.merge_cornwall_and_isles_of_scilly import (
    merge_cornwall_and_isles_of_scilly,
)

direct_payments_columns = [
    DP.LA_AREA,
    DP.YEAR_AS_INTEGER,
    DP.SERVICE_USER_DPRS_DURING_YEAR,
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

    lf = merge_cornwall_and_isles_of_scilly(lf)

    lf = lf.with_columns(
        pl.col(DP.LA_AREA).replace(LANameCorrections.DICT_TO_CORRECT_LA_NAMES)
    )

    lf = calculate_estimated_service_users_employing_staff(lf)

    lf = calculate_remaining_variables(lf)

    summary_lf = create_summary_table(lf)

    utils.sink_to_parquet(
        lf,
        destination,
        append=False,
    )

    utils.sink_to_parquet(
        summary_lf,
        summary_destination,
        append=False,
    )


if __name__ == "__main__":
    print("Running estimate direct payments job")

    args = utils.get_args(
        (
            "--direct_payments_merged_source",
            "S3 URI to read merged dpr data from",
        ),
        (
            "--destination",
            "S3 URI to save estimated dpr data to",
        ),
        (
            "--summary_destination",
            "S3 URI to save summary of estimated dpr data to",
        ),
    )

    main(
        direct_payments_merged_source=args.direct_payments_merged_source,
        destination=args.destination,
        summary_destination=args.summary_destination,
    )

    print("Finished estimate direct payments job")
