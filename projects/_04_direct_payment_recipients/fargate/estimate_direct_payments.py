import polars as pl

from polars_utils import utils
from projects._04_direct_payment_recipients.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)
from utils.column_values.categorical_column_values import ContemporaryCSSR

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

la_name_replacements = {
    "Bath & N E Somerset": ContemporaryCSSR.bath_and_north_east_somerset,
    "Blackburn": ContemporaryCSSR.blackburn_with_darwen,
    "Bournemouth, Christchurch and Poole": ContemporaryCSSR.bournemouth_christchurch_and_poole,
    "Cornwall": ContemporaryCSSR.cornwall_and_isles_of_scilly,
    "East Riding": ContemporaryCSSR.east_riding_of_yorkshire,
    "Isles of Scilly": ContemporaryCSSR.cornwall_and_isles_of_scilly,
    "Medway Towns": ContemporaryCSSR.medway,
    "Southend": ContemporaryCSSR.southend_on_sea,
}


def main(
    direct_payments_merged_source: str,
    destination: str,
    summary_destination: str,
) -> None:
    lf = utils.scan_parquet(
        source=direct_payments_merged_source,
        selected_columns=direct_payments_columns,
    )

    lf.with_columns(pl.col(DP.LA_AREA).replace(la_name_replacements))

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
