import polars as pl

from polars_utils import utils
from projects._04_direct_payment_recipients.direct_payments_config import (
    HACKNEY_SERVICE_USER_DPRS_DURING_YEAR,
)
from projects._04_direct_payment_recipients.fargate.utils.prepare_dpr_utils.calculate_pa_ratio import (
    calculate_pa_ratio,
)
from projects._04_direct_payment_recipients.fargate.utils.prepare_dpr_utils.estimate_proportion_employing_staff import (
    estimate_proportion_employing_staff,
)
from projects._04_direct_payment_recipients.fargate.utils.prepare_dpr_utils.remove_outliers import (
    remove_outliers,
)
from utils.column_names.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)

survey_columns = [DP.YEAR, DP.TOTAL_STAFF_RECODED]
external_columns = [
    DP.LA_AREA,
    DP.YEAR,
    DP.DPRS_ADASS,
    DP.DPRS_EMPLOYING_STAFF_ADASS,
    DP.SERVICE_USER_DPRS_AT_YEAR_END,
    DP.CARER_DPRS_AT_YEAR_END,
    DP.SERVICE_USER_DPRS_DURING_YEAR,
    DP.PROPORTION_IMPORTED,
    DP.HISTORIC_SERVICE_USERS_EMPLOYING_STAFF_ESTIMATE,
]


def main(survey_source: str, external_source: str, destination: str) -> None:
    survey_lf = utils.scan_parquet(survey_source, selected_columns=survey_columns)
    direct_payments_lf = utils.scan_parquet(
        external_source, selected_columns=external_columns
    )

    pa_ratio_lf = calculate_pa_ratio(survey_lf)

    direct_payments_lf = estimate_proportion_employing_staff(direct_payments_lf)
    direct_payments_lf = remove_outliers(direct_payments_lf)

    service_user_dprs = pl.col(DP.SERVICE_USER_DPRS_DURING_YEAR)
    hackney_dprs = (
        pl.when(pl.col(DP.LA_AREA) == "Hackney")
        .then(
            pl.col(DP.YEAR_AS_INTEGER).replace_strict(
                HACKNEY_SERVICE_USER_DPRS_DURING_YEAR,
                default=None,
                return_dtype=pl.Float64,
            )
        )
        .otherwise(None)
    )
    direct_payments_lf = direct_payments_lf.with_columns(
        pl.coalesce(service_user_dprs, hackney_dprs).alias(
            DP.SERVICE_USER_DPRS_DURING_YEAR
        )
    ).with_columns(service_user_dprs.alias(DP.TOTAL_DPRS_DURING_YEAR))

    direct_payments_lf = direct_payments_lf.select(
        DP.LA_AREA,
        DP.YEAR,
        DP.YEAR_AS_INTEGER,
        DP.SERVICE_USER_DPRS_DURING_YEAR,
        DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
        DP.HISTORIC_SERVICE_USERS_EMPLOYING_STAFF_ESTIMATE,
        DP.TOTAL_DPRS_DURING_YEAR,
    ).join(
        pa_ratio_lf.rename({DP.RATIO_ROLLING_AVERAGE: DP.FILLED_POSTS_PER_EMPLOYER}),
        on=DP.YEAR_AS_INTEGER,
        how="left",
    )

    utils.sink_to_parquet(direct_payments_lf, destination)


if __name__ == "__main__":
    print("Running prepare direct payments job")

    args = utils.get_args(
        ("--survey_source", "S3 URI to read ingested IE/PA survey data from"),
        ("--external_source", "S3 URI to read external direct payments data from"),
        ("--destination", "S3 URI to save merged direct payments data to"),
    )

    main(
        survey_source=args.survey_source,
        external_source=args.external_source,
        destination=args.destination,
    )

    print("Finished prepare direct payments job")
