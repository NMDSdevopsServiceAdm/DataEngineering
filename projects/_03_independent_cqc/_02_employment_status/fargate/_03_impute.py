import projects._03_independent_cqc._02_employment_status.fargate.utils.impute_utils as iUtils
from polars_utils import utils

EXTRAPOLATION_PERIOD = "2y"
INTERPOLATION_CAP_PERIOD = "5y"
ROLLING_AVERAGE_PERIOD = "6mo"


def main(
    cleaned_data_source: str,
    imputed_data_destination: str,
) -> None:
    """
    Imputes values to fill gaps between and around known employment status values.

    Short-term gaps in each location and job role's percentages are filled first, then
    those percentages are averaged over a rolling window per primary service type, region
    and job role.

    Args:
        cleaned_data_source (str): path to the cleaned data
        imputed_data_destination (str): destination for output
    """
    lf = utils.scan_parquet(cleaned_data_source)

    lf = iUtils.add_short_term_imputed_percentages(
        lf,
        extrapolation_period=EXTRAPOLATION_PERIOD,
        interpolation_cap_period=INTERPOLATION_CAP_PERIOD,
    )

    lf = iUtils.add_rolling_average_percentages(
        lf,
        rolling_period=ROLLING_AVERAGE_PERIOD,
    )

    utils.sink_to_parquet(
        lazy_df=lf,
        output_path=imputed_data_destination,
    )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--cleaned_data_source",
            "Source s3 directory for cleaned data",
        ),
        (
            "--imputed_data_destination",
            "Destination s3 directory for imputed data",
        ),
    )
    main(
        cleaned_data_source=args.cleaned_data_source,
        imputed_data_destination=args.imputed_data_destination,
    )
