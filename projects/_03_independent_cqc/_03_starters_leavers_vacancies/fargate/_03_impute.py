import projects._03_independent_cqc._03_starters_leavers_vacancies.fargate.utils.impute_utils as iUtils
from polars_utils import utils
from projects._03_independent_cqc.utils.imputation.trendline import (
    add_imputed_trendline,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import (
    StartersLeaversVacanciesColumns as SLVCols,
)

EXTRAPOLATION_PERIOD = "2y"
INTERPOLATION_CAP_PERIOD = "5y"
ROLLING_AVERAGE_PERIOD = "6mo"
MIN_CONTRIBUTORS = 5
EXTRAPOLATION_METHOD = "ratio"
ROLLING_AVERAGE_GROUP_LEVELS = [
    [
        IndCQC.primary_service_type,
        IndCQC.current_region,
        IndCQC.published_job_role_label,
    ],
    [IndCQC.primary_service_type, IndCQC.published_job_role_label],
    [IndCQC.published_job_role_label],
]


def main(
    cleaned_data_source: str,
    imputed_data_destination: str,
) -> None:
    """
    Imputes the turnover rate for each location and job role.

    Short-term gaps are filled first, then the filled rates are averaged over a rolling
    window per group, weighted by employees. Every remaining gap is filled by scaling the
    known rates along the rolling average.

    Args:
        cleaned_data_source (str): path to the cleaned data
        imputed_data_destination (str): destination for output
    """
    lf = utils.scan_parquet(cleaned_data_source)

    lf = add_imputed_trendline(
        lf,
        known_columns=[SLVCols.ascwds_turnover_rate],
        trendline_columns=[SLVCols.ascwds_turnover_rate_imputed_trendline],
        group_columns=[IndCQC.location_id, IndCQC.published_job_role_label],
        extrapolation_period=EXTRAPOLATION_PERIOD,
        interpolation_cap_period=INTERPOLATION_CAP_PERIOD,
    )

    lf = iUtils.add_rolling_avg_rate(
        lf,
        rolling_period=ROLLING_AVERAGE_PERIOD,
        group_levels=ROLLING_AVERAGE_GROUP_LEVELS,
        min_contributors=MIN_CONTRIBUTORS,
    )

    lf = iUtils.add_imputed_rate(lf, extrapolation_method=EXTRAPOLATION_METHOD)

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
