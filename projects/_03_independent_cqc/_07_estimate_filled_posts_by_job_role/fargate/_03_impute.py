from dataclasses import dataclass

import polars as pl

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.impute_utils as iUtils
from polars_utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

# Set streaming chunk size for memory management - each thread (per CPU core) will load
# in a chunk of this size.
pl.Config.set_streaming_chunk_size(50000)


@dataclass
class NumericalValues:
    extrapolation_period: str = "2y"
    interpolation_cap_period: str = "5y"


def main(
    cleaned_data_source: str,
    imputed_data_destination: str,
) -> None:
    """
    Creates estimates of filled posts split by main job role.

    Args:
        cleaned_data_source (str): path to the cleaned data
        imputed_data_destination (str): destination for output
    """

    print("Imputing Cleaned dataset...")

    estimated_job_role_posts_lf = utils.scan_parquet(cleaned_data_source)
    print("Cleaned LazyFrame read in")

    estimated_job_role_posts_lf = iUtils.get_percent_share_ratios(
        estimated_job_role_posts_lf,
        input_col=IndCQC.ascwds_job_role_counts,
        output_col=IndCQC.ascwds_job_role_ratios,
    )

    # The trendline has to exist before the impute that extrapolates along it.
    estimated_job_role_posts_lf = iUtils.create_ascwds_job_role_rolling_ratio(
        estimated_job_role_posts_lf,
        extrapolation_period=NumericalValues.extrapolation_period,
        interpolation_cap_period=NumericalValues.interpolation_cap_period,
    )

    estimated_job_role_posts_lf = iUtils.add_imputed_ascwds_job_role_ratios(
        estimated_job_role_posts_lf
    )

    estimated_job_role_posts_lf = iUtils.add_imputed_ascwds_job_role_counts(
        estimated_job_role_posts_lf
    )

    utils.sink_to_parquet(
        lazy_df=estimated_job_role_posts_lf,
        output_path=imputed_data_destination,
    )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--cleaned_data_source",
            "Source s3 directory for merged data",
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
