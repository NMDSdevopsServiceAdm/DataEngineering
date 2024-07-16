import sys

from pyspark.sql import DataFrame, functions as F

from utils import utils
from utils.column_values.categorical_column_values import (
    CareHome,
    DataSource,
    PrimaryServiceType,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)


estimate_filled_posts_columns: list = [
    IndCQC.location_id,
    IndCQC.cqc_location_import_date,
    IndCQC.ascwds_filled_posts_clean,
    IndCQC.ascwds_filled_posts_dedup_clean,
    IndCQC.primary_service_type,
    IndCQC.rolling_average_model,
    IndCQC.care_home_model,
    IndCQC.extrapolation_care_home_model,
    IndCQC.interpolation_model,
    IndCQC.estimate_filled_posts,
]


def main(
    estimate_filled_posts_source,
    diagnostics_destination,
    summary_diagnostics_destination,
):
    print("Creating diagnostics for known filled posts")

    filled_posts_df: DataFrame = utils.read_from_parquet(
        estimate_filled_posts_source, estimate_filled_posts_columns
    )

    # filter to where ascwds clean is not null

    # reshape df so that cols are: location id, cqc_location_import date, service, ascwds_filled-posts_clean, estimate_source, estimate_value

    # drop rows where estimate value is missing

    # create windows for model/ service splits

    # calculate metrics for distribution (mean, sd, kurtosis, skewness)

    # calculate residuals (abs, %)

    # calculate aggregate residuals (avg abs, avg %, max, % within abs of actual, % within % of actual)

    # calculate date_diff for interpolation and extrapolation


if __name__ == "__main__":
    print("Spark job 'diagnostics_on_known_values' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        estimate_filled_posts_source,
        diagnostics_destination,
        summary_diagnostics_destination,
    ) = utils.collect_arguments(
        (
            "--estimate_filled_posts_source",
            "Source s3 directory for job_estimates",
        ),
        (
            "--diagnostics_destination",
            "A destination directory for outputting summary diagnostics tables.",
        ),
        (
            "--summary_diagnostics_destination",
            "A destination directory for outputting detailed residuals tables with which to make histograms.",
        ),
    )

    main(
        estimate_filled_posts_source,
        diagnostics_destination,
        summary_diagnostics_destination,
    )

    print("Spark job 'diagnostics_on_known_values' complete")
