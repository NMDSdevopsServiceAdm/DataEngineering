from datetime import datetime, date
import sys

from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import StringType

from utils import utils
from utils.column_values.categorical_column_values import CareHome, DataSource
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)
from utils.diagnostics_utils.diagnostics_meta_data import (
    Variables as Values,
    Prefixes,
    CareWorkerToJobsRatio as Ratio,
    ResidualsRequired,
)

estimate_filled_posts_columns: list = [
    IndCQC.location_id,
    IndCQC.cqc_location_import_date,
    IndCQC.ascwds_filled_posts_dedup_clean,
    IndCQC.primary_service_type,
    IndCQC.rolling_average_model,
    IndCQC.care_home_model,
    IndCQC.extrapolation_care_home_model,
    IndCQC.interpolation_model,
    IndCQC.non_res_model,
    IndCQC.estimate_filled_posts,
    IndCQC.people_directly_employed,
]


def main(
    estimate_filled_posts_source,
    diagnostics_destination,
    residuals_destination,
):
    print("Creating diagnostics for known filled posts")

    filled_posts_df: DataFrame = utils.read_from_parquet(
        estimate_filled_posts_source, estimate_filled_posts_columns
    )


if __name__ == "__main__":
    print("Spark job 'diagnostics_on_known_values' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        estimate_filled_posts_source,
        diagnostics_destination,
        residuals_destination,
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
            "--residuals_destination",
            "A destination directory for outputting detailed residuals tables with which to make histograms.",
        ),
    )

    main(
        estimate_filled_posts_source,
        diagnostics_destination,
        residuals_destination,
    )

    print("Spark job 'diagnostics_on_known_values' complete")
