import sys

from pyspark.sql import DataFrame

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
    PartitionKeys as Keys,
)
from utils.diagnostics_utils import diagnostics_utils as dUtils

partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]
estimate_filled_posts_columns: list = [
    IndCQC.location_id,
    IndCQC.cqc_location_import_date,
    IndCQC.ascwds_filled_posts_dedup_clean,
    IndCQC.primary_service_type,
    IndCQC.posts_rolling_average_model,
    IndCQC.care_home_model,
    IndCQC.non_res_with_dormancy_model,
    IndCQC.non_res_without_dormancy_model,
    IndCQC.non_res_pir_linear_regression_model,
    IndCQC.imputed_filled_post_model,
    IndCQC.imputed_posts_care_home_model,
    IndCQC.imputed_posts_non_res_with_dormancy_model,
    IndCQC.estimate_filled_posts,
    Keys.year,
    Keys.month,
    Keys.day,
    Keys.import_date,
]
absolute_value_cutoff: float = 10.0
percentage_value_cutoff: float = 0.25
standardised_value_cutoff: float = 1.0


def main(
    estimate_filled_posts_source,
    diagnostics_destination,
    summary_diagnostics_destination,
    charts_destination,
):
    print("Creating diagnostics for known filled posts")

    filled_posts_df: DataFrame = utils.read_from_parquet(
        estimate_filled_posts_source, estimate_filled_posts_columns
    )
    filled_posts_df = dUtils.filter_to_known_values(
        filled_posts_df, IndCQC.ascwds_filled_posts_dedup_clean
    )
    list_of_models = dUtils.create_list_of_models()
    filled_posts_df = dUtils.restructure_dataframe_to_column_wise(
        filled_posts_df, IndCQC.ascwds_filled_posts_dedup_clean, list_of_models
    )
    filled_posts_df = dUtils.filter_to_known_values(
        filled_posts_df, IndCQC.estimate_value
    )

    window = dUtils.create_window_for_model_and_service_splits()

    filled_posts_df = dUtils.calculate_distribution_metrics(filled_posts_df, window)
    filled_posts_df = dUtils.calculate_residuals(
        filled_posts_df, IndCQC.ascwds_filled_posts_dedup_clean
    )
    filled_posts_df = dUtils.calculate_aggregate_residuals(
        filled_posts_df,
        window,
        absolute_value_cutoff,
        percentage_value_cutoff,
        standardised_value_cutoff,
    )
    summary_df = dUtils.create_summary_diagnostics_table(filled_posts_df)

    utils.write_to_parquet(
        filled_posts_df,
        diagnostics_destination,
        mode="overwrite",
        partitionKeys=partition_keys,
    )
    utils.write_to_parquet(
        summary_df,
        summary_diagnostics_destination,
        mode="overwrite",
        partitionKeys=[IndCQC.primary_service_type],
    )


if __name__ == "__main__":
    print("Spark job 'diagnostics_on_known_filled_posts' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        estimate_filled_posts_source,
        diagnostics_destination,
        summary_diagnostics_destination,
        charts_destination,
    ) = utils.collect_arguments(
        (
            "--estimate_filled_posts_source",
            "Source s3 directory for job_estimates",
        ),
        (
            "--diagnostics_destination",
            "A destination directory for outputting full diagnostics tables.",
        ),
        (
            "--summary_diagnostics_destination",
            "A destination directory for outputting summary diagnostics tables.",
        ),
        (
            "--charts_destination",
            "A destination bucket name for saving pdf charts of the diagnostics data.",
        ),
    )

    main(
        estimate_filled_posts_source,
        diagnostics_destination,
        summary_diagnostics_destination,
        charts_destination,
    )

    print("Spark job 'diagnostics_on_known_filled_posts' complete")
