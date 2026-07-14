from polars_utils import utils
from projects._03_independent_cqc.utils.imputation.imputation import model_imputation
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


def main(impute_prepared_ind_cqc_source: str, destination: str) -> None:
    """
    Impute values into ASC-WDS, PIR and Capacity Tracker data.

    Args:
        impute_prepared_ind_cqc_source (str): s3 path to the ind cqc data prepared for imputation.
        destination (str): s3 path to save the output data
    """
    lf = utils.scan_parquet(impute_prepared_ind_cqc_source)

    print("IND CQC LazyFrame prepared for imputation read in")

    lf = model_imputation(
        lf,
        IndCQC.ascwds_pir_merged,
        IndCQC.ascwds_rate_of_change_trendline_model,
        IndCQC.imputed_filled_post_model,
        care_home=False,
        extrapolation_method="ratio",
    )

    lf = model_imputation(
        lf,
        IndCQC.filled_posts_per_bed_ratio,
        IndCQC.ascwds_rate_of_change_trendline_model,
        IndCQC.imputed_filled_posts_per_bed_ratio_model,
        care_home=True,
        extrapolation_method="ratio",
    )

    lf = model_imputation(
        lf,
        IndCQC.ct_care_home_total_employed_cleaned,
        IndCQC.ct_combined_care_home_and_non_res_rate_of_change_trendline,
        IndCQC.ct_care_home_total_employed_imputed,
        care_home=True,
        extrapolation_method="ratio",
    )

    lf = model_imputation(
        lf,
        IndCQC.ct_non_res_care_workers_employed_cleaned,
        IndCQC.ct_combined_care_home_and_non_res_rate_of_change_trendline,
        IndCQC.ct_non_res_care_workers_employed_imputed,
        care_home=False,
        extrapolation_method="ratio",
    )

    lf = lf.with_columns(
        utils.nullify_ct_values_previous_to_first_submission(
            [
                IndCQC.ct_care_home_total_employed_imputed,
                IndCQC.ct_non_res_care_workers_employed_imputed,
            ],
        )
    )

    print(f"Exporting as parquet to {destination}")

    utils.sink_to_parquet(
        lf,
        destination,
    )

    print("Completed imputing independent CQC ASCWDS and PIR")


if __name__ == "__main__":
    print("Running impute independent CQC ASCWDS and PIR job")

    args = utils.get_args(
        (
            "--impute_prepared_ind_cqc_source",
            "S3 URI to read cleaned independent CQC data from",
        ),
        (
            "--destination",
            "S3 URI to save imputed data to",
        ),
    )

    main(
        impute_prepared_ind_cqc_source=args.impute_prepared_ind_cqc_source,
        destination=args.destination,
    )

    print("Finished impute independent CQC ASCWDS and PIR job")
