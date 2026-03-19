from polars_utils import utils
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

cqc_partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]


def main(
    cleaned_ind_cqc_source: str,
    destination: str,
) -> None:
    print("Imputing independent CQC ASCWDS and PIR values...")

    lf = utils.scan_parquet(cleaned_ind_cqc_source)

    # create_unix_timestamp_variable_from_date_column

    # combine_care_home_and_non_res_values_into_single_column - combined_ratio_and_filled_posts

    # model_primary_service_rate_of_change_trendline - ascwds_rate_of_change_trendline_model

    # model_pir_filled_posts

    # merge_ascwds_and_pir_filled_post_submissions

    # model_imputation_with_extrapolation_and_interpolation - imputed_filled_post_model

    # model_imputation_with_extrapolation_and_interpolation - imputed_filled_posts_per_bed_ratio_model

    # model_calculate_rolling_average - posts_rolling_average_model

    # create_banded_bed_count_column

    # model_calculate_rolling_average - banded_bed_ratio_rolling_average_model

    # convert_care_home_ratios_to_posts

    # combine_care_home_and_non_res_values_into_single_column - ct_combined_care_home_and_non_res

    # model_primary_service_rate_of_change_trendline - ct_combined_care_home_and_non_res_rate_of_change_trendline

    # model_imputation_with_extrapolation_and_interpolation - ct_care_home_total_employed_imputed

    # model_imputation_with_extrapolation_and_interpolation - ct_non_res_care_workers_employed_imputed

    # nullify_ct_values_previous_to_first_submission

    print(f"Exporting as parquet to {destination}")

    utils.sink_to_parquet(
        lf,
        destination,
        partition_cols=cqc_partition_keys,
        append=False,
    )

    print("Completed imputing independent CQC ASCWDS and PIR")


if __name__ == "__main__":
    print("Running impute independent CQC ASCWDS and PIR job")

    args = utils.get_args(
        (
            "--cleaned_ind_cqc_source",
            "S3 URI to read cleaned independent CQC data from",
        ),
        (
            "--destination",
            "S3 URI to save imputed data to",
        ),
    )

    main(
        cleaned_ind_cqc_source=args.cleaned_ind_cqc_source,
        destination=args.destination,
    )

    print("Finished impute independent CQC ASCWDS and PIR job")
