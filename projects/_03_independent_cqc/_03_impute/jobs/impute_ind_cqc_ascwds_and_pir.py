import sys
from dataclasses import dataclass

from pyspark.sql import DataFrame

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
    PartitionKeys as Keys,
)
from utils.estimate_filled_posts.models.primary_service_rate_of_change_trendline import (
    model_primary_service_rate_of_change_trendline,
)
from utils.estimate_filled_posts.models.imputation_with_extrapolation_and_interpolation import (
    model_imputation_with_extrapolation_and_interpolation,
)
from utils.estimate_filled_posts.models.rolling_average import (
    model_calculate_rolling_average,
)
from utils.estimate_filled_posts.models.utils import (
    clean_number_of_beds_banded,
    combine_care_home_ratios_and_non_res_posts,
    convert_care_home_ratios_to_filled_posts_and_merge_with_filled_post_values,
)
from projects._03_independent_cqc._03_impute.utils.model_and_merge_pir_filled_posts import (
    model_pir_filled_posts,
    merge_ascwds_and_pir_filled_post_submissions,
)

PartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]


@dataclass
class NumericalValues:
    number_of_days_in_window = 95  # Note: using 95 as a proxy for 3 months


def main(
    cleaned_ind_cqc_source: str,
    imputed_ind_cqc_ascwds_and_pir_destination: str,
    linear_regression_model_source: str,
) -> DataFrame:
    print("Imputing independent CQC ASCWDS and PIR values...")

    df = utils.read_from_parquet(cleaned_ind_cqc_source)

    df = utils.create_unix_timestamp_variable_from_date_column(
        df,
        date_col=IndCQC.cqc_location_import_date,
        date_format="yyyy-MM-dd",
        new_col_name=IndCQC.unix_time,
    )

    df = combine_care_home_ratios_and_non_res_posts(
        df,
        IndCQC.filled_posts_per_bed_ratio,
        IndCQC.ascwds_filled_posts_dedup_clean,
        IndCQC.combined_ratio_and_filled_posts,
    )

    df = model_primary_service_rate_of_change_trendline(
        df,
        IndCQC.combined_ratio_and_filled_posts,
        NumericalValues.number_of_days_in_window,
        IndCQC.ascwds_rate_of_change_trendline_model,
    )

    df = model_pir_filled_posts(df, linear_regression_model_source)

    df = merge_ascwds_and_pir_filled_post_submissions(df)

    df = model_imputation_with_extrapolation_and_interpolation(
        df,
        IndCQC.ascwds_pir_merged,
        IndCQC.ascwds_rate_of_change_trendline_model,
        IndCQC.imputed_filled_post_model,
        care_home=False,
    )

    df = model_imputation_with_extrapolation_and_interpolation(
        df,
        IndCQC.filled_posts_per_bed_ratio,
        IndCQC.ascwds_rate_of_change_trendline_model,
        IndCQC.imputed_filled_posts_per_bed_ratio_model,
        care_home=True,
    )

    df = model_calculate_rolling_average(
        df,
        IndCQC.imputed_filled_post_model,
        NumericalValues.number_of_days_in_window,
        IndCQC.primary_service_type,
        IndCQC.posts_rolling_average_model,
    )

    df = clean_number_of_beds_banded(df)

    df = model_calculate_rolling_average(
        df,
        IndCQC.imputed_filled_posts_per_bed_ratio_model,
        NumericalValues.number_of_days_in_window,
        [IndCQC.primary_service_type, IndCQC.number_of_beds_banded_cleaned],
        IndCQC.banded_bed_ratio_rolling_average_model,
    )

    df = convert_care_home_ratios_to_filled_posts_and_merge_with_filled_post_values(
        df,
        IndCQC.banded_bed_ratio_rolling_average_model,
        IndCQC.posts_rolling_average_model,
    )

    print(f"Exporting as parquet to {imputed_ind_cqc_ascwds_and_pir_destination}")

    utils.write_to_parquet(
        df,
        imputed_ind_cqc_ascwds_and_pir_destination,
        mode="overwrite",
        partitionKeys=PartitionKeys,
    )

    print("Completed imputing independent CQC ASCWDS and PIR")


if __name__ == "__main__":
    print("Spark job 'impute_ind_cqc_ascwds_and_pir' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        cleaned_ind_cqc_source,
        imputed_ind_cqc_ascwds_and_pir_destination,
        linear_regression_model_source,
    ) = utils.collect_arguments(
        (
            "--cleaned_ind_cqc_source",
            "Source s3 directory for cleaned_ind_cqc_filled_posts",
        ),
        (
            "--imputed_ind_cqc_ascwds_and_pir_destination",
            "Destination s3 directory for outputting imputed ind cqc ascwds and pir data",
        ),
        (
            "--linear_regression_model_source",
            "The location of the linear regression model in s3",
        ),
    )

    main(
        cleaned_ind_cqc_source,
        imputed_ind_cqc_ascwds_and_pir_destination,
        linear_regression_model_source,
    )
