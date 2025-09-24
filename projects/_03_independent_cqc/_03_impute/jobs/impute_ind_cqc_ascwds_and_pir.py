import os
import sys
from dataclasses import dataclass

os.environ["SPARK_VERSION"] = "3.5"

from pyspark.sql import DataFrame

import utils.cleaning_utils as cUtils
from projects._03_independent_cqc._03_impute.utils.model_and_merge_pir_filled_posts import (
    merge_ascwds_and_pir_filled_post_submissions,
    model_pir_filled_posts,
)
from projects._03_independent_cqc._03_impute.utils.utils import (
    combine_care_home_and_non_res_values_into_single_column,
)
from projects._03_independent_cqc._06_estimate_filled_posts.utils.models.imputation_with_extrapolation_and_interpolation import (
    model_imputation_with_extrapolation_and_interpolation,
)
from projects._03_independent_cqc._06_estimate_filled_posts.utils.models.primary_service_rate_of_change_trendline import (
    model_primary_service_rate_of_change_trendline,
)
from projects._03_independent_cqc._06_estimate_filled_posts.utils.models.rolling_average import (
    model_calculate_rolling_average,
)
from projects._03_independent_cqc._06_estimate_filled_posts.utils.models.utils import (
    convert_care_home_ratios_to_filled_posts_and_merge_with_filled_post_values,
)
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

PartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]


@dataclass
class NumericalValues:
    number_of_days_in_window: int = 95  # Note: using 95 as a proxy for 3 months
    max_number_of_days_to_interpolate_between: int = 185  # proxy for 6 months


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

    df = combine_care_home_and_non_res_values_into_single_column(
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
        max_days_between_submissions=NumericalValues.max_number_of_days_to_interpolate_between,
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

    df = cUtils.create_banded_bed_count_column(
        df,
        IndCQC.number_of_beds_banded_for_rolling_avg,
        [0, 1, 10, 15, 20, 25, 50, float("Inf")],
    )
    df = model_calculate_rolling_average(
        df,
        IndCQC.imputed_filled_posts_per_bed_ratio_model,
        NumericalValues.number_of_days_in_window,
        [IndCQC.primary_service_type, IndCQC.number_of_beds_banded_for_rolling_avg],
        IndCQC.banded_bed_ratio_rolling_average_model,
    )
    df = df.drop(IndCQC.number_of_beds_banded_for_rolling_avg)

    df = convert_care_home_ratios_to_filled_posts_and_merge_with_filled_post_values(
        df,
        IndCQC.banded_bed_ratio_rolling_average_model,
        IndCQC.posts_rolling_average_model,
    )

    df = df.persist()

    df = combine_care_home_and_non_res_values_into_single_column(
        df,
        IndCQC.ct_care_home_total_employed_cleaned,
        IndCQC.ct_non_res_care_workers_employed,
        IndCQC.ct_combined_care_home_and_non_res,
    )

    df = model_primary_service_rate_of_change_trendline(
        df,
        IndCQC.ct_combined_care_home_and_non_res,
        NumericalValues.number_of_days_in_window,
        IndCQC.ct_combined_care_home_and_non_res_rate_of_change_trendline,
        NumericalValues.max_number_of_days_to_interpolate_between,
    )

    df = model_imputation_with_extrapolation_and_interpolation(
        df,
        IndCQC.ct_care_home_total_employed_cleaned,
        IndCQC.ct_combined_care_home_and_non_res_rate_of_change_trendline,
        IndCQC.ct_care_home_total_employed_imputed,
        care_home=True,
    )

    df = model_imputation_with_extrapolation_and_interpolation(
        df,
        IndCQC.ct_non_res_care_workers_employed,
        IndCQC.ct_combined_care_home_and_non_res_rate_of_change_trendline,
        IndCQC.ct_non_res_care_workers_employed_imputed,
        care_home=False,
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
