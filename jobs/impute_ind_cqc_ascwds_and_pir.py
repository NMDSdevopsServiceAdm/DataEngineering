import sys
from dataclasses import dataclass

from pyspark.sql import DataFrame

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
    PartitionKeys as Keys,
)
from utils.estimate_filled_posts.models.primary_service_rate_of_change_trendline import (
    primary_service_rate_of_change_trendline,
)
from utils.estimate_filled_posts.models.imputation_with_extrapolation_and_interpolation import (
    model_imputation_with_extrapolation_and_interpolation,
)
from utils.estimate_filled_posts.models.rolling_average import (
    model_calculate_rolling_average,
)
from utils.estimate_filled_posts.models.utils import (
    combine_care_home_ratios_and_non_res_posts,
)
from utils.ind_cqc_filled_posts_utils.ascwds_pir_utils.blend_ascwds_pir import (
    blend_pir_and_ascwds_when_ascwds_out_of_date,
)


PartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]


@dataclass
class NumericalValues:
    NUMBER_OF_DAYS_IN_WINDOW = 185  # Note: using 185 as a proxy for 6 months


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

    df = primary_service_rate_of_change_trendline(
        df,
        IndCQC.combined_ratio_and_filled_posts,
        NumericalValues.NUMBER_OF_DAYS_IN_WINDOW,
        IndCQC.ascwds_rate_of_change_trendline_model,
    )

    df = blend_pir_and_ascwds_when_ascwds_out_of_date(
        df, linear_regression_model_source
    )

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

    df = model_imputation_with_extrapolation_and_interpolation(
        df,
        IndCQC.pir_people_directly_employed_dedup,
        IndCQC.ascwds_rate_of_change_trendline_model,
        IndCQC.imputed_non_res_pir_people_directly_employed,
        care_home=False,
    )

    df = combine_care_home_ratios_and_non_res_posts(
        df,
        IndCQC.imputed_filled_posts_per_bed_ratio_model,
        IndCQC.imputed_filled_post_model,
        IndCQC.combined_imputed_ratio_and_filled_posts,
    )

    df = model_calculate_rolling_average(
        df,
        IndCQC.combined_imputed_ratio_and_filled_posts,
        NumericalValues.NUMBER_OF_DAYS_IN_WINDOW,
        IndCQC.primary_service_type,
        IndCQC.rolling_average_model,
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
