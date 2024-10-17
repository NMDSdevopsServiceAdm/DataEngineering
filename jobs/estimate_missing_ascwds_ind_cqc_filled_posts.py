import sys
from dataclasses import dataclass

from pyspark.sql import DataFrame, functions as F, Window

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
    PartitionKeys as Keys,
)
from utils.column_values.categorical_column_values import PrimaryServiceType
from utils.estimate_filled_posts.models.primary_service_rolling_average import (
    model_primary_service_rolling_average,
)
from utils.estimate_filled_posts.models.imputation_with_extrapolation_and_interpolation import (
    model_imputation_with_extrapolation_and_interpolation,
)
from utils.estimate_filled_posts.models.extrapolation import model_extrapolation


PartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]


@dataclass
class NumericalValues:
    NUMBER_OF_DAYS_IN_ROLLING_AVERAGE = 185  # Note: using 185 as a proxy for 6 months


def main(
    cleaned_ind_cqc_source: str,
    estimated_missing_ascwds_ind_cqc_destination: str,
) -> DataFrame:
    print("Estimating missing ASCWDS independent CQC filled posts...")

    cleaned_ind_cqc_df = utils.read_from_parquet(cleaned_ind_cqc_source)

    estimate_missing_ascwds_df = utils.create_unix_timestamp_variable_from_date_column(
        cleaned_ind_cqc_df,
        date_col=IndCQC.cqc_location_import_date,
        date_format="yyyy-MM-dd",
        new_col_name=IndCQC.unix_time,
    )

    estimate_missing_ascwds_df = model_primary_service_rolling_average(
        estimate_missing_ascwds_df,
        IndCQC.filled_posts_per_bed_ratio,
        IndCQC.ascwds_filled_posts_dedup_clean,
        NumericalValues.NUMBER_OF_DAYS_IN_ROLLING_AVERAGE,
        IndCQC.rolling_average_model,
    )

    estimate_missing_ascwds_df = model_imputation_with_extrapolation_and_interpolation(
        estimate_missing_ascwds_df,
        IndCQC.ascwds_filled_posts_dedup_clean,
        IndCQC.rolling_average_model,
        IndCQC.imputed_posts_rolling_avg_model,
        care_home=False,
    )

    estimate_missing_ascwds_df = model_imputation_with_extrapolation_and_interpolation(
        estimate_missing_ascwds_df,
        IndCQC.filled_posts_per_bed_ratio,
        IndCQC.rolling_average_model_filled_posts_per_bed_ratio,
        IndCQC.imputed_ratio_rolling_avg_model,
        care_home=True,
    )
    """
    estimate_missing_ascwds_df = model_extrapolation(
        estimate_missing_ascwds_df, IndCQC.rolling_average_model
    )  # TODO remove
        """
    print(f"Exporting as parquet to {estimated_missing_ascwds_ind_cqc_destination}")

    utils.write_to_parquet(
        estimate_missing_ascwds_df,
        estimated_missing_ascwds_ind_cqc_destination,
        mode="overwrite",
        partitionKeys=PartitionKeys,
    )

    print("Completed estimate missing ASCWDS independent CQC filled posts")


if __name__ == "__main__":
    print("Spark job 'estimate_missing_ascwds_ind_cqc_filled_posts' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        cleaned_ind_cqc_source,
        estimated_missing_ascwds_ind_cqc_destination,
    ) = utils.collect_arguments(
        (
            "--cleaned_ind_cqc_source",
            "Source s3 directory for cleaned_ind_cqc_filled_posts",
        ),
        (
            "--estimated_missing_ascwds_ind_cqc_destination",
            "Destination s3 directory for outputting estimate missing ASCWDS filled posts",
        ),
    )

    main(
        cleaned_ind_cqc_source,
        estimated_missing_ascwds_ind_cqc_destination,
    )
