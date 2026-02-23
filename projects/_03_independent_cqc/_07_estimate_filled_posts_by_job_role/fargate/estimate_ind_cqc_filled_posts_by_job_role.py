import polars as pl

from polars_utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_values.categorical_column_values import (
    EstimateFilledPostsSource,
)

partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

estimates_columns_to_import = [
    IndCQC.cqc_location_import_date,
    IndCQC.unix_time,
    IndCQC.location_id,
    IndCQC.name,
    IndCQC.provider_id,
    IndCQC.services_offered,
    IndCQC.primary_service_type,
    IndCQC.primary_service_type_second_level,
    IndCQC.care_home,
    IndCQC.dormancy,
    IndCQC.number_of_beds,
    IndCQC.imputed_registration_date,
    IndCQC.registered_manager_names,
    IndCQC.ascwds_workplace_import_date,
    IndCQC.establishment_id,
    IndCQC.organisation_id,
    IndCQC.worker_records_bounded,
    IndCQC.ascwds_filled_posts_dedup_clean,
    IndCQC.ascwds_pir_merged,
    IndCQC.ascwds_filtering_rule,
    IndCQC.current_ons_import_date,
    IndCQC.current_cssr,
    IndCQC.current_region,
    IndCQC.current_icb,
    IndCQC.current_rural_urban_indicator_2011,
    IndCQC.current_lsoa21,
    IndCQC.current_msoa21,
    IndCQC.estimate_filled_posts,
    IndCQC.estimate_filled_posts_source,
    Keys.year,
    Keys.month,
    Keys.day,
    Keys.import_date,
]
ascwds_columns_to_import = [
    IndCQC.ascwds_worker_import_date,
    IndCQC.establishment_id,
    IndCQC.main_job_role_clean_labelled,
    IndCQC.ascwds_job_role_counts,
]


def main(
    estimates_source: str,
    ascwds_job_role_counts_source: str,
    estimates_by_job_role_destination: str,
) -> None:
    """
    Creates estimates of filled posts split by main job role.

    Args:
        estimates_source (str): path to the estimates ind cqc filled posts data
        ascwds_job_role_counts_source (str): path to the prepared ascwds job role counts data
        estimates_by_job_role_destination (str): destination for output
    """
    estimated_posts_lf = utils.scan_parquet(
        source=estimates_source,
        selected_columns=estimates_columns_to_import,
    )
    ascwds_job_role_counts_lf = utils.scan_parquet(
        source=ascwds_job_role_counts_source,
        selected_columns=ascwds_columns_to_import,
    )

    estimated_job_role_posts_lf = join_worker_to_estimates_dataframe(
        estimated_posts_lf, ascwds_job_role_counts_lf
    )

    estimated_job_role_posts_lf = nullify_job_role_count_when_source_not_ascwds(
        estimated_job_role_posts_lf
    )

    utils.sink_to_parquet(
        lazy_df=estimated_job_role_posts_lf,
        output_path=estimates_by_job_role_destination,
        partition_cols=partition_keys,
        append=False,
    )


def join_worker_to_estimates_dataframe(
    estimated_filled_posts_lf: pl.LazyFrame,
    aggregated_job_roles_per_establishment_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Join the mainjrid_clean_labels and ascwds_job_role_counts columns from the aggregated worker LazyFrame into the estimated filled post LazyFrame.

    Join as left join where:
      left = estimated filled post LazyFrame
      right = aggregated worker LazyFrame
      where establishment_id matches and ascwds_workplace_import_date == ascwds_worker_import_date.

    Args:
        estimated_filled_posts_lf (pl.LazyFrame): A LazyFrame containing estimated filled posts per workplace.
        aggregated_job_roles_per_establishment_lf (pl.LazyFrame): A LazyFrame with job role counts per workplace.

    Returns:
        pl.LazyFrame: The estimated filled post LazyFrame with columns main_job_role_clean_labelled and ascwds_job_role_counts added.
    """

    merged_lf = estimated_filled_posts_lf.join(
        other=aggregated_job_roles_per_establishment_lf,
        left_on=[IndCQC.establishment_id, IndCQC.ascwds_workplace_import_date],
        right_on=[IndCQC.establishment_id, IndCQC.ascwds_worker_import_date],
        how="left",
    )

    return merged_lf


def nullify_job_role_count_when_source_not_ascwds(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Set job role counts to NULL when source is not ASCDWS.

    This is to ensure that we're only using ASCDWS job role data when ASCDWS data has
    been used for estimated filled posts.

    Args:
        lf (pl.LazyFrame): The estimated filled post by job role LazyFrame.

    Returns:
        pl.LazyFrame: Transformed LazyFrame with ASCDWS job role counts nullified.
    """
    return (
        lf
        # Nullify when conditions not met.
        # 1. Source must be "ascwds_pir_merged"
        # 2. Estimates must equal the value after dedup_clean step.
        .with_columns(
            # when combines comma-separated conditions with "&"
            pl.when(
                # First condition matches label in source column to a literal string.
                pl.col(IndCQC.estimate_filled_posts_source)
                == pl.lit(EstimateFilledPostsSource.ascwds_pir_merged),
                # Second condition compares float values.
                pl.col(IndCQC.estimate_filled_posts)
                == pl.col(IndCQC.ascwds_filled_posts_dedup_clean),
            ).then(IndCQC.ascwds_job_role_counts)
        )
    )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--estimates_source",
            "Source s3 directory for estimated ind cqc filled posts data",
        ),
        (
            "--ascwds_job_role_counts_source",
            "Source s3 directory for parquet ASCWDS worker job role counts dataset",
        ),
        (
            "--estimates_by_job_role_destination",
            "Destination s3 directory",
        ),
    )
    main(
        estimates_source=args.estimates_source,
        ascwds_job_role_counts_source=args.ascwds_job_role_counts_source,
        estimates_by_job_role_destination=args.estimates_by_job_role_destination,
    )
