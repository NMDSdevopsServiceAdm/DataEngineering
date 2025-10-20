import os
import sys

os.environ["SPARK_VERSION"] = "3.5"

from dataclasses import dataclass

from projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.utils import (
    utils as JRutils,
)
from projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.utils.models.extrapolation import (
    extrapolate_job_role_ratios,
)
from projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.utils.models.interpolation import (
    model_job_role_ratio_interpolation,
)
from projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.utils.models.primary_service_rolling_sum import (
    calculate_rolling_sum_of_job_roles,
)
from projects._03_independent_cqc.utils.utils import utils as FPutils
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.value_labels.ascwds_worker.ascwds_worker_jobgroup_dictionary import (
    AscwdsWorkerValueLabelsJobGroup as JobGroupDicts,
)

PartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]
cleaned_ascwds_worker_columns_to_import = [
    IndCQC.ascwds_worker_import_date,
    IndCQC.establishment_id,
    IndCQC.main_job_role_clean_labelled,
]
estimated_ind_cqc_filled_posts_columns_to_import = [
    IndCQC.cqc_location_import_date,
    IndCQC.unix_time,
    IndCQC.location_id,
    IndCQC.name,
    IndCQC.provider_id,
    IndCQC.services_offered,
    IndCQC.primary_service_type,
    IndCQC.care_home,
    IndCQC.dormancy,
    IndCQC.number_of_beds,
    IndCQC.imputed_gac_service_types,
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
    IndCQC.estimate_filled_posts,
    IndCQC.estimate_filled_posts_source,
    Keys.year,
    Keys.month,
    Keys.day,
    Keys.import_date,
]


@dataclass
class NumericalValues:
    number_of_days_in_rolling_sum = 185  # Note: using 185 as a proxy for 6 months


def main(
    estimated_ind_cqc_filled_posts_source: str,
    cleaned_ascwds_worker_source: str,
    estimated_ind_cqc_filled_posts_by_job_role_destination: str,
):
    """
    Creates estimates of filled posts split by main job role.

    Args:
        estimated_ind_cqc_filled_posts_source (str): path to the estimates ind cqc filled posts data
        cleaned_ascwds_worker_source (str): path to the cleaned worker data
        estimated_ind_cqc_filled_posts_by_job_role_destination (str): path to where to save the outputs
    """
    estimated_ind_cqc_filled_posts_df = utils.read_from_parquet(
        estimated_ind_cqc_filled_posts_source,
        selected_columns=estimated_ind_cqc_filled_posts_columns_to_import,
    )
    cleaned_ascwds_worker_df = utils.read_from_parquet(
        cleaned_ascwds_worker_source,
        selected_columns=cleaned_ascwds_worker_columns_to_import,
    )

    # Converted to polars
    aggregated_job_roles_per_establishment_df = (
        JRutils.aggregate_ascwds_worker_job_roles_per_establishment(
            cleaned_ascwds_worker_df, JRutils.list_of_job_roles_sorted
        )
    )

    # Converted to polars
    estimated_ind_cqc_filled_posts_by_job_role_df = JRutils.merge_dataframes(
        estimated_ind_cqc_filled_posts_df,
        aggregated_job_roles_per_establishment_df,
    )

    estimated_ind_cqc_filled_posts_by_job_role_df.cache()
    estimated_ind_cqc_filled_posts_by_job_role_df.count()

    estimated_ind_cqc_filled_posts_by_job_role_df = JRutils.remove_ascwds_job_role_count_when_estimate_filled_posts_source_not_ascwds(
        estimated_ind_cqc_filled_posts_by_job_role_df
    )

    estimated_ind_cqc_filled_posts_by_job_role_df = (
        JRutils.transform_job_role_count_map_to_ratios_map(
            estimated_ind_cqc_filled_posts_by_job_role_df,
            IndCQC.ascwds_job_role_counts,
            IndCQC.ascwds_job_role_ratios,
        )
    )

    estimated_ind_cqc_filled_posts_by_job_role_df = (
        JRutils.calculate_job_group_sum_from_job_role_map_column(
            estimated_ind_cqc_filled_posts_by_job_role_df,
            IndCQC.ascwds_job_role_counts,
            IndCQC.ascwds_job_group_counts,
        )
    )

    estimated_ind_cqc_filled_posts_by_job_role_df = (
        JRutils.calculate_job_group_sum_from_job_role_map_column(
            estimated_ind_cqc_filled_posts_by_job_role_df,
            IndCQC.ascwds_job_role_ratios,
            IndCQC.ascwds_job_group_ratios,
        )
    )

    estimated_ind_cqc_filled_posts_by_job_role_df = (
        JRutils.apply_quality_filters_to_ascwds_job_role_data(
            estimated_ind_cqc_filled_posts_by_job_role_df
        )
    )

    estimated_ind_cqc_filled_posts_by_job_role_df = (
        JRutils.transform_job_role_count_map_to_ratios_map(
            estimated_ind_cqc_filled_posts_by_job_role_df,
            IndCQC.ascwds_job_role_counts_filtered,
            IndCQC.ascwds_job_role_ratios_filtered,
        )
    )

    estimated_ind_cqc_filled_posts_by_job_role_df = model_job_role_ratio_interpolation(
        estimated_ind_cqc_filled_posts_by_job_role_df, JRutils.list_of_job_roles_sorted
    )

    estimated_ind_cqc_filled_posts_by_job_role_df.cache()
    estimated_ind_cqc_filled_posts_by_job_role_df.count()

    estimated_ind_cqc_filled_posts_by_job_role_df = extrapolate_job_role_ratios(
        estimated_ind_cqc_filled_posts_by_job_role_df
    )

    estimated_ind_cqc_filled_posts_by_job_role_df = (
        JRutils.combine_interpolated_and_extrapolated_job_role_ratios(
            estimated_ind_cqc_filled_posts_by_job_role_df
        )
    )

    estimated_ind_cqc_filled_posts_by_job_role_df = (
        JRutils.transform_imputed_job_role_ratios_to_counts(
            estimated_ind_cqc_filled_posts_by_job_role_df,
        )
    )

    estimated_ind_cqc_filled_posts_by_job_role_df = calculate_rolling_sum_of_job_roles(
        estimated_ind_cqc_filled_posts_by_job_role_df,
        NumericalValues.number_of_days_in_rolling_sum,
        JRutils.list_of_job_roles_sorted,
    )

    estimated_ind_cqc_filled_posts_by_job_role_df = (
        JRutils.transform_job_role_count_map_to_ratios_map(
            estimated_ind_cqc_filled_posts_by_job_role_df,
            IndCQC.ascwds_job_role_rolling_sum,
            IndCQC.ascwds_job_role_rolling_ratio,
        )
    )
    estimated_ind_cqc_filled_posts_by_job_role_df = (
        estimated_ind_cqc_filled_posts_by_job_role_df.drop(
            IndCQC.ascwds_job_role_rolling_sum
        )
    )

    estimated_ind_cqc_filled_posts_by_job_role_df.cache()
    estimated_ind_cqc_filled_posts_by_job_role_df.count()

    estimated_ind_cqc_filled_posts_by_job_role_df = FPutils.merge_columns_in_order(
        estimated_ind_cqc_filled_posts_by_job_role_df,
        [
            IndCQC.ascwds_job_role_ratios_filtered,
            IndCQC.ascwds_job_role_ratios_interpolated,
            IndCQC.ascwds_job_role_rolling_ratio,
        ],
        IndCQC.ascwds_job_role_ratios_merged,
        IndCQC.ascwds_job_role_ratios_merged_source,
    )

    estimated_ind_cqc_filled_posts_by_job_role_df = (
        JRutils.create_estimate_filled_posts_by_job_role_map_column(
            estimated_ind_cqc_filled_posts_by_job_role_df
        )
    )

    estimated_ind_cqc_filled_posts_by_job_role_df = JRutils.unpack_mapped_column(
        estimated_ind_cqc_filled_posts_by_job_role_df,
        IndCQC.estimate_filled_posts_by_job_role,
    )

    estimated_ind_cqc_filled_posts_by_job_role_df = (
        JRutils.count_registered_manager_names(
            estimated_ind_cqc_filled_posts_by_job_role_df
        )
    )

    estimated_ind_cqc_filled_posts_by_job_role_df = (
        JRutils.calculate_difference_between_estimate_and_cqc_registered_managers(
            estimated_ind_cqc_filled_posts_by_job_role_df
        )
    )

    estimated_ind_cqc_filled_posts_by_job_role_df = (
        JRutils.calculate_sum_and_proportion_split_of_non_rm_managerial_estimate_posts(
            estimated_ind_cqc_filled_posts_by_job_role_df
        )
    )

    estimated_ind_cqc_filled_posts_by_job_role_df.cache()
    estimated_ind_cqc_filled_posts_by_job_role_df.count()

    estimated_ind_cqc_filled_posts_by_job_role_df = (
        JRutils.recalculate_managerial_filled_posts(
            estimated_ind_cqc_filled_posts_by_job_role_df,
            JRutils.list_of_non_rm_managers,
        )
    )

    estimated_ind_cqc_filled_posts_by_job_role_df = (
        JRutils.overwrite_registered_manager_estimate_with_cqc_count(
            estimated_ind_cqc_filled_posts_by_job_role_df
        )
    )

    estimated_ind_cqc_filled_posts_by_job_role_df = (
        JRutils.recalculate_total_filled_posts(
            estimated_ind_cqc_filled_posts_by_job_role_df,
            JRutils.list_of_job_roles_sorted,
        )
    )

    estimated_ind_cqc_filled_posts_by_job_role_df = JRutils.calculate_difference_between_estimate_filled_posts_and_estimate_filled_posts_from_all_job_roles(
        estimated_ind_cqc_filled_posts_by_job_role_df,
    )

    estimated_ind_cqc_filled_posts_by_job_role_df = (
        JRutils.create_estimate_filled_posts_job_group_columns(
            estimated_ind_cqc_filled_posts_by_job_role_df,
            JobGroupDicts.job_role_to_job_group_dict,
        )
    )

    estimated_ind_cqc_filled_posts_by_job_role_df = (
        JRutils.create_job_role_estimates_data_validation_columns(
            estimated_ind_cqc_filled_posts_by_job_role_df
        )
    )

    utils.write_to_parquet(
        estimated_ind_cqc_filled_posts_by_job_role_df,
        estimated_ind_cqc_filled_posts_by_job_role_destination,
        "overwrite",
        PartitionKeys,
    )


if __name__ == "__main__":
    print("spark job: estimate_ind_cqc_filled_posts_by_job_role starting")
    print(f"job args: {sys.argv}")

    (
        estimated_ind_cqc_filled_posts_source,
        cleaned_ascwds_worker_source,
        estimated_ind_cqc_filled_posts_by_job_role_destination,
    ) = utils.collect_arguments(
        (
            "--estimated_ind_cqc_filled_posts_source",
            "Source s3 directory for estimated ind cqc filled posts data",
        ),
        (
            "--cleaned_ascwds_worker_source",
            "Source s3 directory for parquet ASCWDS worker cleaned dataset",
        ),
        (
            "--estimated_ind_cqc_filled_posts_by_job_role_destination",
            "Destination s3 directory",
        ),
    )

    main(
        estimated_ind_cqc_filled_posts_source,
        cleaned_ascwds_worker_source,
        estimated_ind_cqc_filled_posts_by_job_role_destination,
    )
