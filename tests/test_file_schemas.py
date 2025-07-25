from dataclasses import dataclass

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    ArrayType,
    DateType,
    DoubleType,
    BooleanType,
    MapType,
)

from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerCareHomeCleanColumns as CTCHClean,
    CapacityTrackerNonResCleanColumns as CTNRClean,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.cleaned_data_files.cqc_pir_cleaned import (
    CqcPIRCleanedColumns as CQCPIRClean,
)
from utils.column_names.coverage_columns import CoverageColumns
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
    IndCqcColumns as IndCQC,
    NullGroupedProviderColumns as NGPcol,
)
from utils.column_names.raw_data_files.ascwds_worker_columns import (
    AscwdsWorkerColumns as AWK,
)
from utils.column_names.raw_data_files.ascwds_workplace_columns import (
    AscwdsWorkplaceColumns as AWP,
)
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_names.validation_table_columns import Validation


@dataclass
class ExtractRegisteredManagerNamesSchema:
    extract_registered_manager_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
            StructField(
                CQCLClean.imputed_regulated_activities,
                ArrayType(
                    StructType(
                        [
                            StructField(CQCL.name, StringType(), True),
                            StructField(CQCL.code, StringType(), True),
                            StructField(
                                CQCL.contacts,
                                ArrayType(
                                    StructType(
                                        [
                                            StructField(
                                                CQCL.person_family_name,
                                                StringType(),
                                                True,
                                            ),
                                            StructField(
                                                CQCL.person_given_name,
                                                StringType(),
                                                True,
                                            ),
                                            StructField(
                                                CQCL.person_roles,
                                                ArrayType(StringType(), True),
                                                True,
                                            ),
                                            StructField(
                                                CQCL.person_title, StringType(), True
                                            ),
                                        ]
                                    ),
                                    True,
                                ),
                                True,
                            ),
                        ]
                    ),
                    True,
                ),
                True,
            ),
        ]
    )

    extract_contacts_information_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
            StructField(
                CQCLClean.imputed_regulated_activities,
                ArrayType(
                    StructType(
                        [
                            StructField(CQCL.name, StringType(), True),
                            StructField(CQCL.code, StringType(), True),
                            StructField(
                                CQCL.contacts,
                                ArrayType(
                                    StructType(
                                        [
                                            StructField(
                                                CQCL.person_family_name,
                                                StringType(),
                                                True,
                                            ),
                                            StructField(
                                                CQCL.person_given_name,
                                                StringType(),
                                                True,
                                            ),
                                            StructField(
                                                CQCL.person_roles,
                                                ArrayType(StringType(), True),
                                                True,
                                            ),
                                            StructField(
                                                CQCL.person_title, StringType(), True
                                            ),
                                        ]
                                    ),
                                    True,
                                ),
                                True,
                            ),
                        ]
                    ),
                    True,
                ),
                True,
            ),
        ]
    )
    expected_extract_contacts_information_schema = StructType(
        [
            *extract_contacts_information_schema,
            StructField(
                CQCLClean.contacts_exploded,
                StructType(
                    [
                        StructField(
                            CQCL.person_family_name,
                            StringType(),
                            True,
                        ),
                        StructField(
                            CQCL.person_given_name,
                            StringType(),
                            True,
                        ),
                        StructField(
                            CQCL.person_roles,
                            ArrayType(StringType(), True),
                            True,
                        ),
                        StructField(CQCL.person_title, StringType(), True),
                    ]
                ),
            ),
        ]
    )

    select_and_create_full_name_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
            StructField(CQCL.care_home, StringType(), True),
            StructField(
                CQCLClean.contacts_exploded,
                StructType(
                    [
                        StructField(
                            CQCL.person_family_name,
                            StringType(),
                            True,
                        ),
                        StructField(
                            CQCL.person_given_name,
                            StringType(),
                            True,
                        ),
                        StructField(
                            CQCL.person_roles,
                            ArrayType(StringType(), True),
                            True,
                        ),
                        StructField(CQCL.person_title, StringType(), True),
                    ]
                ),
            ),
        ]
    )
    expected_select_and_create_full_name_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
            StructField(CQCLClean.contacts_full_name, StringType(), True),
        ]
    )

    group_and_collect_names_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
            StructField(CQCL.care_home, StringType(), True),
            StructField(CQCLClean.contacts_full_name, StringType(), True),
        ]
    )
    expected_group_and_collect_names_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
            StructField(
                CQCLClean.registered_manager_names, ArrayType(StringType(), True), True
            ),
        ]
    )

    original_test_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
            StructField(CQCL.care_home, StringType(), True),
            StructField(CQCL.number_of_beds, IntegerType(), True),
        ]
    )
    registered_manager_names_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
            StructField(
                CQCLClean.registered_manager_names, ArrayType(StringType(), True), True
            ),
        ]
    )
    expected_join_with_original_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
            StructField(CQCL.care_home, StringType(), True),
            StructField(CQCL.number_of_beds, IntegerType(), True),
            StructField(
                CQCLClean.registered_manager_names, ArrayType(StringType(), True), True
            ),
        ]
    )


@dataclass
class UtilsSchema:
    cqc_pir_schema = StructType(
        [
            StructField(CQCPIRClean.location_id, StringType(), True),
            StructField(CQCPIRClean.care_home, StringType(), True),
            StructField(CQCPIRClean.cqc_pir_import_date, DateType(), True),
            StructField(CQCPIRClean.pir_submission_date_as_date, DateType(), True),
        ]
    )

    filter_to_max_value_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("date_type_column", DateType(), True),
            StructField("import_date_style_col", StringType(), True),
        ]
    )

    select_rows_with_value_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("value_to_filter_on", StringType(), True),
        ]
    )

    select_rows_with_non_null_values_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("column_with_nulls", FloatType(), True),
        ]
    )


@dataclass
class CleaningUtilsSchemas:
    worker_schema = StructType(
        [
            StructField(AWK.worker_id, StringType(), True),
            StructField(AWK.gender, StringType(), True),
            StructField(AWK.nationality, StringType(), True),
        ]
    )

    expected_schema_with_new_columns = StructType(
        [
            StructField(AWK.worker_id, StringType(), True),
            StructField(AWK.gender, StringType(), True),
            StructField(AWK.nationality, StringType(), True),
            StructField("gender_labels", StringType(), True),
            StructField("nationality_labels", StringType(), True),
        ]
    )

    scale_schema = StructType(
        [
            StructField("int", IntegerType(), True),
            StructField("float", FloatType(), True),
            StructField("non_scale", StringType(), True),
        ]
    )

    expected_scale_schema = StructType(
        [
            *scale_schema,
            StructField("bound_int", IntegerType(), True),
            StructField("bound_float", FloatType(), True),
        ]
    )

    sample_col_to_date_schema = StructType(
        [
            StructField("input_string", StringType(), True),
            StructField("expected_value", DateType(), True),
        ]
    )

    align_dates_primary_schema = StructType(
        [
            StructField(AWPClean.ascwds_workplace_import_date, DateType(), True),
            StructField(AWPClean.location_id, StringType(), True),
        ]
    )

    align_dates_secondary_schema = StructType(
        [
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
            StructField(CQCLClean.location_id, StringType(), True),
        ]
    )

    primary_dates_schema = StructType(
        [
            StructField(AWPClean.ascwds_workplace_import_date, DateType(), True),
        ]
    )

    secondary_dates_schema = StructType(
        [
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
        ]
    )

    expected_aligned_dates_schema = StructType(
        [
            StructField(AWPClean.ascwds_workplace_import_date, DateType(), True),
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
        ]
    )

    expected_merged_dates_schema = StructType(
        [
            StructField(AWPClean.ascwds_workplace_import_date, DateType(), True),
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
            StructField(CQCL.location_id, StringType(), True),
        ]
    )

    reduce_dataset_to_earliest_file_per_month_schema = StructType(
        [
            StructField(CQCLClean.location_id, StringType(), True),
            StructField(CQCLClean.import_date, StringType(), True),
            StructField(Keys.year, StringType(), True),
            StructField(Keys.month, StringType(), True),
            StructField(Keys.day, StringType(), True),
        ]
    )

    cast_to_int_schema = StructType(
        [
            StructField(AWP.location_id, StringType(), True),
            StructField(AWP.total_staff, StringType(), True),
            StructField(AWP.worker_records, StringType(), True),
        ]
    )

    cast_to_int_expected_schema = StructType(
        [
            StructField(AWP.location_id, StringType(), True),
            StructField(AWP.total_staff, IntegerType(), True),
            StructField(AWP.worker_records, IntegerType(), True),
        ]
    )

    filled_posts_per_bed_ratio_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup, DoubleType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.care_home, StringType(), True),
        ]
    )
    expected_filled_posts_per_bed_ratio_schema = StructType(
        [
            *filled_posts_per_bed_ratio_schema,
            StructField(IndCQC.filled_posts_per_bed_ratio, DoubleType(), True),
        ]
    )

    filled_posts_from_beds_and_ratio_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.filled_posts_per_bed_ratio, DoubleType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
        ]
    )
    expected_filled_posts_from_beds_and_ratio_schema = StructType(
        [
            *filled_posts_from_beds_and_ratio_schema,
            StructField(IndCQC.care_home_model, DoubleType(), True),
        ]
    )

    remove_duplicate_locationids_schema = StructType(
        [
            StructField(AWPClean.ascwds_workplace_import_date, DateType(), True),
            StructField(AWPClean.location_id, StringType(), True),
            StructField(AWPClean.master_update_date, DateType(), True),
        ]
    )

    create_banded_bed_count_column_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
        ]
    )
    expected_create_banded_bed_count_column_schema = StructType(
        [
            *create_banded_bed_count_column_schema,
            StructField(IndCQC.number_of_beds_banded, FloatType(), True),
        ]
    )


@dataclass
class FilterCleanedValuesSchema:
    sample_schema = StructType(
        [
            StructField("year", StringType(), True),
            StructField("month", StringType(), True),
            StructField("day", StringType(), True),
            StructField("import_date", StringType(), True),
        ]
    )


@dataclass
class IndCQCDataUtils:
    input_schema_for_adding_estimate_filled_posts_and_source = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField("model_name_1", DoubleType(), True),
            StructField("model_name_2", DoubleType(), True),
            StructField("model_name_3", DoubleType(), True),
        ]
    )

    expected_schema_with_estimate_filled_posts_and_source = StructType(
        [
            *input_schema_for_adding_estimate_filled_posts_and_source,
            StructField(IndCQC.estimate_filled_posts, DoubleType(), True),
            StructField(IndCQC.estimate_filled_posts_source, StringType(), True),
        ]
    )

    merge_columns_in_order_when_df_has_columns_of_multiple_datatypes_schema = (
        StructType(
            [
                StructField(IndCQC.location_id, StringType(), True),
                StructField(IndCQC.care_home_model, DoubleType(), True),
                StructField(
                    IndCQC.ascwds_job_role_ratios, MapType(StringType(), DoubleType())
                ),
            ]
        )
    )

    merge_columns_in_order_when_columns_are_datatype_string_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.ascwds_filled_posts_source, StringType(), True),
            StructField(
                IndCQC.ascwds_job_role_ratios_merged_source, StringType(), True
            ),
        ]
    )

    merge_columns_in_order_using_map_columns_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(
                IndCQC.ascwds_job_role_ratios, MapType(StringType(), DoubleType()), True
            ),
            StructField(
                IndCQC.ascwds_job_role_rolling_ratio,
                MapType(StringType(), DoubleType()),
                True,
            ),
        ]
    )

    expected_merge_columns_in_order_using_map_columns_schema = StructType(
        [
            *merge_columns_in_order_using_map_columns_schema,
            StructField(
                IndCQC.ascwds_job_role_ratios_merged,
                MapType(StringType(), DoubleType()),
                True,
            ),
            StructField(
                IndCQC.ascwds_job_role_ratios_merged_source, StringType(), True
            ),
        ]
    )

    estimated_source_description_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.estimate_filled_posts, FloatType(), True),
            StructField(IndCQC.estimate_filled_posts_source, StringType(), True),
        ]
    )

    get_selected_value_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.unix_time, IntegerType(), False),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, FloatType(), True),
            StructField(IndCQC.posts_rolling_average_model, FloatType(), True),
        ]
    )
    expected_get_selected_value_schema = StructType(
        [
            *get_selected_value_schema,
            StructField("new_column", FloatType(), True),
        ]
    )

    allocate_primary_service_type_second_level_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(
                CQCLClean.imputed_gac_service_types,
                ArrayType(
                    StructType(
                        [
                            StructField(CQCL.name, StringType(), True),
                            StructField(CQCL.description, StringType(), True),
                        ]
                    )
                ),
            ),
        ]
    )
    expected_allocate_primary_service_type_second_level_schema = StructType(
        [
            *allocate_primary_service_type_second_level_schema,
            StructField(IndCQC.primary_service_type_second_level, StringType(), True),
        ]
    )


@dataclass
class CalculateAscwdsFilledPostsSchemas:
    calculate_ascwds_filled_posts_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.total_staff_bounded, IntegerType(), True),
            StructField(IndCQC.worker_records_bounded, IntegerType(), True),
            StructField(IndCQC.ascwds_filled_posts, DoubleType(), True),
            StructField(IndCQC.ascwds_filled_posts_source, StringType(), True),
        ]
    )


@dataclass
class CalculateAscwdsFilledPostsTotalStaffEqualWorkerRecordsSchemas:
    calculate_ascwds_filled_posts_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.total_staff_bounded, IntegerType(), True),
            StructField(IndCQC.worker_records_bounded, IntegerType(), True),
            StructField(IndCQC.ascwds_filled_posts, DoubleType(), True),
            StructField(IndCQC.ascwds_filled_posts_source, StringType(), True),
        ]
    )


@dataclass
class CalculateAscwdsFilledPostsDifferenceInRangeSchemas:
    calculate_ascwds_filled_posts_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.total_staff_bounded, IntegerType(), True),
            StructField(IndCQC.worker_records_bounded, IntegerType(), True),
            StructField(IndCQC.ascwds_filled_posts, DoubleType(), True),
            StructField(IndCQC.ascwds_filled_posts_source, StringType(), True),
        ]
    )


@dataclass
class CleanAscwdsFilledPostOutliersSchema:
    unfiltered_ind_cqc_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.provider_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup, DoubleType(), True),
        ]
    )


@dataclass
class WinsorizeCareHomeFilledPostsPerBedRatioOutliersSchema:
    ind_cqc_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.number_of_beds_banded, FloatType(), True),
            StructField(IndCQC.ascwds_filled_posts, DoubleType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup, DoubleType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, DoubleType(), True),
            StructField(IndCQC.filled_posts_per_bed_ratio, DoubleType(), True),
            StructField(IndCQC.ascwds_filtering_rule, StringType(), True),
        ]
    )

    filter_df_to_care_homes_with_known_beds_and_filled_posts_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, DoubleType(), True),
        ]
    )

    calculate_standardised_residuals_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, DoubleType(), True),
            StructField(IndCQC.expected_filled_posts, DoubleType(), True),
        ]
    )
    expected_calculate_standardised_residuals_schema = StructType(
        [
            *calculate_standardised_residuals_schema,
            StructField(IndCQC.standardised_residual, DoubleType(), True),
        ]
    )

    standardised_residual_percentile_cutoff_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(IndCQC.standardised_residual, DoubleType(), True),
        ]
    )

    expected_standardised_residual_percentile_cutoff_with_percentiles_schema = (
        StructType(
            [
                StructField(IndCQC.location_id, StringType(), True),
                StructField(IndCQC.primary_service_type, StringType(), True),
                StructField(IndCQC.standardised_residual, DoubleType(), True),
                StructField(IndCQC.lower_percentile, DoubleType(), True),
                StructField(IndCQC.upper_percentile, DoubleType(), True),
            ]
        )
    )

    duplicate_ratios_within_standardised_residual_cutoff_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.filled_posts_per_bed_ratio, DoubleType(), True),
            StructField(IndCQC.standardised_residual, DoubleType(), True),
            StructField(IndCQC.lower_percentile, DoubleType(), True),
            StructField(IndCQC.upper_percentile, DoubleType(), True),
        ]
    )

    expected_duplicate_ratios_within_standardised_residual_cutoff_schema = StructType(
        [
            *duplicate_ratios_within_standardised_residual_cutoff_schema,
            StructField(
                IndCQC.filled_posts_per_bed_ratio_within_std_resids, DoubleType(), True
            ),
        ]
    )

    min_and_max_permitted_ratios_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(
                IndCQC.filled_posts_per_bed_ratio_within_std_resids, DoubleType(), True
            ),
            StructField(IndCQC.number_of_beds_banded, FloatType(), True),
        ]
    )
    expected_min_and_max_permitted_ratios_schema = StructType(
        [
            *min_and_max_permitted_ratios_schema,
            StructField(IndCQC.min_filled_posts_per_bed_ratio, DoubleType(), True),
            StructField(IndCQC.max_filled_posts_per_bed_ratio, DoubleType(), True),
        ]
    )

    set_minimum_permitted_ratio_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.filled_posts_per_bed_ratio, DoubleType(), True),
        ]
    )

    winsorize_outliers_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, FloatType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.filled_posts_per_bed_ratio, FloatType(), True),
            StructField(IndCQC.min_filled_posts_per_bed_ratio, FloatType(), True),
            StructField(IndCQC.max_filled_posts_per_bed_ratio, FloatType(), True),
        ]
    )

    combine_dataframes_care_home_schema = StructType(
        [
            *ind_cqc_schema,
            StructField("additional column", DoubleType(), True),
        ]
    )

    combine_dataframes_non_care_home_schema = ind_cqc_schema

    expected_combined_dataframes_schema = combine_dataframes_non_care_home_schema


@dataclass
class EstimateIndCQCFilledPostsSchemas:
    cleaned_ind_cqc_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.current_region, StringType(), True),
            StructField(IndCQC.contemporary_region, StringType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.services_offered, ArrayType(StringType()), True),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(IndCQC.pir_people_directly_employed_dedup, IntegerType(), True),
            StructField(IndCQC.ascwds_filled_posts, FloatType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, FloatType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.cqc_sector, StringType(), True),
            StructField(IndCQC.current_rural_urban_indicator_2011, StringType(), True),
            StructField(
                IndCQC.contemporary_rural_urban_indicator_2011, StringType(), True
            ),
            StructField(IndCQC.ascwds_filled_posts_source, StringType(), True),
            StructField(IndCQC.registration_status, StringType(), True),
        ]
    )


@dataclass
class ValidationUtils:
    validation_schema = StructType(
        [
            StructField(Validation.check, StringType(), True),
            StructField(Validation.check_level, StringType(), True),
            StructField(Validation.check_status, StringType(), True),
            StructField(Validation.constraint, StringType(), True),
            StructField(Validation.constraint_status, StringType(), True),
            StructField(Validation.constraint_message, StringType(), True),
        ]
    )

    size_of_dataset_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
        ]
    )

    index_column_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
        ]
    )

    min_values_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
        ]
    )
    min_values_multiple_columns_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(
                IndCQC.pir_people_directly_employed_cleaned, IntegerType(), True
            ),
        ]
    )

    max_values_schema = min_values_schema
    max_values_multiple_columns_schema = min_values_multiple_columns_schema

    one_column_schema = size_of_dataset_schema
    two_column_schema = index_column_schema
    multiple_rules_schema = index_column_schema

    categorical_values_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_sector, StringType(), True),
        ]
    )

    distinct_values_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_sector, StringType(), True),
        ]
    )

    distinct_values_multiple_columns_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_sector, StringType(), True),
            StructField(IndCQC.dormancy, StringType(), True),
        ]
    )
    add_column_with_length_of_string_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
        ]
    )
    expected_add_column_with_length_of_string_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(Validation.location_id_length, IntegerType(), True),
        ]
    )
    custom_type_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.primary_service_type, StringType(), True),
        ]
    )


@dataclass
class ValidateEstimatedIndCqcFilledPostsData:
    cleaned_ind_cqc_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
        ]
    )
    estimated_ind_cqc_filled_posts_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.ascwds_workplace_import_date, DateType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.cqc_sector, StringType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.primary_service_type, StringType(), True),
            StructField(IndCQC.current_ons_import_date, DateType(), True),
            StructField(IndCQC.current_cssr, StringType(), True),
            StructField(IndCQC.current_region, StringType(), True),
            StructField(
                IndCQC.pir_people_directly_employed_cleaned, IntegerType(), True
            ),
            StructField(IndCQC.total_staff_bounded, IntegerType(), True),
            StructField(IndCQC.worker_records_bounded, IntegerType(), True),
            StructField(IndCQC.ascwds_filled_posts_source, StringType(), True),
            StructField(IndCQC.ascwds_filled_posts, DoubleType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, DoubleType(), True),
            StructField(IndCQC.pir_people_directly_employed_dedup, IntegerType(), True),
            StructField(IndCQC.unix_time, IntegerType(), True),
            StructField(IndCQC.estimate_filled_posts, DoubleType(), True),
            StructField(IndCQC.estimate_filled_posts_source, StringType(), True),
            StructField(IndCQC.posts_rolling_average_model, DoubleType(), True),
            StructField(IndCQC.care_home_model, DoubleType(), True),
        ]
    )
    calculate_expected_size_schema = cleaned_ind_cqc_schema


@dataclass
class RawDataAdjustments:
    worker_data_schema = StructType(
        [
            StructField(AWK.worker_id, StringType(), True),
            StructField(AWK.import_date, StringType(), True),
            StructField(AWK.establishment_id, StringType(), True),
            StructField("other_column", StringType(), True),
        ]
    )

    workplace_data_schema = StructType(
        [
            StructField(AWP.import_date, StringType(), True),
            StructField(AWP.establishment_id, StringType(), True),
            StructField("other_column", StringType(), True),
        ]
    )

    locations_data_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField("other_column", StringType(), True),
        ]
    )


@dataclass
class ASCWDSFilteringUtilsSchemas:
    add_filtering_column_schema = StructType(
        [
            StructField(
                IndCQC.location_id,
                StringType(),
                True,
            ),
            StructField(
                IndCQC.ascwds_filled_posts_dedup_clean,
                FloatType(),
                True,
            ),
        ]
    )
    expected_add_filtering_column_schema = StructType(
        [
            *add_filtering_column_schema,
            StructField(
                IndCQC.ascwds_filtering_rule,
                StringType(),
                True,
            ),
        ]
    )
    update_filtering_rule_schema = StructType(
        [
            StructField(
                IndCQC.location_id,
                StringType(),
                True,
            ),
            StructField(
                IndCQC.ascwds_filled_posts_dedup,
                FloatType(),
                True,
            ),
            StructField(
                IndCQC.ascwds_filled_posts_dedup_clean,
                FloatType(),
                True,
            ),
            StructField(
                IndCQC.ascwds_filtering_rule,
                StringType(),
                True,
            ),
        ]
    )


@dataclass
class NullFilledPostsUsingInvalidMissingDataCodeSchema:
    null_filled_posts_using_invalid_missing_data_code_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup, DoubleType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, DoubleType(), True),
            StructField(IndCQC.ascwds_filtering_rule, StringType(), True),
        ]
    )


@dataclass
class NullGroupedProvidersSchema:
    null_grouped_providers_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.provider_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.establishment_id, StringType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup, DoubleType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, DoubleType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.filled_posts_per_bed_ratio, DoubleType(), True),
            StructField(IndCQC.ascwds_filtering_rule, StringType(), True),
            StructField(IndCQC.pir_people_directly_employed_dedup, DoubleType(), True),
        ]
    )

    calculate_data_for_grouped_provider_identification_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.provider_id, StringType(), True),
            StructField(IndCQC.cqc_location_import_date, DateType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.establishment_id, StringType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, DoubleType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.pir_people_directly_employed_dedup, DoubleType(), True),
        ]
    )
    expected_calculate_data_for_grouped_provider_identification_schema = StructType(
        [
            *calculate_data_for_grouped_provider_identification_schema,
            StructField(NGPcol.location_pir_average, DoubleType(), True),
            StructField(NGPcol.count_of_cqc_locations_in_provider, IntegerType(), True),
            StructField(
                NGPcol.count_of_awcwds_locations_in_provider, IntegerType(), True
            ),
            StructField(
                NGPcol.count_of_awcwds_locations_with_data_in_provider,
                IntegerType(),
                True,
            ),
            StructField(NGPcol.number_of_beds_at_provider, IntegerType(), True),
            StructField(NGPcol.provider_pir_count, IntegerType(), True),
            StructField(NGPcol.provider_pir_sum, DoubleType(), True),
        ]
    )

    identify_potential_grouped_providers_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(NGPcol.count_of_cqc_locations_in_provider, IntegerType(), True),
            StructField(
                NGPcol.count_of_awcwds_locations_in_provider, IntegerType(), True
            ),
            StructField(
                NGPcol.count_of_awcwds_locations_with_data_in_provider,
                IntegerType(),
                True,
            ),
        ]
    )
    expected_identify_potential_grouped_providers_schema = StructType(
        [
            *identify_potential_grouped_providers_schema,
            StructField(
                NGPcol.count_of_awcwds_locations_with_data_in_provider,
                BooleanType(),
                True,
            ),
        ]
    )

    null_care_home_grouped_providers_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup, DoubleType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, DoubleType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(NGPcol.number_of_beds_at_provider, IntegerType(), True),
            StructField(IndCQC.filled_posts_per_bed_ratio, DoubleType(), True),
            StructField(NGPcol.potential_grouped_provider, BooleanType(), True),
            StructField(IndCQC.ascwds_filtering_rule, StringType(), True),
        ]
    )

    null_non_res_grouped_providers_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.care_home, StringType(), True),
            StructField(NGPcol.potential_grouped_provider, BooleanType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup, DoubleType(), True),
            StructField(IndCQC.ascwds_filled_posts_dedup_clean, DoubleType(), True),
            StructField(NGPcol.location_pir_average, DoubleType(), True),
            StructField(NGPcol.provider_pir_count, IntegerType(), True),
            StructField(NGPcol.provider_pir_sum, DoubleType(), True),
            StructField(IndCQC.ascwds_filtering_rule, StringType(), True),
        ]
    )
