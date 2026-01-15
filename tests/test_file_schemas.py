from dataclasses import dataclass

from pyspark.sql.types import (
    ArrayType,
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
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
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
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
