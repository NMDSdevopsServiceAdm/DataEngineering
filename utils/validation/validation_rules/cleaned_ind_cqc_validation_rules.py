from dataclasses import dataclass

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns,
)
from utils.column_values.categorical_columns_by_dataset import (
    CleanedIndCQCCategoricalValues as CatValues,
)
from utils.validation.validation_rule_custom_type import CustomValidationRules
from utils.validation.validation_rule_names import RuleNames as RuleName


@dataclass
class CleanedIndCqcValidationRules:
    rules_to_check = {
        RuleName.size_of_dataset: None,
        RuleName.complete_columns: [
            IndCqcColumns.location_id,
            IndCqcColumns.ascwds_workplace_import_date,
            IndCqcColumns.cqc_location_import_date,
            IndCqcColumns.care_home,
            IndCqcColumns.provider_id,
            IndCqcColumns.cqc_sector,
            IndCqcColumns.registration_status,
            IndCqcColumns.imputed_registration_date,
            IndCqcColumns.primary_service_type,
            IndCqcColumns.contemporary_ons_import_date,
            IndCqcColumns.contemporary_cssr,
            IndCqcColumns.contemporary_region,
            IndCqcColumns.current_ons_import_date,
            IndCqcColumns.current_cssr,
            IndCqcColumns.current_region,
            IndCqcColumns.current_rural_urban_indicator_2011,
            IndCqcColumns.ascwds_filtering_rule,
            IndCqcColumns.specialist_generalist_other_dementia,
            IndCqcColumns.specialist_generalist_other_lda,
            IndCqcColumns.specialist_generalist_other_mh,
        ],
        RuleName.index_columns: [
            IndCqcColumns.location_id,
            IndCqcColumns.cqc_location_import_date,
        ],
        RuleName.min_values: {
            IndCqcColumns.time_registered: 1,
            IndCqcColumns.number_of_beds: 1,
            IndCqcColumns.pir_people_directly_employed_cleaned: 1,
            IndCqcColumns.total_staff_bounded: 1,
            IndCqcColumns.worker_records_bounded: 1,
            IndCqcColumns.filled_posts_per_bed_ratio: 0.0,
        },
        RuleName.max_values: {
            IndCqcColumns.number_of_beds: 500,
            IndCqcColumns.pir_people_directly_employed_cleaned: 1500,
            IndCqcColumns.total_staff_bounded: 3000,
            IndCqcColumns.worker_records_bounded: 3000,
            IndCqcColumns.filled_posts_per_bed_ratio: 20.0,
        },
        RuleName.categorical_values_in_columns: {
            IndCqcColumns.care_home: CatValues.care_home_column_values.categorical_values,
            IndCqcColumns.cqc_sector: CatValues.sector_column_values.categorical_values,
            IndCqcColumns.registration_status: CatValues.registration_status_column_values.categorical_values,
            IndCqcColumns.dormancy: CatValues.dormancy_column_values.categorical_values,
            IndCqcColumns.primary_service_type: CatValues.primary_service_type_column_values.categorical_values,
            IndCqcColumns.contemporary_cssr: CatValues.contemporary_cssr_column_values.categorical_values,
            IndCqcColumns.contemporary_region: CatValues.contemporary_region_column_values.categorical_values,
            IndCqcColumns.current_cssr: CatValues.current_cssr_column_values.categorical_values,
            IndCqcColumns.current_region: CatValues.current_region_column_values.categorical_values,
            IndCqcColumns.current_rural_urban_indicator_2011: CatValues.current_rui_column_values.categorical_values,
            IndCqcColumns.ascwds_filled_posts_source: CatValues.ascwds_filled_posts_source_column_values.categorical_values,
            IndCqcColumns.ascwds_filtering_rule: CatValues.ascwds_filtering_rule_column_values.categorical_values,
            IndCqcColumns.related_location: CatValues.related_location_column_values.categorical_values,
            IndCqcColumns.specialist_generalist_other_dementia: CatValues.specialist_generalist_other_dementia_column_values.categorical_values,
            IndCqcColumns.specialist_generalist_other_lda: CatValues.specialist_generalist_other_lda_column_values.categorical_values,
            IndCqcColumns.specialist_generalist_other_mh: CatValues.specialist_generalist_other_mh_column_values.categorical_values,
        },
        RuleName.distinct_values: {
            IndCqcColumns.care_home: CatValues.care_home_column_values.count_of_categorical_values,
            IndCqcColumns.cqc_sector: CatValues.sector_column_values.count_of_categorical_values,
            IndCqcColumns.registration_status: CatValues.registration_status_column_values.count_of_categorical_values,
            IndCqcColumns.dormancy: CatValues.dormancy_column_values.count_of_categorical_values,
            IndCqcColumns.primary_service_type: CatValues.primary_service_type_column_values.count_of_categorical_values,
            IndCqcColumns.contemporary_cssr: CatValues.contemporary_cssr_column_values.count_of_categorical_values,
            IndCqcColumns.contemporary_region: CatValues.contemporary_region_column_values.count_of_categorical_values,
            IndCqcColumns.current_cssr: CatValues.current_cssr_column_values.count_of_categorical_values,
            IndCqcColumns.current_region: CatValues.current_region_column_values.count_of_categorical_values,
            IndCqcColumns.current_rural_urban_indicator_2011: CatValues.current_rui_column_values.count_of_categorical_values,
            IndCqcColumns.ascwds_filled_posts_source: CatValues.ascwds_filled_posts_source_column_values.count_of_categorical_values,
            IndCqcColumns.ascwds_filtering_rule: CatValues.ascwds_filtering_rule_column_values.count_of_categorical_values,
            IndCqcColumns.related_location: CatValues.related_location_column_values.count_of_categorical_values,
            IndCqcColumns.specialist_generalist_other_dementia: CatValues.specialist_generalist_other_dementia_column_values.count_of_categorical_values,
            IndCqcColumns.specialist_generalist_other_lda: CatValues.specialist_generalist_other_lda_column_values.count_of_categorical_values,
            IndCqcColumns.specialist_generalist_other_mh: CatValues.specialist_generalist_other_mh_column_values.count_of_categorical_values,
        },
        RuleName.custom_type: CustomValidationRules.care_home_and_primary_service_type,
    }
