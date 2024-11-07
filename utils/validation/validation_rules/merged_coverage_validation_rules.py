from dataclasses import dataclass

from utils.column_names.coverage_columns import CoverageColumns
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns,
)
from utils.column_values.categorical_columns_by_dataset import (
    MergedCoverageCategoricalValues as CatValues,
)
from utils.validation.validation_rule_names import RuleNames as RuleName


@dataclass
class MergedCoverageValidationRules:
    rules_to_check = {
        RuleName.size_of_dataset: None,
        RuleName.index_columns: [
            IndCqcColumns.location_id,
            IndCqcColumns.cqc_location_import_date,
        ],
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
            IndCqcColumns.current_ons_import_date,
            IndCqcColumns.current_cssr,
            IndCqcColumns.current_region,
            IndCqcColumns.current_rural_urban_indicator_2011,
        ],
        RuleName.min_values: {
            CoverageColumns.in_ascwds: 0,
        },
        RuleName.max_values: {
            CoverageColumns.in_ascwds: 1,
        },
        RuleName.categorical_values_in_columns: {
            IndCqcColumns.care_home: CatValues.care_home_column_values.categorical_values,
            IndCqcColumns.current_cssr: CatValues.current_cssr_column_values.categorical_values,
            IndCqcColumns.current_region: CatValues.current_region_column_values.categorical_values,
            IndCqcColumns.current_rural_urban_indicator_2011: CatValues.current_rui_column_values.categorical_values,
            IndCqcColumns.cqc_sector: CatValues.sector_column_values.categorical_values,
            IndCqcColumns.dormancy: CatValues.dormancy_column_values.categorical_values,
            IndCqcColumns.primary_service_type: CatValues.primary_service_type_column_values.categorical_values,
            IndCqcColumns.registration_status: CatValues.registration_status_column_values.categorical_values,
        },
        RuleName.distinct_values: {
            IndCqcColumns.care_home: CatValues.care_home_column_values.count_of_categorical_values,
            IndCqcColumns.current_cssr: CatValues.current_cssr_column_values.count_of_categorical_values,
            IndCqcColumns.current_region: CatValues.current_region_column_values.count_of_categorical_values,
            IndCqcColumns.current_rural_urban_indicator_2011: CatValues.current_rui_column_values.count_of_categorical_values,
            IndCqcColumns.cqc_sector: CatValues.sector_column_values.count_of_categorical_values,
            IndCqcColumns.dormancy: CatValues.dormancy_column_values.count_of_categorical_values,
            IndCqcColumns.primary_service_type: CatValues.primary_service_type_column_values.count_of_categorical_values,
            IndCqcColumns.registration_status: CatValues.registration_status_column_values.count_of_categorical_values,
        },
    }
