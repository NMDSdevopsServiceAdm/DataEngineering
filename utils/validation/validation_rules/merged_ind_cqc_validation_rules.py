from dataclasses import dataclass

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns,
)

from utils.validation.validation_rule_names import RuleNames as RuleName
from utils.validation.categorical_column_values import (
    CQCCategoricalValues,
    CQCDistinctValues,
    ONSCategoricalValues,
    ONSDistinctValues,
    IndCQCCategoricalValues,
    IndCQCDistinctValues,
)


@dataclass
class MergedIndCqcValidationRules:
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
            IndCqcColumns.registration_date,
            IndCqcColumns.primary_service_type,
            IndCqcColumns.contemporary_ons_import_date,
            IndCqcColumns.contemporary_cssr,
            IndCqcColumns.contemporary_region,
            IndCqcColumns.current_ons_import_date,
            IndCqcColumns.current_cssr,
            IndCqcColumns.current_region,
            IndCqcColumns.current_rural_urban_indicator_2011,
        ],
        RuleName.index_columns: [
            IndCqcColumns.location_id,
            IndCqcColumns.cqc_location_import_date,
        ],
        RuleName.min_values: {
            IndCqcColumns.number_of_beds: 1,
            IndCqcColumns.people_directly_employed: 0,
            IndCqcColumns.total_staff_bounded: 1,
            IndCqcColumns.worker_records_bounded: 1,
        },
        RuleName.max_values: {
            IndCqcColumns.number_of_beds: 500,
            IndCqcColumns.people_directly_employed: 10000,
            IndCqcColumns.total_staff_bounded: 3000,
            IndCqcColumns.worker_records_bounded: 3000,
        },
        RuleName.categorical_values_in_columns: {
            IndCqcColumns.care_home: CQCCategoricalValues.care_home_values,
            IndCqcColumns.cqc_sector: IndCQCCategoricalValues.cqc_sector,
            IndCqcColumns.registration_status: CQCCategoricalValues.registration_status,
            IndCqcColumns.dormancy: CQCCategoricalValues.dormancy_values,
            IndCqcColumns.primary_service_type: CQCCategoricalValues.primary_service_types,
            IndCqcColumns.contemporary_cssr: ONSCategoricalValues.cssrs,
            IndCqcColumns.contemporary_region: ONSCategoricalValues.regions,
            IndCqcColumns.current_cssr: ONSCategoricalValues.cssrs,
            IndCqcColumns.current_region: ONSCategoricalValues.regions,
            IndCqcColumns.current_rural_urban_indicator_2011: ONSCategoricalValues.rural_urban_indicators,
        },
        RuleName.distinct_values: {
            IndCqcColumns.care_home: CQCDistinctValues.care_home_values,
            IndCqcColumns.cqc_sector: IndCQCDistinctValues.cqc_sector,
            IndCqcColumns.registration_status: CQCDistinctValues.registration_status_values,
            IndCqcColumns.dormancy: CQCDistinctValues.dormancy_values,
            IndCqcColumns.primary_service_type: CQCDistinctValues.primary_service_types,
            IndCqcColumns.contemporary_cssr: ONSDistinctValues.cssrs,
            IndCqcColumns.contemporary_region: ONSDistinctValues.regions,
            IndCqcColumns.current_cssr: ONSDistinctValues.cssrs,
            IndCqcColumns.current_region: ONSDistinctValues.regions,
            IndCqcColumns.current_rural_urban_indicator_2011: ONSDistinctValues.rural_urban_indicators,
        },
    }
