from dataclasses import dataclass

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned_values import (
    CqcLocationCleanedValues as CQCLValues,
)

from utils.validation.validation_rule_names import RuleNames as RuleName
from utils.validation.categorical_column_values import (
    CQCCategoricalValues,
    ONSCategoricalValues,
)


@dataclass
class MergedIndCqcValidationRules:
    rules_to_check = {
        RuleName.size_of_dataset: None,
        RuleName.complete_columns: [
            IndCqcColumns.location_id,
            IndCqcColumns.ascwds_workplace_import_date,
            IndCqcColumns.cqc_location_import_date,
            IndCqcColumns.cqc_pir_import_date,
            IndCqcColumns.care_home,
            IndCqcColumns.provider_id,
            IndCqcColumns.cqc_sector,
            IndCqcColumns.registration_status,
            IndCqcColumns.registration_date,
            IndCqcColumns.dormancy,
            IndCqcColumns.number_of_beds,
            IndCqcColumns.primary_service_type,
            IndCqcColumns.contemporary_ons_import_date,
            IndCqcColumns.contemporary_cssr,
            IndCqcColumns.contemporary_region,
            IndCqcColumns.current_ons_import_date,
            IndCqcColumns.current_cssr,
            IndCqcColumns.current_region,
            IndCqcColumns.current_rural_urban_indicator_2011,
            IndCqcColumns.people_directly_employed,
            IndCqcColumns.establishment_id,
            IndCqcColumns.organisation_id,
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
            IndCqcColumns.cqc_sector: [CQCLValues.independent],
            IndCqcColumns.registration_status: [CQCLValues.registered],
            IndCqcColumns.dormancy: CQCCategoricalValues.dormancy_values,
            IndCqcColumns.primary_service_type: CQCCategoricalValues.primary_service_types,
            IndCqcColumns.contemporary_cssr: ONSCategoricalValues.cssrs,
            IndCqcColumns.contemporary_region: ONSCategoricalValues.regions,
            IndCqcColumns.current_cssr: ONSCategoricalValues.cssrs,
            IndCqcColumns.current_region: ONSCategoricalValues.regions,
            IndCqcColumns.current_rural_urban_indicator_2011: ONSCategoricalValues.rural_urban_indicators,
        },
    }
