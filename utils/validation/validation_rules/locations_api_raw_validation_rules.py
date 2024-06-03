from dataclasses import dataclass

from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned_values import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)

from utils.validation.validation_rule_names import RuleNames as RuleName
from utils.validation.categorical_column_values import (
    CQCCategoricalValues,
    CQCDistinctValues,
)
from utils.column_values.categorical_column_values import (
    Dormancy,
    RegistrationStatus,
    PrimaryServiceType,
)

dormancy_column_values = Dormancy(CQCL.dormancy, contains_null_values=True)
registration_status_column_values = RegistrationStatus(CQCL.registration_status)
primary_service_type_column_values = PrimaryServiceType(CQCLClean.primary_service_type)


@dataclass
class LocationsAPIRawValidationRules:
    rules_to_check = {
        RuleName.complete_columns: [
            CQCL.location_id,
            Keys.import_date,
            CQCL.care_home,
            CQCL.provider_id,
            CQCL.registration_status,
            CQCL.registration_date,
            CQCL.name,
        ],
        RuleName.index_columns: [
            CQCL.location_id,
            Keys.import_date,
        ],
        RuleName.min_values: {
            CQCL.number_of_beds: 0,
        },
        RuleName.max_values: {
            CQCL.number_of_beds: 500,
        },
        RuleName.categorical_values_in_columns: {
            CQCL.care_home: CQCCategoricalValues.care_home_values,
            CQCL.registration_status: registration_status_column_values.categorical_values,
            CQCL.dormancy: dormancy_column_values.categorical_values,
        },
        RuleName.distinct_values: {
            CQCL.care_home: CQCDistinctValues.care_home_values,
            CQCL.registration_status: registration_status_column_values.count_of_categorical_values,
            CQCL.dormancy: dormancy_column_values.count_of_categorical_values,
        },
    }
