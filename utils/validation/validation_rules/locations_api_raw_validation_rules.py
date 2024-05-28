from dataclasses import dataclass

from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)

from utils.validation.validation_rule_names import RuleNames as RuleName
from utils.validation.categorical_column_values import (
    CQCCategoricalValues,
    CQCDistinctValues,
)


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
            CQCL.registration_status: CQCCategoricalValues.registration_status_raw,
            CQCL.dormancy: CQCCategoricalValues.dormancy_values,
        },
        RuleName.distinct_values: {
            CQCL.care_home: CQCDistinctValues.care_home_values,
            CQCL.registration_status: CQCDistinctValues.registration_status_raw_values,
            CQCL.dormancy: CQCDistinctValues.dormancy_values,
        },
    }
