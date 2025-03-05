from dataclasses import dataclass

from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)
from utils.validation.validation_rule_names import RuleNames as RuleName


@dataclass
class LocationsAPIRawValidationRules:
    rules_to_check = {
        RuleName.complete_columns: [
            CQCL.location_id,
            Keys.import_date,
            CQCL.name,
        ],
        RuleName.index_columns: [
            CQCL.location_id,
            Keys.import_date,
        ],
    }
