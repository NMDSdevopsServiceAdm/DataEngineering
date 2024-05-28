from dataclasses import dataclass

from utils.column_names.raw_data_files.cqc_provider_api_columns import (
    CqcProviderApiColumns as CQCP,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)
from utils.validation.validation_rule_names import RuleNames as RuleName


@dataclass
class ProvidersAPIRawValidationRules:
    rules_to_check = {
        RuleName.complete_columns: [
            Keys.import_date,
            CQCP.provider_id,
            CQCP.name,
        ],
        RuleName.index_columns: [
            CQCP.provider_id,
            Keys.import_date,
        ],
    }
