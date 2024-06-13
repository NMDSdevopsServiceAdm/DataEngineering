from dataclasses import dataclass

from utils.column_names.raw_data_files.cqc_pir_columns import (
    CqcPirColumns as CQCPIR,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)
from utils.validation.validation_rule_names import RuleNames as RuleName


@dataclass
class PIRRawValidationRules:
    rules_to_check = {
        RuleName.complete_columns: [
            Keys.import_date,
            CQCPIR.location_id,
            CQCPIR.people_directly_employed,
            CQCPIR.pir_type,
            CQCPIR.pir_submission_date,
        ],
        RuleName.index_columns: [
            CQCPIR.location_id,
            Keys.import_date,
            CQCPIR.pir_type,
            CQCPIR.pir_submission_date,
        ],
        RuleName.max_values: {
            CQCPIR.people_directly_employed: 10000,
        },
        RuleName.min_values: {
            CQCPIR.people_directly_employed: 0,
        },
    }
