from dataclasses import dataclass

from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLCLean,
)


@dataclass
class Validation:
    check: str = "check"
    check_level: str = "check_level"
    check_status: str = "check_status"
    constraint: str = "constraint"
    constraint_status: str = "constraint_status"
    constraint_message: str = "constraint_message"
    location_id_length: str = CQCLCLean.location_id + "_length"
    provider_id_length: str = CQCLCLean.provider_id + "_length"
