from dataclasses import dataclass

from utils.column_names.cleaned_data_files.cqc_pir_cleaned_values import (
    CqcPirColumns as PIR,
    CqcPIRCleanedColumns as PIRClean,
)


@dataclass
class PIRType:
    """The possible values of the PIR type column in CQC PIR data"""

    column_name: str = PIR.pir_type

    residential: str = "Residential"
    community: str = "Community"

    values_list = [residential, community]


@dataclass
class CareHome:
    """The possible values of the care home column in CQC PIR data"""

    column_name: str = PIRClean.care_home

    care_home: str = "Y"
    not_care_home: str = "N"

    values_list = [care_home, not_care_home]
