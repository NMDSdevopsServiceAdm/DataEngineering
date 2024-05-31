from dataclasses import dataclass

from utils.column_names.cleaned_data_files.cqc_provider_cleaned_values import (
    CqcProviderCleanedColumns as CQCPClean,
)


@dataclass
class Sector:
    """The possible values of the sector column in CQC data"""

    column_name: str = CQCPClean.cqc_sector

    local_authority: str = "Local authority"
    independent: str = "Independent"

    values_list = [local_authority, independent]
