from dataclasses import dataclass

from utils.column_names.cleaned_data_files.cqc_location_cleaned_values import (
    CqcLocationCleanedColumns as CQCLClean,
)


@dataclass
class CQCLocationsValueLabelsPrimaryServiceType:
    """The possible values of the primary service type column in CQC locations data"""

    column_name: str = CQCLClean.primary_service_type
    care_home_with_nursing: str = "Care home with nursing"
    care_home_only: str = "Care home without nursing"
    non_residential: str = "non-residential"

    values_list = [care_home_with_nursing, care_home_only, non_residential]
