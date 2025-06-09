from dataclasses import dataclass

from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_values.categorical_column_values import (
    PrimaryServiceTypeSecondLevel,
)


@dataclass
class CqcServiceToPrimaryServiceTypeLookup:
    """A dict where keys = CQC gac service description and values = second level primary service type"""

    cqc_service_to_primary_service_type_second_level = {
        "Shared Lives": PrimaryServiceTypeSecondLevel.shared_lives,
        "Care home service with nursing": PrimaryServiceTypeSecondLevel.care_home_with_nursing,
        "Care home service without nursing": PrimaryServiceTypeSecondLevel.care_home_only,
        "Domiciliary care service": PrimaryServiceTypeSecondLevel.non_residential,
        "Community health care services - Nurses Agency only": PrimaryServiceTypeSecondLevel.non_residential,
        "Supported living service": PrimaryServiceTypeSecondLevel.non_residential,
        "Extra Care housing services": PrimaryServiceTypeSecondLevel.non_residential,
        "Residential substance misuse treatment and/or rehabilitation service": PrimaryServiceTypeSecondLevel.other_residential,
        "Hospice services": PrimaryServiceTypeSecondLevel.other_residential,
        "Acute services with overnight beds": PrimaryServiceTypeSecondLevel.other_residential,
    }
