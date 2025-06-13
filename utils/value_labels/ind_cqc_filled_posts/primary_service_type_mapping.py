from dataclasses import dataclass

from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_values.categorical_column_values import (
    PrimaryServiceTypeSecondLevel,
)


@dataclass
class CqcServiceToPrimaryServiceTypeSecondLevelLookup:
    """
    A dict where keys = CQC gac service description and values = second level primary service type

    The order of these items determines the primary_service_second_level allocated to locations
    that offer multiple services.

    Changing the order of this dict will change how services are allocated.
    """

    dict = {
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
