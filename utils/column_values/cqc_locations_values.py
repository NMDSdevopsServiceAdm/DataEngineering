from dataclasses import dataclass

from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)

from utils.column_names.cleaned_data_files.cqc_location_cleaned_values import (
    CqcLocationCleanedColumns as CQCLClean,
)

from utils.column_values.cqc_providers_values import Sector
from utils.column_values.categorical_column_values import (
    Dormancy,
    LocationType,
    RegistrationStatus,
    PrimaryServiceType,
    Services,
)


dormancy_column_values = Dormancy(CQCL.dormancy, contains_null_values=True)
location_column_values = LocationType(CQCL.type)
registration_status_column_values = RegistrationStatus(CQCL.registration_status)
primary_service_type_column_values = PrimaryServiceType(CQCLClean.primary_service_type)
services_column_values = Services(CQCLClean.gac_service_types)
