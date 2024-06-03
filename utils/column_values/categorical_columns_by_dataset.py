from dataclasses import dataclass

from utils.column_names.cleaned_data_files.cqc_location_cleaned_values import (
    CqcLocationCleanedColumns as CQCLClean,
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_names.cleaned_data_files.cqc_pir_cleaned_values import (
    CqcPirColumns as PIR,
    CqcPIRCleanedColumns as PIRClean,
)
from utils.column_names.cleaned_data_files.cqc_provider_cleaned_values import (
    CqcProviderCleanedColumns as CQCPClean,
)
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned_values import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.cleaned_data_files.ons_cleaned_values import (
    OnsCleanedColumns as ONS,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)


from utils.column_values.categorical_column_values import (
    Dormancy,
    LocationType,
    RegistrationStatus,
    PrimaryServiceType,
    Services,
    CareHome,
    PIRType,
    Sector,
    MainJobRole,
    Region,
    RUI,
    CSSR,
    ASCWDSFilledPostsSource,
    EstimateFilledPostsSource,
)


@dataclass
class LocationApiRawCategoricalValues:
    dormancy_column_values = Dormancy(CQCL.dormancy, contains_null_values=True)
    registration_status_column_values = RegistrationStatus(CQCL.registration_status)
    care_home_column_values = CareHome(CQCL.care_home)


services_column_values = Services(CQCLClean.gac_service_types)
location_column_values = LocationType(CQCL.type)
primary_service_type_column_values = PrimaryServiceType(CQCLClean.primary_service_type)
pir_type_column_values = PIRType(PIR.pir_type)
sector_column_values = Sector(CQCPClean.cqc_sector)
main_job_role_column_values = MainJobRole(AWKClean.main_job_role_labelled)
current_region_column_values = Region(ONS.current_region)
contemporary_region_column_values = Region(ONS.contemporary_region)
current_rui_column_values = RUI(ONS.current_rural_urban_ind_11)
contemporary_rui_column_values = RUI(ONS.contemporary_rural_urban_ind_11)
current_cssr_column_values = CSSR(ONS.current_cssr)
contemporary_cssr_column_values = CSSR(ONS.contemporary_cssr)
ascwds_filled_posts_source_column_values = ASCWDSFilledPostsSource(
    IndCQC.ascwds_filled_posts_source
)
estimate_filled_posts_source_column_values = EstimateFilledPostsSource(
    IndCQC.estimate_filled_posts_source
)


@dataclass
class FeatureEngineeringCategoricalValues:
    current_region_column_values = Region(ONS.current_region)
    services_column_values = Services(CQCLClean.gac_service_types)
    current_rui_column_values = RUI(ONS.current_rural_urban_ind_11)
    dormancy_column_values = Dormancy(CQCL.dormancy, contains_null_values=True)
