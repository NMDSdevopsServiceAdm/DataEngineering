from dataclasses import dataclass

from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_names.cleaned_data_files.cqc_pir_cleaned_values import (
    CqcPIRCleanedColumns as PIRClean,
)
from utils.column_names.cleaned_data_files.cqc_provider_cleaned_values import (
    CqcProviderCleanedColumns as CQCPClean,
)
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.cleaned_data_files.ons_cleaned_values import (
    OnsCleanedColumns as ONSClean,
    ONScol as ONS,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)

from utils.column_values.categorical_column_values import (
    Dormancy,
    RegistrationStatus,
    PrimaryServiceType,
    Services,
    CareHome,
    Sector,
    MainJobRoleLabels,
    MainJobRoleID,
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


@dataclass
class ASCWDSWorkerCleanedCategoricalValues:
    main_job_role_labels_column_values = MainJobRoleLabels(
        AWKClean.main_job_role_labelled
    )
    main_job_role_id_column_values = MainJobRoleID(AWKClean.main_job_role_id)


@dataclass
class LocationsApiCleanedCategoricalValues:
    care_home_column_values = CareHome(CQCLClean.care_home)
    sector_column_values = Sector(CQCLClean.cqc_sector)
    dormancy_column_values = Dormancy(CQCLClean.dormancy, contains_null_values=True)
    registration_status_column_values = RegistrationStatus(
        CQCLClean.registration_status,
        value_to_remove=RegistrationStatus.deregistered,
    )
    primary_service_type_column_values = PrimaryServiceType(
        CQCLClean.primary_service_type
    )
    current_region_column_values = Region(CQCLClean.current_region)
    contemporary_region_column_values = Region(CQCLClean.contemporary_region)
    current_rui_column_values = RUI(CQCLClean.current_rural_urban_ind_11)
    current_cssr_column_values = CSSR(CQCLClean.current_cssr)
    contemporary_cssr_column_values = CSSR(CQCLClean.contemporary_cssr)


@dataclass
class ProvidersApiCleanedCategoricalValues:
    sector_column_values = Sector(CQCPClean.cqc_sector)


@dataclass
class PIRCleanedCategoricalValues:
    care_home_column_values = CareHome(PIRClean.care_home)


@dataclass
class PostcodeDirectoryCleanedCategoricalValues:
    current_region_column_values = Region(ONSClean.current_region)
    contemporary_region_column_values = Region(ONSClean.contemporary_region)
    current_rui_column_values = RUI(ONSClean.current_rural_urban_ind_11)
    contemporary_rui_column_values = RUI(ONSClean.contemporary_rural_urban_ind_11)
    current_cssr_column_values = CSSR(ONSClean.current_cssr)
    contemporary_cssr_column_values = CSSR(ONSClean.contemporary_cssr)


@dataclass
class PostcodeDirectoryRawCategoricalValues:
    region_column_values = Region(ONS.region)
    rui_column_values = RUI(ONS.rural_urban_indicator_2011)
    cssr_column_values = CSSR(ONS.cssr)


@dataclass
class MergedIndCQCCategoricalValues:
    care_home_column_values = CareHome(IndCQC.care_home)
    sector_column_values = Sector(IndCQC.cqc_sector)
    dormancy_column_values = Dormancy(IndCQC.dormancy, contains_null_values=True)
    registration_status_column_values = RegistrationStatus(
        IndCQC.registration_status,
        value_to_remove=RegistrationStatus.deregistered,
    )
    primary_service_type_column_values = PrimaryServiceType(IndCQC.primary_service_type)
    current_region_column_values = Region(IndCQC.current_region)
    contemporary_region_column_values = Region(IndCQC.contemporary_region)
    current_rui_column_values = RUI(IndCQC.current_rural_urban_indicator_2011)
    current_cssr_column_values = CSSR(IndCQC.current_cssr)
    contemporary_cssr_column_values = CSSR(IndCQC.contemporary_cssr)


@dataclass
class CleanedIndCQCCategoricalValues:
    care_home_column_values = CareHome(IndCQC.care_home)
    sector_column_values = Sector(
        IndCQC.cqc_sector, value_to_remove=Sector.local_authority
    )
    dormancy_column_values = Dormancy(IndCQC.dormancy, contains_null_values=True)
    registration_status_column_values = RegistrationStatus(
        IndCQC.registration_status,
        value_to_remove=RegistrationStatus.deregistered,
    )
    primary_service_type_column_values = PrimaryServiceType(IndCQC.primary_service_type)
    current_region_column_values = Region(IndCQC.current_region)
    contemporary_region_column_values = Region(IndCQC.contemporary_region)
    current_rui_column_values = RUI(IndCQC.current_rural_urban_indicator_2011)
    current_cssr_column_values = CSSR(IndCQC.current_cssr)
    contemporary_cssr_column_values = CSSR(IndCQC.contemporary_cssr)
    ascwds_filled_posts_source_column_values = ASCWDSFilledPostsSource(
        IndCQC.ascwds_filled_posts_source, contains_null_values=True
    )


@dataclass
class FeatureEngineeringCategoricalValues:
    current_region_column_values = Region(IndCQC.current_region)
    services_column_values = Services(IndCQC.gac_service_types)
    current_rui_column_values = RUI(IndCQC.current_rural_urban_indicator_2011)
    dormancy_column_values = Dormancy(IndCQC.dormancy, contains_null_values=True)
    care_home_column_values = CareHome(IndCQC.care_home)


@dataclass
class EstimatedIndCQCFilledPostsCategoricalValues:
    care_home_column_values = CareHome(IndCQC.care_home)
    primary_service_type_column_values = PrimaryServiceType(IndCQC.primary_service_type)
    current_region_column_values = Region(IndCQC.current_region)
    current_cssr_column_values = CSSR(IndCQC.current_cssr)
    ascwds_filled_posts_source_column_values = ASCWDSFilledPostsSource(
        IndCQC.ascwds_filled_posts_source, contains_null_values=True
    )
    estimate_filled_posts_source_column_values = EstimateFilledPostsSource(
        IndCQC.estimate_filled_posts_source
    )