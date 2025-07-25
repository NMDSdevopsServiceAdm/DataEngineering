from dataclasses import dataclass

from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.cleaned_data_files.cqc_pir_cleaned import (
    CqcPIRCleanedColumns as PIRClean,
)
from utils.column_names.cleaned_data_files.cqc_provider_cleaned import (
    CqcProviderCleanedColumns as CQCPClean,
)
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.cleaned_data_files.ons_cleaned import (
    OnsCleanedColumns as ONSClean,
    ONScol as ONS,
)
from utils.column_names.coverage_columns import CoverageColumns
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)

from utils.column_values.categorical_column_values import (
    ASCWDSFilledPostsSource,
    AscwdsFilteringRule,
    CareHome,
    ContemporaryCSSR,
    CurrentCSSR,
    Dormancy,
    EstimateFilledPostsSource,
    InAscwds,
    MainJobRoleID,
    MainJobRoleLabels,
    PrimaryServiceType,
    PrimaryServiceTypeSecondLevel,
    Region,
    RegistrationStatus,
    RelatedLocation,
    RUI,
    Sector,
    Services,
    Specialisms,
    SpecialistGeneralistOther,
)


@dataclass
class ASCWDSWorkerCleanedCategoricalValues:
    main_job_role_labels_column_values = MainJobRoleLabels(
        AWKClean.main_job_role_clean_labelled
    )
    main_job_role_id_column_values = MainJobRoleID(AWKClean.main_job_role_clean)


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
    current_cssr_column_values = CurrentCSSR(CQCLClean.current_cssr)
    contemporary_cssr_column_values = ContemporaryCSSR(CQCLClean.contemporary_cssr)
    related_location_column_values = RelatedLocation(CQCLClean.related_location)


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
    current_cssr_column_values = CurrentCSSR(ONSClean.current_cssr)
    contemporary_cssr_column_values = ContemporaryCSSR(ONSClean.contemporary_cssr)


@dataclass
class PostcodeDirectoryRawCategoricalValues:
    region_column_values = Region(ONS.region)
    rui_column_values = RUI(ONS.rural_urban_indicator_2011)
    cssr_column_values = ContemporaryCSSR(ONS.cssr)


@dataclass
class MergedIndCQCCategoricalValues:
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
    current_cssr_column_values = CurrentCSSR(
        IndCQC.current_cssr, value_to_remove=CurrentCSSR.isles_of_scilly
    )
    contemporary_cssr_column_values = ContemporaryCSSR(
        IndCQC.contemporary_cssr, value_to_remove=ContemporaryCSSR.isles_of_scilly
    )
    related_location_column_values = RelatedLocation(CQCLClean.related_location)


@dataclass
class MergedCoverageCategoricalValues:
    care_home_column_values = CareHome(IndCQC.care_home)
    current_cssr_column_values = CurrentCSSR(IndCQC.current_cssr)
    current_region_column_values = Region(IndCQC.current_region)
    current_rui_column_values = RUI(IndCQC.current_rural_urban_indicator_2011)
    sector_column_values = Sector(IndCQC.cqc_sector)
    dormancy_column_values = Dormancy(IndCQC.dormancy, contains_null_values=True)
    in_ascwds_column_values = InAscwds(CoverageColumns.in_ascwds)
    primary_service_type_column_values = PrimaryServiceType(IndCQC.primary_service_type)
    registration_status_column_values = RegistrationStatus(
        IndCQC.registration_status,
        value_to_remove=RegistrationStatus.deregistered,
    )


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
    current_cssr_column_values = CurrentCSSR(
        IndCQC.current_cssr, value_to_remove=CurrentCSSR.isles_of_scilly
    )
    contemporary_cssr_column_values = ContemporaryCSSR(
        IndCQC.contemporary_cssr, value_to_remove=ContemporaryCSSR.isles_of_scilly
    )
    ascwds_filled_posts_source_column_values = ASCWDSFilledPostsSource(
        IndCQC.ascwds_filled_posts_source, contains_null_values=True
    )
    ascwds_filtering_rule_column_values = AscwdsFilteringRule(
        IndCQC.ascwds_filtering_rule
    )
    related_location_column_values = RelatedLocation(CQCLClean.related_location)
    specialist_generalist_other_dementia_column_values = SpecialistGeneralistOther(
        IndCQC.specialist_generalist_other_dementia
    )
    specialist_generalist_other_lda_column_values = SpecialistGeneralistOther(
        IndCQC.specialist_generalist_other_lda
    )
    specialist_generalist_other_mh_column_values = SpecialistGeneralistOther(
        IndCQC.specialist_generalist_other_mh
    )


@dataclass
class ImputedIndCqcAscwdsAndPirCategoricalValues:
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
    current_cssr_column_values = CurrentCSSR(
        IndCQC.current_cssr, value_to_remove=CurrentCSSR.isles_of_scilly
    )
    contemporary_cssr_column_values = ContemporaryCSSR(
        IndCQC.contemporary_cssr, value_to_remove=ContemporaryCSSR.isles_of_scilly
    )
    ascwds_filled_posts_source_column_values = ASCWDSFilledPostsSource(
        IndCQC.ascwds_filled_posts_source, contains_null_values=True
    )
    ascwds_filtering_rule_column_values = AscwdsFilteringRule(
        IndCQC.ascwds_filtering_rule
    )


@dataclass
class FeatureEngineeringCategoricalValues:
    current_region_column_values = Region(IndCQC.current_region)
    services_column_values = Services(IndCQC.imputed_gac_service_types)
    specialisms_column_values = Specialisms(IndCQC.imputed_specialisms)
    current_rui_column_values = RUI(IndCQC.current_rural_urban_indicator_2011)
    dormancy_column_values = Dormancy(IndCQC.dormancy)
    related_location_column_values = RelatedLocation(IndCQC.related_location)
    care_home_column_care_home_values = CareHome(
        IndCQC.care_home, value_to_remove=CareHome.not_care_home
    )
    care_home_column_non_care_home_values = CareHome(
        IndCQC.care_home, value_to_remove=CareHome.care_home
    )


@dataclass
class EstimatedIndCQCFilledPostsCategoricalValues:
    care_home_column_values = CareHome(IndCQC.care_home)
    primary_service_type_column_values = PrimaryServiceType(IndCQC.primary_service_type)
    primary_service_type_second_level_column_values = PrimaryServiceTypeSecondLevel(
        IndCQC.primary_service_type_second_level
    )
    current_region_column_values = Region(IndCQC.current_region)
    current_cssr_column_values = CurrentCSSR(
        IndCQC.current_cssr, value_to_remove=CurrentCSSR.isles_of_scilly
    )
    ascwds_filled_posts_source_column_values = ASCWDSFilledPostsSource(
        IndCQC.ascwds_filled_posts_source, contains_null_values=True
    )
    estimate_filled_posts_source_column_values = EstimateFilledPostsSource(
        IndCQC.estimate_filled_posts_source
    )


@dataclass
class DiagnosticOnKnownFilledPostsCategoricalValues:
    estimate_filled_posts_source_column_values = EstimateFilledPostsSource(
        IndCQC.estimate_filled_posts_source,
        value_to_remove=EstimateFilledPostsSource.ascwds_pir_merged,
    )
