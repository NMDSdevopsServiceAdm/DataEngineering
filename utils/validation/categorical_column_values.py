from dataclasses import dataclass

from utils.column_values.cqc_locations_values import (
    PrimaryServiceType,
    Sector,
    RegistrationStatus,
    Dormancy,
)
from utils.column_values.cqc_pir_values import (
    CareHome,
)
from utils.column_values.ind_cqc_pipeline_values import (
    ASCWDSFilledPostsSource as ASCWDSSource,
    EstimateFilledPostsSource as EstimateSource,
)
from utils.feature_engineering_resources.feature_engineering_region import (
    FeatureEngineeringValueLabelsRegion as Region,
)
from utils.feature_engineering_resources.feature_engineering_rui import (
    FeatureEngineeringValueLabelsRUI as RUI,
)
from utils.value_labels.ascwds_worker.ascwds_worker_mainjrid import (
    AscwdsWorkerValueLabelsMainjrid as MainJobRole,
)
from utils.column_values.ons_postcode_directory_values import (
    CurrentCSSR,
)


@dataclass
class CQCCategoricalValues:
    primary_service_types = [
        PrimaryServiceType.care_home_only,
        PrimaryServiceType.care_home_with_nursing,
        PrimaryServiceType.non_residential,
    ]
    care_home_values = [CareHome.care_home, CareHome.not_care_home]
    dormancy_values = [Dormancy.dormant, Dormancy.not_dormant]
    cqc_sector = [Sector.independent, Sector.local_authority]
    registration_status = [RegistrationStatus.registered]
    registration_status_raw = [
        RegistrationStatus.registered,
        RegistrationStatus.deregistered,
    ]


@dataclass
class CQCDistinctValues:
    primary_service_types = len(CQCCategoricalValues.primary_service_types)
    care_home_values = len(CQCCategoricalValues.care_home_values)
    dormancy_values = len(CQCCategoricalValues.dormancy_values) + 1  # can be null
    registration_status_values = len(CQCCategoricalValues.registration_status)
    cqc_sector_values = len(CQCCategoricalValues.cqc_sector)
    registration_status_raw_values = len(CQCCategoricalValues.registration_status_raw)


@dataclass
class ASCWDSCategoricalValues:
    main_job_role_id = list(MainJobRole.labels_dict.keys())
    main_job_role_labelled = list(MainJobRole.labels_dict.values())


@dataclass
class ASCWDSDistinctValues:
    main_job_role_id_values = len(ASCWDSCategoricalValues.main_job_role_id)
    main_job_role_labelled_values = len(ASCWDSCategoricalValues.main_job_role_labelled)


@dataclass
class IndCQCCategoricalValues:
    cqc_sector = [Sector.independent]
    ascwds_filled_posts_source = [
        ASCWDSSource.worker_records_and_total_staff,
        ASCWDSSource.only_total_staff,
        ASCWDSSource.only_worker_records,
        ASCWDSSource.average_of_total_staff_and_worker_records,
    ]
    estimate_filled_posts_source = [
        EstimateSource.rolling_average_model,
        EstimateSource.care_home_model,
        EstimateSource.interpolation_model,
        EstimateSource.extrapolation_model,
        EstimateSource.ascwds_filled_posts_clean_deduplicated,
        EstimateSource.non_res_with_pir_model,
    ]


@dataclass
class IndCQCDistinctValues:
    cqc_sector = len(IndCQCCategoricalValues.cqc_sector)
    ascwds_filled_posts_source = (
        len(IndCQCCategoricalValues.ascwds_filled_posts_source) + 1
    )  # can be null
    estimate_filled_posts_source = len(
        IndCQCCategoricalValues.estimate_filled_posts_source
    )


@dataclass
class ONSCategoricalValues:
    rural_urban_indicators = list(RUI.labels_dict.values())
    regions = list(Region.labels_dict.values())
    cssrs = CurrentCSSR.values_list


@dataclass
class ONSDistinctValues:
    rural_urban_indicators = len(ONSCategoricalValues.rural_urban_indicators)
    regions = len(ONSCategoricalValues.regions)
    cssrs = len(ONSCategoricalValues.cssrs)
