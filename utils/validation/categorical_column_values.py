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
from utils.feature_engineering_dictionaries.feature_engineering_region import (
    FeatureEngineeringValueLabelsRegion as Region,
)
from utils.feature_engineering_dictionaries.feature_engineering_rui import (
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
    cssrs = [
        CurrentCSSR.barking_and_dagenham,
        CurrentCSSR.barnet,
        CurrentCSSR.barnsley,
        CurrentCSSR.bath_and_north_east_somerset,
        CurrentCSSR.bedford,
        CurrentCSSR.bexley,
        CurrentCSSR.birmingham,
        CurrentCSSR.blackburn_with_darwen,
        CurrentCSSR.blackpool,
        CurrentCSSR.bolton,
        CurrentCSSR.bournemouth,
        CurrentCSSR.bournemouth_christchurch_and_poole,
        CurrentCSSR.bracknell_forest,
        CurrentCSSR.bradford,
        CurrentCSSR.brent,
        CurrentCSSR.brighton_and_hove,
        CurrentCSSR.bristol,
        CurrentCSSR.bromley,
        CurrentCSSR.buckinghamshire,
        CurrentCSSR.bury,
        CurrentCSSR.calderdale,
        CurrentCSSR.cambridgeshire,
        CurrentCSSR.camden,
        CurrentCSSR.central_bedfordshire,
        CurrentCSSR.cheshire_east,
        CurrentCSSR.cheshire_west_and_chester,
        CurrentCSSR.city_of_london,
        CurrentCSSR.cornwall,
        CurrentCSSR.coventry,
        CurrentCSSR.croydon,
        CurrentCSSR.cumberland,
        CurrentCSSR.cumbria,
        CurrentCSSR.darlington,
        CurrentCSSR.derby,
        CurrentCSSR.derbyshire,
        CurrentCSSR.devon,
        CurrentCSSR.doncaster,
        CurrentCSSR.dorset,
        CurrentCSSR.dudley,
        CurrentCSSR.durham,
        CurrentCSSR.ealing,
        CurrentCSSR.east_riding_of_yorkshire,
        CurrentCSSR.east_sussex,
        CurrentCSSR.enfield,
        CurrentCSSR.essex,
        CurrentCSSR.gateshead,
        CurrentCSSR.gloucestershire,
        CurrentCSSR.greenwich,
        CurrentCSSR.hackney,
        CurrentCSSR.halton,
        CurrentCSSR.hammersmith_and_fulham,
        CurrentCSSR.hampshire,
        CurrentCSSR.haringey,
        CurrentCSSR.harrow,
        CurrentCSSR.hartlepool,
        CurrentCSSR.havering,
        CurrentCSSR.herefordshire,
        CurrentCSSR.hertfordshire,
        CurrentCSSR.hillingdon,
        CurrentCSSR.hounslow,
        CurrentCSSR.isle_of_wight,
        CurrentCSSR.isles_of_scilly,
        CurrentCSSR.islington,
        CurrentCSSR.kensington_and_chelsea,
        CurrentCSSR.kent,
        CurrentCSSR.kingston_upon_hull,
        CurrentCSSR.kingston_upon_thames,
        CurrentCSSR.kirklees,
        CurrentCSSR.knowsley,
        CurrentCSSR.lambeth,
        CurrentCSSR.lancashire,
        CurrentCSSR.leeds,
        CurrentCSSR.leicester,
        CurrentCSSR.leicestershire,
        CurrentCSSR.lewisham,
        CurrentCSSR.lincolnshire,
        CurrentCSSR.liverpool,
        CurrentCSSR.luton,
        CurrentCSSR.manchester,
        CurrentCSSR.medway,
        CurrentCSSR.merton,
        CurrentCSSR.middlesbrough,
        CurrentCSSR.milton_keynes,
        CurrentCSSR.newcastle_upon_tyne,
        CurrentCSSR.newham,
        CurrentCSSR.norfolk,
        CurrentCSSR.north_east_lincolnshire,
        CurrentCSSR.north_lincolnshire,
        CurrentCSSR.north_northamptonshire,
        CurrentCSSR.north_somerset,
        CurrentCSSR.north_tyneside,
        CurrentCSSR.north_yorkshire,
        CurrentCSSR.northamptonshire,
        CurrentCSSR.northumberland,
        CurrentCSSR.nottingham,
        CurrentCSSR.nottinghamshire,
        CurrentCSSR.oldham,
        CurrentCSSR.oxfordshire,
        CurrentCSSR.peterborough,
        CurrentCSSR.plymouth,
        CurrentCSSR.poole,
        CurrentCSSR.portsmouth,
        CurrentCSSR.reading,
        CurrentCSSR.redbridge,
        CurrentCSSR.redcar_and_cleveland,
        CurrentCSSR.richmond_upon_thames,
        CurrentCSSR.rochdale,
        CurrentCSSR.rotherham,
        CurrentCSSR.rutland,
        CurrentCSSR.salford,
        CurrentCSSR.sandwell,
        CurrentCSSR.sefton,
        CurrentCSSR.sheffield,
        CurrentCSSR.shropshire,
        CurrentCSSR.slough,
        CurrentCSSR.solihull,
        CurrentCSSR.somerset,
        CurrentCSSR.south_gloucestershire,
        CurrentCSSR.south_tyneside,
        CurrentCSSR.southampton,
        CurrentCSSR.southend_on_sea,
        CurrentCSSR.southwark,
        CurrentCSSR.st_helens,
        CurrentCSSR.staffordshire,
        CurrentCSSR.stockport,
        CurrentCSSR.stockton_on_tees,
        CurrentCSSR.stoke_on_trent,
        CurrentCSSR.suffolk,
        CurrentCSSR.sunderland,
        CurrentCSSR.surrey,
        CurrentCSSR.sutton,
        CurrentCSSR.swindon,
        CurrentCSSR.tameside,
        CurrentCSSR.telford_and_wrekin,
        CurrentCSSR.thurrock,
        CurrentCSSR.torbay,
        CurrentCSSR.tower_hamlets,
        CurrentCSSR.trafford,
        CurrentCSSR.wakefield,
        CurrentCSSR.walsall,
        CurrentCSSR.waltham_forest,
        CurrentCSSR.wandsworth,
        CurrentCSSR.warrington,
        CurrentCSSR.warwickshire,
        CurrentCSSR.west_berkshire,
        CurrentCSSR.west_northamptonshire,
        CurrentCSSR.west_sussex,
        CurrentCSSR.westminster,
        CurrentCSSR.westmorland_and_furness,
        CurrentCSSR.wigan,
        CurrentCSSR.wiltshire,
        CurrentCSSR.windsor_and_maidenhead,
        CurrentCSSR.wirral,
        CurrentCSSR.wokingham,
        CurrentCSSR.wolverhampton,
        CurrentCSSR.worcestershire,
        CurrentCSSR.york,
    ]


@dataclass
class ONSDistinctValues:
    rural_urban_indicators = len(ONSCategoricalValues.rural_urban_indicators)
    regions = len(ONSCategoricalValues.regions)
    cssrs = len(ONSCategoricalValues.cssrs)
