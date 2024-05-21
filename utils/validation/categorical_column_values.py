from dataclasses import dataclass

from utils.column_names.cleaned_data_files.cqc_location_cleaned_values import (
    CqcLocationCleanedValues as CQCLValues,
)
from utils.feature_engineering_dictionaries import (
    REGION_LOOKUP,
    RURAL_URBAN_INDICATOR_LOOKUP,
)
from utils.value_labels.ascwds_worker.ascwds_worker_mainjrid import (
    AscwdsWorkerValueLabelsMainjrid as MainJobRole,
)


@dataclass
class CQCCategoricalValues:
    primary_service_types = [
        CQCLValues.care_home_only,
        CQCLValues.care_home_with_nursing,
        CQCLValues.non_residential,
    ]
    care_home_values = ["Y", "N"]
    dormancy_values = ["Y", "N"]
    cqc_sector = [CQCLValues.independent, CQCLValues.local_authority]
    registration_status = [CQCLValues.registered]


@dataclass
class CQCDistinctValues:
    primary_service_types = len(CQCCategoricalValues.primary_service_types)
    care_home_values = len(CQCCategoricalValues.care_home_values)
    dormancy_values = len(CQCCategoricalValues.dormancy_values) + 1  # can be null
    registration_status_values = len(CQCCategoricalValues.registration_status)
    cqc_sector_values = len(CQCCategoricalValues.cqc_sector)


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
    cqc_sector = [CQCLValues.independent]
    ascwds_filled_posts_source = [
        "worker records and total staff were the same",
        "only totalstaff was provided",
        "only wkrrecs was provided",
        "average of total staff and worker records as both were similar",
    ]


@dataclass
class IndCQCDistinctValues:
    cqc_sector = len(IndCQCCategoricalValues.cqc_sector)
    ascwds_filled_posts_source = (
        len(IndCQCCategoricalValues.ascwds_filled_posts_source) + 1
    )  # can be null


@dataclass
class ONSCategoricalValues:
    rural_urban_indicators = list(RURAL_URBAN_INDICATOR_LOOKUP.values())
    regions = list(REGION_LOOKUP.values())
    cssrs = [
        "Barking & Dagenham",
        "Barnet",
        "Barnsley",
        "Bath and North East Somerset",
        "Bedford",
        "Bexley",
        "Birmingham",
        "Blackburn with Darwen",
        "Blackpool",
        "Bolton",
        "Bournemouth",
        "Bournemouth Christchurch and Poole",
        "Bracknell Forest",
        "Bradford",
        "Brent",
        "Brighton & Hove",
        "Bristol",
        "Bromley",
        "Buckinghamshire",
        "Bury",
        "Calderdale",
        "Cambridgeshire",
        "Camden",
        "Central Bedfordshire",
        "Cheshire East",
        "Cheshire West & Chester",
        "City of London",
        "Cornwall",
        "Coventry",
        "Croydon",
        "Cumberland",
        "Cumbria",
        "Darlington",
        "Derby",
        "Derbyshire",
        "Devon",
        "Doncaster",
        "Dorset",
        "Dudley",
        "Durham",
        "Ealing",
        "East Riding of Yorkshire",
        "East Sussex",
        "Enfield",
        "Essex",
        "Gateshead",
        "Gloucestershire",
        "Greenwich",
        "Hackney",
        "Halton",
        "Hammersmith & Fulham",
        "Hampshire",
        "Haringey",
        "Harrow",
        "Hartlepool",
        "Havering",
        "Herefordshire",
        "Hertfordshire",
        "Hillingdon",
        "Hounslow",
        "Isle of Wight",
        "Isles of Scilly",
        "Islington",
        "Kensington & Chelsea",
        "Kent",
        "Kingston upon Hull",
        "Kingston upon Thames",
        "Kirklees",
        "Knowsley",
        "Lambeth",
        "Lancashire",
        "Leeds",
        "Leicester",
        "Leicestershire",
        "Lewisham",
        "Lincolnshire",
        "Liverpool",
        "Luton",
        "Manchester",
        "Medway",
        "Merton",
        "Middlesbrough",
        "Milton Keynes",
        "Newcastle upon Tyne",
        "Newham",
        "Norfolk",
        "North East Lincolnshire",
        "North Lincolnshire",
        "North Northamptonshire",
        "North Somerset",
        "North Tyneside",
        "North Yorkshire",
        "Northamptonshire",
        "Northumberland",
        "Nottingham",
        "Nottinghamshire",
        "Oldham",
        "Oxfordshire",
        "Peterborough",
        "Plymouth",
        "Poole",
        "Portsmouth",
        "Reading",
        "Redbridge",
        "Redcar & Cleveland",
        "Richmond upon Thames",
        "Rochdale",
        "Rotherham",
        "Rutland",
        "Salford",
        "Sandwell",
        "Sefton",
        "Sheffield",
        "Shropshire",
        "Slough",
        "Solihull",
        "Somerset",
        "South Gloucestershire",
        "South Tyneside",
        "Southampton",
        "Southend on Sea",
        "Southwark",
        "St Helens",
        "Staffordshire",
        "Stockport",
        "Stockton on Tees",
        "Stoke on Trent",
        "Suffolk",
        "Sunderland",
        "Surrey",
        "Sutton",
        "Swindon",
        "Tameside",
        "Telford & Wrekin",
        "Thurrock",
        "Torbay",
        "Tower Hamlets",
        "Trafford",
        "Wakefield",
        "Walsall",
        "Waltham Forest",
        "Wandsworth",
        "Warrington",
        "Warwickshire",
        "West Berkshire",
        "West Northamptonshire",
        "West Sussex",
        "Westminster",
        "Westmorland and Furness",
        "Wigan",
        "Wiltshire",
        "Windsor & Maidenhead",
        "Wirral",
        "Wokingham",
        "Wolverhampton",
        "Worcestershire",
        "York",
    ]


@dataclass
class ONSDistinctValues:
    rural_urban_indicators = len(ONSCategoricalValues.rural_urban_indicators)
    regions = len(ONSCategoricalValues.regions)
    cssrs = len(ONSCategoricalValues.cssrs)
