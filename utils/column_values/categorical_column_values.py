from dataclasses import dataclass, asdict


@dataclass
class ColumnValues:
    column_name: str
    value_to_remove: str = None
    contains_null_values: bool = False

    def __post_init__(self):
        self.categorical_values = self.list_values()
        self.count_of_categorical_values = self.count_values()

    def list_values(self) -> list:
        value_to_remove = self.value_to_remove
        dict_values = asdict(self)
        dict_values.pop("column_name")
        dict_values.pop("value_to_remove")
        dict_values.pop("contains_null_values")
        list_values = list(dict_values.values())
        if value_to_remove in list_values:
            list_values.remove(value_to_remove)
        return list_values

    def count_values(self) -> int:
        count = len(self.categorical_values)
        total_count = count + 1 if self.contains_null_values == True else count
        return total_count


@dataclass
class Dormancy(ColumnValues):
    """The possible values of the dormancy column in CQC locations data"""

    dormant: str = "Y"
    not_dormant: str = "N"


@dataclass
class LocationType(ColumnValues):
    """The possible values of the type column in CQC locations data"""

    social_care_identifier: str = "Social Care Org"
    nhs_healthcare_identifier: str = "NHS Healthcare Organisation"
    independent_healthcare_identifier: str = "Independent Healthcare Org"
    primary_medical_identifier: str = "Primary Medical Services"
    independent_ambulance_identifier: str = "Independent Ambulance"
    primary_dental_identifier: str = "Primary Dental Care"


@dataclass
class RegistrationStatus(ColumnValues):
    """The possible values of the registration status column in CQC locations data"""

    registered: str = "Registered"
    deregistered: str = "Deregistered"


@dataclass
class PrimaryServiceType(ColumnValues):
    """The possible values of the primary service type column in CQC locations data"""

    care_home_with_nursing: str = "Care home with nursing"
    care_home_only: str = "Care home without nursing"
    non_residential: str = "non-residential"


@dataclass
class Services(ColumnValues):
    """The possible values of the GAC service types column in CQC locations data"""

    care_home_service_with_nursing: str = "Care home service with nursing"
    care_home_service_without_nursing: str = "Care home service without nursing"
    community_based_services_for_people_who_misuse_substances: str = (
        "Community based services for people who misuse substances"
    )
    hospice_services: str = "Hospice services"
    domiciliary_care_service: str = "Domiciliary care service"
    remote_clinical_advice_service: str = "Remote clinical advice service"
    acute_services_without_overnight_beds: str = (
        "Acute services without overnight beds / listed acute services with or without overnight beds"
    )
    specialist_college_service: str = "Specialist college service"
    ambulance_service: str = "Ambulance service"
    extra_care_housing_services: str = "Extra Care housing services"
    urgent_care_services: str = "Urgent care services"
    supported_living_service: str = "Supported living service"
    prison_healthcare_services: str = "Prison Healthcare Services"
    community_based_services_for_people_with_mental_health_needs: str = (
        "Community based services for people with mental health needs"
    )
    community_healthcare_service: str = "Community healthcare service"
    community_based_services_for_people_with_a_learning_disability: str = (
        "Community based services for people with a learning disability"
    )
    community_health_care_services_nurses_agency_only: str = (
        "Community health care services - Nurses Agency only"
    )
    dental_service: str = "Dental service"
    mobile_doctors_service: str = "Mobile doctors service"
    long_term_conditions_services: str = "Long term conditions services"
    doctors_consultation_service: str = "Doctors consultation service"
    shared_lives: str = "Shared Lives"
    acute_services_with_overnight_beds: str = "Acute services with overnight beds"
    diagnostic_and_screening_service: str = "Diagnostic and/or screening service"
    residential_substance_misuse_treatment_and_rehabilitation_service: str = (
        "Residential substance misuse treatment and/or rehabilitation service"
    )
    rehabilitation_services: str = "Rehabilitation services"
    doctors_treatment_service: str = "Doctors treatment service"
    hospice_services_at_home: str = "Hospice services at home"
    hospital_services_for_people_with_mental_health_needs: str = (
        "Hospital services for people with mental health needs, learning disabilities and problems with substance misuse"
    )


@dataclass
class PIRType(ColumnValues):
    """The possible values of the PIR type column in CQC PIR data"""

    residential: str = "Residential"
    community: str = "Community"


@dataclass
class CareHome(ColumnValues):
    """The possible values of the care home column in CQC PIR data"""

    care_home: str = "Y"
    not_care_home: str = "N"


@dataclass
class Sector(ColumnValues):
    """The possible values of the sector column in CQC data"""

    local_authority: str = "Local authority"
    independent: str = "Independent"


@dataclass
class MainJobRoleLabels(ColumnValues):
    """The possible values of the main job role column in ASCWDS data"""

    not_known: str = "Not known"
    senior_management: str = "Senior Management"
    middle_management: str = "Middle Management"
    first_line_manager: str = "First Line Manager"
    registered_manager: str = "Registered Manager"
    supervisor: str = "Supervisor"
    social_worker: str = "Social Worker"
    senior_care_worker: str = "Senior Care Worker"
    care_worker: str = "Care Worker"
    community_support_and_outreach: str = "Community Support and Outreach Work"
    employment_support: str = "Employment Support"
    advocacy: str = "Advice Guidance and Advocacy"
    occupational_therapist: str = "Occupational Therapist"
    registered_nurse: str = "Registered Nurse"
    allied_health_professional: str = "Allied Health Professional"
    technician: str = "Technician"
    other_care_role: str = "Other care-providing job role"
    care_related_staff: str = (
        "Managers and staff in care-related but not care-providing roles"
    )
    admin_staff: str = "Administrative or office staff not care-providing"
    ancillary_staff: str = "Ancillary staff not care-providing"
    other_non_care_related_staff: str = "Other non-care-providing job roles"
    activites_worker: str = "Activities worker or co-ordinator"
    safeguarding_officer: str = "Safeguarding and reviewing officer"
    occupational_therapist_assistant: str = "Occupational therapist assistant"
    registered_nursing_associate: str = "Registered Nursing Associate"
    nursing_assistant: str = "Nursing Assistant"
    assessment_officer: str = "Assessment officer"
    care_coordinator: str = "Care co-ordinator"
    care_navigator: str = "Care navigator"
    childrens_roles: str = "Any Childrens/young peoples job role"
    deputy_manager: str = "Deputy manager"
    learning_and_development_lead: str = "Learning and development lead"
    team_leader: str = "Team leader"
    data_analyst: str = "Data Analyst"
    data_governance_manager: str = "Data Governance Manager"
    it_and_digital_support: str = "IT and Digital Support"
    it_manager: str = "IT Manager"
    it_service_desk_worker: str = "IT Service Desk Manager"
    software_developer: str = "Software Developer"
    support_worker: str = "Support Worker"


@dataclass
class MainJobRoleID(ColumnValues):
    """The possible values of the main job role column in ASCWDS data"""

    not_known: str = "-1"
    senior_management: str = "1"
    middle_management: str = "2"
    first_line_manager: str = "3"
    registered_manager: str = "4"
    supervisor: str = "5"
    social_worker: str = "6"
    senior_care_worker: str = "7"
    care_worker: str = "8"
    community_support_and_outreach: str = "9"
    employment_support: str = "10"
    advocacy: str = "11"
    occupational_therapist: str = "15"
    registered_nurse: str = "16"
    allied_health_professional: str = "17"
    technician: str = "22"
    other_care_role: str = "23"
    care_related_staff: str = "24"
    admin_staff: str = "25"
    ancillary_staff: str = "26"
    other_non_care_related_staff: str = "27"
    activites_worker: str = "34"
    safeguarding_officer: str = "35"
    occupational_therapist_assistant: str = "36"
    registered_nursing_associate: str = "37"
    nursing_assistant: str = "38"
    assessment_officer: str = "39"
    care_coordinator: str = "40"
    care_navigator: str = "41"
    childrens_roles: str = "42"
    deputy_manager: str = "43"
    learning_and_development_lead: str = "44"
    team_leader: str = "45"
    data_analyst: str = "46"
    data_governance_manager: str = "47"
    it_and_digital_support: str = "48"
    it_manager: str = "49"
    it_service_desk_worker: str = "50"
    software_developer: str = "51"
    support_worker: str = "52"


@dataclass
class Region(ColumnValues):
    """The possible values of the region columns in ONS data"""

    east_midlands: str = "East Midlands"
    eastern: str = "Eastern"
    london: str = "London"
    north_east: str = "North East"
    north_west: str = "North West"
    south_east: str = "South East"
    south_west: str = "South West"
    west_midlands: str = "West Midlands"
    yorkshire_and_the_humber: str = "Yorkshire and the Humber"


@dataclass
class RUI(ColumnValues):
    """The possible values of the rural urban indicator columns in ONS data"""

    rural_hamlet_sparse: str = "Rural hamlet and isolated dwellings in a sparse setting"
    rural_hamlet: str = "Rural hamlet and isolated dwellings"
    rural_village: str = "Rural village"
    rural_town_sparse: str = "Rural town and fringe in a sparse setting"
    rural_town: str = "Rural town and fringe"
    urban_city_sparse: str = "Urban city and town in a sparse setting"
    urban_city: str = "Urban city and town"
    urban_major: str = "Urban major conurbation"
    urban_minor: str = "Urban minor conurbation"
    rural_village_sparse: str = "Rural village in a sparse setting"


@dataclass
class CSSR(ColumnValues):
    """The possible values of the local authority columns in ONS data"""

    barking_and_dagenham: str = "Barking & Dagenham"
    barnet: str = "Barnet"
    barnsley: str = "Barnsley"
    bath_and_north_east_somerset: str = "Bath and North East Somerset"
    bedford: str = "Bedford"
    bexley: str = "Bexley"
    birmingham: str = "Birmingham"
    blackburn_with_darwen: str = "Blackburn with Darwen"
    blackpool: str = "Blackpool"
    bolton: str = "Bolton"
    bournemouth: str = "Bournemouth"
    bournemouth_christchurch_and_poole: str = "Bournemouth Christchurch and Poole"
    bracknell_forest: str = "Bracknell Forest"
    bradford: str = "Bradford"
    brent: str = "Brent"
    brighton_and_hove: str = "Brighton & Hove"
    bristol: str = "Bristol"
    bromley: str = "Bromley"
    buckinghamshire: str = "Buckinghamshire"
    bury: str = "Bury"
    calderdale: str = "Calderdale"
    cambridgeshire: str = "Cambridgeshire"
    camden: str = "Camden"
    central_bedfordshire: str = "Central Bedfordshire"
    cheshire_east: str = "Cheshire East"
    cheshire_west_and_chester: str = "Cheshire West & Chester"
    city_of_london: str = "City of London"
    cornwall: str = "Cornwall"
    coventry: str = "Coventry"
    croydon: str = "Croydon"
    cumberland: str = "Cumberland"
    cumbria: str = "Cumbria"
    darlington: str = "Darlington"
    derby: str = "Derby"
    derbyshire: str = "Derbyshire"
    devon: str = "Devon"
    doncaster: str = "Doncaster"
    dorset: str = "Dorset"
    dudley: str = "Dudley"
    durham: str = "Durham"
    ealing: str = "Ealing"
    east_riding_of_yorkshire: str = "East Riding of Yorkshire"
    east_sussex: str = "East Sussex"
    enfield: str = "Enfield"
    essex: str = "Essex"
    gateshead: str = "Gateshead"
    gloucestershire: str = "Gloucestershire"
    greenwich: str = "Greenwich"
    hackney: str = "Hackney"
    halton: str = "Halton"
    hammersmith_and_fulham: str = "Hammersmith & Fulham"
    hampshire: str = "Hampshire"
    haringey: str = "Haringey"
    harrow: str = "Harrow"
    hartlepool: str = "Hartlepool"
    havering: str = "Havering"
    herefordshire: str = "Herefordshire"
    hertfordshire: str = "Hertfordshire"
    hillingdon: str = "Hillingdon"
    hounslow: str = "Hounslow"
    isle_of_wight: str = "Isle of Wight"
    isles_of_scilly: str = "Isles of Scilly"
    islington: str = "Islington"
    kensington_and_chelsea: str = "Kensington & Chelsea"
    kent: str = "Kent"
    kingston_upon_hull: str = "Kingston upon Hull"
    kingston_upon_thames: str = "Kingston upon Thames"
    kirklees: str = "Kirklees"
    knowsley: str = "Knowsley"
    lambeth: str = "Lambeth"
    lancashire: str = "Lancashire"
    leeds: str = "Leeds"
    leicester: str = "Leicester"
    leicestershire: str = "Leicestershire"
    lewisham: str = "Lewisham"
    lincolnshire: str = "Lincolnshire"
    liverpool: str = "Liverpool"
    luton: str = "Luton"
    manchester: str = "Manchester"
    medway: str = "Medway"
    merton: str = "Merton"
    middlesbrough: str = "Middlesbrough"
    milton_keynes: str = "Milton Keynes"
    newcastle_upon_tyne: str = "Newcastle upon Tyne"
    newham: str = "Newham"
    norfolk: str = "Norfolk"
    north_east_lincolnshire: str = "North East Lincolnshire"
    north_lincolnshire: str = "North Lincolnshire"
    north_northamptonshire: str = "North Northamptonshire"
    north_somerset: str = "North Somerset"
    north_tyneside: str = "North Tyneside"
    north_yorkshire: str = "North Yorkshire"
    northamptonshire: str = "Northamptonshire"
    northumberland: str = "Northumberland"
    nottingham: str = "Nottingham"
    nottinghamshire: str = "Nottinghamshire"
    oldham: str = "Oldham"
    oxfordshire: str = "Oxfordshire"
    peterborough: str = "Peterborough"
    plymouth: str = "Plymouth"
    poole: str = "Poole"
    portsmouth: str = "Portsmouth"
    reading: str = "Reading"
    redbridge: str = "Redbridge"
    redcar_and_cleveland: str = "Redcar & Cleveland"
    richmond_upon_thames: str = "Richmond upon Thames"
    rochdale: str = "Rochdale"
    rotherham: str = "Rotherham"
    rutland: str = "Rutland"
    salford: str = "Salford"
    sandwell: str = "Sandwell"
    sefton: str = "Sefton"
    sheffield: str = "Sheffield"
    shropshire: str = "Shropshire"
    slough: str = "Slough"
    solihull: str = "Solihull"
    somerset: str = "Somerset"
    south_gloucestershire: str = "South Gloucestershire"
    south_tyneside: str = "South Tyneside"
    southampton: str = "Southampton"
    southend_on_sea: str = "Southend on Sea"
    southwark: str = "Southwark"
    st_helens: str = "St Helens"
    staffordshire: str = "Staffordshire"
    stockport: str = "Stockport"
    stockton_on_tees: str = "Stockton on Tees"
    stoke_on_trent: str = "Stoke on Trent"
    suffolk: str = "Suffolk"
    sunderland: str = "Sunderland"
    surrey: str = "Surrey"
    sutton: str = "Sutton"
    swindon: str = "Swindon"
    tameside: str = "Tameside"
    telford_and_wrekin: str = "Telford & Wrekin"
    thurrock: str = "Thurrock"
    torbay: str = "Torbay"
    tower_hamlets: str = "Tower Hamlets"
    trafford: str = "Trafford"
    wakefield: str = "Wakefield"
    walsall: str = "Walsall"
    waltham_forest: str = "Waltham Forest"
    wandsworth: str = "Wandsworth"
    warrington: str = "Warrington"
    warwickshire: str = "Warwickshire"
    west_berkshire: str = "West Berkshire"
    west_northamptonshire: str = "West Northamptonshire"
    west_sussex: str = "West Sussex"
    westminster: str = "Westminster"
    westmorland_and_furness: str = "Westmorland and Furness"
    wigan: str = "Wigan"
    wiltshire: str = "Wiltshire"
    windsor_and_maidenhead: str = "Windsor & Maidenhead"
    wirral: str = "Wirral"
    wokingham: str = "Wokingham"
    wolverhampton: str = "Wolverhampton"
    worcestershire: str = "Worcestershire"
    york: str = "York"


@dataclass
class ASCWDSFilledPostsSource(ColumnValues):
    """The possible values of the ASCWDS filled posts source column in the independent CQC estimates pipeline"""

    worker_records_and_total_staff: str = "worker records and total staff were the same"
    only_total_staff: str = "only totalstaff was provided"
    only_worker_records: str = "only wkrrecs was provided"
    average_of_total_staff_and_worker_records: str = (
        "average of total staff and worker records as both were similar"
    )


@dataclass
class EstimateFilledPostsSource(ColumnValues):
    """The possible values of the estimate filled posts source column in the independent CQC estimates pipeline"""

    rolling_average_model: str = "rolling_average_model"
    care_home_model: str = "care_home_model"
    interpolation_model: str = "interpolation_model"
    extrapolation_model: str = "extrapolation_model"
    ascwds_filled_posts_clean_deduplicated: str = (
        "ascwds_filled_posts_clean_deduplicated"
    )
    non_res_with_pir_model: str = "non_res_with_pir_model"


@dataclass
class CQCRatingsValues(ColumnValues):
    current: str = "Current"
    historic: str = "Historic"
    outstanding: str = "Outstanding"
    good: str = "Good"


@dataclass
class ParentsOrSinglesAndSubs(ColumnValues):
    singles_and_subs: str = "singles_and_subs"
    parents: str = "parents"


@dataclass
class IsParent(ColumnValues):
    is_parent: str = "Yes"
    is_not_parent: str = "No"


@dataclass
class Subject(ColumnValues):
    single_sub_subject_value: str = "CQC Reconcilliation Work"
    parent_subject_value: str = "CQC Reconcilliation Work - Parent"


@dataclass
class SingleSubDescription(ColumnValues):
    single_sub_deregistered_description: str = "Potential (new): Deregistered ID"
    single_sub_reg_type_description: str = "Potential (new): Regtype"
