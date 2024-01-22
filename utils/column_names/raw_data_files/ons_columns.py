from dataclasses import dataclass


@dataclass
class OnsPostcodeDirectoryColumns:
    built_up_are_sub_division: str = "buasd11"
    built_up_area: str = "bua11"
    cancer_alliance: str = "calncv"
    census_area_statistics_ward: str = "casward"
    census_lower_layer_super_output_area_2001: str = "lsoa01"
    census_lower_layer_super_output_area_2011: str = "lsoa11"
    census_lower_layer_super_output_area_2021: str = "lsoa21"
    census_middle_layer_super_output_area_2001: str = "msoa01"
    census_middle_layer_super_output_area_2011: str = "msoa11"
    census_middle_layer_super_output_area_2021: str = "msoa21"
    census_output_area_2001: str = "oa01"
    census_output_area_2011: str = "oa11"
    census_output_area_2021:str = "oa21"
    census_output_area_classification_2001: str = "oac01"
    census_output_area_classification_2011: str = "oac11"
    census_workplace_zone_2011: str = "wz11"
    clinical_commissioning_group: str = "ccg"
    country: str = "ctry"
    county: str = "oscty"
    county_electoral_division: str = "ced"
    date_of_introduction: str = "dointr"
    date_of_termination: str = "doterm"
    day: str = "day"
    electoral_ward: str = "osward"
    european_electoral_region: str = "eer"
    former_strategic_health_authority: str = "oshlthau"
    grid_reference_positional_quality_indicator: str = "osgrdind"
    import_date: str = "import_date"
    index_of_multiple_deprivation: str = "imd"
    integrated_care_board:str = "icb"
    international_terratorial_levels: str = "itl"
    latitude: str = "lat"
    local_enterprise_partnership_first_instance: str = "lep1"
    local_enterprise_partnership_second_instance: str = "lep2"
    local_learning_and_skills_council: str = "teclec"
    local_or_unitary_authority: str = "oslaua"
    longitude: str = "long"
    lower_super_output_area: str = "lsoa"
    middle_super_output_area: str = "msoa"
    month: str = "month"
    national_grid_reference_easting: str = "oseast1m"
    national_grid_reference_northing: str = "osnrth1m"
    national_park: str = "park"
    nhs_england_region: str = "nhser"
    parish: str = "parish"
    police_force_area: str = "pfa"
    postcode_eight_characters: str = "pcd2"
    postcode_seven_characters: str = "pcd"
    postcode_user_type: str = "usertype"
    postcode_variable_length: str = "pcds"
    primary_care_trust: str = "pct"
    region: str = "rgn"
    rural_urban_indicator: str = "ru_ind"
    rural_urban_indicator_2001: str = "ur01ind"
    rural_urban_indicator_2011: str = "ru11ind"
    standard_statistical_region: str = "streg"
    statistical_ward_2005: str = "statsward"
    sub_integrated_care_board_location:str = "sicbl"
    sustainability_and_transformation_partnership: str = "stp"
    travel_to_work_area: str = "ttwa"
    westminster_parliamentary_consitituency: str = "pcon"
    year: str = "year"


