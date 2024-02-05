from dataclasses import dataclass


@dataclass
class OnsPostcodeDirectoryColumns:
    postcode:str = "hybrid_postcode"
    cssr:str = "cssr"
    region:str = "region"
    sub_icb:str = "sub_icb"
    icb:str = "icb"
    icb_region:str ="icb_region"
    ccg:str = "ccg"
    latitude:str = "lat"
    longitude:str ="long"
    imd_score:str ="imd"
    lower_super_output_area_2011:str = "lsoa11"
    middle_super_output_area_2011:str = "msoa11"
    rural_urban_indicator_2011:str = "ru11ind"
    lower_super_output_area_2021:str = "lsoa21"
    middle_super_output_area_2021:str ="msoa21"
    westminster_parliamentary_consitituency: str = "pcon"
    year:str ="year"
    month:str ="month"
    day:str ="day"
    import_date:str = "import_date"
    
@dataclass
class ONSPartitionKeys:
    year: str = OnsPostcodeDirectoryColumns.year
    month: str = OnsPostcodeDirectoryColumns.month
    day: str = OnsPostcodeDirectoryColumns.day
    import_date: str = OnsPostcodeDirectoryColumns.import_date