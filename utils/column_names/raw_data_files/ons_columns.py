from dataclasses import dataclass


@dataclass
class OnsPostcodeDirectoryColumns:
    postcode: str = "HYBRID_POSTCODE"
    cssr: str = "CSSR"
    region: str = "Region"
    sub_icb: str = "Sub_ICB"
    icb: str = "ICB"
    icb_region: str = "ICB_Region"
    ccg: str = "CCG"
    latitude: str = "lat"
    longitude: str = "long"
    imd_score: str = "imd"
    lower_super_output_area_2011: str = "lsoa11"
    middle_super_output_area_2011: str = "msoa11"
    rural_urban_indicator_2011: str = "ru11ind"
    lower_super_output_area_2021: str = "lsoa21"
    middle_super_output_area_2021: str = "msoa21"
    parliamentary_constituency: str = "pcon"
    year: str = "year"
    month: str = "month"
    day: str = "day"
    import_date: str = "import_date"


@dataclass
class ONSPartitionKeys:
    year: str = OnsPostcodeDirectoryColumns.year
    month: str = OnsPostcodeDirectoryColumns.month
    day: str = OnsPostcodeDirectoryColumns.day
    import_date: str = OnsPostcodeDirectoryColumns.import_date
