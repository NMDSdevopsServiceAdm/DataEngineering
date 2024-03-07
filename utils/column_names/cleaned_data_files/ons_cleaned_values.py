from dataclasses import dataclass

from utils.column_names.raw_data_files.ons_columns import (
    OnsPostcodeDirectoryColumns as ONScol,
)


@dataclass
class OnsCleanedColumns(ONScol):
    contemporary_ons_import_date: str = "contemporary_ons_import_date"
    contemporary_cssr: str = "contemporary_" + ONScol.cssr
    contemporary_region: str = "contemporary_" + ONScol.region
    contemporary_sub_icb: str = "contemporary_" + ONScol.sub_icb
    contemporary_icb: str = "contemporary_" + ONScol.icb
    contemporary_ccg: str = "contemporary_" + ONScol.ccg
    contemporary_latitude: str = "contemporary_" + ONScol.latitude
    contemporary_longitude: str = "contemporary_" + ONScol.longitude
    contemporary_imd_score: str = "contemporary_" + ONScol.imd_score
    contemporary_lsoa11: str = "contemporary_" + ONScol.lower_super_output_area_2011
    contemporary_msoa11: str = "contemporary_" + ONScol.middle_super_output_area_2011
    contemporary_rural_urban_ind_11: str = "contemporary_" + ONScol.rural_urban_indicator_2011
    contemporary_lsoa21: str = "contemporary_" + ONScol.lower_super_output_area_2021
    contemporary_msoa21: str = "contemporary_" + ONScol.middle_super_output_area_2021
    contemporary_constituancy: str = "contemporary_" + ONScol.westminster_parliamentary_consitituency
    contemporary_geography_columns:list = [
        contemporary_ons_import_date,
        contemporary_cssr,
        contemporary_region,
        contemporary_sub_icb,
        contemporary_icb,
        contemporary_ccg,
        contemporary_latitude,
        contemporary_longitude,
        contemporary_imd_score,
        contemporary_lsoa11,
        contemporary_msoa11,
        contemporary_rural_urban_ind_11,
        contemporary_lsoa21,
        contemporary_msoa21,
        contemporary_constituancy,
    ]
    current_ons_import_date: str = "current_ons_import_date"
    current_cssr: str = "current_" + ONScol.cssr
    current_region: str = "current_" + ONScol.region
    current_sub_icb: str = "current_" + ONScol.sub_icb
    current_icb: str = "current_" + ONScol.icb
    current_ccg: str = "current_" + ONScol.ccg
    current_latitude: str = "current_" + ONScol.latitude
    current_longitude: str = "current_" + ONScol.longitude
    current_imd_score: str = "current_" + ONScol.imd_score
    current_lsoa11: str = "current_" + ONScol.lower_super_output_area_2011
    current_msoa11: str = "current_" + ONScol.middle_super_output_area_2011
    current_rural_urban_ind_11: str = "current_" + ONScol.rural_urban_indicator_2011
    current_lsoa21: str = "current_" + ONScol.lower_super_output_area_2021
    current_msoa21: str = "current_" + ONScol.middle_super_output_area_2021
    current_constituancy: str = "current_" + ONScol.westminster_parliamentary_consitituency
    current_geography_columns:list = [
        current_ons_import_date,
        current_cssr,
        current_region,
        current_sub_icb,
        current_icb,
        current_ccg,
        current_latitude,
        current_longitude,
        current_imd_score,
        current_lsoa11,
        current_msoa11,
        current_rural_urban_ind_11,
        current_lsoa21,
        current_msoa21,
        current_constituancy,
    ]
