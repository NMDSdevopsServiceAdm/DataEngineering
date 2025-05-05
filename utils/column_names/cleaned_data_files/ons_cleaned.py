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
    contemporary_icb_region: str = "contemporary_" + ONScol.icb_region
    contemporary_ccg: str = "contemporary_" + ONScol.ccg
    contemporary_latitude: str = "contemporary_" + ONScol.latitude
    contemporary_longitude: str = "contemporary_" + ONScol.longitude
    contemporary_imd_score: str = "contemporary_" + ONScol.imd_score
    contemporary_lsoa11: str = "contemporary_" + ONScol.lower_super_output_area_2011
    contemporary_msoa11: str = "contemporary_" + ONScol.middle_super_output_area_2011
    contemporary_rural_urban_ind_11: str = (
        "contemporary_" + ONScol.rural_urban_indicator_2011
    )
    contemporary_lsoa21: str = "contemporary_" + ONScol.lower_super_output_area_2021
    contemporary_msoa21: str = "contemporary_" + ONScol.middle_super_output_area_2021
    parliamentary_constituency: str = (
        "contemporary_" + ONScol.parliamentary_constituency
    )
    current_ons_import_date: str = "current_ons_import_date"
    current_cssr: str = "current_" + ONScol.cssr
    current_region: str = "current_" + ONScol.region
    current_sub_icb: str = "current_" + ONScol.sub_icb
    current_icb: str = "current_" + ONScol.icb
    current_icb_region: str = "current_" + ONScol.icb_region
    current_ccg: str = "current_" + ONScol.ccg
    current_latitude: str = "current_" + ONScol.latitude
    current_longitude: str = "current_" + ONScol.longitude
    current_imd_score: str = "current_" + ONScol.imd_score
    current_lsoa11: str = "current_" + ONScol.lower_super_output_area_2011
    current_msoa11: str = "current_" + ONScol.middle_super_output_area_2011
    current_rural_urban_ind_11: str = "current_" + ONScol.rural_urban_indicator_2011
    current_lsoa21: str = "current_" + ONScol.lower_super_output_area_2021
    current_msoa21: str = "current_" + ONScol.middle_super_output_area_2021
    current_constituency: str = "current_" + ONScol.parliamentary_constituency


contemporary_geography_columns: list = [
    OnsCleanedColumns.contemporary_ons_import_date,
    OnsCleanedColumns.contemporary_cssr,
    OnsCleanedColumns.contemporary_region,
    OnsCleanedColumns.contemporary_sub_icb,
    OnsCleanedColumns.contemporary_icb,
    OnsCleanedColumns.contemporary_icb_region,
    OnsCleanedColumns.contemporary_ccg,
    OnsCleanedColumns.contemporary_latitude,
    OnsCleanedColumns.contemporary_longitude,
    OnsCleanedColumns.contemporary_imd_score,
    OnsCleanedColumns.contemporary_lsoa11,
    OnsCleanedColumns.contemporary_msoa11,
    OnsCleanedColumns.contemporary_rural_urban_ind_11,
    OnsCleanedColumns.contemporary_lsoa21,
    OnsCleanedColumns.contemporary_msoa21,
    OnsCleanedColumns.parliamentary_constituency,
]

current_geography_columns: list = [
    OnsCleanedColumns.current_ons_import_date,
    OnsCleanedColumns.current_cssr,
    OnsCleanedColumns.current_region,
    OnsCleanedColumns.current_sub_icb,
    OnsCleanedColumns.current_icb,
    OnsCleanedColumns.current_icb_region,
    OnsCleanedColumns.current_ccg,
    OnsCleanedColumns.current_latitude,
    OnsCleanedColumns.current_longitude,
    OnsCleanedColumns.current_imd_score,
    OnsCleanedColumns.current_lsoa11,
    OnsCleanedColumns.current_msoa11,
    OnsCleanedColumns.current_rural_urban_ind_11,
    OnsCleanedColumns.current_lsoa21,
    OnsCleanedColumns.current_msoa21,
    OnsCleanedColumns.current_constituency,
]
