from dataclasses import dataclass


@dataclass
class OnsPostcodeDataAliases:
    ons_postcode: str = "ons_postcode"
    region_alias: str = "ons_region"
    nhs_england_region_alias: str = "nhs_england_region"
    country_alias: str = "country"
    lsoa_alias: str = "lsoa"
    msoa_alias: str = "msoa"
    ccg_alias: str = "clinical_commisioning_group"
    rural_urban_indicator_alias: str = "rural_urban_indicator"
    import_date_alias: str = "ons_import_date"
