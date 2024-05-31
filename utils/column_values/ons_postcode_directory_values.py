from dataclasses import dataclass

from utils.column_names.cleaned_data_files.ons_cleaned_values import (
    OnsCleanedColumns as ONS,
)


@dataclass
class Region:
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

    values_list = [
        east_midlands,
        eastern,
        london,
        north_east,
        north_west,
        south_east,
        south_west,
        west_midlands,
        yorkshire_and_the_humber,
    ]


@dataclass
class CurrentRegion(Region):
    """The possible values of the current region column in ONS data"""

    column_name: str = ONS.current_region


@dataclass
class ContemporaryRegion(Region):
    """The possible values of the contemporary region column in ONS data"""

    column_name: str = ONS.contemporary_region


@dataclass
class RUI:
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

    values_list = [
        rural_hamlet,
        rural_hamlet_sparse,
        rural_village,
        rural_village_sparse,
        rural_town,
        rural_town_sparse,
        urban_city,
        urban_city_sparse,
        urban_major,
        urban_minor,
    ]


@dataclass
class CurrentRUI(RUI):
    """The possible values of the current rural urban indicator column in ONS data"""

    column_name: str = ONS.current_rural_urban_ind_11


@dataclass
class ContemporaryRUI(RUI):
    """The possible values of the contemporary rural urban indicator column in ONS data"""

    column_name: str = ONS.contemporary_rural_urban_ind_11
