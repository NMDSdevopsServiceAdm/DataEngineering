from dataclasses import dataclass

from utils.column_names.cleaned_data_files.ons_cleaned import OnsCleanedColumns


@dataclass
class OnspdIcbRegion:
    """The possible values of the ICB Region column in ONS Postcode Directory data"""

    column_name: str = OnsCleanedColumns.icb_region

    east_of_england: str = "East of England"
    london: str = "London"
    midlands: str = "Midlands"
    north_east_and_yorkshire: str = "North East and Yorkshire"
    north_west: str = "North West"
    south_east: str = "South East"
    south_west: str = "South West"

    labels_dict = {
        "1": east_of_england,
        "2": midlands,
        "3": london,
        "4": north_east_and_yorkshire,
        "5": north_west,
        "6": south_east,
        "7": south_west,
        "40000003": london,
        "40000005": south_east,
        "40000006": south_west,
        "40000007": east_of_england,
        "40000010": north_west,
        "40000011": midlands,
        "40000012": north_east_and_yorkshire,
    }
