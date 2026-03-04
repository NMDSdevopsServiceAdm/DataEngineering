from dataclasses import dataclass

from utils.column_names.cleaned_data_files.ons_cleaned import OnsCleanedColumns
from utils.column_values.categorical_column_values import Region


@dataclass
class OnspdRegion:
    """The possible values of the region column in ONS Postcode Directory data"""

    column_name: str = OnsCleanedColumns.region

    labels_dict = {
        "1": Region.eastern,
        "2": Region.east_midlands,
        "3": Region.london,
        "4": Region.north_east,
        "5": Region.north_west,
        "6": Region.south_east,
        "7": Region.south_west,
        "8": Region.west_midlands,
        "9": Region.yorkshire_and_the_humber,
    }
