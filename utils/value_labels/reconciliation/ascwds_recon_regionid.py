from dataclasses import dataclass

from utils.column_names.raw_data_files.ascwds_workplace_columns import (
    AscwdsWorkplaceColumns as AWP,
)


@dataclass
class AscwdsWorkplaceValueLabelsRegionid:
    """The possible values of the regionid column in ascwds workplace data"""

    column_name: str = AWP.region_id

    labels_dict = {
        "1": "I - Eastern",
        "2": "C - East Midlands",
        "3": "G - London",
        "4": "B - North East",
        "5": "F - North West",
        "6": "H - South East",
        "7": "D - South West",
        "8": "E - West Midlands",
        "9": "J - Yorkshire Humber",
    }
