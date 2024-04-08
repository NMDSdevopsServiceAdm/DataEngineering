from dataclasses import dataclass

from utils.column_names.raw_data_files.ascwds_workplace_columns import (
    AscwdsWorkplaceColumns as AWP,
)


@dataclass
class AscwdsWorkplaceValueLabelsEsttype:
    """The possible values of the esttype column in ascwds workplace data"""

    column_name: str = AWP.establishment_type

    labels_dict = {
        "0": "Not known",
        "1": "Local authority (adult services)",
        "2": "Local authority (childrens services)",
        "3": "Local authority (generic/other)",
        "4": "Local authority owned",
        "6": "Private sector",
        "7": "Voluntary/Charity",
        "8": "Other",
    }
