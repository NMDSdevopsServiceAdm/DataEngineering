from dataclasses import dataclass

from utils.column_names.raw_data_files.ascwds_workplace_columns import (
    AscwdsWorkplaceColumns as AWP,
)


@dataclass
class AscwdsWorkplaceValueLabelsIsParent:
    """The possible values of the parentpermission column in ascwds workplace data"""

    column_name: str = AWP.parent_permission

    labels_dict = {
        "0": "No",
        "1": "Yes",
    }
