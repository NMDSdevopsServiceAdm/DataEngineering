from dataclasses import dataclass

from utils.column_names.raw_data_files.ascwds_workplace_columns import (
    AscwdsWorkplaceColumns as AWP,
)


@dataclass
class AscwdsWorkplaceValueLabelsRegtype:
    """The possible values of the regtype column in ascwds workplace data"""

    column_name: str = AWP.registration_type

    labels_dict = {
        "0": "Not regulated",
        "2": "CQC regulated",
    }
