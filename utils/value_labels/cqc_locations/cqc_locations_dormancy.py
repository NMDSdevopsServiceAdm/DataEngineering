from dataclasses import dataclass

from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)


@dataclass
class CQCLocationsValueLabelsDormancy:
    """The possible values of the dormancy column in CQC locations data"""

    column_name: str = CQCL.dormancy

    dormant: str = "Y"
    not_dormant: str = "N"

    values_list = [dormant, not_dormant]
