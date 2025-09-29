"""
This is a 1:1 copy of the non-polars file, created to remove the import of pyspark
"""

from dataclasses import dataclass


@dataclass
class RecordsToRemoveInLocationsData:
    """
    This class contains the locations ids that should be removed from
    the locations data.

    Dental Practice:
    The location is listed once as a social care org in the locations
    dataset but is lited as Primary Dental Care on every other row and
    in the providers dataset. The location ID is enough to identify
    and remove this row.

    Temporary Registration:
    The location is listed once as registered in the locations dataset,
    but conatins barely any data and appears to have deregistered very
    quickly. The location ID is enough to identify and remove this row.
    """

    dental_practice: str = "1-12082335777"
    temp_registration: str = "1-127367030"
