import json
from datetime import datetime
from pathlib import Path

import polars as pl

from polars_utils.cleaning_utils import DATE_FORMAT
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    NewCqcLocationApiColumns as CQCL,
)

CONFIG = Path(__file__).parent / "exclusions.json"
EXCLUSIONS = json.loads(CONFIG.read_text())


def is_unique_worker_data() -> pl.Expr:
    """Identifies unique records by excluding known duplicates in the raw data.

    There are no required args but the expression should be used on a DataFrame
    which include columns:
            - workerid
            - ascwds_worker_import_date (a Date column)
            - establishmentid

    Returns:
        pl.Expr: an expression that shows which records are marked not as exclusions
    """
    duplicate_workers = [
        {
            AWKClean.worker_id: row["workerid"],
            AWKClean.ascwds_worker_import_date: datetime.strptime(
                row["ascwds_worker_import_date"], DATE_FORMAT
            ).date(),
            AWKClean.establishment_id: row["establishmentid"],
        }
        for row in EXCLUSIONS["worker"]
    ]
    return (
        pl.struct(
            pl.col(AWKClean.worker_id),
            pl.col(AWKClean.ascwds_worker_import_date),
            pl.col(AWKClean.establishment_id),
        )
        .is_in(duplicate_workers)
        .not_()
    )


def is_valid_location() -> pl.Expr:
    """Identifies valid records based on locationId exclusions.

    Known issues so far...

    - Dental Practice:
        The location is listed once as a social care org in the locations
        dataset but is lited as Primary Dental Care on every other row and
        in the providers dataset. The location ID is enough to identify
        and remove this row.

    - Temporary Registration:
        The location is listed once as registered in the locations dataset,
        but conatins barely any data and appears to have deregistered very
        quickly. The location ID is enough to identify and remove this row.

    Returns:
        pl.Expr: an expression that shows which records are not in exclusions
    """
    invalid_locations = EXCLUSIONS["locationId"].values()
    return pl.col(CQCL.location_id).is_in(invalid_locations).not_()
