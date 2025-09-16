import itertools
import json
from pathlib import Path

import polars as pl

from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    NewCqcLocationApiColumns as CQCL,
)

CONFIG = Path(__file__).parent / "exclusions.json"
EXCLUSIONS = json.loads(CONFIG.read_text())


def is_duplicated_worker_data() -> pl.Expr:
    """Identifies records known to be duplicates in the raw data.

    There are no required args but the expression should be used on a DataFrame
    which include columns:
            - workerid
            - ascwds_worker_import_date
            - establishmentid

    Returns:
        pl.Expr: an expression that shows which records are marked for exclusions
    """
    duplicate_workers = EXCLUSIONS["worker"]
    return (
        pl.struct(
            pl.col("workerid"),
            pl.col("ascwds_worker_import_date"),
            pl.col("establishmentid"),
        )
        .is_in(duplicate_workers)
        .not_()
    )


def is_duplicated_workplace_data() -> pl.Expr:
    """Identifies duplicate workplace records based on establishmentid.

    These are duplicates in the sense that the same data has been uploaded to ASCWDS for multiple accounts.

    This has been passed on to the support team (03/06/2025) to investigate which may affect what we do with
    these locations long term but in the short term we're simply removing them from ASCWDS.

    There are two sets of workplaces here.
      - Four locations who submit the exact same ASCWDS files on the same day.
      - 18 separate locations, seemingly unrelated, all submit identical data on the same day.

    There are no required args but the expression should be used on a DataFrame
    which include columns:
            - establishmentid

    Returns:
        pl.Expr: an expression that shows which records are marked for exclusions
    """
    duplicate_workplaces = EXCLUSIONS["workplace"]["establishmentid"]
    duplicates = list(itertools.chain.from_iterable(duplicate_workplaces.values()))
    return pl.col(AWPClean.establishment_id).is_in(duplicates)


def is_invalid_location() -> pl.Expr:
    """Identifies invalid records based on locationId for known records.

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
        pl.Expr: an expression that shows which records are marked for exclusions
    """
    invalid_locations = EXCLUSIONS["locationId"].values()
    return pl.col(CQCL.location_id).is_in(invalid_locations)
