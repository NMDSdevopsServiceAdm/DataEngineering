from datetime import date

import polars as pl
import polars.testing as pl_testing

import projects._03_independent_cqc.utils.join_utils as job
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.cleaned_data_files.cqc_pir_cleaned import (
    CqcPIRCleanedColumns as CQCPIRClean,
)
from utils.column_values.categorical_column_values import Sector

CLEAN_CQC_LOCATION_FOR_MERGE_SCHEMA = pl.Schema(
    [
        (CQCLClean.location_id, pl.String()),
        (CQCLClean.cqc_location_import_date, pl.Date()),
        (CQCLClean.cqc_sector, pl.String()),
        (CQCLClean.care_home, pl.String()),
        (CQCLClean.number_of_beds, pl.Int64()),
    ]
)
CLEAN_CQC_LOCATION_FOR_MERGE_ROWS = [
    ("1-001", date(2024, 1, 1), Sector.independent, "Y", 10),
    ("1-002", date(2024, 1, 1), Sector.independent, "N", None),
    ("1-003", date(2024, 1, 1), Sector.independent, "N", None),
    ("1-001", date(2024, 2, 1), Sector.independent, "Y", 10),
    ("1-002", date(2024, 2, 1), Sector.independent, "N", None),
    ("1-003", date(2024, 2, 1), Sector.independent, "N", None),
    ("1-001", date(2024, 3, 1), Sector.independent, "Y", 10),
    ("1-002", date(2024, 3, 1), Sector.independent, "N", None),
    ("1-003", date(2024, 3, 1), Sector.independent, "N", None),
]  # fmt: skip

DATA_TO_MERGE_WITHOUT_CARE_HOME_COL_SCHEMA = pl.Schema(
    [
        (AWPClean.location_id, pl.String()),
        (AWPClean.ascwds_workplace_import_date, pl.Date()),
        (AWPClean.establishment_id, pl.String()),
        (AWPClean.total_staff, pl.Int64()),
    ]
)
DATA_TO_MERGE_WITHOUT_CARE_HOME_COL_ROWS = [
    ("1-001", date(2024, 1, 1), "1", 1),
    ("1-003", date(2024, 1, 1), "3", 2),
    ("1-001", date(2024, 1, 5), "1", 3),
    ("1-001", date(2024, 1, 9), "1", 4),
    ("1-003", date(2024, 1, 9), "3", 5),
    ("1-003", date(2024, 3, 1), "4", 6),
]

EXPECTED_MERGED_WITHOUT_CARE_HOME_COL_SCHEMA = pl.Schema(
    list(CLEAN_CQC_LOCATION_FOR_MERGE_SCHEMA.items())
    + [
        (AWPClean.ascwds_workplace_import_date, pl.Date()),
        (AWPClean.establishment_id, pl.String()),
        (AWPClean.total_staff, pl.Int64()),
    ]
)
EXPECTED_MERGED_WITHOUT_CARE_HOME_COL_ROWS = [
    ("1-001", date(2024, 1, 1), Sector.independent, "Y", 10, date(2024, 1, 1), "1", 1),
    ("1-002", date(2024, 1, 1), Sector.independent, "N", None, date(2024, 1, 1), None, None),
    ("1-003", date(2024, 1, 1), Sector.independent, "N", None, date(2024, 1, 1), "3", 2),
    ("1-001", date(2024, 2, 1), Sector.independent, "Y", 10, date(2024, 1, 9), "1", 4),
    ("1-002", date(2024, 2, 1), Sector.independent, "N", None, date(2024, 1, 9), None, None),
    ("1-003", date(2024, 2, 1), Sector.independent, "N", None, date(2024, 1, 9), "3", 5),
    ("1-001", date(2024, 3, 1), Sector.independent, "Y", 10, date(2024, 3, 1), None, None),
    ("1-002", date(2024, 3, 1), Sector.independent, "N", None, date(2024, 3, 1), None, None),
    ("1-003", date(2024, 3, 1), Sector.independent, "N", None, date(2024, 3, 1), "4", 6),
]  # fmt: skip

DATA_TO_MERGE_WITH_CARE_HOME_COL_SCHEMA = pl.Schema(
    [
        (CQCPIRClean.location_id, pl.String()),
        (CQCPIRClean.care_home, pl.String()),
        (CQCPIRClean.cqc_pir_import_date, pl.Date()),
        (CQCPIRClean.pir_people_directly_employed_cleaned, pl.Int64()),
    ]
)
DATA_TO_MERGE_WITH_CARE_HOME_COL_ROWS = [
    ("1-001", "Y", date(2024, 1, 1), 10),
    ("1-002", "N", date(2024, 1, 1), 20),
    ("1-003", "Y", date(2024, 1, 1), 30),
    ("1-001", "Y", date(2024, 2, 1), 1),
    ("1-002", "N", date(2024, 2, 1), 4),
]

EXPECTED_MERGED_WITH_CARE_HOME_COL_SCHEMA = pl.Schema(
    list(CLEAN_CQC_LOCATION_FOR_MERGE_SCHEMA.items())
    + [
        (CQCPIRClean.cqc_pir_import_date, pl.Date()),
        (CQCPIRClean.pir_people_directly_employed_cleaned, pl.Int64()),
    ]
)
EXPECTED_MERGED_WITH_CARE_HOME_COL_ROWS = [
    ("1-001", date(2024, 1, 1), Sector.independent, "Y", 10, date(2024, 1, 1), 10),
    ("1-002", date(2024, 1, 1), Sector.independent, "N", None, date(2024, 1, 1), 20),
    ("1-003", date(2024, 1, 1), Sector.independent, "N", None, date(2024, 1, 1), None),
    ("1-001", date(2024, 2, 1), Sector.independent, "Y", 10, date(2024, 2, 1), 1),
    ("1-002", date(2024, 2, 1), Sector.independent, "N", None, date(2024, 2, 1), 4),
    ("1-003", date(2024, 2, 1), Sector.independent, "N", None, date(2024, 2, 1), None),
    ("1-001", date(2024, 3, 1), Sector.independent, "Y", 10, date(2024, 2, 1), 1),
    ("1-002", date(2024, 3, 1), Sector.independent, "N", None, date(2024, 2, 1), 4),
    ("1-003", date(2024, 3, 1), Sector.independent, "N", None, date(2024, 2, 1), None),
]  # fmt: skip


class TestJoinDataIntoCqcLf:
    def test_returns_expected_data_when_care_home_column_not_required(self):
        cqc_lf = pl.LazyFrame(
            data=CLEAN_CQC_LOCATION_FOR_MERGE_ROWS,
            schema=CLEAN_CQC_LOCATION_FOR_MERGE_SCHEMA,
            orient="row",
        )
        join_lf = pl.LazyFrame(
            data=DATA_TO_MERGE_WITHOUT_CARE_HOME_COL_ROWS,
            schema=DATA_TO_MERGE_WITHOUT_CARE_HOME_COL_SCHEMA,
            orient="row",
        )

        returned_lf = job.join_data_into_cqc_lf(
            cqc_lf,
            join_lf,
            AWPClean.location_id,
            AWPClean.ascwds_workplace_import_date,
        )

        expected_merged_lf = pl.LazyFrame(
            data=EXPECTED_MERGED_WITHOUT_CARE_HOME_COL_ROWS,
            schema=EXPECTED_MERGED_WITHOUT_CARE_HOME_COL_SCHEMA,
            orient="row",
        ).select(returned_lf.collect_schema().names())

        pl_testing.assert_frame_equal(returned_lf, expected_merged_lf)

    def test_returns_expected_data_when_care_home_column_is_required(self):
        cqc_lf = pl.LazyFrame(
            data=CLEAN_CQC_LOCATION_FOR_MERGE_ROWS,
            schema=CLEAN_CQC_LOCATION_FOR_MERGE_SCHEMA,
            orient="row",
        )
        join_lf = pl.LazyFrame(
            data=DATA_TO_MERGE_WITH_CARE_HOME_COL_ROWS,
            schema=DATA_TO_MERGE_WITH_CARE_HOME_COL_SCHEMA,
            orient="row",
        )

        returned_lf = job.join_data_into_cqc_lf(
            cqc_lf,
            join_lf,
            CQCPIRClean.location_id,
            CQCPIRClean.cqc_pir_import_date,
            CQCPIRClean.care_home,
        )

        expected_merged_lf = pl.LazyFrame(
            data=EXPECTED_MERGED_WITH_CARE_HOME_COL_ROWS,
            schema=EXPECTED_MERGED_WITH_CARE_HOME_COL_SCHEMA,
            orient="row",
        ).select(returned_lf.collect_schema().names())

        pl_testing.assert_frame_equal(returned_lf, expected_merged_lf)
