from datetime import date

import polars as pl
import polars.testing as pl_testing

import projects._01_ingest.ascwds.fargate.utils.clean_workplace_utils as job
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)


class TestValidWorkplaceFilter:
    def test_valid_workplace_filter_returns_correct_rows(self):
        input_lf = pl.LazyFrame(
            {
                AWPClean.organisation_id: [
                    "305",  # test account
                    "123",
                    "1234",
                    "28470",  # test account
                ],
                AWPClean.establishment_id: [
                    "1",
                    "48904",  # duplicate
                    "12",
                    "50640",  # duplicate
                ],
            }
        )
        expected_lf = pl.LazyFrame(
            {
                AWPClean.organisation_id: ["1234"],
                AWPClean.establishment_id: ["12"],
            }
        )

        returned_lf = input_lf.filter(job.valid_workplace_filter())

        pl_testing.assert_frame_equal(expected_lf, returned_lf)


class TestRemoveRowsWithDuplicateLocationIds:
    def test_function_returns_expected_rows(self):
        test_lf = pl.LazyFrame(
            {
                AWPClean.establishment_id: [
                    "1", # duplicate locationid
                    "2", # duplicate locationid
                    "3", # keep - not duplicated
                    "4", # keep - null locationid
                    "5", # keep - null locationid
                    "6", # keep - not duplicate within import date.
                    "7", # keep - not duplicate within import date.
                ],
                AWPClean.location_id: [
                    "1-001",
                    "1-001",
                    "1-003",
                    None,
                    None,
                    "1-006",
                    "1-006",
                ],
                AWPClean.ascwds_workplace_import_date: [
                    date(2026, 1, 1),
                    date(2026, 1, 1),
                    date(2026, 1, 1),
                    date(2026, 1, 1),
                    date(2026, 1, 1),
                    date(2026, 1, 1),
                    date(2026, 1, 2),
                ],
            }
        ) # fmt: skip

        returned_lf = test_lf.filter(job.remove_rows_with_duplicate_location_ids())
        expected_lf = test_lf.filter(
            ~pl.col(AWPClean.establishment_id).is_in(["1", "2"])
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)
