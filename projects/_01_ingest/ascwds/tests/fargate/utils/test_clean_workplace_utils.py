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
    def test_remove_duplicate_location_ids_within_import_date(self):
        test_schema = [
            AWPClean.establishment_id,
            AWPClean.location_id,
            AWPClean.ascwds_workplace_import_date,
        ]
        test_lf = pl.LazyFrame(
            [
                ("1", "1-001", date(2026, 1, 1)),  # duplicate location_id
                ("2", "1-001", date(2026, 1, 1)),  # duplicate location_id
                ("3", "1-003", date(2026, 1, 1)),  # keep - unique
                ("4", None, date(2026, 1, 1)),  # keep - null location_id
                ("5", None, date(2026, 1, 1)),  # keep - null location_id
                ("6", "1-006", date(2026, 1, 1)),  # keep - unique within import date
                ("7", "1-006", date(2026, 1, 2)),  # keep - unique within import date
            ],
            schema=test_schema,
            orient="row",
        )
        returned_lf = test_lf.filter(job.remove_rows_with_duplicate_location_ids())
        expected_lf = pl.LazyFrame(
            [
                ("3", "1-003", date(2026, 1, 1)),
                ("4", None, date(2026, 1, 1)),
                ("5", None, date(2026, 1, 1)),
                ("6", "1-006", date(2026, 1, 1)),
                ("7", "1-006", date(2026, 1, 2)),
            ],
            schema=test_schema,
            orient="row",
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)
