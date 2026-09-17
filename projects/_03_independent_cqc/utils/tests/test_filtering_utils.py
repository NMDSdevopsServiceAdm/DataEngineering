from datetime import date
from unittest.mock import Mock, patch

import polars as pl

import projects._03_independent_cqc.utils.filtering_utils as job
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

PATCH_PATH = "projects._03_independent_cqc.utils.filtering_utils"


class TestGetMatchedAscwdsDates:
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_get_matched_ascwds_dates_returns_distinct_dates_present_in_metadata(
        self, scan_parquet_mock: Mock
    ):
        scan_parquet_mock.return_value = pl.LazyFrame(
            {
                IndCQC.ascwds_workplace_import_date: [
                    date(2024, 10, 8),
                    date(2024, 10, 8),
                    date(2024, 11, 1),
                ]
            }
        )

        returned_dates = job.get_matched_ascwds_dates(
            metadata_source="some/metadata/source",
            date_col=IndCQC.ascwds_workplace_import_date,
        )

        scan_parquet_mock.assert_called_once_with("some/metadata/source")
        assert sorted(returned_dates.to_list()) == [
            date(2024, 10, 8),
            date(2024, 11, 1),
        ]

    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_get_matched_ascwds_dates_excludes_nulls(self, scan_parquet_mock: Mock):
        scan_parquet_mock.return_value = pl.LazyFrame(
            {
                IndCQC.ascwds_workplace_import_date: [date(2024, 10, 8), None],
            }
        )

        returned_dates = job.get_matched_ascwds_dates(
            metadata_source="some/metadata/source",
            date_col=IndCQC.ascwds_workplace_import_date,
        )

        assert returned_dates.to_list() == [date(2024, 10, 8)]
