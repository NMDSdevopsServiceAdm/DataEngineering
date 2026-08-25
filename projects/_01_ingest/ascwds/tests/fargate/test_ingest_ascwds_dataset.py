from unittest.mock import Mock, patch

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._01_ingest.ascwds.fargate.ingest_ascwds_dataset as job
from utils.column_names.raw_data_files.ascwds_worker_columns import (
    AscwdsWorkerColumns as AWK,
)

PATCH_PATH = "projects._01_ingest.ascwds.fargate.ingest_ascwds_dataset"

TEST_CSV_SOURCE = "projects/_01_ingest/unittest_data/test_ingest_ascwds_dataset.csv"


class TestMain:
    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    def test_reads_source_csv_with_no_type_inference(self, mock_sink_to_parquet: Mock):
        job.main(TEST_CSV_SOURCE, "s3://dest-bucket/")

        expected_columns = [
            AWK.establishment_id,
            AWK.worker_id,
            AWK.main_job_role_id,
            AWK.import_date,
            "jr01work",
        ]
        expected_lf = pl.LazyFrame(
            {
                AWK.establishment_id: ["estab_1", "estab_2", "estab_3"],
                AWK.worker_id: ["worker_1", "worker_2", "worker_3"],
                AWK.main_job_role_id: ["8", "6", "1"],
                AWK.import_date: ["20260101", "20260101", "20260115"],
                "jr01work": [
                    "05",
                    "12",
                    "00",
                ],  # leading zeros preserved: no type inference
            },
            schema={column: pl.String for column in expected_columns},
        )

        returned_lf = mock_sink_to_parquet.call_args.kwargs["lazy_df"]
        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    source = "s3://source-bucket/domain=ASCWDS/dataset=worker/file.csv"
    destination_prefix = "s3://dest-bucket/"

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.pl.scan_csv")
    def test_sinks_to_a_path_derived_from_the_source_key_and_destination_prefix(
        self,
        mock_scan_csv: Mock,
        mock_sink_to_parquet: Mock,
    ):
        mock_lf = Mock(spec=pl.LazyFrame)
        mock_lf.collect_schema.return_value.names.return_value = []
        mock_scan_csv.return_value = mock_lf

        job.main(self.source, self.destination_prefix)

        mock_scan_csv.assert_called_once_with(
            self.source,
            separator="|",
            infer_schema=False,
        )
        mock_sink_to_parquet.assert_called_once_with(
            lazy_df=mock_lf,
            output_path=f"{self.destination_prefix}domain=ASCWDS/dataset=worker/",
        )


class TestRaiseErrorIfMainjridIncludesUnknownValues:
    def test_returns_original_lf_unchanged_if_mainjrid_column_not_present(self):
        lf = pl.LazyFrame({AWK.establishment_id: ["estab_1"]})

        returned_lf = job.raise_error_if_mainjrid_includes_unknown_values(lf)

        pl_testing.assert_frame_equal(returned_lf, lf)

    def test_returns_original_lf_unchanged_if_mainjrid_present_and_all_values_known(
        self,
    ):
        lf = pl.LazyFrame({AWK.main_job_role_id: ["1", "8"]})

        returned_lf = job.raise_error_if_mainjrid_includes_unknown_values(lf)

        pl_testing.assert_frame_equal(returned_lf, lf)

    def test_raises_error_if_mainjrid_includes_unknown_values(self):
        lf = pl.LazyFrame({AWK.main_job_role_id: ["1", "-1"]})

        with pytest.raises(
            ValueError,
            match=r"Error: this file contains 1 unknown mainjrid record\(s\)",
        ):
            job.raise_error_if_mainjrid_includes_unknown_values(lf)
