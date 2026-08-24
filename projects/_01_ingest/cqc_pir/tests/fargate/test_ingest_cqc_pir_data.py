from unittest.mock import Mock, patch

import polars as pl
import polars.testing as pl_testing

import projects._01_ingest.cqc_pir.fargate.ingest_cqc_pir_data as job
from utils.column_names.raw_data_files.cqc_pir_columns import CqcPirColumns as PIRCols

PATCH_PATH = "projects._01_ingest.cqc_pir.fargate.ingest_cqc_pir_data"

TEST_CSV_SOURCE = "projects/_01_ingest/unittest_data/test_ingest_cqc_pir_data.csv"


class TestMain:
    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    def test_reads_source_csv_into_the_defined_schema(self, mock_sink_to_parquet: Mock):
        job.main(TEST_CSV_SOURCE, "s3://dest-bucket/")

        expected_lf = pl.LazyFrame(
            {
                PIRCols.location_id: ["1-1000000001", "1-1000000002", "1-1000000003"],
                PIRCols.location_name: ["Location A", "Location B", "Location C"],
                PIRCols.pir_type: ["Residential", "Community", "Shared Lives"],
                PIRCols.pir_submission_date: ["01-Jan-26", "15-Jan-26", "30-Jan-26"],
                PIRCols.pir_people_directly_employed: [10, 20, 30],
                PIRCols.staff_leavers: [1, 3, None],
                PIRCols.staff_vacancies: [2, 0, None],
                PIRCols.shared_lives_leavers: [None, None, 4],
                PIRCols.shared_lives_vacancies: [None, None, 0],
                PIRCols.primary_inspection_category: [
                    "Residential social care",
                    "Community based adult social care services",
                    "Community based adult social care services",
                ],
                PIRCols.region: ["South East", "East Midlands", "West Midlands"],
                PIRCols.local_authority: ["Kent", "Derby", "Stoke-on-Trent"],
                PIRCols.number_of_beds: [5, 0, 0],
                PIRCols.domiciliary_care: ["No", "Yes", "No"],
                PIRCols.location_status: ["Active", "Active", "Active"],
            },
            schema=job.PIR_SCHEMA,
        )

        returned_lf = mock_sink_to_parquet.call_args.kwargs["lazy_df"]
        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    source = "s3://source-bucket/domain=CQC/dataset=pir/file.csv"
    destination_prefix = "s3://dest-bucket/"

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.pl.scan_csv")
    def test_sinks_to_a_path_derived_from_the_source_key_and_destination_prefix(
        self,
        mock_scan_csv: Mock,
        mock_sink_to_parquet: Mock,
    ):
        mock_lf = Mock(spec=pl.LazyFrame)
        mock_scan_csv.return_value = mock_lf

        job.main(self.source, self.destination_prefix)

        mock_scan_csv.assert_called_once_with(
            self.source,
            schema=job.PIR_SCHEMA,
            encoding="utf8-lossy",
        )
        mock_sink_to_parquet.assert_called_once_with(
            lazy_df=mock_lf,
            output_path=f"{self.destination_prefix}domain=CQC/dataset=pir/",
        )
