from unittest.mock import Mock, patch

import polars as pl

import projects._01_ingest.cqc_pir.fargate.ingest_cqc_pir_data as job

PATCH_PATH = "projects._01_ingest.cqc_pir.fargate.ingest_cqc_pir_data"


class TestMain:
    source = "s3://source-bucket/domain=CQC/dataset=pir/file.csv"
    destination = "s3://dest-bucket/domain=CQC/dataset=pir/"

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.pl.scan_csv")
    def test_reads_source_with_defined_schema_and_sinks_to_destination_directory(
        self,
        mock_scan_csv: Mock,
        mock_sink_to_parquet: Mock,
    ):
        mock_lf = Mock(spec=pl.LazyFrame)
        mock_scan_csv.return_value = mock_lf

        job.main(self.source, self.destination)

        mock_scan_csv.assert_called_once_with(self.source, schema=job.PIR_SCHEMA)
        mock_sink_to_parquet.assert_called_once_with(
            lazy_df=mock_lf,
            output_path="s3://dest-bucket/domain=CQC/dataset=pir/",
        )
