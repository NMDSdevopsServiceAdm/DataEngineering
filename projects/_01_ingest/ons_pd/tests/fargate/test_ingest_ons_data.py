from unittest.mock import Mock, patch

import polars as pl
import polars.testing as pl_testing

import projects._01_ingest.ons_pd.fargate.ingest_ons_data as job
from projects._01_ingest.unittest_data.polars_ingest_test_file_data import (
    IngestOnsDataTest,
)
from utils.column_names.raw_data_files.ons_columns import (
    OnsPostcodeDirectoryColumns as ONS,
)

PATCH_PATH = "projects._01_ingest.ons_pd.fargate.ingest_ons_data"

TEST_CSV_SOURCE = "projects/_01_ingest/unittest_data/test_ingest_ons_data.csv"


class TestMain:
    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    def test_reads_source_csv_with_no_type_inference(self, mock_sink_to_parquet: Mock):
        job.main(TEST_CSV_SOURCE, "s3://dest-bucket/")

        expected_columns = [
            ONS.postcode,
            ONS.cssr,
            ONS.region,
            ONS.sub_icb,
            ONS.icb,
            ONS.icb_region,
            ONS.lower_super_output_area_2021,
            ONS.middle_super_output_area_2021,
            ONS.rural_urban_indicator_2011,
            ONS.month,
            ONS.import_date,
        ]
        expected_lf = pl.LazyFrame(
            IngestOnsDataTest.expected_reads_source_csv_with_no_type_inference,
            schema={column: pl.String for column in expected_columns},
        )

        returned_lf = mock_sink_to_parquet.call_args.kwargs["lazy_df"]
        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    source = "s3://source-bucket/domain=ONS/dataset=postcode_directory/file.csv"
    destination_prefix = "s3://dest-bucket/"

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.pl.scan_csv")
    def test_sinks_to_a_path_derived_from_the_source_key_and_destination_prefix(
        self, mock_scan_csv: Mock, mock_sink_to_parquet: Mock
    ):
        mock_lf = Mock(spec=pl.LazyFrame)
        mock_scan_csv.return_value = mock_lf

        job.main(self.source, self.destination_prefix)

        mock_scan_csv.assert_called_once_with(
            self.source,
            separator=",",
            infer_schema=False,
        )
        mock_sink_to_parquet.assert_called_once_with(
            lazy_df=mock_lf,
            output_path=f"{self.destination_prefix}domain=ONS/dataset=postcode_directory/",
        )
