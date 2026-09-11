from unittest.mock import Mock, patch

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._01_ingest.direct_payment_recipients.fargate.ingest_dpr_data as job
from utils.column_names.direct_payments_column_names import (
    DirectPaymentColumnNames as DPR,
)

PATCH_PATH = "projects._01_ingest.direct_payment_recipients.fargate.ingest_dpr_data"

SURVEY_CSV_SOURCE = (
    "projects/_01_ingest/direct_payment_recipients/unittest_data/"
    "test_ingest_dpr_survey_data.csv"
)
EXTERNAL_CSV_SOURCE = (
    "projects/_01_ingest/direct_payment_recipients/unittest_data/"
    "test_ingest_dpr_external_data.csv"
)


class TestMain:
    @pytest.mark.parametrize(
        "dataset, source, schema, expected_data",
        [
            pytest.param(
                "survey",
                SURVEY_CSV_SOURCE,
                job.SURVEY_SCHEMA,
                {
                    DPR.YEAR: [2020, 2021],
                    DPR.TOTAL_STAFF_RECODED: [15.5, None],
                },
                id="survey_dataset",
            ),
            pytest.param(
                "external",
                EXTERNAL_CSV_SOURCE,
                job.EXTERNAL_SCHEMA,
                {
                    DPR.SERVICE_USER_DPRS_DURING_YEAR: [1234.0, None],
                    DPR.SERVICE_USER_DPRS_AT_YEAR_END: [None, None],
                    DPR.CARER_DPRS_AT_YEAR_END: [None, None],
                    DPR.LA_AREA: [
                        "Cheshire West & Chester",
                        "Bournemouth, Christchurch and Poole",
                    ],
                    DPR.DPRS_ADASS: [None, None],
                    DPR.DPRS_EMPLOYING_STAFF_ADASS: [None, None],
                    DPR.YEAR: [2020, 2020],
                    DPR.PROPORTION_IMPORTED: [None, None],
                    DPR.HISTORIC_SERVICE_USERS_EMPLOYING_STAFF_ESTIMATE: [0.2, 0.2],
                    DPR.FILLED_POSTS_PER_EMPLOYER: [2.02, 2.02],
                },
                id="external_dataset",
            ),
        ],
    )
    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    def test_reads_source_csv_into_the_defined_schema(
        self,
        mock_sink_to_parquet: Mock,
        dataset,
        source,
        schema,
        expected_data,
    ):
        job.main(source, dataset, "s3://dest-bucket/")

        expected_lf = pl.LazyFrame(expected_data, schema=schema)

        returned_lf = mock_sink_to_parquet.call_args.kwargs["lazy_df"]
        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.pl.scan_csv")
    def test_sinks_to_the_given_destination_with_the_schema_for_the_given_dataset(
        self,
        mock_scan_csv: Mock,
        mock_sink_to_parquet: Mock,
    ):
        mock_lf = Mock(spec=pl.LazyFrame)
        mock_scan_csv.return_value = mock_lf
        source = "s3://source-bucket/domain=DPR/raw_data=2026/survey.csv"
        destination = "s3://dest-bucket/domain=01_dpr/dataset=survey/"

        job.main(source, "survey", destination)

        mock_scan_csv.assert_called_once_with(
            source,
            schema=job.SURVEY_SCHEMA,
            encoding="utf8-lossy",
        )
        mock_sink_to_parquet.assert_called_once_with(
            lazy_df=mock_lf,
            output_path=destination,
        )

    def test_raises_value_error_when_dataset_is_not_survey_or_external(self):
        with pytest.raises(
            ValueError,
            match="Unknown dataset 'invalid'. Must be either 'survey' or 'external'.",
        ):
            job.main("s3://source-bucket/file.csv", "invalid", "s3://dest-bucket/")
