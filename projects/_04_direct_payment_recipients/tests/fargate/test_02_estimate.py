import unittest
from unittest.mock import ANY, Mock, call, patch

import polars as pl

import projects._04_direct_payment_recipients.fargate._02_estimate as job
from utils.column_names.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)

PATCH_PATH: str = "projects._04_direct_payment_recipients.fargate._02_estimate"


class EstimateDirectPaymentsTests(unittest.TestCase):
    SOME_SOURCE = "some/source"
    SOME_DESTINATION = "some/destination"
    SOME_OTHER_DESTINATION = "some/other/destination"

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.create_summary_table")
    @patch(f"{PATCH_PATH}.calculate_remaining_variables")
    @patch(f"{PATCH_PATH}.calculate_estimated_service_users_employing_staff")
    @patch(f"{PATCH_PATH}.merge_cornwall_and_isles_of_scilly")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_succeeds(
        self,
        scan_parquet_mock: Mock,
        calculate_estimated_service_users_employing_staff_mock: Mock,
        calculate_remaining_variables_mock: Mock,
        create_summary_table_mock: Mock,
        merge_cornwall_and_isles_of_scilly_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(
            self.SOME_SOURCE,
            self.SOME_DESTINATION,
            self.SOME_OTHER_DESTINATION,
        )

        scan_parquet_mock.assert_called_once_with(
            source=self.SOME_SOURCE,
            selected_columns=job.direct_payments_columns,
        )

        merge_cornwall_and_isles_of_scilly_mock.assert_called_once()
        calculate_estimated_service_users_employing_staff_mock.assert_called_once()
        calculate_remaining_variables_mock.assert_called_once()
        create_summary_table_mock.assert_called_once()

        self.assertEqual(sink_to_parquet_mock.call_count, 2)
        sink_to_parquet_mock.assert_has_calls(
            [
                call(
                    ANY,
                    self.SOME_DESTINATION,
                ),
                call(
                    ANY,
                    self.SOME_OTHER_DESTINATION,
                ),
            ]
        )


def test_output_float_columns_are_float32():
    merged_lf = pl.LazyFrame(
        [
            ("Leeds", 2015, 100.0, 0.5, None, 100.0, 2.0),
            ("Leeds", 2016, None, None, None, None, 2.0),
            ("Leeds", 2017, 120.0, 0.6, None, 120.0, 2.0),
            ("Hackney", 2015, 80.0, None, 0.4, 80.0, 2.0),
            ("Hackney", 2016, 90.0, 0.5, None, 90.0, 2.0),
            ("Hackney", 2017, None, None, None, None, 2.0),
        ],
        schema={
            DP.LA_AREA: pl.String,
            DP.YEAR_AS_INTEGER: pl.Int32,
            DP.SERVICE_USER_DPRS_DURING_YEAR: pl.Float32,
            DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: pl.Float32,
            DP.HISTORIC_SERVICE_USERS_EMPLOYING_STAFF_ESTIMATE: pl.Float32,
            DP.TOTAL_DPRS_DURING_YEAR: pl.Float32,
            DP.FILLED_POSTS_PER_EMPLOYER: pl.Float32,
        },
        orient="row",
    )

    with (
        patch(f"{PATCH_PATH}.utils.scan_parquet", return_value=merged_lf),
        patch(f"{PATCH_PATH}.utils.sink_to_parquet") as sink_mock,
    ):
        job.main("source", "destination", "summary_destination")

    assert sink_mock.call_count == 2
    for sink_call in sink_mock.call_args_list:
        schema = sink_call.args[0].collect().schema
        float_dtypes = {dtype for dtype in schema.values() if dtype.is_float()}
        assert float_dtypes == {pl.Float32}
