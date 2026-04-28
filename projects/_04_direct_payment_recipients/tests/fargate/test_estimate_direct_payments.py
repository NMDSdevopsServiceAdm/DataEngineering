import unittest
from unittest.mock import ANY, Mock, patch

import polars as pl
import polars.testing as pl_testing

import projects._04_direct_payment_recipients.fargate.estimate_direct_payments as job

PATCH_PATH: str = (
    "projects._04_direct_payment_recipients.fargate.estimate_direct_payments"
)


class EstimateDirectPaymentsTests(unittest.TestCase):
    SOME_SOURCE = "some/source"
    SOME_DESTINATION = "some/destination"
    SOME_OTHER_DESTINATION = "some/other/destination"

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_succeeds(
        self,
        scan_parquet_mock: Mock,
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

        sink_to_parquet_mock.assert_called_once_with(
            ANY,
            self.SOME_DESTINATION,
            append=False,
        )
