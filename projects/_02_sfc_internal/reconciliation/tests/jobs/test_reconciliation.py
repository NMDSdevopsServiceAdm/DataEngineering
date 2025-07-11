import unittest
import warnings
from unittest.mock import Mock, patch

import projects._02_sfc_internal.reconciliation.jobs.reconciliation as job
from projects._02_sfc_internal.unittest_data.sfc_test_file_data import (
    ReconciliationData as Data,
)
from projects._02_sfc_internal.unittest_data.sfc_test_file_schemas import (
    ReconciliationSchema as Schemas,
)
from utils import utils

PATCH_PATH: str = "projects._02_sfc_internal.reconciliation.jobs.reconciliation"


class ReconciliationTests(unittest.TestCase):
    TEST_CQC_LOCATION_API_SOURCE = "some/source"
    TEST_ASCWDS_WORKPLACE_SOURCE = "another/source"
    TEST_SINGLE_SUB_DESTINATION = "some/destination"
    TEST_PARENT_DESTINATION = "another/destination"

    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.test_cqc_location_api_df = self.spark.createDataFrame(
            Data.input_cqc_location_api_rows,
            Schemas.input_cqc_location_api_schema,
        )
        self.test_clean_ascwds_workplace_df = self.spark.createDataFrame(
            Data.input_ascwds_workplace_rows,
            Schemas.input_ascwds_workplace_schema,
        )

        warnings.simplefilter("ignore", ResourceWarning)


class MainTests(ReconciliationTests):
    def setUp(self) -> None:
        super().setUp()

    @patch(f"{PATCH_PATH}.utils.write_to_parquet")
    @patch(f"{PATCH_PATH}.utils.read_from_parquet")
    def test_main_run(
        self,
        read_from_parquet_patch: Mock,
        write_to_parquet_patch: Mock,
    ):
        read_from_parquet_patch.side_effect = [
            self.test_cqc_location_api_df,
            self.test_clean_ascwds_workplace_df,
        ]

        job.main(
            self.TEST_CQC_LOCATION_API_SOURCE,
            self.TEST_ASCWDS_WORKPLACE_SOURCE,
            self.TEST_SINGLE_SUB_DESTINATION,
            self.TEST_PARENT_DESTINATION,
        )

        self.assertEqual(read_from_parquet_patch.call_count, 2)
        self.assertEqual(write_to_parquet_patch.call_count, 2)
