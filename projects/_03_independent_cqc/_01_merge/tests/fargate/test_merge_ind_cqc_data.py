import unittest
from unittest.mock import ANY, Mock, patch

import polars as pl
import polars.testing as pl_testing

import projects._03_independent_cqc._01_merge.fargate.merge_ind_cqc_data as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    MergeIndCQCData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    MergeIndCQCSchemas as Schemas,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

PATCH_PATH = "projects._03_independent_cqc._01_merge.fargate.merge_ind_cqc_data"


class IndCQCMergeTests(unittest.TestCase):
    TEST_CQC_LOCATION_SOURCE = "some/directory"
    TEST_CQC_PIR_SOURCE = "some/other/directory"
    TEST_ASCWDS_WORKPLACE_SOURCE = "some/other/other/directory"
    TEST_CT_NON_RES_SOURCE = "yet/another/directory"
    TEST_CT_CARE_HOME_SOURCE = "one/more/directory"
    TEST_DESTINATION = "an/other/directory"
    partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

    mock_data = Mock(name="data")

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.join_data_into_cqc_lf")
    @patch(f"{PATCH_PATH}.utils.scan_parquet", return_value=mock_data)
    def test_main_runs_successfully(
        self,
        scan_parquet_mock: Mock,
        join_data_into_cqc_lf_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):

        job.main(
            self.TEST_CQC_LOCATION_SOURCE,
            self.TEST_CQC_PIR_SOURCE,
            self.TEST_ASCWDS_WORKPLACE_SOURCE,
            self.TEST_CT_NON_RES_SOURCE,
            self.TEST_CT_CARE_HOME_SOURCE,
            self.TEST_DESTINATION,
        )

        self.assertEqual(scan_parquet_mock.call_count, 5)
        self.assertEqual(join_data_into_cqc_lf_mock.call_count, 4)
        sink_to_parquet_mock.assert_called_once_with(
            ANY,
            self.TEST_DESTINATION,
            partition_cols=self.partition_keys,
            append=False,
        )

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_outputs_dataset_with_correct_columns(
        self,
        scan_parquet_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        test_cqc_location_lf = pl.LazyFrame(
            data=Data.cqc_location_data,
            schema=Schemas.cqc_location_schema,
        )
        test_cqc_pir_lf = pl.LazyFrame(
            data=Data.cqc_pir_data, schema=Schemas.cqc_pir_schema
        )
        test_ascwds_workplace_lf = pl.LazyFrame(
            data=Data.ascwds_workplace_data,
            schema=Schemas.ascwds_workplace_schema,
        )
        test_ct_non_res_lf = pl.LazyFrame(
            data=Data.ct_non_res_data, schema=Schemas.ct_non_res_schema
        )
        test_ct_care_home_lf = pl.LazyFrame(
            data=Data.ct_care_home_data,
            schema=Schemas.ct_care_home_schema,
        )
        scan_parquet_mock.side_effect = [
            test_cqc_location_lf,
            test_cqc_pir_lf,
            test_ascwds_workplace_lf,
            test_ct_non_res_lf,
            test_ct_care_home_lf,
        ]
        job.main(
            self.TEST_CQC_LOCATION_SOURCE,
            self.TEST_CQC_PIR_SOURCE,
            self.TEST_ASCWDS_WORKPLACE_SOURCE,
            self.TEST_CT_NON_RES_SOURCE,
            self.TEST_CT_CARE_HOME_SOURCE,
            self.TEST_DESTINATION,
        )
        expected_lf = pl.LazyFrame(
            data=Data.expected_data, schema=Schemas.expected_schema
        )

        pl_testing.assert_frame_equal(sink_to_parquet_mock.call_args[0][0], expected_lf)
