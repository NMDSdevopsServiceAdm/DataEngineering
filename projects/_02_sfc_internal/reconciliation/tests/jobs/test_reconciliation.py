import warnings
from datetime import date
from unittest.mock import Mock, patch

import projects._02_sfc_internal.reconciliation.jobs.reconciliation as job
from projects._02_sfc_internal.unittest_data.sfc_test_file_data import (
    ReconciliationData as Data,
)
from projects._02_sfc_internal.unittest_data.sfc_test_file_schemas import (
    ReconciliationSchema as Schemas,
)
from tests.base_test import SparkBaseTest
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)

PATCH_PATH: str = "projects._02_sfc_internal.reconciliation.jobs.reconciliation"


class ReconciliationTests(SparkBaseTest):
    TEST_CQC_DEREG_LOCATIONS_SOURCE = "some/source"
    TEST_ASCWDS_WORKPLACE_SOURCE = "another/source"
    TEST_SINGLE_SUB_DESTINATION = "some/destination"
    TEST_PARENT_DESTINATION = "another/destination"

    def setUp(self) -> None:
        self.test_cqc_dereg_locations_df = self.spark.createDataFrame(
            Data.input_cqc_dereg_locations_rows,
            Schemas.input_cqc_dereg_locations_schema,
        )
        self.test_clean_ascwds_workplace_df = self.spark.createDataFrame(
            Data.input_ascwds_workplace_rows,
            Schemas.input_ascwds_workplace_schema,
        )

        warnings.simplefilter("ignore", ResourceWarning)


class MainTests(ReconciliationTests):
    @patch(f"{PATCH_PATH}.utils.write_to_parquet")
    @patch(f"{PATCH_PATH}.utils.read_from_parquet")
    def test_main_run(
        self,
        read_from_parquet_patch: Mock,
        write_to_parquet_patch: Mock,
    ):
        read_from_parquet_patch.side_effect = [
            self.test_cqc_dereg_locations_df,
            self.test_clean_ascwds_workplace_df,
        ]

        job.main(
            self.TEST_CQC_DEREG_LOCATIONS_SOURCE,
            self.TEST_ASCWDS_WORKPLACE_SOURCE,
            self.TEST_SINGLE_SUB_DESTINATION,
            self.TEST_PARENT_DESTINATION,
        )

        self.assertEqual(read_from_parquet_patch.call_count, 2)
        self.assertEqual(write_to_parquet_patch.call_count, 2)


class MainDefensiveFilterTests(SparkBaseTest):
    TEST_CQC_DEREG_LOCATIONS_SOURCE = "some/source"
    TEST_ASCWDS_WORKPLACE_SOURCE = "another/source"
    TEST_SINGLE_SUB_DESTINATION = "some/destination"
    TEST_PARENT_DESTINATION = "another/destination"

    @patch(f"{PATCH_PATH}.utils.write_to_parquet")
    @patch(
        f"{PATCH_PATH}.rUtils.create_reconciliation_output_for_ascwds_parent_accounts"
    )
    @patch(
        f"{PATCH_PATH}.rUtils.create_reconciliation_output_for_ascwds_single_and_sub_accounts"
    )
    @patch(f"{PATCH_PATH}.rUtils.filter_to_locations_relevant_to_reconcilition_process")
    @patch(f"{PATCH_PATH}.rUtils.join_cqc_location_data_into_ascwds_workplace_df")
    @patch(f"{PATCH_PATH}.rUtils.prepare_latest_cleaned_ascwds_workforce_data")
    @patch(f"{PATCH_PATH}.rUtils.collect_dates_to_use")
    @patch(f"{PATCH_PATH}.utils.read_from_parquet")
    def test_main_filters_out_rows_removed_by_purge_date_filter_before_further_processing(
        self,
        read_from_parquet_patch: Mock,
        collect_dates_to_use_patch: Mock,
        prepare_latest_patch: Mock,
        join_patch: Mock,
        filter_to_relevant_patch: Mock,
        single_and_sub_patch: Mock,
        parents_patch: Mock,
        write_to_parquet_patch: Mock,
    ):
        ascwds_workplace_df = self.spark.createDataFrame(
            Data.purge_filter_ascwds_workplace_rows,
            Schemas.purge_filter_ascwds_workplace_schema,
        )
        read_from_parquet_patch.side_effect = [Mock(), ascwds_workplace_df]
        collect_dates_to_use_patch.return_value = (date(2024, 4, 1), date(2024, 3, 1))
        prepare_latest_patch.return_value = (Mock(), Mock())
        join_patch.return_value = Mock()
        filter_to_relevant_patch.return_value = Mock()
        single_and_sub_patch.return_value = Mock()
        parents_patch.return_value = Mock()

        job.main(
            self.TEST_CQC_DEREG_LOCATIONS_SOURCE,
            self.TEST_ASCWDS_WORKPLACE_SOURCE,
            self.TEST_SINGLE_SUB_DESTINATION,
            self.TEST_PARENT_DESTINATION,
        )

        filtered_df = prepare_latest_patch.call_args[0][0]
        remaining_establishment_ids = {
            row[AWPClean.establishment_id] for row in filtered_df.collect()
        }
        self.assertEqual(remaining_establishment_ids, {"not_purged"})
        self.assertNotIn(AWPClean.removed_by_purge_date_filter, filtered_df.columns)
