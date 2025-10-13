import unittest
from unittest.mock import ANY, Mock, patch

import projects._03_independent_cqc._01_merge.jobs.merge_ind_cqc_data as job
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_data import (
    MergeIndCQCData as Data,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_schemas import (
    MergeIndCQCData as Schemas,
)
from utils import utils
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.cleaned_data_files.cqc_pir_cleaned import (
    CqcPIRCleanedColumns as CQCPIRClean,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

PATCH_PATH: str = "projects._03_independent_cqc._01_merge.jobs.merge_ind_cqc_data"


class MergeIndCQCDatasetTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.test_clean_cqc_location_df = self.spark.createDataFrame(
            Data.clean_cqc_location_for_merge_rows,
            Schemas.clean_cqc_location_for_merge_schema,
        )
        self.test_data_with_care_home_col = self.spark.createDataFrame(
            Data.data_to_merge_with_care_home_col_rows,
            Schemas.data_to_merge_with_care_home_col_schema,
        )
        self.test_data_without_care_home_col = self.spark.createDataFrame(
            Data.data_to_merge_without_care_home_col_rows,
            Schemas.data_to_merge_without_care_home_col_schema,
        )


class MainTests(MergeIndCQCDatasetTests):
    def setUp(self) -> None:
        super().setUp()

    TEST_CQC_LOCATION_SOURCE = "some/directory"
    TEST_GAC_SOURCE = "some/directory"
    TEST_REG_ACT_SOURCE = "some/directory"
    TEST_SPEC_SOURCE = "some/directory"
    TEST_PCM_SOURCE = "some/directory"
    TEST_CQC_PIR_SOURCE = "some/other/directory"
    TEST_ASCWDS_WORKPLACE_SOURCE = "some/other/directory"
    TEST_CT_NON_RES_SOURCE = "yet/another/directory"
    TEST_CT_CARE_HOME_SOURCE = "one/more/directory"
    TEST_DESTINATION = "some/other/directory"
    partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

    @patch(f"{PATCH_PATH}.utils.write_to_parquet")
    @patch(f"{PATCH_PATH}.remove_specialist_colleges")
    @patch(f"{PATCH_PATH}.join_data_into_cqc_df")
    @patch(f"{PATCH_PATH}.utils.select_rows_with_value")
    @patch(f"{PATCH_PATH}.utils.join_dimension")
    @patch(f"{PATCH_PATH}.utils.read_from_parquet")
    def test_main_runs(
        self,
        read_from_parquet_patch: Mock,
        join_dimension_patch: Mock,
        select_rows_with_value_mock: Mock,
        join_data_into_cqc_df_mock: Mock,
        remove_specialist_colleges_mock: Mock,
        write_to_parquet_patch: Mock,
    ):
        read_from_parquet_patch.side_effect = [
            self.test_clean_cqc_location_df,
            self.test_clean_cqc_location_df,
            self.test_clean_cqc_location_df,
            self.test_clean_cqc_location_df,
            self.test_clean_cqc_location_df,
            self.test_data_with_care_home_col,
            self.test_data_without_care_home_col,
            self.test_data_with_care_home_col,
            self.test_data_with_care_home_col,
        ]

        job.main(
            self.TEST_CQC_LOCATION_SOURCE,
            self.TEST_GAC_SOURCE,
            self.TEST_REG_ACT_SOURCE,
            self.TEST_SPEC_SOURCE,
            self.TEST_PCM_SOURCE,
            self.TEST_CQC_PIR_SOURCE,
            self.TEST_ASCWDS_WORKPLACE_SOURCE,
            self.TEST_CT_NON_RES_SOURCE,
            self.TEST_CT_CARE_HOME_SOURCE,
            self.TEST_DESTINATION,
        )

        self.assertEqual(read_from_parquet_patch.call_count, 9)
        self.assertEqual(join_dimension_patch.call_count, 4)
        self.assertEqual(select_rows_with_value_mock.call_count, 3)
        self.assertEqual(join_data_into_cqc_df_mock.call_count, 4)
        remove_specialist_colleges_mock.assert_called_once()

        write_to_parquet_patch.assert_called_once_with(
            ANY,
            self.TEST_DESTINATION,
            mode="overwrite",
            partitionKeys=self.partition_keys,
        )

    def test_join_data_into_cqc_df_returns_expected_data_when_care_home_column_not_required(
        self,
    ):
        returned_df = job.join_data_into_cqc_df(
            self.test_clean_cqc_location_df,
            self.test_data_without_care_home_col,
            AWPClean.location_id,
            AWPClean.ascwds_workplace_import_date,
        )

        expected_merged_df = self.spark.createDataFrame(
            Data.expected_merged_without_care_home_col_rows,
            Schemas.expected_merged_without_care_home_col_schema,
        )

        returned_data = returned_df.sort(
            CQCLClean.cqc_location_import_date, CQCLClean.location_id
        ).collect()
        expected_data = expected_merged_df.select(returned_df.columns).collect()

        self.assertEqual(returned_data, expected_data)

    def test_join_data_into_cqc_df_returns_expected_data_when_care_home_column_is_required(
        self,
    ):
        returned_df = job.join_data_into_cqc_df(
            self.test_clean_cqc_location_df,
            self.test_data_with_care_home_col,
            CQCPIRClean.location_id,
            CQCPIRClean.cqc_pir_import_date,
            CQCPIRClean.care_home,
        )

        expected_merged_df = self.spark.createDataFrame(
            Data.expected_merged_with_care_home_col_rows,
            Schemas.expected_merged_with_care_home_col_schema,
        )

        returned_data = returned_df.sort(
            CQCLClean.cqc_location_import_date, CQCLClean.location_id
        ).collect()
        expected_data = expected_merged_df.select(returned_df.columns).collect()

        self.assertEqual(returned_data, expected_data)


class RemoveSpecialistCollegesTests(MergeIndCQCDatasetTests):
    def setUp(self) -> None:
        super().setUp()

        self.mock_cqc_df = Mock()
        self.mock_cqc_df.join.return_value = Mock()

    def test_remove_specialist_colleges_removes_rows_where_specialist_college_is_only_service(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.test_only_service_specialist_colleges_rows,
            Schemas.remove_specialist_colleges_schema,
        )
        returned_df = job.remove_specialist_colleges(test_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_only_service_specialist_colleges_rows,
            Schemas.remove_specialist_colleges_schema,
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_remove_specialist_colleges_does_not_remove_rows_where_specialist_college_is_one_of_multiple_services(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.test_multiple_services_specialist_colleges_rows,
            Schemas.remove_specialist_colleges_schema,
        )
        returned_df = job.remove_specialist_colleges(test_df)
        expected_df = self.spark.createDataFrame(
            Data.test_multiple_services_specialist_colleges_rows,
            Schemas.remove_specialist_colleges_schema,
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_remove_specialist_colleges_does_not_remove_rows_where_specialist_college_is_not_listed_in_services(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.test_without_specialist_colleges_rows,
            Schemas.remove_specialist_colleges_schema,
        )
        returned_df = job.remove_specialist_colleges(test_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_without_specialist_colleges_rows,
            Schemas.remove_specialist_colleges_schema,
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_remove_specialist_colleges_does_not_remove_rows_where_gac_service_type_struct_is_empty(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.test_empty_array_specialist_colleges_rows,
            Schemas.remove_specialist_colleges_schema,
        )
        returned_df = job.remove_specialist_colleges(test_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_empty_array_specialist_colleges_rows,
            Schemas.remove_specialist_colleges_schema,
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_remove_specialist_colleges_does_not_remove_rows_where_gac_service_type_column_is_null(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.test_null_row_specialist_colleges_rows,
            Schemas.remove_specialist_colleges_schema,
        )
        returned_df = job.remove_specialist_colleges(test_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_null_row_specialist_colleges_rows,
            Schemas.remove_specialist_colleges_schema,
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())


if __name__ == "__main__":
    unittest.main(warnings="ignore")
