import unittest
from unittest.mock import ANY, Mock, patch

from pyspark.sql.dataframe import DataFrame

import jobs.clean_ascwds_worker_data as job
from utils.utils import get_spark
from tests.test_file_data import ASCWDSWorkerData, ASCWDSWorkplaceData
from tests.test_file_schemas import ASCWDSWorkerSchemas, ASCWDSWorkplaceSchemas
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.raw_data_files.ascwds_worker_columns import (
    PartitionKeys,
)


class IngestASCWDSWorkerDatasetTests(unittest.TestCase):
    TEST_WORKER_SOURCE = "s3://some_bucket/some_worker_source_key"
    TEST_WORKPLACE_SOURCE = "s3://some_bucket/some_workplace_source_key"
    TEST_DESTINATION = "s3://some_bucket/some_destination_key"
    partition_keys = [
        PartitionKeys.year,
        PartitionKeys.month,
        PartitionKeys.day,
        PartitionKeys.import_date,
    ]

    def setUp(self) -> None:
        self.spark = get_spark()
        self.test_ascwds_worker_df = self.spark.createDataFrame(
            ASCWDSWorkerData.worker_rows, ASCWDSWorkerSchemas.worker_schema
        )
        self.test_ascwds_workplace_df = self.spark.createDataFrame(
            ASCWDSWorkplaceData.workplace_rows, ASCWDSWorkplaceSchemas.workplace_schema
        )


class MainTests(IngestASCWDSWorkerDatasetTests):
    def setUp(self) -> None:
        super().setUp()

    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main(self, read_from_parquet_mock: Mock, write_to_parquet_mock: Mock):
        read_from_parquet_mock.return_value = self.test_ascwds_worker_df

        job.main(
            self.TEST_WORKER_SOURCE, self.TEST_WORKPLACE_SOURCE, self.TEST_DESTINATION
        )

        read_from_parquet_mock.assert_any_call(
            self.TEST_WORKER_SOURCE, job.WORKER_COLUMNS
        )
        read_from_parquet_mock.assert_any_call(
            self.TEST_WORKPLACE_SOURCE, job.WORKPLACE_COLUMNS
        )
        write_to_parquet_mock.assert_called_once_with(
            ANY,
            self.TEST_DESTINATION,
            mode="overwrite",
            partitionKeys=self.partition_keys,
        )


class RemoveWorkersWithoutWorkplacesTests(IngestASCWDSWorkerDatasetTests):
    def setUp(self) -> None:
        super().setUp()

    def test_remove_invalid_worker_records_returns_df(self):
        returned_df = job.remove_workers_without_workplaces(
            self.test_ascwds_worker_df, self.test_ascwds_workplace_df
        )

        self.assertIsInstance(returned_df, DataFrame)

    def test_remove_invalid_worker_records_removed_expected_workers(self):
        returned_df = job.remove_workers_without_workplaces(
            self.test_ascwds_worker_df, self.test_ascwds_workplace_df
        )

        expected_df = self.spark.createDataFrame(
            ASCWDSWorkerData.expected_remove_workers_without_workplaces_rows,
            ASCWDSWorkerSchemas.worker_schema,
        )

        expected_rows = expected_df.collect()
        returned_rows = (
            returned_df.select(*expected_df.columns)
            .orderBy(AWKClean.location_id)
            .collect()
        )

        self.assertCountEqual(returned_df.columns, self.test_ascwds_worker_df.columns)
        self.assertEqual(expected_rows, returned_rows)


class CreateCleanMainJobRoleColumnTests(IngestASCWDSWorkerDatasetTests):
    def setUp(self) -> None:
        super().setUp()

        test_main_job_role_df = self.spark.createDataFrame(
            ASCWDSWorkerData.create_clean_main_job_role_column_rows,
            ASCWDSWorkerSchemas.create_clean_main_job_role_column_schema,
        )
        returned_df = job.create_clean_main_job_role_column(test_main_job_role_df)

        expected_df = self.spark.createDataFrame(
            ASCWDSWorkerData.expected_create_clean_main_job_role_column_rows,
            ASCWDSWorkerSchemas.expected_create_clean_main_job_role_column_schema,
        )
        self.returned_data = returned_df.orderBy(
            AWKClean.worker_id, AWKClean.ascwds_worker_import_date
        ).collect()
        self.expected_data = expected_df.collect()

    def test_create_clean_main_job_role_column_returns_expected_main_job_role_clean_values(
        self,
    ):
        for i in range(len(self.returned_data)):
            self.assertEqual(
                self.returned_data[i][AWKClean.main_job_role_clean],
                self.expected_data[i][AWKClean.main_job_role_clean],
                f"Returned value in row {i} does not match expected",
            )

    def test_create_clean_main_job_role_column_returns_expected_main_job_role_clean_labels(
        self,
    ):
        for i in range(len(self.returned_data)):
            self.assertEqual(
                self.returned_data[i][AWKClean.main_job_role_clean_labelled],
                self.expected_data[i][AWKClean.main_job_role_clean_labelled],
                f"Returned value in row {i} does not match expected",
            )


class ReplaceCareNavigatorWithCareCoordinatorTests(IngestASCWDSWorkerDatasetTests):
    def setUp(self) -> None:
        super().setUp()

        test_df_with_care_navigator = self.spark.createDataFrame(
            ASCWDSWorkerData.replace_care_navigator_with_care_coordinator_values_updated_when_care_navigator_is_present_rows,
            ASCWDSWorkerSchemas.replace_care_navigator_with_care_coordinator_schema,
        )
        returned_df_with_care_navigator = (
            job.replace_care_navigator_with_care_coordinator(
                test_df_with_care_navigator
            )
        )
        expected_df_with_care_navigator = self.spark.createDataFrame(
            ASCWDSWorkerData.expected_replace_care_navigator_with_care_coordinator_values_updated_when_care_navigator_is_present_rows,
            ASCWDSWorkerSchemas.replace_care_navigator_with_care_coordinator_schema,
        )
        self.returned_data = returned_df_with_care_navigator.collect()
        self.expected_data = expected_df_with_care_navigator.collect()

    def test_care_navigator_is_replaced_with_care_coordinator_in_main_job_role_clean_column(
        self,
    ):
        self.assertEqual(
            self.returned_data[0][AWKClean.main_job_role_clean],
            self.expected_data[0][AWKClean.main_job_role_clean],
        )

    def test_replace_care_navigator_with_care_coordinator_doesnt_replace_the_value_in_other_columns_in_df(
        self,
    ):
        self.assertEqual(
            self.returned_data[0][AWKClean.worker_id],
            self.expected_data[0][AWKClean.worker_id],
        )

    def test_replace_care_navigator_with_care_coordinator_doesnt_change_data_when_care_navigator_value_not_present(
        self,
    ):
        test_df = self.spark.createDataFrame(
            ASCWDSWorkerData.replace_care_navigator_with_care_coordinator_values_remain_unchanged_when_care_navigator_not_present_rows,
            ASCWDSWorkerSchemas.replace_care_navigator_with_care_coordinator_schema,
        )
        returned_df = job.replace_care_navigator_with_care_coordinator(test_df)
        expected_df = self.spark.createDataFrame(
            ASCWDSWorkerData.expected_replace_care_navigator_with_care_coordinator_values_remain_unchanged_when_care_navigator_not_present_rows,
            ASCWDSWorkerSchemas.replace_care_navigator_with_care_coordinator_schema,
        )
        returned_data = returned_df.sort(AWKClean.worker_id).collect()
        expected_data = expected_df.collect()

        self.assertEqual(returned_data, expected_data)


class ImputeNotKnownJobRolesTests(IngestASCWDSWorkerDatasetTests):
    def setUp(self) -> None:
        super().setUp()

    def test_impute_not_known_job_roles_returns_next_known_value_when_before_first_known_value(
        self,
    ):
        test_df = self.spark.createDataFrame(
            ASCWDSWorkerData.impute_not_known_job_roles_returns_next_known_value_when_before_first_known_value_rows,
            ASCWDSWorkerSchemas.impute_not_known_job_roles_schema,
        )
        returned_df = job.impute_not_known_job_roles(test_df)
        expected_df = self.spark.createDataFrame(
            ASCWDSWorkerData.expected_impute_not_known_job_roles_returns_next_known_value_when_before_first_known_value_rows,
            ASCWDSWorkerSchemas.impute_not_known_job_roles_schema,
        )

        returned_data = returned_df.sort(
            AWKClean.worker_id, AWKClean.ascwds_worker_import_date
        ).collect()
        expected_data = expected_df.collect()

        self.assertEqual(returned_data, expected_data)

    def test_impute_not_known_job_roles_returns_previously_known_value_when_after_known_value(
        self,
    ):
        test_df = self.spark.createDataFrame(
            ASCWDSWorkerData.impute_not_known_job_roles_returns_previously_known_value_when_after_known_value_rows,
            ASCWDSWorkerSchemas.impute_not_known_job_roles_schema,
        )
        returned_df = job.impute_not_known_job_roles(test_df)
        expected_df = self.spark.createDataFrame(
            ASCWDSWorkerData.expected_impute_not_known_job_roles_returns_previously_known_value_when_after_known_value_rows,
            ASCWDSWorkerSchemas.impute_not_known_job_roles_schema,
        )

        returned_data = returned_df.sort(
            AWKClean.worker_id, AWKClean.ascwds_worker_import_date
        ).collect()
        expected_data = expected_df.collect()

        self.assertEqual(returned_data, expected_data)

    def test_impute_not_known_job_roles_returns_previously_known_value_when_in_between_known_values(
        self,
    ):
        test_df = self.spark.createDataFrame(
            ASCWDSWorkerData.impute_not_known_job_roles_returns_previously_known_value_when_in_between_known_values_rows,
            ASCWDSWorkerSchemas.impute_not_known_job_roles_schema,
        )
        returned_df = job.impute_not_known_job_roles(test_df)
        expected_df = self.spark.createDataFrame(
            ASCWDSWorkerData.expected_impute_not_known_job_roles_returns_previously_known_value_when_in_between_known_values_rows,
            ASCWDSWorkerSchemas.impute_not_known_job_roles_schema,
        )

        returned_data = returned_df.sort(
            AWKClean.worker_id, AWKClean.ascwds_worker_import_date
        ).collect()
        expected_data = expected_df.collect()

        self.assertEqual(returned_data, expected_data)

    def test_impute_not_known_job_roles_returns_not_known_when_job_role_never_known(
        self,
    ):
        test_df = self.spark.createDataFrame(
            ASCWDSWorkerData.impute_not_known_job_roles_returns_not_known_when_job_role_never_known_rows,
            ASCWDSWorkerSchemas.impute_not_known_job_roles_schema,
        )
        returned_df = job.impute_not_known_job_roles(test_df)
        expected_df = self.spark.createDataFrame(
            ASCWDSWorkerData.expected_impute_not_known_job_roles_returns_not_known_when_job_role_never_known_rows,
            ASCWDSWorkerSchemas.impute_not_known_job_roles_schema,
        )

        self.assertEqual(returned_df.collect(), expected_df.collect())


class RemoveWorkersWithNotKnownJobRoleTests(IngestASCWDSWorkerDatasetTests):
    def setUp(self) -> None:
        super().setUp()

    def test_remove_workers_with_not_known_job_role_returns_expected_dataframe(self):
        test_df = self.spark.createDataFrame(
            ASCWDSWorkerData.remove_workers_with_not_known_job_role_rows,
            ASCWDSWorkerSchemas.remove_workers_with_not_known_job_role_schema,
        )
        returned_df = job.remove_workers_with_not_known_job_role(test_df)

        expected_df = self.spark.createDataFrame(
            ASCWDSWorkerData.expected_remove_workers_with_not_known_job_role_rows,
            ASCWDSWorkerSchemas.remove_workers_with_not_known_job_role_schema,
        )

        self.assertEqual(returned_df.collect(), expected_df.collect())


if __name__ == "__main__":
    unittest.main(warnings="ignore")
