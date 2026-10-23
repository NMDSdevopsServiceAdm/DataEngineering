from unittest.mock import Mock, patch

import projects._03_independent_cqc._03_starters_leavers_vacancies.fargate._01_merge as job
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

PATCH_PATH = (
    "projects._03_independent_cqc._03_starters_leavers_vacancies.fargate._01_merge"
)


class TestMain:
    EMPLOYMENT_STATUS_ESTIMATE_SOURCE = "some/source"
    PREPARED_SLV_DATASET_SOURCE = "other/source"
    MERGED_DATA_DESTINATION = "some/destination"

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        employment_status_estimate_lf = Mock()
        workplace_lf = Mock()
        scan_parquet_mock.side_effect = [employment_status_estimate_lf, workplace_lf]

        job.main(
            employment_status_estimate_source=self.EMPLOYMENT_STATUS_ESTIMATE_SOURCE,
            prepared_slv_dataset_source=self.PREPARED_SLV_DATASET_SOURCE,
            merged_data_destination=self.MERGED_DATA_DESTINATION,
        )

        scan_parquet_mock.assert_any_call(source=self.EMPLOYMENT_STATUS_ESTIMATE_SOURCE)
        scan_parquet_mock.assert_any_call(self.PREPARED_SLV_DATASET_SOURCE)

        employment_status_estimate_lf.join.assert_called_once_with(
            workplace_lf,
            on=[
                IndCQC.establishment_id,
                IndCQC.ascwds_workplace_import_date,
                IndCQC.published_job_role_label,
            ],
            how="left",
        )

        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=employment_status_estimate_lf.join.return_value,
            output_path=self.MERGED_DATA_DESTINATION,
        )
