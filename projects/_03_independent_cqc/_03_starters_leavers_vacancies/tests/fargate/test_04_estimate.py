from unittest.mock import Mock, call, patch

import projects._03_independent_cqc._03_starters_leavers_vacancies.fargate._04_estimate as job
from polars_utils.column_types import CategoricalColumnTypes as CatColType
from utils.column_names.ind_cqc_pipeline_columns import (
    StartersLeaversVacanciesColumns as SLVCols,
)

PATCH_PATH = (
    "projects._03_independent_cqc._03_starters_leavers_vacancies.fargate._04_estimate"
)


class TestMain:
    IMPUTED_DATA_SOURCE = "some/source"
    ESTIMATED_DATA_DESTINATION = "some/destination"

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.utils.coalesce_with_source_labels", return_value=(1, 2))
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        coalesce_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(self.IMPUTED_DATA_SOURCE, self.ESTIMATED_DATA_DESTINATION)

        scan_parquet_mock.assert_called_once_with(self.IMPUTED_DATA_SOURCE)
        assert coalesce_mock.call_args_list == [
            call(
                cols=[SLVCols.ascwds_starters_clean],
                name=SLVCols.estimated_starters,
                source_dtype=CatColType.StartersEstimateSourceEnumType,
            ),
            call(
                cols=[SLVCols.ascwds_leavers_clean],
                name=SLVCols.estimated_leavers,
                source_dtype=CatColType.LeaversEstimateSourceEnumType,
            ),
            call(
                cols=[SLVCols.ascwds_vacancies_clean],
                name=SLVCols.estimated_vacancies,
                source_dtype=CatColType.VacanciesEstimateSourceEnumType,
            ),
        ]
        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=scan_parquet_mock.return_value.with_columns.return_value,
            output_path=self.ESTIMATED_DATA_DESTINATION,
        )
