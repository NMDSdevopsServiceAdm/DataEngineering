from datetime import date
from unittest.mock import ANY, Mock, patch

import polars as pl

import projects._03_independent_cqc._02_employment_status.fargate._01_merge as job
from polars_utils.column_types import CategoricalColumnTypes as CatColType
from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerCareHomeCleanColumns as CTCHClean,
)
from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerNonResCleanColumns as CTNRClean,
)
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.cleaned_data_files.cqc_pir_cleaned import (
    CqcPIRCleanedColumns as CQCPIRClean,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.slv_job_role_columns import SLVJobRoleColumns as SLVCols
from utils.column_values.categorical_column_values import CareHome

PATCH_PATH = "projects._03_independent_cqc._02_employment_status.fargate._01_merge"


class TestMain:
    METADATA_SOURCE = "some/source"
    JOB_ROLE_ESTIMATES_SOURCE = "another/source"
    PREPARED_WORKER_SOURCE = "worker/source"
    EMPLOYMENT_STATUS_RATES_SOURCE = "employment/status/rates/source"
    CLEANED_CQC_PIR_SOURCE = "pir/source"
    CLEANED_CT_CARE_HOME_SOURCE = "ct/care/home/source"
    CLEANED_CT_NON_RES_SOURCE = "ct/non/res/source"
    MERGED_DATA_DESTINATION = "some/destination"

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.join_data_into_cqc_lf")
    @patch(f"{PATCH_PATH}.mUtils.apply_employment_status_magic_numbers")
    @patch(f"{PATCH_PATH}.pl.scan_csv")
    @patch(f"{PATCH_PATH}.mUtils.collapse_job_role_estimates_to_published_labels")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        collapse_job_role_estimates_to_published_labels_mock: Mock,
        scan_csv_mock: Mock,
        apply_employment_status_magic_numbers_mock: Mock,
        join_data_into_cqc_lf_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(
            self.METADATA_SOURCE,
            self.JOB_ROLE_ESTIMATES_SOURCE,
            self.PREPARED_WORKER_SOURCE,
            self.EMPLOYMENT_STATUS_RATES_SOURCE,
            self.CLEANED_CQC_PIR_SOURCE,
            self.CLEANED_CT_CARE_HOME_SOURCE,
            self.CLEANED_CT_NON_RES_SOURCE,
            self.MERGED_DATA_DESTINATION,
        )

        assert len(scan_parquet_mock.call_args_list) == 6

        scan_parquet_mock.assert_any_call(
            source=self.METADATA_SOURCE, selected_columns=job.metadata_columns
        )
        scan_parquet_mock.assert_any_call(
            source=self.JOB_ROLE_ESTIMATES_SOURCE,
            selected_columns=job.job_role_estimates_columns,
        )
        scan_parquet_mock.assert_any_call(
            self.PREPARED_WORKER_SOURCE, selected_columns=job.worker_columns
        )
        scan_parquet_mock.assert_any_call(
            source=self.CLEANED_CQC_PIR_SOURCE,
            selected_columns=job.cleaned_cqc_pir_columns,
        )
        scan_parquet_mock.assert_any_call(
            source=self.CLEANED_CT_CARE_HOME_SOURCE,
            selected_columns=job.cleaned_ct_care_home_columns,
        )
        scan_parquet_mock.assert_any_call(
            source=self.CLEANED_CT_NON_RES_SOURCE,
            selected_columns=job.cleaned_ct_non_res_columns,
        )

        collapse_job_role_estimates_to_published_labels_mock.assert_called_once()

        assert join_data_into_cqc_lf_mock.call_count == 3

        apply_employment_status_magic_numbers_mock.assert_called_once()

        scan_csv_mock.assert_called_once_with(
            self.EMPLOYMENT_STATUS_RATES_SOURCE, schema=ANY
        )

        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=ANY,
            output_path=self.MERGED_DATA_DESTINATION,
        )

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.mUtils.apply_employment_status_magic_numbers")
    @patch(f"{PATCH_PATH}.pl.scan_csv")
    @patch(f"{PATCH_PATH}.mUtils.collapse_job_role_estimates_to_published_labels")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_joins_pir_and_capacity_tracker_data_without_dtype_errors(
        self,
        scan_parquet_mock: Mock,
        collapse_job_role_estimates_to_published_labels_mock: Mock,
        scan_csv_mock: Mock,
        apply_employment_status_magic_numbers_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        # Dtypes here mirror what the real pipeline produces: job_role_estimates_lf's
        # location_id is a namespaced Categorical, metadata's care_home is the
        # CareHomeEnumType, and PIR/CT's cleaned data already carries care_home as
        # CareHomeEnumType too (cast at their own clean stage) but leaves location
        # ids as plain String - so this exercises the location_id cast the joins
        # below rely on.
        job_role_estimates_lf = pl.LazyFrame(
            {
                IndCQC.id_per_locationid_import_date: [1],
                IndCQC.location_id: pl.Series(
                    ["1-001"], dtype=CatColType.LocationCatType
                ),
                IndCQC.cqc_location_import_date: [date(2024, 1, 1)],
                SLVCols.published_job_role_label: ["care_worker"],
            }
        )
        metadata_lf = pl.LazyFrame(
            {
                IndCQC.id_per_locationid_import_date: [1],
                IndCQC.care_home: pl.Series(
                    [CareHome.care_home], dtype=CatColType.CareHomeEnumType
                ),
                IndCQC.establishment_id: pl.Series(
                    ["est-1"], dtype=CatColType.EstablishmentCatType
                ),
                IndCQC.ascwds_workplace_import_date: [date(2024, 1, 1)],
            }
        )
        pir_lf = pl.LazyFrame(
            {
                CQCPIRClean.location_id: ["1-001"],
                CQCPIRClean.cqc_pir_import_date: [date(2024, 1, 1)],
                CQCPIRClean.care_home: pl.Series(
                    [CareHome.care_home], dtype=CatColType.CareHomeEnumType
                ),
                CQCPIRClean.staff_leavers: [3],
                CQCPIRClean.staff_vacancies: [2],
            }
        )
        ct_care_home_lf = pl.LazyFrame(
            {
                CTCHClean.cqc_id: ["1-001"],
                CTCHClean.ct_care_home_import_date: [date(2024, 1, 1)],
                CTCHClean.care_home: pl.Series(
                    [CareHome.care_home], dtype=CatColType.CareHomeEnumType
                ),
                CTCHClean.agency_nurses_employed: [1],
                CTCHClean.agency_care_workers_employed: [2],
                CTCHClean.agency_non_care_workers_employed: [3],
                CTCHClean.hours_agency: [12.5],
            }
        )
        ct_non_res_lf = pl.LazyFrame(
            {
                CTNRClean.cqc_id: ["1-001"],
                CTNRClean.ct_non_res_import_date: [date(2024, 1, 1)],
                # deliberately not_care_home: a genuine care home location shouldn't
                # match non-res Capacity Tracker data, since care_home is a join key
                CTNRClean.care_home: pl.Series(
                    [CareHome.not_care_home], dtype=CatColType.CareHomeEnumType
                ),
                CTNRClean.hours_agency_dom_care: [7.5],
            }
        )
        worker_lf = pl.LazyFrame(
            {
                AWKClean.location_id: pl.Series(
                    ["1-001"], dtype=CatColType.LocationCatType
                ),
                AWKClean.establishment_id: pl.Series(
                    ["est-1"], dtype=CatColType.EstablishmentCatType
                ),
                AWKClean.ascwds_worker_import_date: [date(2024, 1, 1)],
                SLVCols.published_job_role_label: ["care_worker"],
            }
        )
        scan_parquet_mock.side_effect = [
            metadata_lf,
            job_role_estimates_lf,
            pir_lf,
            ct_care_home_lf,
            ct_non_res_lf,
            worker_lf,
        ]
        collapse_job_role_estimates_to_published_labels_mock.side_effect = lambda lf: lf
        apply_employment_status_magic_numbers_mock.side_effect = lambda lf, rates_lf: lf

        job.main(
            self.METADATA_SOURCE,
            self.JOB_ROLE_ESTIMATES_SOURCE,
            self.PREPARED_WORKER_SOURCE,
            self.EMPLOYMENT_STATUS_RATES_SOURCE,
            self.CLEANED_CQC_PIR_SOURCE,
            self.CLEANED_CT_CARE_HOME_SOURCE,
            self.CLEANED_CT_NON_RES_SOURCE,
            self.MERGED_DATA_DESTINATION,
        )

        returned_df = sink_to_parquet_mock.call_args.kwargs["lazy_df"].collect()
        assert returned_df[CQCPIRClean.staff_leavers][0] == 3
        assert returned_df[CQCPIRClean.staff_vacancies][0] == 2
        assert returned_df[CTCHClean.hours_agency][0] == 12.5
        assert returned_df[CTNRClean.hours_agency_dom_care][0] is None
