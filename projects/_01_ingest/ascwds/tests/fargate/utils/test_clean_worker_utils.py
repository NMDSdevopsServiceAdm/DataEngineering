from datetime import date

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._01_ingest.ascwds.fargate.utils.clean_worker_utils as job
from projects._01_ingest.unittest_data.polars_ingest_test_file_data import (
    TestCleanAscwdsWorkerUtilsData as Data,
)
from projects._01_ingest.unittest_data.polars_ingest_test_file_schema import (
    TestCleanAscwdsWorkerUtilsSchemas as Schemas,
)
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)


class TestRemoveWorkersWithoutWorkplaces:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.remove_workers_without_workplaces_test_cases
        ],
    )
    def test_removes_workers_without_matching_workplace(self, case):
        worker_lf = pl.LazyFrame(
            case.worker_data, schema=Schemas.worker_schema, orient="row"
        )
        workplace_lf = pl.LazyFrame(
            case.workplace_data, schema=Schemas.workplace_schema, orient="row"
        )
        expected_lf = pl.LazyFrame(
            case.expected_data,
            schema=Schemas.expected_remove_workers_without_workplaces_schema,
            orient="row",
        )

        returned_lf = job.remove_workers_without_workplaces(worker_lf, workplace_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)

    def test_keeps_workers_with_matching_workplace(self):
        case = Data.remove_workers_without_workplaces_test_cases[1]
        worker_lf = pl.LazyFrame(
            case.worker_data, schema=Schemas.worker_schema, orient="row"
        )
        workplace_lf = pl.LazyFrame(
            case.workplace_data, schema=Schemas.workplace_schema, orient="row"
        )

        returned_lf = job.remove_workers_without_workplaces(worker_lf, workplace_lf)

        assert (
            returned_lf.select(pl.len()).collect().item()
            == worker_lf.select(pl.len()).collect().item()
        )


class TestRemapMainjridCodes:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.remap_mainjrid_codes_test_cases
        ],
    )
    def test_function_returns_expected_values(self, case):
        input_lf = pl.LazyFrame(
            case.input_data, schema=Schemas.remap_mainjrid_codes_schema, orient="row"
        )
        expected_lf = pl.LazyFrame(
            case.expected_data,
            schema=Schemas.remap_mainjrid_codes_schema,
            orient="row",
        )

        returned_lf = job.remap_mainjrid_codes(input_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestImputeNotKnownJobRoles:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.impute_not_known_job_roles_test_cases
        ],
    )
    def test_function_returns_expected_values(self, case):
        input_lf = pl.LazyFrame(
            case.input_data,
            schema=Schemas.impute_not_known_job_roles_schema,
            orient="row",
        )
        expected_lf = pl.LazyFrame(
            case.expected_data,
            schema=Schemas.impute_not_known_job_roles_schema,
            orient="row",
        )

        returned_lf = job.impute_not_known_job_roles(input_lf)

        pl_testing.assert_frame_equal(
            returned_lf.sort(AWKClean.worker_id, AWKClean.ascwds_worker_import_date),
            expected_lf.sort(AWKClean.worker_id, AWKClean.ascwds_worker_import_date),
        )


class TestCreateCleanMainJobRoleColumn:
    case = Data.create_clean_main_job_role_column_case
    input_lf = pl.LazyFrame(
        case.input_data,
        schema=Schemas.create_clean_main_job_role_column_schema,
        orient="row",
    )
    labels_lf = pl.LazyFrame(
        case.labels_data, schema=Schemas.data_labels_schema, orient="row"
    )
    expected_lf = pl.LazyFrame(
        case.expected_data,
        schema=Schemas.expected_create_clean_main_job_role_column_schema,
        orient="row",
    )
    returned_lf = job.create_clean_main_job_role_column(input_lf, labels_lf)

    def test_returns_expected_main_job_role_clean_values(self):
        pl_testing.assert_frame_equal(
            self.returned_lf.sort(
                AWKClean.worker_id, AWKClean.ascwds_worker_import_date
            ).select(AWKClean.worker_id, AWKClean.main_job_role_clean),
            self.expected_lf.sort(
                AWKClean.worker_id, AWKClean.ascwds_worker_import_date
            ).select(AWKClean.worker_id, AWKClean.main_job_role_clean),
        )

    def test_returns_expected_main_job_role_clean_labelled_values(self):
        pl_testing.assert_frame_equal(
            self.returned_lf.sort(
                AWKClean.worker_id, AWKClean.ascwds_worker_import_date
            ).select(AWKClean.worker_id, AWKClean.main_job_role_clean_labelled),
            self.expected_lf.sort(
                AWKClean.worker_id, AWKClean.ascwds_worker_import_date
            ).select(AWKClean.worker_id, AWKClean.main_job_role_clean_labelled),
        )

    def test_filters_out_unresolvable_job_role_rows(self):
        returned_worker_ids = (
            self.returned_lf.select(AWKClean.worker_id).collect().to_series().to_list()
        )

        assert "102" not in returned_worker_ids


class TestCreateCleanEmploymentStatusColumn:
    case = Data.create_clean_employment_status_column_case
    input_lf = pl.LazyFrame(
        case.input_data,
        schema=Schemas.create_clean_employment_status_column_schema,
        orient="row",
    )
    labels_lf = pl.LazyFrame(
        case.labels_data, schema=Schemas.data_labels_schema, orient="row"
    )
    expected_lf = pl.LazyFrame(
        case.expected_data,
        schema=Schemas.expected_create_clean_employment_status_column_schema,
        orient="row",
    )
    returned_lf = job.create_clean_employment_status_column(input_lf, labels_lf)

    def test_returns_expected_employment_status_clean_values(self):
        pl_testing.assert_frame_equal(
            self.returned_lf.sort(AWKClean.worker_id).select(
                AWKClean.worker_id, AWKClean.employment_status_clean
            ),
            self.expected_lf.sort(AWKClean.worker_id).select(
                AWKClean.worker_id, AWKClean.employment_status_clean
            ),
        )

    def test_returns_expected_employment_status_clean_labelled_values(self):
        pl_testing.assert_frame_equal(
            self.returned_lf.sort(AWKClean.worker_id).select(
                AWKClean.worker_id, AWKClean.employment_status_clean_labelled
            ),
            self.expected_lf.sort(AWKClean.worker_id).select(
                AWKClean.worker_id, AWKClean.employment_status_clean_labelled
            ),
        )

    def test_does_not_change_row_count(self):
        assert (
            self.returned_lf.select(pl.len()).collect().item()
            == self.input_lf.select(pl.len()).collect().item()
        )


def raw_modelling_lf(values: dict[str, str | None] | None = None) -> pl.LazyFrame:
    """Returns one all-string raw worker row, with every modelling column null
    unless set in `values`."""
    modelling_columns = [
        *job.WORKER_CATEGORICAL_COLUMNS,
        *job.WORKER_NUMERIC_COLUMNS,
        *job.WORKER_DATE_COLUMNS,
    ]
    row = {
        AWKClean.worker_id: "1",
        **{column: None for column in modelling_columns},
        **(values or {}),
    }
    return pl.LazyFrame([row], schema={column: pl.String for column in row})


class TestCastRawWorkerModellingColumns:
    def test_returns_categorical_for_code_columns(self):
        schema = job.cast_raw_worker_modelling_columns(
            raw_modelling_lf()
        ).collect_schema()

        assert all(
            schema[column] == pl.Categorical
            for column in job.WORKER_CATEGORICAL_COLUMNS
        )

    def test_returns_float32_for_numeric_columns(self):
        schema = job.cast_raw_worker_modelling_columns(
            raw_modelling_lf()
        ).collect_schema()

        assert all(
            schema[column] == pl.Float32 for column in job.WORKER_NUMERIC_COLUMNS
        )

    def test_returns_null_for_unparseable_numeric_value(self):
        returned_df = job.cast_raw_worker_modelling_columns(
            raw_modelling_lf({AWKClean.age: "unknown"})
        ).collect()

        assert returned_df[AWKClean.age].to_list() == [None]

    def test_returns_date_type_for_date_columns(self):
        schema = job.cast_raw_worker_modelling_columns(
            raw_modelling_lf()
        ).collect_schema()

        assert all(schema[column] == pl.Date for column in job.WORKER_DATE_COLUMNS)

    @pytest.mark.parametrize(
        "raw_value, expected_date",
        [
            pytest.param("01/03/2015", date(2015, 3, 1), id="day_and_month_one_digit"),
            pytest.param("25/12/2019", date(2019, 12, 25), id="day_above_twelve"),
            pytest.param("", None, id="blank_string"),
            pytest.param(None, None, id="null"),
        ],
    )
    def test_returns_expected_date_for_day_first_string(self, raw_value, expected_date):
        returned_df = job.cast_raw_worker_modelling_columns(
            raw_modelling_lf({AWKClean.start_date: raw_value})
        ).collect()

        assert returned_df[AWKClean.start_date].to_list() == [expected_date]

    @pytest.mark.parametrize("raw_value", ["2015-03-01", "not a date", "13/13/2015"])
    def test_raises_for_unparseable_date_value(self, raw_value):
        worker_lf = raw_modelling_lf({AWKClean.pay_change_date: raw_value})

        with pytest.raises(pl.exceptions.PolarsError):
            job.cast_raw_worker_modelling_columns(worker_lf).collect()

    def test_keeps_other_columns_unchanged(self):
        input_lf = raw_modelling_lf()

        returned_lf = job.cast_raw_worker_modelling_columns(input_lf)

        assert returned_lf.collect_schema().names() == input_lf.collect_schema().names()
        assert returned_lf.collect()[AWKClean.worker_id].to_list() == ["1"]
