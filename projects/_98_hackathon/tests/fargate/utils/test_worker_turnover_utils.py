from datetime import date

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._98_hackathon.fargate.utils.worker_turnover_utils as job
from projects._98_hackathon.unittest_data.polars_hackathon_test_file_data import (
    WorkerTurnoverData as Data,
)
from projects._98_hackathon.unittest_data.polars_hackathon_test_file_data import (
    WORKER_LABELS_DICT,
    coded_worker_row,
    raw_worker_row,
)
from projects._98_hackathon.unittest_data.polars_hackathon_test_file_schemas import (
    WorkerTurnoverSchemas as Schemas,
)
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from polars_utils.cleaning_utils import build_labels_lf
from utils.column_names.hackathon_columns import HackathonColumns as HackCols


class TestFilterWorkerTurnoverData:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.filter_worker_turnover_data_cases
        ],
    )
    def test_returns_expected_rows(self, case):
        input_lf = pl.LazyFrame(
            case.input_rows, schema=Schemas.filter_worker_schema, orient="row"
        )
        expected_lf = pl.LazyFrame(
            case.expected_rows, schema=Schemas.filter_worker_schema, orient="row"
        )

        returned_lf = job.filter_worker_turnover_data(input_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestGetLatestSaveDatePerEstablishment:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.get_latest_save_date_per_establishment_cases
        ],
    )
    def test_returns_expected_rows(self, case):
        input_lf = pl.LazyFrame(
            case.input_rows, schema=Schemas.latest_save_date_input_schema, orient="row"
        )
        snapshot_lf = pl.LazyFrame(
            case.snapshot_rows or case.input_rows,
            schema=Schemas.latest_save_date_input_schema,
            orient="row",
        )
        expected_lf = pl.LazyFrame(
            case.expected_rows, schema=Schemas.latest_save_date_schema, orient="row"
        )

        returned_lf = job.get_latest_save_date_per_establishment(input_lf, snapshot_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)


class TestAddEstablishmentActiveFlag:
    @staticmethod
    def build_input_lfs(case):
        worker_lf = pl.LazyFrame(
            case.worker_rows, schema=Schemas.active_input_schema, orient="row"
        )
        save_dates_lf = pl.LazyFrame(
            case.save_date_rows, schema=Schemas.latest_save_date_schema, orient="row"
        )
        return worker_lf, save_dates_lf

    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.add_establishment_active_flag_cases
        ],
    )
    def test_returns_expected_rows(self, case):
        worker_lf, save_dates_lf = self.build_input_lfs(case)
        expected_lf = pl.LazyFrame(
            case.expected_rows, schema=Schemas.active_output_schema, orient="row"
        )

        returned_lf = job.add_establishment_active_flag(
            worker_lf, save_dates_lf, case.n_months
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)

    def test_outputs_integer_gaps_and_boolean_flag(self):
        worker_lf, save_dates_lf = self.build_input_lfs(
            Data.add_establishment_active_flag_cases[0]
        )

        returned_schema = job.add_establishment_active_flag(
            worker_lf, save_dates_lf, n_months=6
        ).collect_schema()

        assert returned_schema[HackCols.months_since_save_start] == pl.Int32
        assert returned_schema[HackCols.months_since_save_end] == pl.Int32
        assert returned_schema[HackCols.establishment_active] == pl.Boolean


class TestAddMainServiceType:
    @pytest.mark.parametrize(
        "case",
        [pytest.param(case, id=case.id) for case in Data.add_main_service_type_cases],
    )
    def test_returns_expected_rows(self, case):
        input_lf = pl.LazyFrame(
            case.input_rows, schema=Schemas.main_service_input_schema, orient="row"
        )
        expected_lf = pl.LazyFrame(
            case.expected_rows,
            schema=Schemas.main_service_type_schema,
            orient="row",
        )

        returned_lf = job.add_main_service_type(input_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestAddIsLeaverFlag:
    @pytest.mark.parametrize(
        "case",
        [pytest.param(case, id=case.id) for case in Data.add_is_leaver_flag_cases],
    )
    def test_returns_expected_rows(self, case):
        input_lf = pl.LazyFrame(
            case.input_rows, schema=Schemas.is_leaver_input_schema, orient="row"
        )
        expected_lf = pl.LazyFrame(
            case.expected_rows, schema=Schemas.is_leaver_output_schema, orient="row"
        )

        returned_lf = job.add_is_leaver_flag(input_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)


def raw_worker_lf(*rows: dict) -> pl.LazyFrame:
    """Returns raw worker rows as a LazyFrame with all-string columns."""
    return pl.LazyFrame(list(rows), schema={column: pl.String for column in rows[0]})


class TestAddRawWorkerColumns:
    flagged_lf = pl.LazyFrame(
        [("1-001", "1", date(2021, 1, 5))],
        schema=Schemas.raw_worker_join_input_schema,
        orient="row",
    )

    def test_adds_all_modelling_columns_after_input_columns(self):
        raw_lf = raw_worker_lf(raw_worker_row("1", "20210105"))

        returned_lf = job.add_raw_worker_columns(self.flagged_lf, raw_lf)

        returned_names = returned_lf.collect_schema().names()
        input_names = Schemas.raw_worker_join_input_schema.names()
        assert returned_names == [
            *input_names,
            *job.WORKER_CATEGORICAL_COLUMNS,
            *job.WORKER_NUMERIC_COLUMNS,
            *job.WORKER_DATE_COLUMNS,
        ]
        assert len(returned_names) - len(input_names) == 25

    def test_excludes_highest_qualification_level(self):
        raw_lf = raw_worker_lf(raw_worker_row("1", "20210105"))

        returned_names = (
            job.add_raw_worker_columns(self.flagged_lf, raw_lf).collect_schema().names()
        )

        assert AWKClean.highest_qual_level not in returned_names

    def test_returns_raw_values_for_worker_with_matching_keys(self):
        raw_lf = raw_worker_lf(
            raw_worker_row(
                "1",
                "20210105",
                **{
                    AWKClean.gender: "1",
                    AWKClean.age: "42",
                },
            )
        )

        returned_df = job.add_raw_worker_columns(self.flagged_lf, raw_lf).collect()

        assert returned_df[AWKClean.gender].to_list() == ["1"]
        assert returned_df[AWKClean.age].to_list() == [42.0]

    @pytest.mark.parametrize(
        "raw_row",
        [
            pytest.param(
                raw_worker_row("2", "20210105", **{AWKClean.gender: "1"}),
                id="other_worker",
            ),
            pytest.param(
                raw_worker_row("1", "20210105", "1-002", **{AWKClean.gender: "1"}),
                id="other_establishment",
            ),
            pytest.param(
                raw_worker_row("1", "20220105", **{AWKClean.gender: "1"}),
                id="other_import_date",
            ),
        ],
    )
    def test_returns_null_columns_when_keys_do_not_match(self, raw_row):
        returned_df = job.add_raw_worker_columns(
            self.flagged_lf, raw_worker_lf(raw_row)
        ).collect()

        assert returned_df.height == 1
        assert returned_df[AWKClean.gender].to_list() == [None]

    def test_does_not_parse_dates_of_raw_rows_outside_worker_data(self):
        raw_lf = raw_worker_lf(
            raw_worker_row("1", "20210105"),
            raw_worker_row("2", "20100105", **{AWKClean.pay_change_date: "not a date"}),
        )

        returned_df = job.add_raw_worker_columns(self.flagged_lf, raw_lf).collect()

        assert returned_df.height == 1

    @pytest.mark.parametrize(
        "column, expected_type",
        [
            pytest.param(AWKClean.gender, pl.Categorical, id="code_column"),
            pytest.param(
                AWKClean.social_care_qual_held, pl.Categorical, id="social_care_qual"
            ),
            pytest.param(AWKClean.age, pl.Float32, id="numeric_column"),
            pytest.param(AWKClean.days_sick, pl.Float32, id="days_sick"),
            pytest.param(AWKClean.start_in_sector, pl.Float32, id="start_in_sector"),
            pytest.param(AWKClean.year_of_entry, pl.Float32, id="year_of_entry"),
        ],
    )
    def test_returns_expected_column_type(self, column, expected_type):
        raw_lf = raw_worker_lf(raw_worker_row("1", "20210105"))

        schema = job.add_raw_worker_columns(self.flagged_lf, raw_lf).collect_schema()

        assert schema[column] == expected_type

    def test_returns_date_type_for_date_columns(self):
        raw_lf = raw_worker_lf(raw_worker_row("1", "20210105"))

        schema = job.add_raw_worker_columns(self.flagged_lf, raw_lf).collect_schema()

        assert all(schema[column] == pl.Date for column in job.WORKER_DATE_COLUMNS)

    @pytest.mark.parametrize(
        "column, raw_value, expected_number",
        [
            pytest.param(AWKClean.days_sick, "3.5", 3.5, id="decimal_days"),
            pytest.param(AWKClean.days_sick, ".5", 0.5, id="no_leading_digit"),
            pytest.param(AWKClean.days_sick, " ", None, id="blank_days"),
            pytest.param(AWKClean.start_in_sector, "2015", 2015.0, id="year"),
            pytest.param(AWKClean.year_of_entry, "-1", -1.0, id="not_known_code_kept"),
        ],
    )
    def test_returns_expected_number_for_numeric_string(
        self, column, raw_value, expected_number
    ):
        raw_lf = raw_worker_lf(raw_worker_row("1", "20210105", **{column: raw_value}))

        returned_df = job.add_raw_worker_columns(self.flagged_lf, raw_lf).collect()

        assert returned_df[column].to_list() == [expected_number]

    def test_returns_null_for_unparseable_numeric_value(self):
        raw_lf = raw_worker_lf(
            raw_worker_row("1", "20210105", **{AWKClean.age: "unknown"})
        )

        returned_df = job.add_raw_worker_columns(self.flagged_lf, raw_lf).collect()

        assert returned_df[AWKClean.age].to_list() == [None]

    @pytest.mark.parametrize(
        "raw_value, expected_date",
        [
            pytest.param("01/03/2015", date(2015, 3, 1), id="day_first_when_ambiguous"),
            pytest.param("1/3/2015", date(2015, 3, 1), id="no_zero_padding"),
            pytest.param("25/12/2019", date(2019, 12, 25), id="day_above_twelve"),
            pytest.param("", None, id="blank_string"),
            pytest.param(None, None, id="null"),
        ],
    )
    def test_returns_expected_date_for_day_first_string(self, raw_value, expected_date):
        raw_lf = raw_worker_lf(
            raw_worker_row("1", "20210105", **{AWKClean.start_date: raw_value})
        )

        returned_df = job.add_raw_worker_columns(self.flagged_lf, raw_lf).collect()

        assert returned_df[AWKClean.start_date].to_list() == [expected_date]

    @pytest.mark.parametrize("raw_value", ["2015-03-01", "not a date", "13/13/2015"])
    def test_raises_for_unparseable_date_value(self, raw_value):
        raw_lf = raw_worker_lf(
            raw_worker_row("1", "20210105", **{AWKClean.pay_change_date: raw_value})
        )

        with pytest.raises(pl.exceptions.PolarsError):
            job.add_raw_worker_columns(self.flagged_lf, raw_lf).collect()

    def test_does_not_duplicate_rows_for_unique_keys(self):
        flagged_lf = pl.LazyFrame(
            [
                ("1-001", "1", date(2021, 1, 5)),
                ("1-001", "2", date(2021, 1, 5)),
            ],
            schema=Schemas.raw_worker_join_input_schema,
            orient="row",
        )
        raw_lf = raw_worker_lf(
            raw_worker_row("1", "20210105"), raw_worker_row("2", "20210105")
        )

        returned_df = job.add_raw_worker_columns(flagged_lf, raw_lf).collect()

        assert returned_df.height == 2


def coded_worker_lf(*rows: tuple) -> pl.LazyFrame:
    return pl.LazyFrame(rows, schema=Schemas.labelled_input_schema, orient="row")


class TestAddLabelledWorkerColumns:
    labels_lf = build_labels_lf(WORKER_LABELS_DICT)
    labelled_columns = [
        HackCols.gender_labelled,
        HackCols.ethnicity_labelled,
        HackCols.disabled_labelled,
        HackCols.hourly_or_annual_pay_labelled,
        HackCols.social_care_qual_held_labelled,
        HackCols.level_social_care_qualification_held_labelled,
        HackCols.care_certificate_status_labelled,
        HackCols.apprentice_labelled,
        HackCols.zero_hours_labelled,
        HackCols.source_of_recruitment_labelled,
    ]

    def test_adds_a_labelled_column_for_each_of_the_10_coded_columns(self):
        worker_lf = coded_worker_lf(coded_worker_row("1"))

        returned_names = (
            job.add_labelled_worker_columns(worker_lf, self.labels_lf)
            .collect_schema()
            .names()
        )

        assert returned_names == [
            *Schemas.labelled_input_schema.names(),
            *self.labelled_columns,
        ]

    def test_keeps_raw_code_columns_unchanged(self):
        worker_lf = coded_worker_lf(
            coded_worker_row(
                "1", **{AWKClean.gender: "1", AWKClean.nationality: "826"}
            ),
            coded_worker_row("2"),
        )

        returned_df = job.add_labelled_worker_columns(
            worker_lf, self.labels_lf
        ).collect()

        assert returned_df[AWKClean.gender].to_list() == ["1", None]
        assert returned_df[AWKClean.nationality].to_list() == ["826", None]

    @pytest.mark.parametrize(
        "column, labelled_column, code, expected_label",
        [
            pytest.param(
                AWKClean.gender, HackCols.gender_labelled, "1", "Male", id="gender"
            ),
            pytest.param(
                AWKClean.ethnicity,
                HackCols.ethnicity_labelled,
                "31",
                "English / Welsh / Scottish / Northern Irish / British",
                id="ethnicity",
            ),
            pytest.param(
                AWKClean.disabled,
                HackCols.disabled_labelled,
                "0",
                "No",
                id="disabled",
            ),
            pytest.param(
                AWKClean.hourly_or_annual_pay,
                HackCols.hourly_or_annual_pay_labelled,
                "252",
                "Hourly",
                id="hourly_or_annual_pay",
            ),
            pytest.param(
                AWKClean.social_care_qual_held,
                HackCols.social_care_qual_held_labelled,
                "1",
                "Yes",
                id="social_care_qual_held",
            ),
            pytest.param(
                AWKClean.level_social_care_qualification_held,
                HackCols.level_social_care_qualification_held_labelled,
                "8",
                "Level 8 or above",
                id="level_social_care_qualification_held",
            ),
            pytest.param(
                AWKClean.care_certificate_status,
                HackCols.care_certificate_status_labelled,
                "3",
                "In progress or partially completed",
                id="care_certificate_status",
            ),
            pytest.param(
                AWKClean.apprentice,
                HackCols.apprentice_labelled,
                "2",
                "No",
                id="apprentice",
            ),
            pytest.param(
                AWKClean.zero_hours,
                HackCols.zero_hours_labelled,
                "-2",
                "Not known",
                id="zero_hours",
            ),
            pytest.param(
                AWKClean.source_of_recruitment,
                HackCols.source_of_recruitment_labelled,
                "225",
                "Not known",
                id="source_of_recruitment",
            ),
        ],
    )
    def test_returns_label_for_mapped_code(
        self, column, labelled_column, code, expected_label
    ):
        worker_lf = coded_worker_lf(coded_worker_row("1", **{column: code}))

        returned_df = job.add_labelled_worker_columns(
            worker_lf, self.labels_lf
        ).collect()

        assert returned_df[labelled_column].to_list() == [expected_label]

    def test_returns_null_for_blank_code(self):
        worker_lf = coded_worker_lf(coded_worker_row("1", **{AWKClean.gender: ""}))

        returned_df = job.add_labelled_worker_columns(
            worker_lf, self.labels_lf
        ).collect()

        assert returned_df[HackCols.gender_labelled].to_list() == [None]

    def test_returns_null_when_raw_code_is_null(self):
        worker_lf = coded_worker_lf(coded_worker_row("1"))

        returned_df = job.add_labelled_worker_columns(
            worker_lf, self.labels_lf
        ).collect()

        assert returned_df[HackCols.gender_labelled].to_list() == [None]

    def test_returns_unmapped_label_for_unmapped_code(self):
        worker_lf = coded_worker_lf(coded_worker_row("1", **{AWKClean.gender: "99"}))

        returned_df = job.add_labelled_worker_columns(
            worker_lf, self.labels_lf
        ).collect()

        assert returned_df[HackCols.gender_labelled].to_list() == ["Unmapped"]

    def test_keeps_raw_code_for_unmapped_code(self):
        worker_lf = coded_worker_lf(coded_worker_row("1", **{AWKClean.gender: "99"}))

        returned_df = job.add_labelled_worker_columns(
            worker_lf, self.labels_lf
        ).collect()

        assert returned_df[AWKClean.gender].to_list() == ["99"]

    def test_returns_categorical_type_for_labelled_columns(self):
        worker_lf = coded_worker_lf(coded_worker_row("1", **{AWKClean.gender: "1"}))

        schema = job.add_labelled_worker_columns(
            worker_lf, self.labels_lf
        ).collect_schema()

        assert [schema[column] for column in self.labelled_columns] == [
            pl.Categorical
        ] * 10

    def test_does_not_change_row_count(self):
        worker_lf = coded_worker_lf(
            coded_worker_row("1", **{AWKClean.gender: "1"}),
            coded_worker_row("1", **{AWKClean.gender: "1"}),
            coded_worker_row("2", **{AWKClean.gender: "2"}),
        )

        returned_df = job.add_labelled_worker_columns(
            worker_lf, self.labels_lf
        ).collect()

        assert returned_df.height == 3
