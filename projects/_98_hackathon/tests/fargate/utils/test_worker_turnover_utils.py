from dataclasses import fields
from datetime import date
from unittest.mock import patch

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._98_hackathon.fargate.utils.worker_turnover_utils as job
from polars_utils.column_types import CategoricalColumnTypes as CatColType
from projects._98_hackathon.unittest_data.polars_hackathon_test_file_data import (
    EEA_CODES,
)
from projects._98_hackathon.unittest_data.polars_hackathon_test_file_data import (
    WorkerTurnoverData as Data,
)
from projects._98_hackathon.unittest_data.polars_hackathon_test_file_data import (
    raw_worker_row,
)
from projects._98_hackathon.unittest_data.polars_hackathon_test_file_schemas import (
    WorkerTurnoverSchemas as Schemas,
)
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.cleaned_data_files.ons_cleaned import (
    OnsCleanedColumns as ONSClean,
)
from utils.column_names.data_labels_columns import DataLabelsColumns as DLC
from utils.column_names.hackathon_columns import HackathonColumns as HackCols
from utils.column_names.hackathon_columns import (
    HackathonOutputColumns as OutCols,
)
from utils.column_values.ascwds_labelled_vocab import (
    EthnicityGroupLabels,
    EthnicityID,
    NotKnownWorkerCodes,
    SourceOfRecruitmentID,
    SourceOfRecruitmentWithinOutsideAscLabels,
)


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


class TestAddTurnoverRate:
    @pytest.mark.parametrize(
        "case",
        [pytest.param(case, id=case.id) for case in Data.add_turnover_rate_cases],
    )
    def test_returns_expected_rows(self, case):
        expected_lf = pl.LazyFrame(
            case.expected_rows, schema=Schemas.turnover_rate_schema, orient="row"
        )
        input_lf = expected_lf.drop(HackCols.turnover_rate)

        returned_lf = job.add_turnover_rate(input_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)


class TestAddHeadcountReconciliationGap:
    MAX_GAP = 1

    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.add_headcount_reconciliation_gap_cases
        ],
    )
    def test_returns_expected_rows(self, case):
        expected_lf = pl.LazyFrame(
            case.expected_rows, schema=Schemas.headcount_gap_schema, orient="row"
        )
        input_lf = expected_lf.drop(
            HackCols.headcount_reconciliation_gap,
            HackCols.headcount_gap_within_tolerance,
        )

        returned_lf = job.add_headcount_reconciliation_gap(input_lf, self.MAX_GAP)

        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)


class TestAddValidForAnalysisFlag:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.add_valid_for_analysis_flag_cases
        ],
    )
    def test_returns_expected_rows(self, case):
        expected_lf = pl.LazyFrame(
            case.expected_rows, schema=Schemas.valid_for_analysis_schema, orient="row"
        )
        input_lf = expected_lf.drop(
            HackCols.plausible_turnover, HackCols.valid_for_analysis
        )

        returned_lf = job.add_valid_for_analysis_flag(input_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)


class TestAddSplitColumn:
    @pytest.mark.parametrize(
        "import_date, expected_split",
        [
            pytest.param(date(2019, 12, 1), None, id="before_train_start"),
            pytest.param(date(2020, 1, 1), "train", id="train_start"),
            pytest.param(date(2023, 6, 1), "train", id="within_train"),
            pytest.param(date(2024, 11, 1), "train", id="train_end"),
            pytest.param(date(2024, 12, 1), None, id="after_train_end"),
            pytest.param(date(2025, 10, 1), None, id="before_predict"),
            pytest.param(date(2025, 11, 1), "predict", id="predict_date"),
            pytest.param(date(2025, 12, 1), None, id="after_predict"),
            pytest.param(None, None, id="null_date"),
        ],
    )
    def test_returns_expected_split(self, import_date, expected_split):
        input_lf = pl.LazyFrame(
            {AWKClean.ascwds_worker_import_date: [import_date]},
            schema={AWKClean.ascwds_worker_import_date: pl.Date},
        )

        returned_df = job.add_split_column(input_lf).collect()

        assert returned_df[HackCols.split].to_list() == [expected_split]

    def test_returns_categorical_split_column(self):
        input_lf = pl.LazyFrame(
            {AWKClean.ascwds_worker_import_date: [date(2020, 1, 1)]},
        )

        returned_lf = job.add_split_column(input_lf)

        assert returned_lf.collect_schema()[HackCols.split] == pl.Categorical


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


class TestAddCountryCodeGroup:
    @pytest.mark.parametrize(
        "case",
        [pytest.param(case, id=case.id) for case in Data.add_country_code_group_cases],
    )
    def test_returns_expected_rows(self, case):
        expected_schema = pl.Schema(
            [
                (case.code_column, pl.Categorical()),
                (case.group_column, pl.Categorical()),
            ]
        )
        expected_lf = pl.LazyFrame(
            case.expected_rows, schema=expected_schema, orient="row"
        )
        input_lf = expected_lf.drop(case.group_column)

        returned_lf = job.add_country_code_group(input_lf, case.code_column)

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    @pytest.mark.parametrize(
        "code_column, group_column, eea_label",
        [
            (AWKClean.nationality, HackCols.nationality_group, "EEA (non-British)"),
            (
                AWKClean.country_of_birth,
                HackCols.country_of_birth_group,
                "EEA (non-UK)",
            ),
        ],
    )
    @pytest.mark.parametrize("code", EEA_CODES)
    def test_returns_eea_label_for_each_eea_code(
        self, code, code_column, group_column, eea_label
    ):
        input_lf = pl.LazyFrame(
            [(code,)], schema=pl.Schema([(code_column, pl.Categorical())]), orient="row"
        )

        returned_df = job.add_country_code_group(input_lf, code_column).collect()

        assert returned_df[group_column].to_list() == [eea_label]

    def test_raises_value_error_for_unsupported_column(self):
        input_lf = pl.LazyFrame({AWKClean.gender: ["1"]})

        with pytest.raises(ValueError):
            job.add_country_code_group(input_lf, AWKClean.gender)


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


class TestCombinePayAndHours:
    @pytest.mark.parametrize(
        "case",
        [pytest.param(case, id=case.id) for case in Data.combine_pay_and_hours_cases],
    )
    def test_returns_expected_rows(self, case):
        input_lf = pl.LazyFrame(
            case.input_rows, schema=Schemas.pay_input_schema, orient="row"
        )
        expected_lf = pl.LazyFrame(
            case.expected_rows, schema=Schemas.pay_output_schema, orient="row"
        )

        returned_lf = job.combine_pay_and_hours(input_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestAddNlwAndPayDifference:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.add_nlw_and_pay_difference_cases
        ],
    )
    def test_returns_expected_rows(self, case):
        expected_lf = pl.LazyFrame(
            case.expected_rows, schema=Schemas.nlw_output_schema, orient="row"
        )
        input_lf = expected_lf.drop(
            HackCols.nlw_hourly_rate, HackCols.hourly_pay_minus_nlw
        )

        returned_lf = job.add_nlw_and_pay_difference(input_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestAddHourlyPayMinusNlwClean:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.add_hourly_pay_minus_nlw_clean_cases
        ],
    )
    def test_returns_expected_rows(self, case):
        expected_lf = pl.LazyFrame(
            case.expected_rows,
            schema=Schemas.hourly_pay_minus_nlw_clean_schema,
            orient="row",
        )
        input_lf = expected_lf.drop(HackCols.hourly_pay_minus_nlw_clean)

        returned_lf = job.add_hourly_pay_minus_nlw_clean(
            input_lf, case.upper_percentile
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestImputePayAndSickness:
    @pytest.mark.parametrize(
        "case",
        [pytest.param(case, id=case.id) for case in Data.impute_pay_and_sickness_cases],
    )
    def test_returns_expected_rows(self, case):
        expected_lf = pl.LazyFrame(
            case.expected_rows,
            schema=Schemas.impute_pay_and_sickness_schema,
            orient="row",
        )
        input_lf = expected_lf.drop(
            HackCols.hourly_pay_minus_nlw_clean_imputed, HackCols.days_sick_imputed
        )

        returned_lf = job.impute_pay_and_sickness(input_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestCleanAge:
    @pytest.mark.parametrize(
        "case",
        [pytest.param(case, id=case.id) for case in Data.clean_age_cases],
    )
    def test_returns_expected_rows(self, case):
        expected_lf = pl.LazyFrame(
            case.expected_rows, schema=Schemas.clean_age_schema, orient="row"
        )
        input_lf = expected_lf.drop(HackCols.age_clean)

        returned_lf = job.clean_age(input_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestImputeAge:
    @staticmethod
    def assert_imputes(rows: list[tuple]) -> None:
        expected_lf = pl.LazyFrame(rows, schema=Schemas.impute_age_schema, orient="row")
        input_lf = expected_lf.drop(HackCols.age_imputed, HackCols.age_source)

        returned_lf = job.impute_age(input_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_impute_age_keeps_observed_age_and_marks_source_observed(self):
        self.assert_imputes(Data.impute_age_observed_rows)

    def test_impute_age_leaves_worker_with_no_age_null(self):
        self.assert_imputes(Data.impute_age_no_age_rows)

    def test_impute_age_steps_up_each_year_from_first_observed_month(self):
        self.assert_imputes(Data.impute_age_steps_up_rows)

    def test_impute_age_steps_down_a_year_for_each_year_before_first_observation(
        self,
    ):
        self.assert_imputes(Data.impute_age_steps_down_rows)

    def test_impute_age_pins_birth_month_from_adjacent_plus_one_change(self):
        self.assert_imputes(Data.impute_age_pinned_rows)

    @pytest.mark.parametrize("rows", Data.impute_age_gapped_change_cases)
    def test_impute_age_does_not_pin_when_age_change_spans_a_gap(self, rows):
        self.assert_imputes(rows)

    @pytest.mark.parametrize("rows", Data.impute_age_not_pinned_cases)
    def test_impute_age_ignores_decreases_and_jumps_when_pinning(self, rows):
        self.assert_imputes(rows)

    def test_impute_age_uses_first_pinned_change_when_there_are_several(self):
        self.assert_imputes(Data.impute_age_several_pins_rows)

    def test_impute_age_keeps_observed_age_that_disagrees_with_birth_month(self):
        self.assert_imputes(Data.impute_age_disagreeing_rows)

    def test_impute_age_does_not_mix_workers_or_establishments(self):
        self.assert_imputes(Data.impute_age_independent_rows)

    def test_impute_age_gives_same_result_for_unsorted_input(self):
        self.assert_imputes(Data.impute_age_steps_up_rows[::-1])

    def test_impute_age_returns_expected_types(self):
        returned_schema = job.impute_age(
            pl.LazyFrame(
                Data.impute_age_observed_rows,
                schema=Schemas.impute_age_schema,
                orient="row",
            ).drop(HackCols.age_imputed, HackCols.age_source)
        ).collect_schema()

        assert returned_schema[HackCols.age_imputed] == pl.Float32
        assert returned_schema[HackCols.age_source] == pl.Categorical


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
            pytest.param(AWKClean.gender, pl.String, id="code_column"),
            pytest.param(
                AWKClean.social_care_qual_held, pl.String, id="social_care_qual"
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
            pytest.param(AWKClean.days_sick, "0", 0.0, id="zero_kept"),
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


class TestFixStartDateTypos:
    @pytest.mark.parametrize(
        "case",
        [pytest.param(case, id=case.id) for case in Data.fix_start_date_typos_cases],
    )
    def test_returns_expected_rows(self, case):
        input_lf = pl.LazyFrame(
            case.input_rows, schema=Schemas.fix_start_date_typos_schema, orient="row"
        )
        expected_lf = pl.LazyFrame(
            case.expected_rows, schema=Schemas.fix_start_date_typos_schema, orient="row"
        )

        returned_lf = job.fix_start_date_typos(input_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestCleanStartDate:
    @pytest.mark.parametrize(
        "case",
        [pytest.param(case, id=case.id) for case in Data.clean_start_date_cases],
    )
    def test_returns_expected_rows(self, case):
        input_lf = pl.LazyFrame(
            case.input_rows, schema=Schemas.clean_start_date_schema, orient="row"
        )
        expected_lf = pl.LazyFrame(
            case.expected_rows,
            schema=Schemas.clean_start_date_schema,
            orient="row",
        )

        returned_lf = job.clean_start_date(input_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestAddHourlyPayDiffToCssrMedian:
    import_date = date(2021, 1, 5)
    schema = {
        AWKClean.ascwds_worker_import_date: pl.Date,
        ONSClean.current_cssr: pl.Categorical,
        AWKClean.main_job_role_clean_labelled: pl.Categorical,
        HackCols.hourly_pay: pl.Float32,
        HackCols.hourly_pay_diff_to_cssr_median: pl.Float32,
    }

    def expected_lf(self, *rows: tuple) -> pl.LazyFrame:
        """Rows of import date, cssr, job role, hourly pay, expected pay difference."""
        return pl.LazyFrame(list(rows), schema=self.schema, orient="row")

    def test_subtracts_median_of_import_date_cssr_and_role_group(self):
        expected_lf = self.expected_lf(
            (self.import_date, "Leeds", "care_worker", 10.0, -2.0),
            (self.import_date, "Leeds", "care_worker", 12.0, 0.0),
            (self.import_date, "Leeds", "care_worker", 20.0, 8.0),
        )
        input_lf = expected_lf.drop(HackCols.hourly_pay_diff_to_cssr_median)

        returned_lf = job.add_hourly_pay_diff_to_cssr_median(input_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_medians_are_separate_per_cssr_role_and_import_date(self):
        expected_lf = self.expected_lf(
            (self.import_date, "Leeds", "care_worker", 10.0, -2.0),
            (self.import_date, "Leeds", "care_worker", 14.0, 2.0),
            (self.import_date, "York", "care_worker", 20.0, -5.0),
            (self.import_date, "York", "care_worker", 30.0, 5.0),
            (self.import_date, "Leeds", "senior_care_worker", 16.0, -1.0),
            (self.import_date, "Leeds", "senior_care_worker", 18.0, 1.0),
            (date(2021, 2, 5), "Leeds", "care_worker", 40.0, -3.0),
            (date(2021, 2, 5), "Leeds", "care_worker", 46.0, 3.0),
        )
        input_lf = expected_lf.drop(HackCols.hourly_pay_diff_to_cssr_median)

        returned_lf = job.add_hourly_pay_diff_to_cssr_median(input_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_returns_zero_diff_for_single_row_group(self):
        expected_lf = self.expected_lf(
            (self.import_date, "Leeds", "care_worker", 10.0, 0.0)
        )
        input_lf = expected_lf.drop(HackCols.hourly_pay_diff_to_cssr_median)

        returned_lf = job.add_hourly_pay_diff_to_cssr_median(input_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_returns_null_diff_when_every_pay_in_group_is_null(self):
        expected_lf = self.expected_lf(
            (self.import_date, "Leeds", "care_worker", None, None),
            (self.import_date, "Leeds", "care_worker", None, None),
        )
        input_lf = expected_lf.drop(HackCols.hourly_pay_diff_to_cssr_median)

        returned_lf = job.add_hourly_pay_diff_to_cssr_median(input_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_median_ignores_null_hourly_pay_and_diff_is_null_for_null_pay(self):
        expected_lf = self.expected_lf(
            (self.import_date, "Leeds", "care_worker", 10.0, -2.0),
            (self.import_date, "Leeds", "care_worker", None, None),
            (self.import_date, "Leeds", "care_worker", 14.0, 2.0),
        )
        input_lf = expected_lf.drop(HackCols.hourly_pay_diff_to_cssr_median)

        returned_lf = job.add_hourly_pay_diff_to_cssr_median(input_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    @pytest.mark.parametrize(
        "cssr, role",
        [
            pytest.param(None, "care_worker", id="null_cssr"),
            pytest.param("Leeds", None, id="null_role"),
        ],
    )
    def test_returns_null_diff_when_cssr_or_role_is_null(self, cssr, role):
        expected_lf = self.expected_lf((self.import_date, cssr, role, 10.0, None))
        input_lf = expected_lf.drop(HackCols.hourly_pay_diff_to_cssr_median)

        returned_lf = job.add_hourly_pay_diff_to_cssr_median(input_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


@patch(f"{job.__name__}.pmUtils.run_postcode_matching")
class TestAddGeographyFromPostcode:
    worker_lf = pl.LazyFrame(
        [("1-001", "1", date(2021, 1, 5))],
        schema=Schemas.raw_worker_join_input_schema,
        orient="row",
    )
    workplace_lf = pl.LazyFrame(
        [
            (
                "1-001",
                date(2021, 1, 5),
                "53",
                "1-ORG",
                "LS1 1AA",
                "1-LOC",
                "Workplace",
                "1 Street",
            )
        ],
        schema={
            AWPClean.establishment_id: CatColType.EstablishmentCatType,
            AWPClean.ascwds_workplace_import_date: pl.Date,
            AWPClean.main_service_id_clean: pl.String,
            AWPClean.organisation_id: pl.String,
            AWPClean.postcode: pl.String,
            AWPClean.location_id: pl.String,
            AWPClean.establishment_name: pl.String,
            AWPClean.address: pl.String,
        },
        orient="row",
    )
    matched_lf = pl.LazyFrame(
        [("1-001", date(2021, 1, 5), "53", "1-ORG", "Leeds", "North")],
        schema={
            AWPClean.establishment_id: CatColType.EstablishmentCatType,
            CQCLClean.cqc_location_import_date: pl.Date,
            AWPClean.main_service_id_clean: pl.String,
            AWPClean.organisation_id: pl.String,
            ONSClean.current_cssr: pl.String,
            ONSClean.current_region: pl.String,
        },
        orient="row",
    )

    def test_returns_workplace_keys_main_service_organisation_and_matched_geography(
        self, run_postcode_matching_mock
    ):
        run_postcode_matching_mock.return_value = self.matched_lf

        returned_df = job.add_geography_from_postcode(
            self.workplace_lf, self.worker_lf, pl.LazyFrame(), "corrections.csv"
        ).collect()

        assert returned_df.columns == [
            AWPClean.establishment_id,
            AWPClean.ascwds_workplace_import_date,
            AWPClean.main_service_id_clean,
            AWPClean.organisation_id,
            ONSClean.current_cssr,
            ONSClean.current_region,
        ]
        assert returned_df.row(0) == (
            "1-001",
            date(2021, 1, 5),
            "53",
            "1-ORG",
            "Leeds",
            "North",
        )

    def test_returns_geography_columns_as_categorical(self, run_postcode_matching_mock):
        run_postcode_matching_mock.return_value = self.matched_lf

        returned_schema = job.add_geography_from_postcode(
            self.workplace_lf, self.worker_lf, pl.LazyFrame(), "corrections.csv"
        ).collect_schema()

        assert returned_schema[ONSClean.current_cssr] == pl.Categorical
        assert returned_schema[ONSClean.current_region] == pl.Categorical

    def test_matches_workplace_columns_renamed_to_cqc_names(
        self, run_postcode_matching_mock
    ):
        run_postcode_matching_mock.return_value = self.matched_lf
        ons_lf = pl.LazyFrame()

        job.add_geography_from_postcode(
            self.workplace_lf, self.worker_lf, ons_lf, "corrections.csv"
        ).collect()

        locations_lf = run_postcode_matching_mock.call_args.args[0]
        assert locations_lf.collect().columns == [
            AWPClean.establishment_id,
            CQCLClean.cqc_location_import_date,
            AWPClean.main_service_id_clean,
            AWPClean.organisation_id,
            CQCLClean.postal_code,
            CQCLClean.location_id,
            CQCLClean.name,
            CQCLClean.postal_address_line1,
        ]
        assert run_postcode_matching_mock.call_args.args[1] is ons_lf
        assert run_postcode_matching_mock.call_args.args[2] == "corrections.csv"

    def test_matches_only_workplaces_in_worker_data(self, run_postcode_matching_mock):
        run_postcode_matching_mock.return_value = self.matched_lf
        other_workplace_lf = self.workplace_lf.with_columns(
            pl.lit("1-002")
            .cast(CatColType.EstablishmentCatType)
            .alias(AWPClean.establishment_id)
        )
        workplace_lf = pl.concat([self.workplace_lf, other_workplace_lf])

        job.add_geography_from_postcode(
            workplace_lf, self.worker_lf, pl.LazyFrame(), "corrections.csv"
        ).collect()

        locations_lf = run_postcode_matching_mock.call_args.args[0]
        assert locations_lf.collect()[AWPClean.establishment_id].to_list() == ["1-001"]


class TestAddYearsInRoleAndSector:
    keys = {
        AWKClean.establishment_id: ["1-001"],
        AWKClean.ascwds_worker_import_date: [date(2021, 1, 5)],
    }
    save_dates_lf = pl.LazyFrame(
        {**keys, HackCols.latest_save_date: [date(2022, 1, 1)]},
        schema_overrides={HackCols.latest_save_date: pl.Date},
    )

    worker_lf = pl.LazyFrame(
        {
            **keys,
            AWKClean.start_date: [date(2020, 1, 1)],
            AWKClean.start_in_sector: [2015.0],
        },
        schema_overrides={
            AWKClean.start_date: pl.Date,
            AWKClean.start_in_sector: pl.Float32,
        },
    )

    def test_returns_years_in_role_as_days_over_365_25(self):
        returned_df = job.add_years_in_role_and_sector(
            self.worker_lf,
            self.save_dates_lf,
        ).collect()

        assert returned_df[HackCols.years_in_role].to_list() == pytest.approx(
            [731 / 365.25]
        )

    def test_returns_years_in_sector_as_save_year_minus_start_year(self):
        returned_df = job.add_years_in_role_and_sector(
            self.worker_lf, self.save_dates_lf
        ).collect()

        assert returned_df[HackCols.years_in_sector].to_list() == [7.0]

    def test_returns_null_years_in_role_when_start_date_is_after_save_date(self):
        worker_lf = self.worker_lf.with_columns(
            pl.lit(date(2022, 1, 2)).alias(AWKClean.start_date)
        )

        returned_df = job.add_years_in_role_and_sector(
            worker_lf, self.save_dates_lf
        ).collect()

        assert returned_df[HackCols.years_in_role].to_list() == [None]
        assert returned_df[HackCols.years_in_sector].to_list() == [7.0]

    def test_returns_null_years_in_sector_when_start_year_is_after_save_year(self):
        worker_lf = self.worker_lf.with_columns(
            pl.lit(2023.0, dtype=pl.Float32).alias(AWKClean.start_in_sector)
        )

        returned_df = job.add_years_in_role_and_sector(
            worker_lf, self.save_dates_lf
        ).collect()

        assert returned_df[HackCols.years_in_sector].to_list() == [None]
        assert returned_df[HackCols.years_in_role].to_list() == pytest.approx(
            [731 / 365.25]
        )

    def test_returns_zero_years_when_start_is_on_save_date_and_year(self):
        worker_lf = self.worker_lf.with_columns(
            pl.lit(date(2022, 1, 1)).alias(AWKClean.start_date),
            pl.lit(2022.0, dtype=pl.Float32).alias(AWKClean.start_in_sector),
        )

        returned_df = job.add_years_in_role_and_sector(
            worker_lf, self.save_dates_lf
        ).collect()

        assert returned_df[HackCols.years_in_role].to_list() == [0.0]
        assert returned_df[HackCols.years_in_sector].to_list() == [0.0]

    def test_returns_float32_year_columns_and_drops_save_date(self):
        returned_schema = job.add_years_in_role_and_sector(
            self.worker_lf, self.save_dates_lf
        ).collect_schema()

        assert returned_schema.names() == [
            *self.keys,
            AWKClean.start_date,
            AWKClean.start_in_sector,
            HackCols.years_in_role,
            HackCols.years_in_sector,
        ]
        assert returned_schema[HackCols.years_in_role] == pl.Float32
        assert returned_schema[HackCols.years_in_sector] == pl.Float32

    def test_returns_null_years_when_start_values_are_null(self):
        worker_lf = self.worker_lf.with_columns(
            pl.lit(None, dtype=pl.Date).alias(AWKClean.start_date),
            pl.lit(None, dtype=pl.Float32).alias(AWKClean.start_in_sector),
        )

        returned_df = job.add_years_in_role_and_sector(
            worker_lf, self.save_dates_lf
        ).collect()

        assert returned_df.select(HackCols.years_in_role, HackCols.years_in_sector).row(
            0
        ) == (None, None)

    def test_returns_null_years_when_establishment_has_no_save_date(self):
        no_save_date_lf = self.save_dates_lf.with_columns(
            pl.lit(None, dtype=pl.Date).alias(HackCols.latest_save_date)
        )

        returned_df = job.add_years_in_role_and_sector(
            self.worker_lf, no_save_date_lf
        ).collect()

        assert returned_df.select(HackCols.years_in_role, HackCols.years_in_sector).row(
            0
        ) == (None, None)


class TestNullNotKnownCodes:
    not_known_codes = {
        AWKClean.gender: NotKnownWorkerCodes.gender,
        AWKClean.ethnicity: NotKnownWorkerCodes.ethnicity,
        AWKClean.disabled: NotKnownWorkerCodes.disabled,
        AWKClean.social_care_qual_held: NotKnownWorkerCodes.social_care_qual_held,
        AWKClean.level_social_care_qualification_held: (
            NotKnownWorkerCodes.level_social_care_qualification_held
        ),
        AWKClean.apprentice: NotKnownWorkerCodes.apprentice,
        AWKClean.source_of_recruitment: NotKnownWorkerCodes.source_of_recruitment,
        AWKClean.nationality: NotKnownWorkerCodes.nationality,
        AWKClean.country_of_birth: NotKnownWorkerCodes.country_of_birth,
    }

    def codes_lf(self, **values: str | float | None) -> pl.LazyFrame:
        schema = {
            **{column: pl.String for column in job.WORKER_CATEGORICAL_COLUMNS},
            **{column: pl.Float32 for column in job.WORKER_NUMERIC_COLUMNS},
        }
        return pl.LazyFrame({c: [values.get(c)] for c in schema}, schema=schema)

    @pytest.mark.parametrize("code", ["-1", "-8"])
    @pytest.mark.parametrize("column", job.WORKER_CATEGORICAL_COLUMNS)
    def test_returns_null_for_negative_code(self, column, code):
        returned_df = job.null_not_known_codes(
            self.codes_lf(**{column: code})
        ).collect()

        assert returned_df[column].to_list() == [None]

    @pytest.mark.parametrize("value", [-1.0, -8.0, -2.5])
    @pytest.mark.parametrize("column", job.WORKER_NUMERIC_COLUMNS)
    def test_returns_null_for_negative_number(self, column, value):
        returned_df = job.null_not_known_codes(
            self.codes_lf(**{column: value})
        ).collect()

        assert returned_df[column].to_list() == [None]

    @pytest.mark.parametrize("column", job.WORKER_CATEGORICAL_COLUMNS)
    def test_keeps_non_negative_code(self, column):
        returned_df = job.null_not_known_codes(self.codes_lf(**{column: "1"})).collect()

        assert returned_df[column].to_list() == ["1"]

    @pytest.mark.parametrize("column", job.WORKER_NUMERIC_COLUMNS)
    def test_keeps_zero_number(self, column):
        returned_df = job.null_not_known_codes(self.codes_lf(**{column: 0.0})).collect()

        assert returned_df[column].to_list() == [0.0]

    @pytest.mark.parametrize("column, code", not_known_codes.items())
    def test_returns_null_for_not_known_code(self, column, code):
        returned_df = job.null_not_known_codes(
            self.codes_lf(**{column: code})
        ).collect()

        assert returned_df[column].to_list() == [None]

    @pytest.mark.parametrize(
        "column, code",
        [
            pytest.param(AWKClean.gender, "4", id="gender_other"),
            pytest.param(AWKClean.ethnicity, "98", id="ethnicity_other"),
            pytest.param(AWKClean.disabled, "0", id="disabled_no"),
            pytest.param(AWKClean.source_of_recruitment, "224", id="other_sources"),
            pytest.param(AWKClean.care_certificate_status, "2", id="not_started"),
            pytest.param(AWKClean.zero_hours, "1", id="zero_hours_yes"),
            pytest.param(AWKClean.training_flag, "0", id="trainflag_no"),
        ],
    )
    def test_keeps_other_codes(self, column, code):
        returned_df = job.null_not_known_codes(
            self.codes_lf(**{column: code})
        ).collect()

        assert returned_df[column].to_list() == [code]

    def test_keeps_nulls(self):
        returned_df = job.null_not_known_codes(self.codes_lf()).collect()

        assert returned_df.row(0) == (None,) * returned_df.width

    def test_keeps_column_types(self):
        input_lf = self.codes_lf()

        returned_lf = job.null_not_known_codes(input_lf)

        assert returned_lf.collect_schema() == input_lf.collect_schema()

    def test_gives_null_label_after_labelling(self):
        nulled_lf = job.null_not_known_codes(self.codes_lf(**{AWKClean.gender: "3"}))

        returned_df = job.add_worker_labels(
            nulled_lf.with_columns(pl.col(AWKClean.gender).cast(pl.Categorical))
        ).collect()

        assert returned_df[f"{AWKClean.gender}_labels"].to_list() == [None]

    @pytest.mark.parametrize(
        "code_column, group_column",
        [
            (AWKClean.nationality, HackCols.nationality_group),
            (AWKClean.country_of_birth, HackCols.country_of_birth_group),
        ],
    )
    def test_gives_null_group_after_grouping(self, code_column, group_column):
        nulled_lf = job.null_not_known_codes(self.codes_lf(**{code_column: "999"}))

        returned_df = job.add_country_code_group(
            nulled_lf.with_columns(pl.col(code_column).cast(pl.Categorical)),
            code_column,
        ).collect()

        assert returned_df[group_column].to_list() == [None]


class TestAddWorkerLabels:
    coded_columns = [
        AWKClean.gender,
        AWKClean.ethnicity,
        AWKClean.disabled,
        AWKClean.social_care_qual_held,
        AWKClean.level_social_care_qualification_held,
        AWKClean.care_certificate_status,
        AWKClean.apprentice,
        AWKClean.zero_hours,
        AWKClean.source_of_recruitment,
        AWKClean.training_flag,
    ]
    label_columns = [f"{column}_labels" for column in coded_columns]
    group_label_columns = [
        HackCols.ethnicity_group_labels,
        HackCols.scerec_wo_group_labels,
    ]

    def codes_lf(self, code: str | None) -> pl.LazyFrame:
        return pl.LazyFrame(
            {column: [code] for column in self.coded_columns},
            schema={column: pl.Categorical for column in self.coded_columns},
        )

    def test_adds_a_label_column_beside_each_coded_column(self):
        returned_lf = job.add_worker_labels(self.codes_lf("1"))

        assert returned_lf.collect_schema().names() == [
            *self.coded_columns,
            *self.label_columns,
            *self.group_label_columns,
        ]

    def test_returns_label_for_mapped_code(self):
        returned_df = job.add_worker_labels(self.codes_lf("1")).collect()

        assert returned_df[f"{AWKClean.gender}_labels"].to_list() == ["Male"]

    def test_returns_null_label_for_null_code(self):
        returned_df = job.add_worker_labels(self.codes_lf(None)).collect()

        assert returned_df.select(self.label_columns).row(0) == (None,) * len(
            self.label_columns
        )

    @pytest.mark.parametrize(
        "column, code, label",
        [
            pytest.param(AWKClean.training_flag, "0", "No", id="trainflag_no"),
            pytest.param(AWKClean.training_flag, "1", "Yes", id="trainflag_yes"),
        ],
    )
    def test_returns_label_for_training_flag_code(self, column, code, label):
        returned_df = job.add_worker_labels(self.codes_lf(code)).collect()

        assert returned_df[f"{column}_labels"].to_list() == [label]

    def add_group_labels(self, expected_df: pl.DataFrame) -> pl.DataFrame:
        """Runs `add_worker_labels` on `expected_df` less its group label columns.

        Other coded columns are added as nulls. Returns the columns of `expected_df`.
        """
        input_df = expected_df.drop(self.group_label_columns, strict=False)
        input_lf = input_df.lazy().with_columns(
            pl.lit(None, dtype=pl.Categorical).alias(column)
            for column in self.coded_columns
            if column not in input_df.columns
        )

        return job.add_worker_labels(input_lf).select(expected_df.columns).collect()

    def test_recodes_ethnicity_codes_to_group_labels(self):
        white = EthnicityGroupLabels.white
        mixed = EthnicityGroupLabels.mixed
        asian = EthnicityGroupLabels.asian
        black = EthnicityGroupLabels.black
        other = EthnicityGroupLabels.other
        expected_df = pl.DataFrame(
            {
                AWKClean.ethnicity: [
                    EthnicityID.english_welsh_scottish_northern_irish_british,
                    EthnicityID.irish,
                    EthnicityID.gypsy_or_irish_traveller,
                    EthnicityID.other_white,
                    EthnicityID.white_and_black_caribbean,
                    EthnicityID.white_and_black_african,
                    EthnicityID.white_and_asian,
                    EthnicityID.other_mixed,
                    EthnicityID.indian,
                    EthnicityID.pakistani,
                    EthnicityID.bangladeshi,
                    EthnicityID.chinese,
                    EthnicityID.other_asian,
                    EthnicityID.african,
                    EthnicityID.caribbean,
                    EthnicityID.other_black_african_caribbean,
                    EthnicityID.arab,
                    EthnicityID.other_ethnic_group,
                    NotKnownWorkerCodes.ethnicity,
                    None,
                ],
                HackCols.ethnicity_group_labels: [
                    *[white] * 4,
                    *[mixed] * 4,
                    *[asian] * 5,
                    *[black] * 3,
                    *[other] * 2,
                    None,
                    None,
                ],
            },
            schema={
                AWKClean.ethnicity: pl.Categorical,
                HackCols.ethnicity_group_labels: pl.Categorical,
            },
        )

        returned_df = self.add_group_labels(expected_df)

        pl_testing.assert_frame_equal(returned_df, expected_df, categorical_as_str=True)

    def test_recodes_source_of_recruitment_codes_to_within_outside_asc_labels(self):
        within_outside = SourceOfRecruitmentWithinOutsideAscLabels
        expected_df = pl.DataFrame(
            {
                AWKClean.source_of_recruitment: [
                    SourceOfRecruitmentID.adult_care_local_authority,
                    SourceOfRecruitmentID.adult_care_private_or_voluntary,
                    SourceOfRecruitmentID.internal_promotion,
                    SourceOfRecruitmentID.agency,
                    SourceOfRecruitmentID.health_sector,
                    SourceOfRecruitmentID.other_sector,
                    SourceOfRecruitmentID.not_previously_employed,
                    SourceOfRecruitmentID.other_sources,
                    SourceOfRecruitmentID.childrens_social_care,
                    SourceOfRecruitmentID.first_role_after_education,
                    NotKnownWorkerCodes.source_of_recruitment,
                    None,
                ],
                HackCols.scerec_wo_group_labels: [
                    *[within_outside.within_asc] * 4,
                    *[within_outside.outside_asc] * 6,
                    None,
                    None,
                ],
            },
            schema={
                AWKClean.source_of_recruitment: pl.Categorical,
                HackCols.scerec_wo_group_labels: pl.Categorical,
            },
        )

        returned_df = self.add_group_labels(expected_df)

        pl_testing.assert_frame_equal(returned_df, expected_df, categorical_as_str=True)

    def test_keeps_raw_code_as_label_for_unmapped_code(self):
        returned_df = job.add_worker_labels(self.codes_lf("999")).collect()

        assert returned_df[f"{AWKClean.gender}_labels"].to_list() == ["999"]

    def test_label_columns_are_categorical(self):
        returned_schema = job.add_worker_labels(self.codes_lf("1")).collect_schema()

        assert all(returned_schema[c] == pl.Categorical for c in self.label_columns)


class TestImputeWorkerColumns:
    @pytest.mark.parametrize(
        "case",
        [pytest.param(case, id=case.id) for case in Data.impute_worker_columns_cases],
    )
    def test_returns_expected_rows(self, case):
        input_lf = pl.LazyFrame(
            case.input_rows, schema=Schemas.impute_schema, orient="row"
        )
        expected_lf = pl.LazyFrame(
            case.expected_rows, schema=Schemas.impute_schema, orient="row"
        )

        returned_lf = job.impute_worker_columns(input_lf, [HackCols.hours])

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_imputes_expected_columns(self):
        assert job.IMPUTE_COLUMNS == Schemas.impute_columns


class TestSelectAndRenameOutputColumns:
    # Columns the job drops: `age` and `hourly_pay_minus_nlw` also share a name with
    # an output column that comes from a different source column.
    dropped_columns = [
        AWKClean.age,
        AWKClean.days_sick,
        AWKClean.start_date,
        AWKClean.pay_change_date,
        HackCols.age_clean,
        HackCols.age_source,
        HackCols.hourly_pay_minus_nlw,
        HackCols.hourly_pay_minus_nlw_clean,
        HackCols.hourly_pay,
        HackCols.nlw_hourly_rate,
        HackCols.split,
        HackCols.valid_for_analysis,
        AWKClean.ascwds_worker_save_date,
        AWKClean.created_date,
        AWKClean.employment_status_clean_labelled,
        AWKClean.year_of_entry,
        f"{AWKClean.level_social_care_qualification_held}_{DLC.label}s",
        f"{AWKClean.apprentice}_{DLC.label}s",
        f"{AWKClean.source_of_recruitment}_{DLC.label}s",
    ]

    @classmethod
    def build_lf(cls) -> pl.LazyFrame:
        """One row where each column holds its own name, in reverse output order."""
        columns = [*cls.dropped_columns, *reversed(job.OUTPUT_COLUMN_SOURCES.values())]
        return pl.LazyFrame({column: [column] for column in columns})

    def test_returns_output_columns_in_output_order(self):
        returned_lf = job.select_and_rename_output_columns(self.build_lf())

        expected_columns = [field.name for field in fields(OutCols)]
        assert returned_lf.collect_schema().names() == expected_columns

    def test_renames_each_source_column_to_its_output_name(self):
        returned_df = job.select_and_rename_output_columns(self.build_lf()).collect()

        for output, source in job.OUTPUT_COLUMN_SOURCES.items():
            assert returned_df[output].to_list() == [source]

    def test_age_comes_from_imputed_age_not_raw_age(self):
        returned_df = job.select_and_rename_output_columns(self.build_lf()).collect()

        assert returned_df[OutCols.age].to_list() == [HackCols.age_imputed]

    def test_hourly_pay_minus_nlw_comes_from_clean_imputed_column(self):
        returned_df = job.select_and_rename_output_columns(self.build_lf()).collect()

        assert returned_df[OutCols.hourly_pay_minus_nlw].to_list() == [
            HackCols.hourly_pay_minus_nlw_clean_imputed
        ]

    def test_drops_columns_not_in_output(self):
        returned_lf = job.select_and_rename_output_columns(self.build_lf())

        assert not set(self.dropped_columns) - {
            OutCols.age,
            OutCols.hourly_pay_minus_nlw,
        } & set(returned_lf.collect_schema().names())
