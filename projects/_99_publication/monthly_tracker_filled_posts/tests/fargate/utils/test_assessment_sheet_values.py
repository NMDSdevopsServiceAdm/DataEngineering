import datetime

import polars as pl
import pytest

from projects._99_publication.monthly_tracker_filled_posts.fargate.utils import (
    assessment_cell_map as cell_map,
)
from projects._99_publication.monthly_tracker_filled_posts.fargate.utils import (
    assessment_sheet_values as job,
)
from projects._99_publication.monthly_tracker_filled_posts.fargate.utils.assessment_sheet_text import (
    TermFromDates,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.publication_columns import PublicationColumns as Pub
from utils.column_values.categorical_column_values import (
    PublishedJobGroupLabels,
    PublishedRegion,
)


VALUE_COLUMNS = job.value_columns(cell_map.ASSESSMENT_SHEETS)
EXPLICIT_VALUE_COLUMNS = [
    Pub.unfiltered_filled_posts,
    Pub.unfiltered_locationid_count,
    Pub.unfiltered_filled_posts_april_pct_change,
    Pub.unfiltered_locationid_count_april_pct_change,
    Pub.unfiltered_short_sfc_cum_pct_change,
    Pub.unfiltered_short_locationid_count_cum_pct_change,
    Pub.unfiltered_filled_posts_prev_run,
    Pub.unfiltered_filled_posts_pct_change_vs_prev_run,
]
OTHER_VALUE_COLUMNS = [c for c in VALUE_COLUMNS if c not in EXPLICIT_VALUE_COLUMNS]


def _generic_value(column: str, date_index: int, service_index: int) -> int:
    """A value that differs by column, import date and service type."""
    return (
        (VALUE_COLUMNS.index(column) + 1) * 10_000 + 100 * date_index + (service_index)
    )


def _assessment_lf(
    import_dates: list[datetime.date],
    job_role: str = PublishedJobGroupLabels.all_job_roles,
    region: str = PublishedRegion.england,
) -> pl.LazyFrame:
    """One row per import date and service type; values rise with the date."""
    rows = [
        {
            IndCQC.cqc_location_import_date: import_date,
            Pub.period_label_short: (import_date - datetime.timedelta(days=1)).strftime(
                "%b %Y"
            ),
            IndCQC.main_job_role_clean_labelled: job_role,
            IndCQC.current_region: region,
            IndCQC.primary_service_type: service_type,
            Pub.unfiltered_filled_posts: 1000 * (i + 1) + 10 * j,
            Pub.unfiltered_locationid_count: 100 * (i + 1) + j,
            Pub.unfiltered_filled_posts_april_pct_change: 0.1 * (i + 1),
            Pub.unfiltered_locationid_count_april_pct_change: 0.2 * (i + 1),
            Pub.unfiltered_short_sfc_cum_pct_change: 0.3 * (i + 1),
            Pub.unfiltered_short_locationid_count_cum_pct_change: 0.4 * (i + 1),
            Pub.unfiltered_filled_posts_prev_run: 5000 * (i + 1) + 10 * j,
            Pub.unfiltered_filled_posts_pct_change_vs_prev_run: 0.5 * (i + 1),
        }
        | {column: _generic_value(column, i, j) for column in OTHER_VALUE_COLUMNS}
        for i, import_date in enumerate(import_dates)
        for j, service_type in enumerate(job.SERVICE_TYPE_ORDER)
    ]
    return pl.LazyFrame(rows)


APRIL_2024 = datetime.date(2024, 4, 1)
APRIL_2025 = datetime.date(2025, 4, 1)
APRIL_2026 = datetime.date(2026, 4, 1)
IMPORT_DATES = [APRIL_2024, APRIL_2025, APRIL_2026, datetime.date(2026, 5, 1)]
TERM_FROM_DATES = TermFromDates(short=APRIL_2026, medium=APRIL_2025, long=APRIL_2024)


class TestPrepareAssessmentDf:
    def test_keeps_only_all_job_roles_in_england(self):
        lf = pl.concat(
            [
                _assessment_lf(IMPORT_DATES[:1]),
                _assessment_lf(IMPORT_DATES[:1], job_role="Care worker"),
                _assessment_lf(IMPORT_DATES[:1], region="North East"),
            ]
        )

        df = job.prepare_assessment_df(lf, VALUE_COLUMNS)

        assert df.height == 1

    def test_keeps_every_import_date(self):
        import_dates = [
            datetime.date(2024, 4, 1),
            datetime.date(2024, 5, 1),
            datetime.date(2025, 4, 1),
            datetime.date(2025, 10, 1),
            datetime.date(2026, 4, 1),
            datetime.date(2026, 5, 1),
        ]

        df = job.prepare_assessment_df(_assessment_lf(import_dates), VALUE_COLUMNS)

        assert df[IndCQC.cqc_location_import_date].to_list() == import_dates

    def test_sorts_by_import_date(self):
        lf = _assessment_lf(IMPORT_DATES[:3]).sort(
            IndCQC.cqc_location_import_date, descending=True
        )

        df = job.prepare_assessment_df(lf, VALUE_COLUMNS)

        assert df[IndCQC.cqc_location_import_date].to_list() == IMPORT_DATES[:3]

    def test_spreads_each_value_column_across_service_types(self):
        df = job.prepare_assessment_df(_assessment_lf(IMPORT_DATES[:1]), VALUE_COLUMNS)

        assert df.select(
            [
                f"{Pub.unfiltered_filled_posts}|{service}"
                for service in job.SERVICE_TYPE_ORDER
            ]
        ).rows() == [(1000, 1010, 1020, 1030, 1040)]


class TestBlockCells:
    def test_writes_rows_down_and_across_from_the_top_left_cell(self):
        cells = job.block_cells("C24", [[1, 2], [3, 4]])

        assert cells == {"C24": 1, "D24": 2, "C25": 3, "D25": 4}


class TestFilledPostsValues:
    @pytest.fixture(scope="class")
    def cells(self) -> dict[str, object]:
        return job.filled_posts_values(
            job.prepare_assessment_df(_assessment_lf(IMPORT_DATES), VALUE_COLUMNS)
        )

    def test_annual_table_has_a_row_per_april_import_date_as_period_month(self, cells):
        assert [cells[f"B{row}"] for row in (24, 25, 26)] == [
            datetime.date(2024, 3, 1),
            datetime.date(2025, 3, 1),
            datetime.date(2026, 3, 1),
        ]
        assert "B27" not in cells

    def test_annual_table_spreads_service_types_across_columns(self, cells):
        assert [cells[f"{col}24"] for col in "CDEFG"] == [1000, 1010, 1020, 1030, 1040]

    def test_annual_percent_change_starts_one_row_below_the_first_date(self, cells):
        assert "J24" not in cells
        assert cells["J25"] == pytest.approx(0.2)

    def test_monthly_table_starts_at_the_latest_april(self, cells):
        assert [cells[f"B{row}"] for row in (65, 66)] == [
            datetime.date(2026, 3, 1),
            datetime.date(2026, 4, 1),
        ]
        assert "B67" not in cells

    def test_monthly_percent_change_starts_on_the_first_row(self, cells):
        assert cells["J65"] == pytest.approx(0.9)

    def test_percent_cells_are_rounded_to_one_decimal_place_of_a_percentage(self):
        lf = _assessment_lf(IMPORT_DATES).with_columns(
            pl.col(Pub.unfiltered_filled_posts_april_pct_change) + 0.0021
        )
        df = job.prepare_assessment_df(lf, VALUE_COLUMNS)

        cells = job.filled_posts_values(df)

        assert cells["J25"] == 0.202

    def test_count_cells_are_rounded_to_whole_numbers(self):
        lf = _assessment_lf(IMPORT_DATES).with_columns(
            pl.col(Pub.unfiltered_filled_posts) + 0.6
        )
        df = job.prepare_assessment_df(lf, VALUE_COLUMNS)

        cells = job.filled_posts_values(df)

        assert cells["C24"] == 1001

    def test_raises_when_periods_exceed_the_table_rows(self):
        april_dates = [datetime.date(2000 + year, 4, 1) for year in range(14)]
        df = job.prepare_assessment_df(_assessment_lf(april_dates), VALUE_COLUMNS)

        with pytest.raises(ValueError, match="exceed the 13 rows"):
            job.filled_posts_values(df)


class TestCompPrevPublicationValues:
    @pytest.fixture(scope="class")
    def cells(self) -> dict[str, object]:
        return job.comp_prev_publication_values(
            job.prepare_assessment_df(_assessment_lf(IMPORT_DATES), VALUE_COLUMNS)
        )

    def test_blocks_list_annual_rows_then_monthly_rows_as_period_months(self, cells):
        expected_dates = [
            datetime.date(2024, 3, 1),
            datetime.date(2025, 3, 1),
            datetime.date(2026, 3, 1),
            datetime.date(2026, 4, 1),
        ]
        for first_row in (23, 64, 106):
            assert [cells[f"C{first_row + i}"] for i in range(4)] == expected_dates
            assert f"C{first_row + 4}" not in cells

    def test_previous_publication_holds_the_previous_run_filled_posts(self, cells):
        assert [cells[f"{col}23"] for col in "DEFGH"] == [5000, 5010, 5020, 5030, 5040]

    def test_current_publication_holds_this_runs_filled_posts(self, cells):
        assert [cells[f"{col}64"] for col in "DEFGH"] == [1000, 1010, 1020, 1030, 1040]

    def test_difference_holds_the_percentage_change_from_the_previous_run(self, cells):
        assert [cells[f"{col}106"] for col in "DEFGH"] == [0.5] * 5

    def test_raises_when_periods_exceed_the_block_rows(self):
        april_dates = [datetime.date(1990 + year, 4, 1) for year in range(38)]
        df = job.prepare_assessment_df(_assessment_lf(april_dates), VALUE_COLUMNS)

        with pytest.raises(ValueError, match="exceed the 37 rows"):
            job.comp_prev_publication_values(df)


class TestComparisonSheetValues:
    CARE_HOMES = 1
    NON_RESIDENTIAL = 4

    @pytest.fixture(scope="class")
    def df(self) -> pl.DataFrame:
        return job.prepare_assessment_df(_assessment_lf(IMPORT_DATES), VALUE_COLUMNS)

    def test_rows_start_at_the_term_start_date_as_period_months(self, df):
        cells = job.comparison_sheet_values(df, cell_map.COMP_SHORT_TERM, APRIL_2026)

        assert [cells[f"{col}{row}"] for col in "BK" for row in (29, 30)] == [
            datetime.date(2026, 3, 1),
            datetime.date(2026, 4, 1),
        ] * 2
        assert "B31" not in cells

    def test_an_earlier_term_start_includes_more_rows(self, df):
        cells = job.comparison_sheet_values(df, cell_map.COMP_MEDIUM_TERM, APRIL_2025)

        assert [cells[f"B{row}"] for row in (29, 30, 31)] == [
            datetime.date(2025, 3, 1),
            datetime.date(2026, 3, 1),
            datetime.date(2026, 4, 1),
        ]

    def test_care_homes_block_reads_the_all_care_homes_rows(self, df):
        cells = job.comparison_sheet_values(df, cell_map.COMP_SHORT_TERM, APRIL_2026)

        assert [cells[f"{col}29"] for col in "CDEF"] == [
            _generic_value(column, 2, self.CARE_HOMES)
            for column in (
                Pub.filtered_short_sfc_filled_posts,
                Pub.filtered_short_ct_total_employed,
                Pub.filtered_short_sfc_cum_pct_change,
                Pub.filtered_short_ct_cum_pct_change,
            )
        ]

    def test_non_residential_block_reads_the_non_residential_rows(self, df):
        cells = job.comparison_sheet_values(df, cell_map.COMP_SHORT_TERM, APRIL_2026)

        assert [cells[f"{col}29"] for col in "LMNO"] == [
            _generic_value(column, 2, self.NON_RESIDENTIAL)
            for column in (
                Pub.filtered_short_sfc_filled_posts,
                Pub.filtered_short_ct_total_employed,
                Pub.filtered_short_sfc_cum_pct_change,
                Pub.filtered_short_ct_cum_pct_change,
            )
        ]

    def test_number_of_locations_cells_take_the_latest_row(self, df):
        cells = job.comparison_sheet_values(df, cell_map.COMP_SHORT_TERM, APRIL_2026)

        assert (cells["D25"], cells["M25"]) == (
            _generic_value(Pub.filtered_short_locationid_count, 3, self.CARE_HOMES),
            _generic_value(
                Pub.filtered_short_locationid_count, 3, self.NON_RESIDENTIAL
            ),
        )

    def test_no_filters_sheet_has_number_of_locations_for_each_period(self, df):
        cells = job.comparison_sheet_values(df, cell_map.NO_FILTERS, APRIL_2026)

        assert (cells["G29"], cells["P29"]) == (301, 304)
        assert "D25" not in cells

    def test_both_long_term_sheets_keep_only_quarterly_periods_from_the_term_start(
        self,
    ):
        # Period months March, April, June, July, September, October, December.
        import_dates = [
            datetime.date(2024, 4, 1),
            datetime.date(2024, 5, 1),
            datetime.date(2024, 7, 1),
            datetime.date(2024, 8, 1),
            datetime.date(2024, 10, 1),
            datetime.date(2024, 11, 1),
            datetime.date(2025, 1, 1),
        ]
        df = job.prepare_assessment_df(_assessment_lf(import_dates), VALUE_COLUMNS)

        for sheet in (cell_map.COMP_LONG_TERM, cell_map.NO_FILTERS):
            cells = job.SHEET_VALUE_BUILDERS[sheet.name](df, TERM_FROM_DATES)

            assert [cells[f"B{row}"] for row in (29, 30, 31, 32)] == [
                datetime.date(2024, 3, 1),
                datetime.date(2024, 6, 1),
                datetime.date(2024, 9, 1),
                datetime.date(2024, 12, 1),
            ]
            assert "B33" not in cells

    def test_long_term_periods_before_the_term_start_are_not_kept(self):
        import_dates = [datetime.date(2024, 1, 1), datetime.date(2024, 4, 1)]
        df = job.prepare_assessment_df(_assessment_lf(import_dates), VALUE_COLUMNS)

        cells = job.SHEET_VALUE_BUILDERS[cell_map.COMP_LONG_TERM.name](
            df, TERM_FROM_DATES
        )

        assert cells["B29"] == datetime.date(2024, 3, 1)
        assert "B30" not in cells

    def test_medium_term_keeps_every_month_from_its_start(self):
        import_dates = [
            datetime.date(2024, 4, 1),
            datetime.date(2025, 4, 1),
            datetime.date(2025, 10, 1),
            datetime.date(2026, 4, 1),
            datetime.date(2026, 5, 1),
        ]
        df = job.prepare_assessment_df(_assessment_lf(import_dates), VALUE_COLUMNS)

        cells = job.comparison_sheet_values(df, cell_map.COMP_MEDIUM_TERM, APRIL_2025)

        assert [cells[f"B{row}"] for row in (29, 30, 31, 32)] == [
            datetime.date(2025, 3, 1),
            datetime.date(2025, 9, 1),
            datetime.date(2026, 3, 1),
            datetime.date(2026, 4, 1),
        ]
        assert "B33" not in cells

    def test_raises_when_the_sheet_has_no_input_ranges_for_a_block(self, df):
        sheet = cell_map.SheetMap("Empty sheet", (), 0)

        with pytest.raises(ValueError, match="No 'care_homes' input ranges"):
            job.comparison_sheet_values(df, sheet, APRIL_2026)

    def test_raises_when_periods_exceed_the_sheet_rows(self):
        april_dates = [datetime.date(2000 + year, 4, 1) for year in range(26)]
        df = job.prepare_assessment_df(_assessment_lf(april_dates), VALUE_COLUMNS)

        with pytest.raises(ValueError, match="exceed the 25 rows"):
            job.comparison_sheet_values(
                df, cell_map.COMP_MEDIUM_TERM, datetime.date(2000, 4, 1)
            )
