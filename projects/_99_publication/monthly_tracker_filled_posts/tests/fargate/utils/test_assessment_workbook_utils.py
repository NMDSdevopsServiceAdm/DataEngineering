import datetime
import zipfile
from pathlib import Path

import openpyxl
import polars as pl
import pytest
from openpyxl.styles import Font

from projects._99_publication.monthly_tracker_filled_posts.fargate.utils import (
    assessment_cell_map as cell_map,
)
from projects._99_publication.monthly_tracker_filled_posts.fargate.utils import (
    assessment_workbook_utils as job,
)
from projects._99_publication.monthly_tracker_filled_posts.tests.templates import (
    template_support as support,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.publication_columns import PublicationColumns as Pub
from utils.column_values.categorical_column_values import (
    PublishedJobGroupLabels,
    PublishedRegion,
)


def _sheet_with_arial_columns_b_to_d() -> openpyxl.worksheet.worksheet.Worksheet:
    worksheet = openpyxl.Workbook().active
    dimension = worksheet.column_dimensions["B"]
    dimension.min, dimension.max = 2, 4
    dimension.font = Font(name="Arial", sz=12)
    dimension.number_format = "#,##0"
    return worksheet


class TestCellExists:
    def test_is_false_until_the_cell_is_created(self):
        worksheet = openpyxl.Workbook().active

        before = job.cell_exists(worksheet, 2, 2)
        worksheet.cell(2, 2)

        assert (before, job.cell_exists(worksheet, 2, 2)) == (False, True)


class TestColumnDimension:
    def test_returns_the_dimension_covering_a_grouped_column(self):
        worksheet = _sheet_with_arial_columns_b_to_d()

        assert job.column_dimension(worksheet, 3) is worksheet.column_dimensions["B"]

    def test_returns_none_for_a_column_without_a_dimension(self):
        worksheet = _sheet_with_arial_columns_b_to_d()

        assert job.column_dimension(worksheet, 9) is None


class TestWriteInputCell:
    def test_gives_a_new_cell_its_column_style(self):
        worksheet = _sheet_with_arial_columns_b_to_d()

        job.write_input_cell(worksheet, 3, 3, "text")

        cell = worksheet.cell(3, 3)
        assert (cell.value, cell.font.name, cell.font.sz) == ("text", "Arial", 12)

    def test_gives_a_new_cell_its_column_number_format(self):
        worksheet = _sheet_with_arial_columns_b_to_d()

        job.write_input_cell(worksheet, 3, 3, 1234)

        assert worksheet.cell(3, 3).number_format == "#,##0"

    def test_leaves_a_new_cell_in_a_column_without_a_dimension_unstyled(self):
        worksheet = _sheet_with_arial_columns_b_to_d()

        job.write_input_cell(worksheet, 3, 9, "text")

        cell = worksheet.cell(3, 9)
        assert (cell.value, cell.font.name) == ("text", worksheet.cell(4, 9).font.name)

    def test_keeps_the_style_of_an_existing_cell(self):
        worksheet = _sheet_with_arial_columns_b_to_d()
        worksheet.cell(3, 3).font = Font(name="Calibri", sz=9)

        job.write_input_cell(worksheet, 3, 3, "text")

        assert worksheet.cell(3, 3).font.name == "Calibri"


PERIOD_LABELS = ["Mar 2026"]


@pytest.fixture(scope="module")
def values() -> dict[str, dict[str, object]]:
    return support.generic_values(full=True)


@pytest.fixture(scope="module")
def filled_path(tmp_path_factory, values) -> Path:
    path = tmp_path_factory.mktemp("filled") / "assessment.xlsx"
    job.fill_assessment_workbook(
        PERIOD_LABELS, TERM_FROM_DATES, values, cell_map.TEMPLATE_PATH, path
    )
    return path


def _copy_template_with(tmp_path: Path, sheet: str, cell: str, value: object) -> Path:
    workbook = openpyxl.load_workbook(cell_map.TEMPLATE_PATH)
    workbook[sheet][cell] = value
    path = tmp_path / "changed_template.xlsx"
    workbook.save(path)
    return path


class TestFillAssessmentWorkbook:
    def test_values_land_in_the_mapped_cells(self, filled_path, values):
        saved = openpyxl.load_workbook(filled_path)

        wrong = [
            (sheet, cell)
            for sheet, cells in values.items()
            for cell, value in cells.items()
            if cell not in job.sheet_text(PERIOD_LABELS, TERM_FROM_DATES)[sheet]
            and saved[sheet][cell].value != value
        ]

        assert wrong == []

    def test_every_sheet_title_names_the_latest_period(self, tmp_path, values):
        output = tmp_path / "out.xlsx"

        job.fill_assessment_workbook(
            ["Sep 2025", "Mar 2026"],
            TERM_FROM_DATES,
            values,
            cell_map.TEMPLATE_PATH,
            output,
        )

        saved = openpyxl.load_workbook(output)
        titles = {
            sheet.name: saved[sheet.name][cell_map.TITLE_CELL].value
            for sheet in cell_map.ASSESSMENT_SHEETS
        }
        assert set(titles.values()) == {"End of March 2026"}

    def test_cells_outside_the_inputs_are_unchanged(self, filled_path):
        template = openpyxl.load_workbook(cell_map.TEMPLATE_PATH)
        saved = openpyxl.load_workbook(filled_path)

        changed = [
            (sheet.name, cell.coordinate)
            for sheet in cell_map.ASSESSMENT_SHEETS
            for row in template[sheet.name].iter_rows()
            for cell in row
            if (cell.row, cell.column)
            not in support.cells_in([r.cells for r in sheet.input_ranges])
            and saved[sheet.name][cell.coordinate].value != cell.value
        ]

        assert changed == []

    def test_saved_workbook_holds_no_formulas(self, filled_path):
        saved = openpyxl.load_workbook(filled_path)

        formulas = [
            (ws.title, cell.coordinate)
            for ws in saved
            for row in ws.iter_rows()
            for cell in row
            if cell.data_type == "f"
        ]

        assert formulas == []

    def test_charts_keep_count_and_series_references(self, filled_path):
        with (
            zipfile.ZipFile(cell_map.TEMPLATE_PATH) as before,
            zipfile.ZipFile(filled_path) as after,
        ):
            changed = [
                sheet.name
                for sheet in cell_map.ASSESSMENT_SHEETS
                if len(support.charts(after, sheet.name)) != sheet.chart_count
                or support.chart_signature(after, sheet)
                != support.chart_signature(before, sheet)
            ]

        assert changed == []

    def test_raises_when_a_value_targets_a_cell_outside_the_inputs(self, tmp_path):
        with pytest.raises(job.UnmappedCellError, match="'Filled posts'!B9"):
            job.fill_assessment_workbook(
                PERIOD_LABELS,
                TERM_FROM_DATES,
                {"Filled posts": {"B9": "overwritten"}},
                cell_map.TEMPLATE_PATH,
                tmp_path / "out.xlsx",
            )

    def test_raises_when_a_value_targets_an_unknown_sheet(self, tmp_path):
        with pytest.raises(job.UnmappedCellError, match="'Missing'!B3"):
            job.fill_assessment_workbook(
                PERIOD_LABELS,
                TERM_FROM_DATES,
                {"Missing": {"B3": "x"}},
                cell_map.TEMPLATE_PATH,
                tmp_path / "out.xlsx",
            )

    def test_raises_when_a_label_differs(self, tmp_path):
        template = _copy_template_with(tmp_path, "Filled posts", "C23", "Renamed")

        with pytest.raises(job.TemplateMismatchError, match="'Filled posts'!C23"):
            job.fill_assessment_workbook(
                PERIOD_LABELS, TERM_FROM_DATES, {}, template, tmp_path / "out.xlsx"
            )

    def test_saves_nothing_when_error_raised(self, tmp_path):
        template = _copy_template_with(tmp_path, "Filled posts", "C23", "Renamed")
        output = tmp_path / "out.xlsx"

        with pytest.raises(job.TemplateMismatchError):
            job.fill_assessment_workbook(
                PERIOD_LABELS, TERM_FROM_DATES, {}, template, output
            )

        assert not output.exists()

    def test_raises_when_a_mapped_sheet_is_missing(self, tmp_path):
        workbook = openpyxl.load_workbook(cell_map.TEMPLATE_PATH)
        del workbook["No filters - long term"]
        template = tmp_path / "missing_sheet.xlsx"
        workbook.save(template)

        with pytest.raises(job.TemplateMismatchError, match="No filters - long term"):
            job.fill_assessment_workbook(
                PERIOD_LABELS, TERM_FROM_DATES, {}, template, tmp_path / "out.xlsx"
            )


class TestTitleText:
    def test_uses_the_latest_period_by_date_not_by_text(self):
        assert job.title_text(["Sep 2025", "Mar 2026"]) == "End of March 2026"

    def test_raises_when_there_are_no_period_labels(self):
        with pytest.raises(ValueError, match="period_labels is empty"):
            job.title_text([])


class TestCompPrevRows:
    def test_lists_march_periods_then_periods_after_the_latest_march(self):
        months = [
            datetime.date(2026, 5, 1),
            datetime.date(2025, 9, 1),
            datetime.date(2025, 3, 1),
            datetime.date(2026, 3, 1),
            datetime.date(2026, 4, 1),
            datetime.date(2026, 5, 1),
        ]

        rows = job.comp_prev_rows(months)

        assert rows == [
            (datetime.date(2025, 3, 1), "Annual"),
            (datetime.date(2026, 3, 1), "Annual"),
            (datetime.date(2026, 4, 1), "Monthly"),
            (datetime.date(2026, 5, 1), "Monthly"),
        ]

    def test_raises_when_no_period_is_a_march(self):
        with pytest.raises(ValueError, match="no March"):
            job.comp_prev_rows([datetime.date(2026, 4, 1)])


class TestSheetText:
    PERIOD_LABELS = ["Mar 2025", "Sep 2025", "Mar 2026", "Sep 2026"]

    def test_short_term_heading_and_note_use_the_latest_march(self):
        text = job.sheet_text(self.PERIOD_LABELS, TERM_FROM_DATES)[
            cell_map.COMP_SHORT_TERM.name
        ]

        assert text[cell_map.HEADING_CELL] == (
            "Comparison of Skills for Care's estimate of filled posts to Capacity"
            " Tracker - Since March 2026"
        )
        assert text[cell_map.CRITERIA_NOTE_CELL] == (
            "- Must have a Capacity Tracker figure in all months from end of"
            " March 2026 onwards."
        )

    def test_headings_follow_the_term_start_dates_not_the_period_labels(self):
        term_from_dates = job.TermFromDates(
            short=datetime.date(2027, 4, 1),
            medium=datetime.date(2026, 4, 1),
            long=APRIL_2024,
        )

        text = job.sheet_text(self.PERIOD_LABELS, term_from_dates)

        assert text[cell_map.COMP_SHORT_TERM.name][cell_map.HEADING_CELL].endswith(
            "Since March 2027"
        )
        assert text[cell_map.COMP_MEDIUM_TERM.name][cell_map.HEADING_CELL].endswith(
            "Since March 2026"
        )

    def test_medium_term_heading_and_note_use_the_march_before(self):
        text = job.sheet_text(self.PERIOD_LABELS, TERM_FROM_DATES)[
            cell_map.COMP_MEDIUM_TERM.name
        ]

        assert text[cell_map.HEADING_CELL].endswith("Since March 2025")
        assert text[cell_map.CRITERIA_NOTE_CELL].endswith("end of March 2025 onwards.")

    def test_filled_posts_chart_heading_uses_the_latest_march(self):
        text = job.sheet_text(self.PERIOD_LABELS, TERM_FROM_DATES)[
            cell_map.FILLED_POSTS.name
        ]

        assert text[cell_map.FILLED_POSTS_CHART_HEADING_CELL] == (
            "Comparison of percentage change in filled posts and CQC locations"
            " since March 2026"
        )

    def test_only_short_and_medium_term_sheets_get_a_heading_and_note(self):
        text = job.sheet_text(self.PERIOD_LABELS, TERM_FROM_DATES)

        with_note = {
            name for name, cells in text.items() if cell_map.CRITERIA_NOTE_CELL in cells
        }
        assert with_note == {
            cell_map.COMP_SHORT_TERM.name,
            cell_map.COMP_MEDIUM_TERM.name,
        }

    def test_raises_when_no_period_label_is_a_march(self):
        with pytest.raises(ValueError, match="no March"):
            job.sheet_text(["Sep 2025", "Oct 2025"], TERM_FROM_DATES)

    def test_comp_prev_publication_labels_are_annual_rows_then_monthly_rows(self):
        # Sep 2025 is between the Marches, so it is not a row; May 2026 is repeated.
        period_labels = [
            "Mar 2025",
            "Sep 2025",
            "Mar 2026",
            "Apr 2026",
            "May 2026",
            "Jun 2026",
            "May 2026",
        ]

        text = job.sheet_text(period_labels, TERM_FROM_DATES)[
            cell_map.COMP_PREV_PUBLICATION.name
        ]

        labels = {cell: value for cell, value in text.items() if cell != "B3"}
        expected_rows = ["Annual", "Annual", "Monthly", "Monthly", "Monthly"]
        assert labels == {
            f"B{first_row + i}": label
            for first_row in (23, 64, 106)
            for i, label in enumerate(expected_rows)
        }

    def test_raises_when_periods_exceed_the_label_slots(self):
        # 21 March periods and 17 months after the latest March.
        labels = [f"Mar {year}" for year in range(2000, 2021)]
        labels += [f"{month} 2020" for month in ("Apr", "May", "Jun", "Jul", "Aug")]
        labels += [f"{month} 2020" for month in ("Sep", "Oct", "Nov", "Dec")]
        labels += [f"{month} 2021" for month in ("Jan", "Feb", "Apr", "May")]
        labels += [f"{month} 2021" for month in ("Jun", "Jul", "Aug", "Sep")]

        with pytest.raises(ValueError, match="exceed the 37 rows"):
            job.sheet_text(labels, TERM_FROM_DATES)


ALL_VALUE_COLUMNS = list(
    dict.fromkeys(
        column
        for sheet in cell_map.ASSESSMENT_SHEETS
        for column in job._value_columns(sheet)
    )
)
EXPLICIT_VALUE_COLUMNS = [
    Pub.filled_posts,
    Pub.locationid_count,
    Pub.filled_posts_april_perc_change,
    Pub.locationid_count_april_perc_change,
    Pub.filled_posts_cum_perc_change_short,
    Pub.locationid_count_cum_perc_change_short,
    Pub.prev_run_filled_posts,
    Pub.filled_posts_perc_change_vs_prev_run,
]
OTHER_VALUE_COLUMNS = [c for c in ALL_VALUE_COLUMNS if c not in EXPLICIT_VALUE_COLUMNS]


def _generic_value(column: str, date_index: int, service_index: int) -> int:
    """A value that differs by column, import date and service type."""
    return (
        (ALL_VALUE_COLUMNS.index(column) + 1) * 10_000
        + 100 * date_index
        + (service_index)
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
            Pub.filled_posts: 1000 * (i + 1) + 10 * j,
            Pub.locationid_count: 100 * (i + 1) + j,
            Pub.filled_posts_april_perc_change: 0.1 * (i + 1),
            Pub.locationid_count_april_perc_change: 0.2 * (i + 1),
            Pub.filled_posts_cum_perc_change_short: 0.3 * (i + 1),
            Pub.locationid_count_cum_perc_change_short: 0.4 * (i + 1),
            Pub.prev_run_filled_posts: 5000 * (i + 1) + 10 * j,
            Pub.filled_posts_perc_change_vs_prev_run: 0.5 * (i + 1),
        }
        | {column: _generic_value(column, i, j) for column in OTHER_VALUE_COLUMNS}
        for i, import_date in enumerate(import_dates)
        for j, service_type in enumerate(job.SERVICE_TYPE_ORDER)
    ]
    return pl.LazyFrame(rows)


VALUE_COLUMNS = ALL_VALUE_COLUMNS
APRIL_2024 = datetime.date(2024, 4, 1)
APRIL_2025 = datetime.date(2025, 4, 1)
APRIL_2026 = datetime.date(2026, 4, 1)
IMPORT_DATES = [APRIL_2024, APRIL_2025, APRIL_2026, datetime.date(2026, 5, 1)]
TERM_FROM_DATES = job.TermFromDates(
    short=APRIL_2026, medium=APRIL_2025, long=APRIL_2024
)


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
            [f"{Pub.filled_posts}|{service}" for service in job.SERVICE_TYPE_ORDER]
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

    def test_raises_when_periods_exceed_the_table_rows(self):
        april_dates = [datetime.date(2000 + year, 4, 1) for year in range(14)]
        df = job.prepare_assessment_df(_assessment_lf(april_dates), VALUE_COLUMNS)

        with pytest.raises(ValueError, match="exceed the 13 rows"):
            job.filled_posts_values(df)


class TestFillAssessmentWorkbookFromLf:

    def test_saves_a_workbook_holding_the_filled_posts_values(self, tmp_path):
        output = tmp_path / "out.xlsx"

        job.fill_assessment_workbook_from_lf(
            _assessment_lf(IMPORT_DATES),
            (cell_map.FILLED_POSTS,),
            TERM_FROM_DATES,
            cell_map.TEMPLATE_PATH,
            output,
        )

        sheet = openpyxl.load_workbook(output)[cell_map.FILLED_POSTS.name]
        assert (sheet["C24"].value, sheet["B3"].value) == (1000, "End of April 2026")

    def test_raises_for_a_sheet_without_a_values_builder(self, tmp_path):
        with pytest.raises(ValueError, match="No values builder"):
            job.fill_assessment_workbook_from_lf(
                _assessment_lf(IMPORT_DATES),
                (cell_map.SheetMap("Unknown sheet", (), 0),),
                TERM_FROM_DATES,
                cell_map.TEMPLATE_PATH,
                tmp_path / "out.xlsx",
            )
