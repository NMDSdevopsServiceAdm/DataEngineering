"""Helpers for writing input cells into the assessment template."""

import datetime
from collections.abc import Callable
from copy import copy
from dataclasses import dataclass
from pathlib import Path

import boto3
import openpyxl
import polars as pl
from openpyxl.utils import get_column_letter
from openpyxl.utils.cell import coordinate_to_tuple
from openpyxl.worksheet.cell_range import CellRange
from openpyxl.worksheet.dimensions import ColumnDimension
from openpyxl.worksheet.worksheet import Worksheet

from projects._99_publication.monthly_tracker_filled_posts.fargate.utils import (
    assessment_cell_map as cell_map,
)
from projects._99_publication.monthly_tracker_filled_posts.fargate.utils import (
    publication_utils,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.publication_columns import PublicationColumns as Pub
from utils.column_values.categorical_column_values import (
    PublishedJobGroupLabels,
    PublishedRegion,
)
from utils.file_utils import split_s3_uri


PERIOD_LABEL_SHORT_FORMAT = "%b %Y"
PERIOD_LABEL_LONG_FORMAT = "%B %Y"
APRIL = 4
SERVICE_TYPE_SEPARATOR = "|"
CARE_HOMES_SERVICE_TYPE = "All CQC care homes"
NON_RESIDENTIAL_SERVICE_TYPE = "non-residential"
SERVICE_TYPE_ORDER = [
    "All CQC locations",
    CARE_HOMES_SERVICE_TYPE,
    "Care home with nursing",
    "Care home without nursing",
    NON_RESIDENTIAL_SERVICE_TYPE,
]
# Comparison sheet blocks: note prefix of the block's input ranges and the
# service type rows that feed it.
COMPARISON_BLOCKS = (
    ("care homes", CARE_HOMES_SERVICE_TYPE),
    ("non-res", NON_RESIDENTIAL_SERVICE_TYPE),
)
MARCH = 3
ANNUAL_LABEL = "Annual"
MONTHLY_LABEL = "Monthly"
HEADING_TEXT = (
    "Comparison of Skills for Care's estimate of filled posts to Capacity Tracker"
    " - Since {date}"
)
CHART_HEADING_TEXT = (
    "Comparison of percentage change in filled posts and CQC locations since {date}"
)
CRITERIA_NOTE_TEXT = (
    "- Must have a Capacity Tracker figure in all months from end of {date} onwards."
)


class TemplateMismatchError(ValueError):
    """The template does not match the cell map."""


class UnmappedCellError(ValueError):
    """A value targets a sheet or cell outside the mapped input cells."""


def cell_exists(worksheet: Worksheet, row: int, col: int) -> bool:
    """Whether the sheet already holds a cell at this position.

    `worksheet.cell()` creates a missing cell, so existence is read from the
    private `_cells` mapping (openpyxl 3.1.5, pinned). A cell stored with the
    default style still counts as existing, which a style check cannot tell
    apart from a missing cell that falls back to its column's style.

    Args:
        worksheet (Worksheet): sheet to look in.
        row (int): 1-based row number.
        col (int): 1-based column number.

    Returns:
        bool: True if the cell exists.
    """
    return (row, col) in worksheet._cells


def column_dimension(worksheet: Worksheet, col: int) -> ColumnDimension | None:
    """Column dimension covering a column; openpyxl groups adjacent equal columns.

    Args:
        worksheet (Worksheet): sheet to look in.
        col (int): 1-based column number.

    Returns:
        ColumnDimension | None: the dimension, or None if the column has none.
    """
    return next(
        (d for d in worksheet.column_dimensions.values() if d.min <= col <= d.max),
        None,
    )


def write_input_cell(worksheet: Worksheet, row: int, col: int, value: object) -> None:
    """Writes a value, giving a new cell its column's style.

    Excel doesn't store a cell whose style equals its column's, and openpyxl
    doesn't apply a column's style to the cells it creates, so a new cell would
    otherwise get the workbook default font and number format.

    Args:
        worksheet (Worksheet): sheet to write to.
        row (int): 1-based row number.
        col (int): 1-based column number.
        value (object): value to write.
    """
    existed = cell_exists(worksheet, row, col)
    cell = worksheet.cell(row, col)
    dimension = column_dimension(worksheet, col)
    if not existed and dimension is not None:
        cell._style = copy(dimension._style)
    cell.value = value


def check_template(workbook: openpyxl.Workbook) -> None:
    """Checks the template holds every mapped sheet and fixed label.

    Args:
        workbook (openpyxl.Workbook): the loaded template.

    Raises:
        TemplateMismatchError: listing every missing sheet and label that differs.
    """
    problems = []
    for sheet in cell_map.ASSESSMENT_SHEETS:
        if sheet.name not in workbook.sheetnames:
            problems.append(f"missing sheet '{sheet.name}'")
            continue
        worksheet = workbook[sheet.name]
        for cell, expected in sheet.labels:
            found = worksheet[cell].value
            if found != expected:
                problems.append(
                    f"'{sheet.name}'!{cell} expected {expected!r}, found {found!r}"
                )
    if problems:
        raise TemplateMismatchError(
            "Template does not match cell map: " + "; ".join(problems)
        )


def _period_dates(period_labels: list[str]) -> list[datetime.datetime]:
    """Parses period labels such as "Mar 2026"; raises if there are none."""
    if not period_labels:
        raise ValueError("period_labels is empty")
    return [
        datetime.datetime.strptime(label, PERIOD_LABEL_SHORT_FORMAT)
        for label in period_labels
    ]


def title_text(period_labels: list[str]) -> str:
    """Title for each sheet, naming the latest period.

    Args:
        period_labels (list[str]): period labels such as "Mar 2026".

    Returns:
        str: e.g. "End of March 2026".
    """
    latest = max(_period_dates(period_labels))
    return f"End of {latest.strftime(PERIOD_LABEL_LONG_FORMAT)}"


def sheet_text(period_labels: list[str]) -> dict[str, dict[str, str]]:
    """Text the writer sets, by sheet name then A1 cell.

    Every sheet gets the title. The short and medium term comparison sheets also
    get a heading and criteria note dated from March: the latest March for short
    term, the March a year before for medium term. "Filled posts" gets a chart
    heading dated from the latest March. "Comp prev publication" gets
    an "Annual" row for each March period, then a "Monthly" row for each other
    period, in each of its label ranges.

    Args:
        period_labels (list[str]): period labels such as "Mar 2026".

    Returns:
        dict[str, dict[str, str]]: text to write.

    Raises:
        ValueError: if no period label is a March, or there are more periods than
            label slots.
    """
    dates = _period_dates(period_labels)

    # March dates the headings and notes refer back to
    marches = [d for d in dates if d.month == MARCH]
    if not marches:
        raise ValueError("period_labels has no March")
    short_term_march = max(marches)
    medium_term_march = short_term_march.replace(year=short_term_march.year - 1)

    # Title on every sheet
    text = {
        sheet.name: {cell_map.TITLE_CELL: title_text(period_labels)}
        for sheet in cell_map.ASSESSMENT_SHEETS
    }

    # Filled posts: chart heading
    text[cell_map.FILLED_POSTS.name][cell_map.FILLED_POSTS_CHART_HEADING_CELL] = (
        CHART_HEADING_TEXT.format(
            date=short_term_march.strftime(PERIOD_LABEL_LONG_FORMAT)
        )
    )

    # Short and medium term comparisons: heading and criteria note
    for sheet, march in (
        (cell_map.COMP_SHORT_TERM, short_term_march),
        (cell_map.COMP_MEDIUM_TERM, medium_term_march),
    ):
        date = march.strftime(PERIOD_LABEL_LONG_FORMAT)
        text[sheet.name][cell_map.HEADING_CELL] = HEADING_TEXT.format(date=date)
        text[sheet.name][cell_map.CRITERIA_NOTE_CELL] = CRITERIA_NOTE_TEXT.format(
            date=date
        )

    # Comp prev publication: an Annual or Monthly label for each period row
    unique_dates = set(dates)
    annual_count = sum(d.month == MARCH for d in unique_dates)
    row_labels = [ANNUAL_LABEL] * annual_count + [MONTHLY_LABEL] * (
        len(unique_dates) - annual_count
    )
    for label_range in cell_map.COMP_PREV_LABEL_RANGES:
        cells = CellRange(label_range)
        if len(row_labels) > cells.size["rows"]:
            raise ValueError(
                f"{len(row_labels)} periods exceed the {cells.size['rows']} rows in"
                f" {label_range}"
            )
        for i, label in enumerate(row_labels):
            text[cell_map.COMP_PREV_PUBLICATION.name][f"B{cells.min_row + i}"] = label
    return text


def _input_cells(sheet: cell_map.SheetMap) -> set[tuple[int, int]]:
    return {
        (row, col)
        for input_range in sheet.input_ranges
        for row, col in CellRange(input_range.cells).cells
    }


def fill_assessment_workbook(
    period_labels: list[str],
    values: dict[str, dict[str, object]],
    template_path: Path,
    output_path: Path,
) -> None:
    """Writes values and period text into the template's input cells and saves a copy.

    The text from `sheet_text` (titles, and the short and medium term comparison
    headings and criteria notes) replaces any value given for those cells.

    Nothing is written unless the template matches the cell map and every value
    targets a mapped input cell. Other cells are left as in the template, and
    charts keep their series references; openpyxl can drop other chart styling,
    so check a saved workbook's charts in Excel.

    Args:
        period_labels (list[str]): the run's period labels, such as "Mar 2026".
        values (dict[str, dict[str, object]]): values to write, keyed by sheet
            name, then by A1 cell reference, e.g. `{"Filled posts": {"C24":
            1000}}`. Cells must be in the sheet's mapped input ranges.
        template_path (Path): template to load.
        output_path (Path): local path to save to.

    Raises:
        UnmappedCellError: if a value targets a sheet or cell outside the mapped
            input ranges. A template that does not match the cell map raises
            `TemplateMismatchError` first, and `period_labels` that are empty
            or have no March raise `ValueError`.
    """
    text = sheet_text(period_labels)
    workbook = openpyxl.load_workbook(template_path)
    check_template(workbook)

    input_cells = {
        sheet.name: _input_cells(sheet) for sheet in cell_map.ASSESSMENT_SHEETS
    }
    unmapped = [
        f"'{name}'!{cell}"
        for name, cells in values.items()
        for cell in cells
        if coordinate_to_tuple(cell) not in input_cells.get(name, set())
    ]
    if unmapped:
        raise UnmappedCellError(
            f"Values outside the mapped input cells: {', '.join(unmapped)}"
        )

    for name, cells in values.items():
        for cell, value in cells.items():
            row, col = coordinate_to_tuple(cell)
            write_input_cell(workbook[name], row, col, value)
    for name, cells in text.items():
        for cell, value in cells.items():
            row, col = coordinate_to_tuple(cell)
            write_input_cell(workbook[name], row, col, value)
    workbook.save(output_path)


def prepare_assessment_df(
    assessment_lf: pl.LazyFrame, value_columns: list[str]
) -> pl.DataFrame:
    """Filters, sorts and widens the assessment data into workbook layout.

    Keeps the "All job roles" rows for England. Import dates before the latest
    April keep their April rows only; the latest April onwards keeps every date.
    Then pivots service types across columns named
    `<value column>|<service type>`, one row per import date.
    Collects once: the result has one row per period, so it is small.

    Args:
        assessment_lf (pl.LazyFrame): assessment data.
        value_columns (list[str]): columns to spread across service types.

    Returns:
        pl.DataFrame: one row per import date, sorted by import date.
    """
    import_date = pl.col(IndCQC.cqc_location_import_date)
    return (
        assessment_lf.filter(
            pl.col(IndCQC.main_job_role_clean_labelled)
            == PublishedJobGroupLabels.all_job_roles,
            pl.col(IndCQC.current_region) == PublishedRegion.england,
        )
        .filter(
            (import_date.dt.month() == APRIL)
            | (import_date >= import_date.filter(import_date.dt.month() == APRIL).max())
        )
        .select(
            IndCQC.cqc_location_import_date,
            Pub.period_label_short,
            pl.col(IndCQC.primary_service_type).cast(pl.String),
            *value_columns,
        )
        .collect()
        .pivot(
            on=IndCQC.primary_service_type,
            index=[IndCQC.cqc_location_import_date, Pub.period_label_short],
            values=value_columns,
            separator=SERVICE_TYPE_SEPARATOR,
        )
        .sort(IndCQC.cqc_location_import_date)
    )


def block_cells(top_left: str, rows: list[list[object]]) -> dict[str, object]:
    """Cells for a block of rows, written down and across from `top_left`.

    Args:
        top_left (str): A1 reference of the block's first cell.
        rows (list[list[object]]): values, one list per row.

    Returns:
        dict[str, object]: value by A1 cell.
    """
    first_row, first_col = coordinate_to_tuple(top_left)
    return {
        f"{get_column_letter(first_col + col)}{first_row + row}": value
        for row, row_values in enumerate(rows)
        for col, value in enumerate(row_values)
    }


def _value_columns(sheet: cell_map.SheetMap) -> list[str]:
    """The data columns feeding the sheet's cells, other than the period label."""
    return list(
        dict.fromkeys(
            r.source_column
            for r in sheet.input_ranges
            if r.source_column not in (None, cell_map.PERIOD_COL, Pub.period_label_long)
        )
    )


def _input_range(sheet: cell_map.SheetMap, note: str) -> cell_map.InputRange:
    """The sheet's input range with this note."""
    return next(r for r in sheet.input_ranges if r.note == note)


def _period_months(import_dates: list[datetime.date]) -> list[datetime.date]:
    """The month each import date reports on."""
    return (
        pl.DataFrame({"import_date": import_dates})
        .select(publication_utils.import_date_to_period_month(pl.col("import_date")))
        .to_series()
        .to_list()
    )


def _table_cells(
    sheet: cell_map.SheetMap,
    period_df: pl.DataFrame,
    date_notes: tuple[str, ...],
    value_notes: tuple[str, ...],
    service_types: tuple[str, ...] = tuple(SERVICE_TYPE_ORDER),
) -> dict[str, object]:
    """Cells for one table: period dates, service type values and % changes.

    Args:
        sheet (cell_map.SheetMap): the sheet the table is on.
        period_df (pl.DataFrame): the table's periods, in workbook order.
        date_notes (tuple[str, ...]): notes of the ranges that hold the period
            months. The first sets how many periods fit.
        value_notes (tuple[str, ...]): notes of the ranges that hold service type
            values, read from each range's source column.
        service_types (tuple[str, ...]): service types the value ranges spread
            across, in column order.

    Returns:
        dict[str, object]: value by A1 cell.

    Raises:
        ValueError: if there are more periods than rows in the table.
    """
    dates_range = CellRange(_input_range(sheet, date_notes[0]).cells)
    import_dates = period_df.get_column(IndCQC.cqc_location_import_date).to_list()
    if len(import_dates) > dates_range.size["rows"]:
        raise ValueError(
            f"{len(import_dates)} periods exceed the {dates_range.size['rows']}"
            f" rows of '{date_notes[0]}'"
        )
    period_dates = [[d] for d in _period_months(import_dates)]

    cells = {}
    for note in date_notes:
        input_range = CellRange(_input_range(sheet, note).cells)
        cells |= block_cells(input_range.coord.split(":")[0], period_dates)
    for note in value_notes:
        input_range = _input_range(sheet, note)
        cell_range = CellRange(input_range.cells)
        rows = period_df.select(
            [
                f"{input_range.source_column}{SERVICE_TYPE_SEPARATOR}{service_type}"
                for service_type in service_types
            ]
        ).rows()
        # A % change range can start below the first date row.
        rows = rows[cell_range.min_row - dates_range.min_row :]
        cells |= block_cells(cell_range.coord.split(":")[0], rows)
    return cells


def filled_posts_values(assessment_df: pl.DataFrame) -> dict[str, object]:
    """Cell values for the "Filled posts" sheet.

    The annual tables hold the April import date rows (March periods). The
    monthly tables hold the rows from the latest April import date onwards.

    Args:
        assessment_df (pl.DataFrame): output of `prepare_assessment_df`.

    Returns:
        dict[str, object]: value by A1 cell.
    """
    import_date = pl.col(IndCQC.cqc_location_import_date)
    annual_df = assessment_df.filter(import_date.dt.month() == APRIL)
    monthly_df = assessment_df.filter(
        import_date >= annual_df[IndCQC.cqc_location_import_date].max()
    )

    cells = {}
    for prefix, period_df in (
        ("annual filled posts", annual_df),
        ("annual CQC locations", annual_df),
        ("monthly filled posts", monthly_df),
        ("monthly CQC locations", monthly_df),
    ):
        cells |= _table_cells(
            cell_map.FILLED_POSTS,
            period_df,
            (f"{prefix} dates", f"{prefix} % change dates"),
            (f"{prefix} by primary service type", f"{prefix} % change"),
        )
    return cells


def comp_prev_publication_values(assessment_df: pl.DataFrame) -> dict[str, object]:
    """Cell values for the "Comp prev publication" sheet.

    Each of the previous publication, current publication and difference blocks
    lists the April import date rows (annual) first, then the later rows
    (monthly), matching the labels written from the period labels.

    Args:
        assessment_df (pl.DataFrame): output of `prepare_assessment_df`.

    Returns:
        dict[str, object]: value by A1 cell.
    """
    import_date = pl.col(IndCQC.cqc_location_import_date)
    annual_df = assessment_df.filter(import_date.dt.month() == APRIL)
    monthly_df = assessment_df.filter(
        import_date > annual_df[IndCQC.cqc_location_import_date].max()
    )
    period_df = pl.concat([annual_df, monthly_df])

    cells = {}
    for date_note, value_note in (
        ("previous publication dates", "previous publication figures"),
        ("current publication dates", "current publication figures"),
        ("difference dates", "difference as percentage"),
    ):
        cells |= _table_cells(
            cell_map.COMP_PREV_PUBLICATION, period_df, (date_note,), (value_note,)
        )
    return cells


def comparison_sheet_values(
    assessment_df: pl.DataFrame, sheet: cell_map.SheetMap, from_date: datetime.date
) -> dict[str, object]:
    """Cell values for a comparison sheet, from the term's start date onwards.

    The care homes block reads the "All CQC care homes" rows and the non-residential
    block reads the "non-residential" rows. A single-cell range takes the latest
    row's value, since every row in the term shares the same number of locations.

    Args:
        assessment_df (pl.DataFrame): output of `prepare_assessment_df`.
        sheet (cell_map.SheetMap): the comparison sheet.
        from_date (datetime.date): first import date of the term.

    Returns:
        dict[str, object]: value by A1 cell.
    """
    period_df = assessment_df.filter(
        pl.col(IndCQC.cqc_location_import_date) >= from_date
    )
    latest_row = period_df.row(-1, named=True)

    cells = {}
    for prefix, service_type in COMPARISON_BLOCKS:
        block_ranges = [r for r in sheet.input_ranges if r.note.startswith(prefix)]
        tables = [
            r
            for r in block_ranges
            if r.source_column is not None and CellRange(r.cells).size["rows"] > 1
        ]
        cells |= _table_cells(
            sheet,
            period_df,
            tuple(r.note for r in tables if r.source_column == cell_map.PERIOD_COL),
            tuple(r.note for r in tables if r.source_column != cell_map.PERIOD_COL),
            (service_type,),
        )
        for r in block_ranges:
            if r.source_column is not None and CellRange(r.cells).size["rows"] == 1:
                column = f"{r.source_column}{SERVICE_TYPE_SEPARATOR}{service_type}"
                cells[r.cells] = latest_row[column]
    return cells


@dataclass(frozen=True)
class TermFromDates:
    """First import date of each comparison term.

    Attributes:
        short (datetime.date): short term start.
        medium (datetime.date): medium term start.
        long (datetime.date): long term start, shared by the long term and no
            filters sheets.
    """

    short: datetime.date
    medium: datetime.date
    long: datetime.date


SHEET_VALUE_BUILDERS: dict[str, Callable[[pl.DataFrame, TermFromDates], dict]] = {
    cell_map.FILLED_POSTS.name: lambda df, _: filled_posts_values(df),
    cell_map.COMP_PREV_PUBLICATION.name: lambda df, _: comp_prev_publication_values(df),
    cell_map.COMP_SHORT_TERM.name: lambda df, dates: comparison_sheet_values(
        df, cell_map.COMP_SHORT_TERM, dates.short
    ),
    cell_map.COMP_MEDIUM_TERM.name: lambda df, dates: comparison_sheet_values(
        df, cell_map.COMP_MEDIUM_TERM, dates.medium
    ),
    cell_map.COMP_LONG_TERM.name: lambda df, dates: comparison_sheet_values(
        df, cell_map.COMP_LONG_TERM, dates.long
    ),
    cell_map.NO_FILTERS.name: lambda df, dates: comparison_sheet_values(
        df, cell_map.NO_FILTERS, dates.long
    ),
}


def fill_assessment_workbook_from_lf(
    assessment_lf: pl.LazyFrame,
    sheets: tuple[cell_map.SheetMap, ...],
    term_from_dates: TermFromDates,
    template_path: Path,
    output_path: Path,
) -> None:
    """Writes the assessment data into the given sheets of the template.

    The workbook is written once, so every sheet's values are built first.

    Args:
        assessment_lf (pl.LazyFrame): assessment data.
        sheets (tuple[cell_map.SheetMap, ...]): sheets to fill.
        term_from_dates (TermFromDates): first import date of each comparison term.
        template_path (Path): template to load.
        output_path (Path): local path to save to.

    Raises:
        ValueError: if a sheet has no values builder yet.
    """
    unsupported = [s.name for s in sheets if s.name not in SHEET_VALUE_BUILDERS]
    if unsupported:
        raise ValueError(f"No values builder for sheets: {', '.join(unsupported)}")

    value_columns = list(
        dict.fromkeys(column for sheet in sheets for column in _value_columns(sheet))
    )
    assessment_df = prepare_assessment_df(assessment_lf, value_columns)
    values = {
        sheet.name: SHEET_VALUE_BUILDERS[sheet.name](assessment_df, term_from_dates)
        for sheet in sheets
    }
    period_labels = assessment_df.get_column(Pub.period_label_short).unique().to_list()
    fill_assessment_workbook(period_labels, values, template_path, output_path)


def upload_to_s3(local_path: Path, s3_directory: str) -> None:
    """Uploads a local file into an S3 directory, keeping its file name.

    Args:
        local_path (Path): file to upload.
        s3_directory (str): S3 directory to put the file in.
    """
    bucket, prefix = split_s3_uri(s3_directory)
    key = f"{prefix.rstrip('/')}/{local_path.name}"
    boto3.client("s3").upload_file(str(local_path), bucket, key)
