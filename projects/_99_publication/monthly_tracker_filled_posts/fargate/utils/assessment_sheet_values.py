"""Cell values for each sheet of the assessment workbook, built from the data."""

import datetime
from collections.abc import Callable, Iterable

import polars as pl
from openpyxl.utils import get_column_letter
from openpyxl.utils.cell import coordinate_to_tuple
from openpyxl.worksheet.cell_range import CellRange

from projects._99_publication.monthly_tracker_filled_posts.fargate.utils import (
    assessment_cell_map as cell_map,
)
from projects._99_publication.monthly_tracker_filled_posts.fargate.utils import (
    publication_utils,
)
from projects._99_publication.monthly_tracker_filled_posts.fargate.utils.assessment_sheet_text import (
    TermFromDates,
    comp_prev_rows,
    period_months,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.publication_columns import PublicationColumns as Pub
from utils.column_values.categorical_column_values import (
    PrimaryServiceType,
    PublishedJobGroupLabels,
    PublishedMainService,
    PublishedRegion,
)

APRIL = 4
SERVICE_TYPE_SEPARATOR = "|"
SERVICE_TYPE_ORDER = [
    PublishedMainService.all_locations,
    PublishedMainService.all_care_homes,
    PrimaryServiceType.care_home_with_nursing,
    PrimaryServiceType.care_home_only,
    PrimaryServiceType.non_residential,
]
# Comparison sheet blocks: key prefix of the block's input ranges and the
# service type rows that feed it.
COMPARISON_BLOCKS = (
    ("care_homes", PublishedMainService.all_care_homes),
    ("non_res", PrimaryServiceType.non_residential),
)
# Period months (March, June, September, December) of the long term sheets.
# The long term start period must be one of these months, or the filter drops the
# row its % change is measured from.
QUARTERLY_PERIOD_MONTHS = (3, 6, 9, 12)


def prepare_assessment_df(
    assessment_lf: pl.LazyFrame, value_columns: list[str]
) -> pl.DataFrame:
    """Filters, sorts and widens the assessment data into workbook layout.

    Keeps the "All job roles" rows for England, then pivots service types across
    columns named `<value column>|<service type>`, one row per import date.
    Collects once: the result has one row per period, so it is small.

    Args:
        assessment_lf (pl.LazyFrame): assessment data.
        value_columns (list[str]): columns to spread across service types.

    Returns:
        pl.DataFrame: one row per import date, sorted by import date.
    """
    return (
        assessment_lf.filter(
            pl.col(IndCQC.main_job_role_clean_labelled)
            == PublishedJobGroupLabels.all_job_roles,
            pl.col(IndCQC.current_region) == PublishedRegion.england,
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


def value_columns(sheets: Iterable[cell_map.SheetMap]) -> list[str]:
    """Source columns the sheets read, excluding the period label columns.

    Args:
        sheets (Iterable[cell_map.SheetMap]): sheets to collect source columns from.

    Returns:
        list[str]: distinct column names in first-seen order.
    """
    return list(
        dict.fromkeys(
            r.source_column
            for sheet in sheets
            for r in sheet.input_ranges
            if r.source_column not in (None, cell_map.PERIOD_COL, Pub.period_label_long)
        )
    )


def _input_range(sheet: cell_map.SheetMap, key: str) -> cell_map.InputRange:
    """The sheet's input range with this key.

    Args:
        sheet (cell_map.SheetMap): sheet to look in.
        key (str): key of the input range.

    Returns:
        cell_map.InputRange: the matching range.

    Raises:
        ValueError: if the sheet has no range with this key.
    """
    for input_range in sheet.input_ranges:
        if input_range.key == key:
            return input_range
    raise ValueError(f"No input range with key '{key}' on '{sheet.name}'")


def _table_cells(
    sheet: cell_map.SheetMap,
    period_df: pl.DataFrame,
    date_keys: tuple[str, ...],
    value_keys: tuple[str, ...],
    service_types: tuple[str, ...] = tuple(SERVICE_TYPE_ORDER),
) -> dict[str, object]:
    """Cells for one table: period dates, service type values and % changes.

    Percent ranges are rounded to one decimal place of a percentage and all other
    value ranges to whole numbers.

    Args:
        sheet (cell_map.SheetMap): the sheet the table is on.
        period_df (pl.DataFrame): the table's periods, in workbook order.
        date_keys (tuple[str, ...]): keys of the ranges that hold the period
            months. The first sets how many periods fit.
        value_keys (tuple[str, ...]): keys of the ranges that hold service type
            values, read from each range's source column.
        service_types (tuple[str, ...]): service types the value ranges spread
            across, in column order.

    Returns:
        dict[str, object]: value by A1 cell.

    Raises:
        ValueError: if there are more periods than rows in the table, or a key has
            no input range.
    """
    dates_range = CellRange(_input_range(sheet, date_keys[0]).cells)
    import_dates = period_df.get_column(IndCQC.cqc_location_import_date).to_list()
    if len(import_dates) > dates_range.size["rows"]:
        raise ValueError(
            f"{len(import_dates)} periods exceed the {dates_range.size['rows']}"
            f" rows of '{date_keys[0]}'"
        )
    period_dates = [[d] for d in period_months(import_dates)]

    cells = {}
    for key in date_keys:
        input_range = CellRange(_input_range(sheet, key).cells)
        cells |= block_cells(input_range.coord.split(":")[0], period_dates)
    for key in value_keys:
        input_range = _input_range(sheet, key)
        cell_range = CellRange(input_range.cells)
        round_values = (
            publication_utils.percentage_as_rounded_fraction
            if input_range.number_format == "percent"
            else publication_utils.round_to_whole_number
        )
        value_columns = [
            f"{input_range.source_column}{SERVICE_TYPE_SEPARATOR}{service_type}"
            for service_type in service_types
        ]
        rows = period_df.select(
            round_values(column).alias(column) for column in value_columns
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
    for table, period_df in (
        ("annual_filled_posts", annual_df),
        ("annual_locations", annual_df),
        ("monthly_filled_posts", monthly_df),
        ("monthly_locations", monthly_df),
    ):
        cells |= _table_cells(
            cell_map.FILLED_POSTS,
            period_df,
            (f"{table}.dates", f"{table}.change_dates"),
            (f"{table}.values", f"{table}.change"),
        )
    return cells


def comp_prev_publication_values(assessment_df: pl.DataFrame) -> dict[str, object]:
    """Cell values for the "Comp prev publication" sheet.

    Each of the previous publication, current publication and difference blocks
    lists its rows in the order of `comp_prev_rows`, which the labels also follow.

    Args:
        assessment_df (pl.DataFrame): output of `prepare_assessment_df`.

    Returns:
        dict[str, object]: value by A1 cell.
    """
    months = period_months(
        assessment_df.get_column(IndCQC.cqc_location_import_date).to_list()
    )
    row_indexes = [months.index(month) for month, _ in comp_prev_rows(months)]
    period_df = assessment_df.select(pl.all().gather(row_indexes))

    cells = {}
    for block in ("previous", "current", "difference"):
        cells |= _table_cells(
            cell_map.COMP_PREV_PUBLICATION,
            period_df,
            (f"{block}.dates",),
            (f"{block}.values",),
        )
    return cells


def comparison_sheet_values(
    assessment_df: pl.DataFrame,
    sheet: cell_map.SheetMap,
    from_date: datetime.date,
    period_months: tuple[int, ...] | None = None,
) -> dict[str, object]:
    """Cell values for a comparison sheet, from the term's start date onwards.

    The care homes block reads the "All CQC care homes" rows and the non-residential
    block reads the "non-residential" rows. A single-cell range takes the latest
    row's value, since every row in the term shares the same number of locations.

    Args:
        assessment_df (pl.DataFrame): output of `prepare_assessment_df`.
        sheet (cell_map.SheetMap): the comparison sheet.
        from_date (datetime.date): first import date of the term.
        period_months (tuple[int, ...] | None): month numbers of the periods to keep,
            or None to keep every month.

    Returns:
        dict[str, object]: value by A1 cell.

    Raises:
        ValueError: if the sheet has no input ranges for a block.
    """
    import_date = pl.col(IndCQC.cqc_location_import_date)
    period_df = assessment_df.filter(import_date >= from_date)
    if period_months is not None:
        period_df = period_df.filter(
            publication_utils.import_date_to_period_month(import_date)
            .dt.month()
            .is_in(period_months)
        )
    latest_row = period_df.row(-1, named=True)

    cells = {}
    for prefix, service_type in COMPARISON_BLOCKS:
        block_ranges = [r for r in sheet.input_ranges if r.key.startswith(f"{prefix}.")]
        if not block_ranges:
            raise ValueError(f"No '{prefix}' input ranges on '{sheet.name}'")
        tables = [
            r
            for r in block_ranges
            if r.source_column is not None and CellRange(r.cells).size["rows"] > 1
        ]
        cells |= _table_cells(
            sheet,
            period_df,
            tuple(r.key for r in tables if r.source_column == cell_map.PERIOD_COL),
            tuple(r.key for r in tables if r.source_column != cell_map.PERIOD_COL),
            (service_type,),
        )
        for r in block_ranges:
            is_single_cell = CellRange(r.cells).size == {"rows": 1, "columns": 1}
            if r.source_column is not None and is_single_cell:
                column = f"{r.source_column}{SERVICE_TYPE_SEPARATOR}{service_type}"
                cells[r.cells] = latest_row[column]
    return cells


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
        df, cell_map.COMP_LONG_TERM, dates.long, QUARTERLY_PERIOD_MONTHS
    ),
    cell_map.NO_FILTERS.name: lambda df, dates: comparison_sheet_values(
        df, cell_map.NO_FILTERS, dates.long, QUARTERLY_PERIOD_MONTHS
    ),
}
