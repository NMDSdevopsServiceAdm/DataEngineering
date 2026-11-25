"""Text the assessment workbook writer sets: titles, headings, notes and row labels."""

import datetime
from dataclasses import dataclass

import polars as pl
from openpyxl.worksheet.cell_range import CellRange

from projects._99_publication.monthly_tracker_filled_posts.fargate.utils import (
    assessment_cell_map as cell_map,
)
from projects._99_publication.monthly_tracker_filled_posts.fargate.utils import (
    publication_utils,
)

PERIOD_LABEL_SHORT_FORMAT = "%b %Y"
PERIOD_LABEL_LONG_FORMAT = "%B %Y"
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


def period_months(import_dates: list[datetime.date]) -> list[datetime.date]:
    """The month each import date reports on."""
    return (
        pl.DataFrame({"import_date": import_dates})
        .select(publication_utils.import_date_to_period_month(pl.col("import_date")))
        .to_series()
        .to_list()
    )


def _period_dates(period_labels: list[str]) -> list[datetime.datetime]:
    """Parses period labels such as "Mar 2026"; raises if there are none."""
    if not period_labels:
        raise ValueError("period_labels is empty")
    return [
        datetime.datetime.strptime(label, PERIOD_LABEL_SHORT_FORMAT)
        for label in period_labels
    ]


def title_text(period_labels: list[str], run_number: int) -> str:
    """Title for each sheet, naming the latest period and the run.

    Args:
        period_labels (list[str]): period labels such as "Mar 2026".
        run_number (int): the run the workbook is built from.

    Returns:
        str: e.g. "End of March 2026 (run 7)".
    """
    latest = max(_period_dates(period_labels))
    return f"End of {latest.strftime(PERIOD_LABEL_LONG_FORMAT)} (run {run_number})"


def comp_prev_rows(
    period_months: list[datetime.date],
) -> list[tuple[datetime.date, str]]:
    """The rows of "Comp prev publication" in order, with their labels.

    An "Annual" row for each March period, then a "Monthly" row for each period
    after the latest March. The labels and the figures both follow this list.

    Args:
        period_months (list[datetime.date]): period months, in any order, repeats
            allowed.

    Returns:
        list[tuple[datetime.date, str]]: period month and label for each row.

    Raises:
        ValueError: if no period month is a March.
    """
    unique_months = sorted(set(period_months))
    marches = [m for m in unique_months if m.month == MARCH]
    if not marches:
        raise ValueError("period months have no March")
    return [(m, ANNUAL_LABEL) for m in marches] + [
        (m, MONTHLY_LABEL) for m in unique_months if m > marches[-1]
    ]


def sheet_text(
    period_labels: list[str], term_from_dates: TermFromDates, run_number: int
) -> dict[str, dict[str, str]]:
    """Text the writer sets, by sheet name then A1 cell.

    - Every sheet gets the title, which names the run.
    - Short, medium and long term comparison sheets get a heading and criteria
      note dated from the month before the term's start.
    - "No filters - long term" gets the long term heading only.
    - "Filled posts" gets a chart heading dated from the short term start.
    - "Comp prev publication" gets an "Annual" row for each March period, then a
      "Monthly" row for each period after the latest March, in each label range.

    Args:
        period_labels (list[str]): period labels such as "Mar 2026".
        term_from_dates (TermFromDates): first import date of each comparison term.
        run_number (int): the run the workbook is built from.

    Returns:
        dict[str, dict[str, str]]: text to write.

    Raises:
        ValueError: if no period label is a March, or there are more periods than
            label slots.
    """
    dates = _period_dates(period_labels)

    # Period months the headings and notes refer back to
    short_term_period, medium_term_period, long_term_period = period_months(
        [term_from_dates.short, term_from_dates.medium, term_from_dates.long]
    )

    # Title on every sheet
    text = {
        sheet.name: {cell_map.TITLE_CELL: title_text(period_labels, run_number)}
        for sheet in cell_map.ASSESSMENT_SHEETS
    }

    # Filled posts: chart heading
    text[cell_map.FILLED_POSTS.name][cell_map.FILLED_POSTS_CHART_HEADING_CELL] = (
        CHART_HEADING_TEXT.format(
            date=short_term_period.strftime(PERIOD_LABEL_LONG_FORMAT)
        )
    )

    # Comparisons: heading and criteria note
    for sheet, period in (
        (cell_map.COMP_SHORT_TERM, short_term_period),
        (cell_map.COMP_MEDIUM_TERM, medium_term_period),
        (cell_map.COMP_LONG_TERM, long_term_period),
    ):
        date = period.strftime(PERIOD_LABEL_LONG_FORMAT)
        text[sheet.name][cell_map.HEADING_CELL] = HEADING_TEXT.format(date=date)
        text[sheet.name][cell_map.CRITERIA_NOTE_CELL] = CRITERIA_NOTE_TEXT.format(
            date=date
        )

    # No filters: heading only
    text[cell_map.NO_FILTERS.name][cell_map.HEADING_CELL] = HEADING_TEXT.format(
        date=long_term_period.strftime(PERIOD_LABEL_LONG_FORMAT)
    )

    # Comp prev publication: an Annual or Monthly label for each period row
    row_labels = [label for _, label in comp_prev_rows([d.date() for d in dates])]
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
