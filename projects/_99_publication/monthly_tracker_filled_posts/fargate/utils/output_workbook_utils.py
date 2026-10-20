import os
import re
import zipfile
from datetime import date, timedelta
from pathlib import Path

import gptables as gpt
import holidays
import pandas as pd
import polars as pl

from projects._99_publication.monthly_tracker_filled_posts.fargate.utils.publication_utils import (
    import_date_to_period_month,
    round_percentage,
)
from projects._99_publication.monthly_tracker_filled_posts.fargate.utils.workbook_helpers import (
    set_column_widths,
)
from projects._99_publication.monthly_tracker_filled_posts.fargate.utils.output_workbook_pages import (
    write_pages,
)
from projects._99_publication.monthly_tracker_filled_posts.fargate.utils.output_workbook_text import (
    NOT_PUBLISHED_LABEL,
    SUPPRESSED_LABEL,
    T0_TITLE,
    T1_TITLE,
    T2_TITLE,
    TABLE_TITLES,
)
from utils.column_names.publication_columns import PublicationColumns as Pub

_PUBLICATION_DAY_OF_MONTH: int = 15
_FIRST_WEEKEND_WEEKDAY: int = 5  # date.weekday(): Saturday

# Flat 12pt Arial, unbranded.
_THEME_CONFIG: dict = {
    "global": {"font_size": 12, "font_name": "Arial", "font_color": "automatic"},
    "title": {"bold": True, "font_size": 12},
    "subtitle": {"font_size": 12},
    "column_heading": {
        "bold": True,
        "top": 1,
        "bottom": 1,
        "text_wrap": 1,
        "align": "center",
        "valign": "vcenter",
    },
    "data": {"text_wrap": 1},
    "source": {"font_size": 12},
    "legend": {"font_size": 12},
    "description_order": ["instructions", "legend", "source", "scope"],
}
_TABLE_ROW_INDEX: int = 4  # blank row between the sheet description and header
_TABLE_INSTRUCTIONS = "This sheet contains 1 table."
_TABLE_COLUMN_WIDTHS: list[int] = [18, 18, 29, 36, 36]
_TABLE_NAMES: dict[str, str] = {
    "T0": "t0_filled_posts",
    "T1": "t1_filled_posts_perc_change",
    "T2": "t2_location_count_perc_change",
}
_COUNT_FORMAT = {"num_format": "#,##0", "align": "right"}
_PERCENTAGE_FORMAT = {"num_format": "0.0%", "align": "right"}


def get_publication_date(latest_import_date: date) -> date:
    """
    Returns the date the publication is released: the 15th of the month of the
    latest period's CQC import date, moved forward to the next working day when
    it falls on a weekend or an England bank holiday.

    Args:
        latest_import_date (date): the latest value of the Period column.

    Returns:
        date: the publication date.
    """
    england_holidays = holidays.country_holidays("GB", subdiv="ENG")
    publication_date = latest_import_date.replace(day=_PUBLICATION_DAY_OF_MONTH)
    while (
        publication_date.weekday() >= _FIRST_WEEKEND_WEEKDAY
        or publication_date in england_holidays
    ):
        publication_date += timedelta(days=1)
    return publication_date


def format_publication_date(publication_date: date) -> str:
    """
    Formats a date as month, ordinal day and year, e.g. "September 15th 2026".

    Args:
        publication_date (date): the date to format.

    Returns:
        str: the formatted date.
    """
    day = publication_date.day
    suffix = (
        "th" if 11 <= day <= 13 else {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")
    )
    return f"{publication_date:%B} {day}{suffix} {publication_date.year}"


def prepare_t0_for_publication(publication_lf: pl.LazyFrame) -> pd.DataFrame:
    """
    Builds the published T0 table: the period label as "Period" and the
    rounded filled posts as "Estimated filled posts", with a suppressed (null)
    value shown as "[s]".

    Collects once, after selecting the published columns, as gptables needs
    pandas.

    Args:
        publication_lf (pl.LazyFrame): the publication data written by the
            output stage.

    Returns:
        pd.DataFrame: the table to publish.
    """
    published_lf = publication_lf.select(
        pl.col(Pub.period_label).alias(Pub.period),
        Pub.region,
        Pub.main_service,
        pl.col(Pub.estimated_filled_posts_rounded).alias(Pub.estimated_filled_posts),
        Pub.cqc_locations,
    )
    return _collect_with_labels(
        published_lf, {Pub.estimated_filled_posts: SUPPRESSED_LABEL}
    )


def prepare_perc_change_for_publication(
    publication_lf: pl.LazyFrame, april_column: str, cumulative_column: str
) -> pd.DataFrame:
    """
    Builds a published percentage change table (T1 or T2): the period label
    as "Period", the change since April as "Annual percentage change" and the
    cumulative short-term change as "Monthly percentage change", both as
    fractions rounded to one decimal place of a percentage, with a null value
    shown as "[x]".

    Collects once, after selecting the published columns, as gptables needs
    pandas.

    Args:
        publication_lf (pl.LazyFrame): the publication data written by the
            output stage.
        april_column (str): the change since April column to publish.
        cumulative_column (str): the cumulative short-term change column to
            publish.

    Returns:
        pd.DataFrame: the table to publish.
    """
    published_lf = publication_lf.select(
        pl.col(Pub.period_label).alias(Pub.period),
        Pub.region,
        Pub.main_service,
        _percentage_as_rounded_fraction(april_column).alias(
            Pub.annual_percentage_change
        ),
        _percentage_as_rounded_fraction(cumulative_column).alias(
            Pub.monthly_percentage_change
        ),
    )
    return _collect_with_labels(
        published_lf,
        {
            Pub.annual_percentage_change: NOT_PUBLISHED_LABEL,
            Pub.monthly_percentage_change: NOT_PUBLISHED_LABEL,
        },
    )


def _percentage_as_rounded_fraction(column: str) -> pl.Expr:
    """
    Rounds a fractional percentage change to one decimal place of a
    percentage (0.0321 -> 0.032) as a Float64 free of Float32 noise.

    Args:
        column (str): the fractional percentage change column.

    Returns:
        pl.Expr: the rounded fraction.
    """
    return (round_percentage(column) / 100).round(3)


def get_latest_import_date(publication_lf: pl.LazyFrame) -> date:
    """
    Returns the latest value of the Period column, a CQC import date.

    Args:
        publication_lf (pl.LazyFrame): the publication data written by the
            output stage.

    Returns:
        date: the latest import date.
    """
    return publication_lf.select(pl.col(Pub.period).max()).collect().item()


def get_latest_period(publication_lf: pl.LazyFrame) -> date:
    """
    Returns the month of the latest published period: the latest period date
    moved back one month, as in the period label.

    Args:
        publication_lf (pl.LazyFrame): the publication data written by the
            output stage.

    Returns:
        date: the first day of the latest period's month.
    """
    return (
        publication_lf.select(import_date_to_period_month(pl.col(Pub.period).max()))
        .collect()
        .item()
    )


def _collect_with_labels(lf: pl.LazyFrame, null_labels: dict[str, str]) -> pd.DataFrame:
    """
    Collects a published table to pandas, replacing nulls in the given columns
    with a text label (mixed text and numbers need pandas' object dtype).

    Args:
        lf (pl.LazyFrame): the table to collect.
        null_labels (dict[str, str]): column name mapped to the label shown
            for its null values.

    Returns:
        pd.DataFrame: the collected table.
    """
    df = lf.collect().to_pandas()
    for column, label in null_labels.items():
        df[column] = df[column].astype(object).where(df[column].notna(), label)
    return df


def build_download_workbook(
    publication_lf: pl.LazyFrame,
    filename: str | Path,
    logo: bytes | None = None,
) -> None:
    """
    Writes the public data download workbook: the Frontpage, Introduction,
    Revisions, Definitions and Contents pages, then one gptables sheet per
    table (T0, T1, T2), each a column selection of the publication data.

    Reading the data and storing the workbook are left to the caller. The data
    is collected once, as it feeds every sheet and the latest period, and each
    table is then collected as gptables needs pandas. The data is small (one
    row per year, plus monthly for the current financial year).

    Args:
        publication_lf (pl.LazyFrame): the publication data written by the
            output stage.
        filename (str | Path): path to write the .xlsx workbook to.
        logo (bytes | None): PNG image shown on the Frontpage, or None for no
            logo.
    """
    publication_lf = publication_lf.collect().lazy()
    latest_period = get_latest_period(publication_lf)
    publication_date = format_publication_date(
        get_publication_date(get_latest_import_date(publication_lf))
    )
    subtitles = [f"Skills for Care's workforce estimates as at {latest_period:%b-%Y}"]
    tables = {
        "T0": prepare_t0_for_publication(publication_lf),
        "T1": prepare_perc_change_for_publication(
            publication_lf,
            Pub.publication_filled_posts_april_perc_change,
            Pub.publication_filled_posts_cumulative_perc_change_short_term,
        ),
        "T2": prepare_perc_change_for_publication(
            publication_lf,
            Pub.publication_locationid_count_april_perc_change,
            Pub.publication_locationid_count_cumulative_perc_change_short_term,
        ),
    }
    titles = {"T0": T0_TITLE, "T1": T1_TITLE, "T2": T2_TITLE}
    value_formats = {
        "T0": _COUNT_FORMAT,
        "T1": _PERCENTAGE_FORMAT,
        "T2": _PERCENTAGE_FORMAT,
    }

    workbook = gpt.GPWorkbook(str(filename))
    workbook.set_theme(gpt.Theme(_THEME_CONFIG))
    write_pages(
        workbook,
        {
            "latest_period": f"{latest_period:%B %Y}",
            "publication_date": publication_date,
        },
        TABLE_TITLES,
        logo,
    )
    for sheet_name, table in tables.items():
        gptable = gpt.GPTable(
            table=table,
            table_name=_TABLE_NAMES[sheet_name],
            title=titles[sheet_name],
            subtitles=subtitles,
            instructions=_TABLE_INSTRUCTIONS,
            index_columns={},
            additional_formatting=_table_formatting(value_formats[sheet_name]),
            table_row_index=_TABLE_ROW_INDEX,
        )
        worksheet = workbook.add_worksheet(sheet_name)
        worksheet.write_gptable(gptable, False, [])
        set_column_widths(worksheet, _TABLE_COLUMN_WIDTHS)
    workbook.close()
    _enable_table_filters(filename, list(_TABLE_NAMES.values()))


def _table_formatting(value_format: dict) -> list[dict]:
    """
    Builds the formatting that boxes a table's label columns (period, region,
    main service) and its two value columns separately, with the value format
    applied to the value columns.

    Args:
        value_format (dict): format of the two value columns.

    Returns:
        list[dict]: gptables additional formatting.
    """
    return [
        {"column": {"columns": [0], "format": {"left": 1}}},
        {"column": {"columns": [2], "format": {"right": 1}}},
        {"column": {"columns": [3], "format": {**value_format, "left": 1}}},
        {"column": {"columns": [4], "format": {**value_format, "right": 1}}},
        {"row": {"rows": -1, "format": {"bottom": 1}}},
    ]


def _enable_table_filters(filename: str | Path, table_names: list[str]) -> None:
    """
    Adds filter buttons to the header of the named Excel tables in a saved
    workbook, which gptables always writes without them.

    Rewrites the workbook through a temporary file, so a failure leaves the
    original untouched.

    Args:
        filename (str | Path): the saved .xlsx workbook, replaced in place.
        table_names (list[str]): names of the tables to add filter buttons to;
            a table that already has a filter is left alone.
    """
    path = Path(filename)
    temp_path = path.with_suffix(".tmp")
    try:
        with (
            zipfile.ZipFile(path) as source,
            zipfile.ZipFile(temp_path, "w", zipfile.ZIP_DEFLATED) as target,
        ):
            for info in source.infolist():
                data = source.read(info.filename)
                if info.filename.startswith("xl/tables/"):
                    data = _add_table_filter(data.decode("utf-8"), table_names).encode(
                        "utf-8"
                    )
                target.writestr(info, data)
        os.replace(temp_path, path)
    finally:
        temp_path.unlink(missing_ok=True)


def _add_table_filter(xml: str, table_names: list[str]) -> str:
    """
    Adds a filter over the whole range of an Excel table's XML if it is one of
    the named tables and has no filter yet.

    Args:
        xml (str): the table's XML.
        table_names (list[str]): names of the tables to add a filter to.

    Returns:
        str: the table's XML.
    """
    name = re.search(r' name="([^"]+)"', xml)
    table_range = re.search(r' ref="([^"]+)"', xml)
    if not (name and table_range) or name.group(1) not in table_names:
        return xml
    if "<autoFilter" in xml:
        return xml
    return xml.replace(
        "<tableColumns", f'<autoFilter ref="{table_range.group(1)}"/><tableColumns', 1
    )
