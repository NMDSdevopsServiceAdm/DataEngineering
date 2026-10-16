from io import BytesIO
import gptables as gpt
import yaml
from xlsxwriter.image import Image

from projects._99_publication.monthly_tracker_filled_posts.fargate.utils.template_paths import (
    TEMPLATES_DIR,
)
from projects._99_publication.monthly_tracker_filled_posts.fargate.utils.workbook_helpers import (
    set_column_widths,
)

_PAGE_CONFIG_PATH = TEMPLATES_DIR / "output_workbook_pages.yaml"
_TEXT_KEYS = ("heading", "text", "link", "url")
_TEXT_PAGES = ["frontpage", "introduction", "revisions", "definitions"]
_FONT = {"font_name": "Arial", "font_size": 12}
_LINK_FONT = {**_FONT, "font_color": "#0563C1", "underline": 1}
_CELL_BORDER = {"border": 1, "valign": "top", "text_wrap": True}
_LOGO_CELL = "A5"
_LOGO_WIDTH_PX = 100
_SCREEN_DPI = 96  # resolution at which xlsxwriter shows an image at its pixel size


def load_page_config() -> dict:
    """
    Loads the wording and layout of the workbook's non-table pages.

    Returns:
        dict: page name mapped to its sheet name, column widths and rows.
    """
    return yaml.safe_load(_PAGE_CONFIG_PATH.read_text(encoding="utf-8"))


def fill_placeholders(rows: list[dict], values: dict[str, str]) -> list[dict]:
    """
    Fills the {placeholders} in a page's row text. Any other braces stay as
    text, and non-string values become strings.

    Args:
        rows (list[dict]): the page's rows.
        values (dict[str, str]): placeholder name mapped to its text.

    Returns:
        list[dict]: the rows with every placeholder replaced.
    """
    return [
        {
            key: _fill_text(value, values) if key in _TEXT_KEYS else value
            for key, value in row.items()
        }
        for row in rows
    ]


def _fill_text(value: object, values: dict[str, str]) -> str:
    """
    Replaces each {placeholder} in a row's text.

    Args:
        value (object): the row's text.
        values (dict[str, str]): placeholder name mapped to its text.

    Returns:
        str: the text with every placeholder replaced.
    """
    text = str(value)
    for name, replacement in values.items():
        text = text.replace("{" + name + "}", replacement)
    return text


def write_pages(
    workbook: gpt.GPWorkbook,
    placeholders: dict[str, str],
    table_titles: dict[str, str],
    logo: bytes | None = None,
) -> None:
    """
    Writes the non-table pages (Frontpage, Introduction, Revisions,
    Definitions, Contents) as the workbook's first sheets.

    Args:
        workbook (gpt.GPWorkbook): the workbook to add the sheets to.
        placeholders (dict[str, str]): text for the page placeholders.
        table_titles (dict[str, str]): table sheet name mapped to its title,
            listed on the Contents page.
        logo (bytes | None): PNG image shown on the Frontpage, or None for no
            logo.
    """
    config = load_page_config()
    for page in _TEXT_PAGES:
        _write_text_page(
            workbook, config[page], placeholders, logo if page == "frontpage" else None
        )
    _write_contents_page(workbook, config["contents"], table_titles)


def _write_text_page(
    workbook: gpt.GPWorkbook,
    page: dict,
    placeholders: dict[str, str],
    logo: bytes | None = None,
) -> None:
    """
    Writes one text page: its rows down the first column.

    Args:
        workbook (gpt.GPWorkbook): the workbook to add the sheet to.
        page (dict): the page's config (sheet name, widths, rows and whether
            text wraps, default true).
        placeholders (dict[str, str]): text for the page placeholders.
        logo (bytes | None): PNG image to show on the page, or None.
    """
    text_format = workbook.add_format({**_FONT, "text_wrap": page.get("wrap", True)})
    heading_format = workbook.add_format({**_FONT, "bold": True})
    link_format = workbook.add_format(_LINK_FONT)
    worksheet = workbook.add_worksheet(page["sheet"])
    set_column_widths(worksheet, page["widths"])
    if logo:
        _insert_logo(worksheet, logo)

    row = 0
    for item in fill_placeholders(page["rows"], placeholders):
        if "blank" in item:
            row += item["blank"]
            continue
        if "url" in item:
            worksheet.write_url(
                row, 0, item["url"], link_format, string=item.get("link", item["url"])
            )
        elif "heading" in item:
            worksheet.write_string(row, 0, item["heading"], heading_format)
        else:
            worksheet.write_string(row, 0, item["text"], text_format)
        row += 1


def _insert_logo(worksheet: object, logo: bytes) -> None:
    """
    Inserts the logo as a decorative image, scaled to a fixed width with its
    proportions kept, whatever resolution the image records.

    Args:
        worksheet (object): the worksheet to insert the image into.
        logo (bytes): the PNG image.
    """
    image = Image(BytesIO(logo))
    shown_width_px = image.width * _SCREEN_DPI / image.x_dpi
    scale = _LOGO_WIDTH_PX / shown_width_px
    worksheet.insert_image(
        _LOGO_CELL,
        "logo.png",
        {
            "image_data": BytesIO(logo),
            "x_scale": scale,
            "y_scale": scale,
            "decorative": True,
            "object_position": 2,
        },
    )


def _write_contents_page(
    workbook: gpt.GPWorkbook, page: dict, table_titles: dict[str, str]
) -> None:
    """
    Writes the Contents page: a bordered table of each table sheet's name and
    title.

    Args:
        workbook (gpt.GPWorkbook): the workbook to add the sheet to.
        page (dict): the page's config (sheet name, title, headers, widths).
        table_titles (dict[str, str]): table sheet name mapped to its title.
    """
    heading_format = workbook.add_format({**_FONT, "bold": True})
    header_format = workbook.add_format({**_FONT, "bold": True, **_CELL_BORDER})
    cell_format = workbook.add_format({**_FONT, **_CELL_BORDER})
    worksheet = workbook.add_worksheet(page["sheet"])
    set_column_widths(worksheet, page["widths"])

    worksheet.write_string(0, 0, page["title"], heading_format)
    header_row = 2
    worksheet.add_table(
        header_row,
        0,
        header_row + len(table_titles),
        1,
        {
            "name": "contents_table",
            "style": None,
            "autofilter": False,
            "columns": [
                {"header": header, "header_format": header_format}
                for header in page["headers"]
            ],
        },
    )
    for row, (sheet, title) in enumerate(table_titles.items(), start=header_row + 1):
        worksheet.write_string(row, 0, sheet, cell_format)
        worksheet.write_string(row, 1, title, cell_format)
