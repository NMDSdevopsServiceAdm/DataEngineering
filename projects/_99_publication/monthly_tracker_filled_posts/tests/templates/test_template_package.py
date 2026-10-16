"""The template file itself: nothing personal or fragile, and how it opens."""

import re
import zipfile

import openpyxl

from projects._99_publication.monthly_tracker_filled_posts.fargate.utils import (
    diagnostics_cell_map as cell_map,
)
from projects._99_publication.monthly_tracker_filled_posts.tests.templates import (
    template_support as support,
)
from projects._99_publication.monthly_tracker_filled_posts.tests.templates.template_support import (
    NS,
)

EXPECTED_THEME_NAME = "Custom 1"
EXPECTED_ACCENTS = {"accent1": "005EB8", "accent2": "008C95"}
CREATOR = "{http://purl.org/dc/elements/1.1/}creator"
LAST_MODIFIED_BY = (
    "{http://schemas.openxmlformats.org/package/2006/metadata/core-properties}"
    "lastModifiedBy"
)
# A drive path, a file URL, or a user's home folder in any part of the package.
LOCAL_PATH = re.compile(r"[A-Za-z]:[\\/]Users[\\/]|file:///|/Users/|/home/")


class TestTemplatePackage:
    def test_has_no_external_links(self, package: zipfile.ZipFile):
        """Opening the template never asks to update links to other workbooks."""
        external_parts = [n for n in package.namelist() if "externalLink" in n]
        workbook_xml = package.read("xl/workbook.xml").decode()

        assert external_parts == []
        assert not re.search(r"<externalReferences", workbook_xml)

    def test_has_no_drawing_shapes(self, package: zipfile.ZipFile):
        """openpyxl drops shapes on save, so notes are cells and no shape remains."""
        shapes = [
            sheet.name
            for sheet in cell_map.DIAGNOSTICS_SHEETS
            for tag in ("xdr:sp", "xdr:grpSp", "xdr:pic")
            if support.drawing(package, sheet.name)[0].findall(f".//{tag}", NS)
        ]

        assert shapes == [], "Sheets that still hold shapes"

    def test_has_no_author_names(self, package: zipfile.ZipFile):
        """The repo is public: the document properties name nobody."""
        core = support.read_xml(package, "docProps/core.xml")
        names = [
            (core.findtext(tag) or "").strip() for tag in (CREATOR, LAST_MODIFIED_BY)
        ]

        assert names == ["", ""]

    def test_has_no_custom_properties_or_printer_settings(
        self, package: zipfile.ZipFile
    ):
        """The sensitivity label and printer settings are removed from the package."""
        extra_parts = [
            part
            for part in package.namelist()
            if part == "docProps/custom.xml" or "printerSettings" in part
        ]

        assert extra_parts == []

    def test_has_no_local_paths(self, package: zipfile.ZipFile):
        """Excel records where a file was saved; no such path may remain."""
        parts_with_paths = [
            part
            for part in package.namelist()
            if LOCAL_PATH.search(package.read(part).decode("latin-1"))
        ]

        assert parts_with_paths == []

    def test_theme_is_custom_1_with_sfc_accents(self, package: zipfile.ZipFile):
        """The Skills for Care colours survive in the workbook theme."""
        scheme = support.read_xml(package, "xl/theme/theme1.xml").find(
            ".//a:clrScheme", NS
        )

        accents = {
            name: scheme.find(f"a:{name}/a:srgbClr", NS).get("val")
            for name in EXPECTED_ACCENTS
        }
        assert (scheme.get("name"), accents) == (EXPECTED_THEME_NAME, EXPECTED_ACCENTS)

    def test_opens_on_the_first_sheet(self, workbook: openpyxl.Workbook):
        """The template opens on Filled posts, with its tab first in view."""
        assert (workbook.index(workbook.active), workbook.views[0].firstSheet) == (0, 0)

    def test_every_tab_is_scrolled_to_the_top_with_a1_selected(
        self, workbook: openpyxl.Workbook
    ):
        """No tab opens scrolled down or with another cell selected."""
        not_at_a1 = [
            ws.title
            for ws in workbook
            if ws.sheet_view.topLeftCell not in (None, "A1")
            or any(
                (sel.activeCell, str(sel.sqref)) != ("A1", "A1")
                for sel in ws.sheet_view.selection
            )
        ]

        assert not_at_a1 == []

    def test_only_the_first_tab_is_selected(self, workbook: openpyxl.Workbook):
        """Grouped tabs would make an edit change several sheets at once."""
        selected_tabs = [ws.title for ws in workbook if ws.sheet_view.tabSelected]

        assert selected_tabs == [cell_map.FILLED_POSTS.name]
