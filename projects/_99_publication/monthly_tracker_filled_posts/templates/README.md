# Workbook templates

Binary `.xlsx` templates opened by the publication workbook jobs.

- Commit data-scrubbed copies only: no real figures and no external links.
- Changes go through a PR by a developer, from a data-scrubbed `.xlsx` supplied by an analyst. Update the cell map and its tests in the same PR.
- Git can't diff a binary file, so describe what changed in the PR.
- The publication Dockerfile copies this folder with no trailing slash on the `COPY` source. A trailing slash would stop edits here from triggering an image rebuild.

## `diagnostics_template.xlsx`

Cell map: `fargate/utils/diagnostics_cell_map.py`. Tests: `tests/templates/`. The map lists the input cells the writer writes (including dates, labels, headings, % change values and the dated notes line); everything else is template and is never written.

- Sheets are undated (`Comp SfC and CT - short term`, `- medium term`, `- long term`, `No filters - long term`). The writer writes their `B7` heading and date range, and the `% change since ...` headings above the % change columns (`E` and `N`). The long-term sheets start at the first import date with data, and workbook dates are the import date minus one month.
- The "Number of locations" cells take the latest row per group: every row in a term's period shares the same count.
- Percentage changes are written by the writer from pipeline columns, not calculated in the workbook: `Filled posts` `J:N` and the `Comp prev publication` difference block `D:H`. Which pipeline columns fill them is still to be confirmed.
- Separate tickets are needed for the `No filters - long term` Capacity Tracker posts (`D`, `M`) and location counts (`G`, `P`), which need new pipeline columns (counts and sums of all Capacity Tracker data at each import date), and for the "Compare previous publications" tables.
- Slots are provisioned in advance and left blank until filled: `Filled posts` annual tables have 13 rows each (3 spare years), monthly blocks 13 rows; `Comp prev publication` blocks have 37 rows; the comparison sheets have 13, 25, 34 and 34 rows. Add rows, extend the chart series and update the cell map when the spare years run out.
- The template has no formulas: every value, including the `Filled posts` totals (`C:D`) and the `Comp prev publication` difference block (`B:H`), is written by the writer.
- Chart % change axes have no fixed maximum or major unit and a minimum of -1%. Category axes are automatic so unfilled slots take no space. Charts carry no per-point data label overrides: openpyxl loses them on save.
- openpyxl also drops these chart elements on save: the rounded-corners setting (charts then get rounded corners), the show-`#N/A`-as-empty setting, the auto-title setting, and the chart style and colour parts. Check charts by eye after writing, or patch the saved chart XML.
- Input cells need a font and number format (`CELL_STYLES` in the cell map). Excel keeps a column's style for cells that match it, and openpyxl does not apply a column's style to cells it writes, so the writer uses `write_input_cell` (`fargate/utils/diagnostics_workbook_utils.py`), which gives a new cell its column's style.
- Strip document metadata (author names, sensitivity label, printer settings) before committing a template, and don't save it in Excel afterwards: saving restores them. A test checks this.
