# Maintaining the monthly tracker workbooks

The monthly tracker publication builds two Excel workbooks for each run. Their layout and wording live in files under `templates/`, not only in code, so changes follow the steps below.

For the template file rules (what to scrub, binary diffs, the image rebuild) see [templates/README.md](templates/README.md). For where the workbooks fit in the monthly cycle see [ARCHIVING_AND_PUBLICATION.md](../../ARCHIVING_AND_PUBLICATION.md) and [update_run_log_runbook.md](../../../scripts/update_run_log_runbook.md).

## The two workbooks

| | Assessment workbook | Data download workbook |
|---|---|---|
| Purpose | Internal review before a run is approved | Public statistics download |
| Built by | `fargate/_02_assessment.py` (every run) | `fargate/_03_output.py` (selected run only) |
| File name | `monthly_tracking_summary_<archive_date>_<run_number>.xlsx` | `monthly_tracker_data_download_<archive_date>_<run_number>.xlsx` |
| S3 folder | `domain=99_publication/dataset=monthly_tracker_filled_posts_02_assessment_workbook/` | `domain=99_publication/dataset=monthly_tracker_filled_posts_03_output_workbook/` |
| Layout from | `templates/assessment_template.xlsx`, opened and filled | gptables table sheets, plus pages written from `templates/output_workbook_pages.yaml` |
| Cell map / config | `fargate/utils/assessment_cell_map.py` | `fargate/utils/output_workbook_utils.py` |
| Writer | `fargate/utils/assessment_workbook_utils.py` | `fargate/utils/output_workbook_utils.py`, `fargate/utils/output_workbook_pages.py` |
| Wording in code | `fargate/utils/assessment_sheet_text.py` | `fargate/utils/output_workbook_text.py` |

Both S3 folders are in the datasets bucket and expire files after 60 days (`terraform/pipeline/s3.tf`). Both are excluded from the publication Glue crawler.

## Who changes what

- Analysts decide the content and supply a data-scrubbed `.xlsx` or the new wording.
- A developer makes the change in a PR. Nobody edits a template or YAML file on S3 or in the container: the files in the repo are the source.
- Changing anything under `templates/` rebuilds the publication image. Keep each change to one PR.

## Change the assessment template

The cell map lists every cell the writer fills (`InputRange`) plus the labels, charts and number formats the template must hold. Everything else in the template is never written. The writer fails if the template and cell map disagree.

1. Get a data-scrubbed `.xlsx` from the analyst with the change made in Excel.
2. Remove document metadata before committing: author names, sensitivity label, printer settings, external links, drawing shapes. Don't re-save the file in Excel afterwards, as that restores them. `tests/templates/test_template_package.py` checks each of these.
3. Replace `templates/assessment_template.xlsx` with the scrubbed file.
4. Update `assessment_cell_map.py` to match: input ranges, fixed `labels`, chart counts, series name cells, slot counts, number formats.
5. Run the template and workbook tests (see [Check a change](#check-a-change)).
6. Describe what changed in the PR: git can't diff a binary file.
7. Open the output of a real run in Excel and look at the charts. openpyxl drops some chart elements on save (listed in the templates README), and only Excel shows the effect.

Don't write formulas into the template. It has none: the writer writes every value, including totals and differences.

## Roll the template slots forward

The template has blank rows provisioned in advance. Constants in `assessment_cell_map.py`: `MONTHLY_SLOTS` (13 rows: short term sheet and the `Filled posts` monthly blocks), `MEDIUM_TERM_SLOTS` (25) and `LONG_TERM_SLOTS` (34, the long term and no filters sheets, with 3 spare years). The `Filled posts` annual tables (13 rows) and the `Comp prev publication` blocks (37 rows) are sized in the cell map's input ranges.

When the spare slots run out:

1. Add rows in the template and extend each chart's series to cover them.
2. Raise the slot constant and the affected input ranges in the cell map.
3. Run the template tests: they check that each series spans the full window and every input slot can be filled.

## Add a comparison sheet to the assessment workbook

1. Add the sheet to the template, with its charts.
2. Add a `SheetMap` to `assessment_cell_map.py`. `_comparison_sheet` builds the standard care homes (`B:F`) and non-residential (`K:O`) layout from a first and last row and the dataset columns that feed it. Use `SheetMap` directly for a different layout.
3. Add it to `ASSESSMENT_SHEETS`: the writer and the template tests only know the sheets listed there.
4. Add the sheet's values to `assessment_sheet_values.py` and its titles or notes to `assessment_sheet_text.py`.
5. Update the expected sheet names in `tests/templates/test_template_cells.py`.

## Change the data download's text

Frontpage, Introduction, Revisions, Definitions and Contents come from `templates/output_workbook_pages.yaml`. Each page is an ordered list of rows: `heading`, `text`, `link` (with `url`), bare `url`, or `blank` (a number of empty rows). `{latest_period}` and `{publication_date}` are filled in when the workbook is built. Column widths are set per page under `widths`. A page wraps its text unless it sets `wrap: false`.

- **Cover and notes wording:** edit the YAML.
- **Add a revision note:** add a `text` row under `revisions`.
- **Table titles:** `TABLE_TITLES` in `output_workbook_text.py`. Contents is built from them.
- **Suppression and not-published labels (`[s]`, `[x]`):** `SUPPRESSED_LABEL` and `NOT_PUBLISHED_LABEL` in `output_workbook_text.py`. The tables and the reconciliation check both use them. The Definitions page in the YAML explains them in plain text, so edit it to match.
- **Publication date:** the 15th of the latest period's month, moved to the next working day when it is a weekend or an England bank holiday (`get_publication_date` in `output_workbook_utils.py`).
- **Fonts and sheet styling:** `_THEME_CONFIG` in `output_workbook_utils.py` and the constants in `output_workbook_pages.py`.

A new or renamed table sheet is a code change to `output_workbook_utils.py` (`TABLE_NAMES`, `prepare_download_tables`) and to the reconciliation check in `fargate/_04_reconcile_output.py`, not a YAML change.

### Frontpage logo

The Frontpage logo is read from `<workspace>-pipeline-resources/publication/logo/skills_for_care_logo.png`, not from the repo. To change it, replace that object. `Publication-Output` fails if the logo is missing or empty, so upload it before the first run in a new environment.

## Check a change

From the repo root:

```bash
uv run pytest projects/_99_publication/monthly_tracker_filled_posts/tests/templates
uv run pytest projects/_99_publication/monthly_tracker_filled_posts/tests/fargate/utils/test_assessment_cell_map.py projects/_99_publication/monthly_tracker_filled_posts/tests/fargate/utils/test_assessment_workbook_utils.py
uv run pytest projects/_99_publication/monthly_tracker_filled_posts/tests/fargate/utils/test_output_workbook_pages.py projects/_99_publication/monthly_tracker_filled_posts/tests/fargate/utils/test_output_workbook_utils.py
```

Run the first for any template change, the second for a cell map change and the third for download workbook changes. They don't open Excel: before the first release after a change, check the charts and layout in Excel on a branch run's workbook (find it under the S3 folders above in the branch's datasets bucket).
