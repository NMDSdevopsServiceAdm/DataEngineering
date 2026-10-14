# Workbook templates

Binary `.xlsx` templates opened by the publication workbook jobs.

- Commit data-scrubbed copies only: no real figures and no external links.
- Changes go through a PR by a developer, from a data-scrubbed `.xlsx` supplied by an analyst. Update the cell map and its tests in the same PR.
- Git can't diff a binary file, so describe what changed in the PR.
- The publication Dockerfile copies this folder with no trailing slash on the `COPY` source. A trailing slash would stop edits here from triggering an image rebuild.

`workbook_pages.yaml` holds the wording and layout of the public download workbook's Frontpage, Introduction, Revisions, Definitions and Contents pages (headings, text, links, blank rows, column widths). Edit it in a PR; `workbook_pages.py` and its tests read it.
