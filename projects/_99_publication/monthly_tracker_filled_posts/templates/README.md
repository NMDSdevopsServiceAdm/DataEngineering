# Workbook templates

Binary `.xlsx` templates opened by the publication workbook jobs.

- Commit data-scrubbed copies only: no real figures and no external links.
- Changes go through a PR by a developer, from a data-scrubbed `.xlsx` supplied by an analyst. Update the cell map and its tests in the same PR.
- Git can't diff a binary file, so describe what changed in the PR.
- The publication Dockerfile copies this folder with no trailing slash on the `COPY` source. A trailing slash would stop edits here from triggering an image rebuild.
