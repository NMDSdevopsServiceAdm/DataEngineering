---
name: sagemaker
description: Use when creating or editing a Jupyter notebook for the `data-engineering-notebook` SageMaker instance, or when asked to "compare two outputs", "diff old vs new dataset", "score a rolling-average duration", "write an ad-hoc notebook", or "plot/chart/visualise" results in a notebook. Provides committed notebook templates for common SageMaker tasks, which double as examples of how to structure ad-hoc notebooks in this repo.
---

# SageMaker notebooks

Ad-hoc analysis runs as Jupyter notebooks on the `data-engineering-notebook` SageMaker instance (launch steps in `NOTEBOOKS.md`). The instance has Polars and matplotlib installed by its lifecycle config (`conda_python3` kernel; Polars comes from conda, so avoid very new Polars APIs), but notebooks don't import this repo (`projects/`, `utils/`) — they stay self-contained. The repo is cloned onto the instance only to deliver the templates (see below).

Claude can't reach AWS or the instance. Claude writes the `.ipynb`; the user gets it onto the instance, runs it and stops the instance. Never run a notebook against AWS from here.

## Start: does this work need a branch?

Ask first: "Does this need its own dev environment (branch data copied from main)?" A pushed branch builds its own buckets and copies main's datasets in (`copy-main-data` in `.circleci/config.yml`); an unpushed one has none.

- **Yes:** create the branch (`<trello-number>-<short-description>`, ≤16 characters), then `git commit --allow-empty -m "<trello-number> - Start branch"` and push it (`commit-push`), so the environment builds while the notebook is written.
- **No** (e.g. reading main's datasets only): no branch or commit.

## Templates

They live in `notebooks/sagemaker/` — outside `.claude/`, which JupyterLab hides, so they are visible in the cloned repo on the instance.

- `notebooks/sagemaker/sagemaker_setup.ipynb` — the bootstrap notebook: clones the repo on the instance (first run) or updates it (later runs), then copies the latest templates into `~/SageMaker/templates/`.
- `notebooks/sagemaker/compare_outputs.ipynb` — diff a source dataset against a compare dataset (e.g. the same output before and after a code change): schema, row counts, row-level and per-column differences on key columns, written to CSV.
- `notebooks/sagemaker/compare_before_after_filter.ipynb` — compare a dataset before and after a filter (matched on keys), split by whether each row is in the filtered values: rows nulled, trendline over time, value distribution (summary stats + histogram), effect on the final estimates column.
- `notebooks/sagemaker/interpolation_durations.ipynb` — score a rolling-average duration on imputed data: jumpiness and accuracy against known values per split and status.

Users open a template from `~/SageMaker/templates/`, **Save As** into their own folder and edit only the parameters cell. Never edit a committed template for a one-off run. First-time setup and instance launch are in `NOTEBOOKS.md`.

To try a template before it merges, push the branch and set `BRANCH` in `sagemaker_setup.ipynb` to it (it only sees pushed commits).

**Stop the instance when finished** — it is billed while running. The lifecycle config also stops it after an hour idle, as a backstop.

## Structure of an ad-hoc notebook

Follow the templates:

1. **Title cell** (markdown): `# ` title, what it does, "Before running", and a reminder to stop the notebook instance.
2. **Parameters**: a markdown cell explaining each parameter, then **one** code cell tagged `parameters` holding every S3 path, setting and column name — including names derived from others (e.g. `f"{VALUE_COLUMN}_before"`). No path or column name is written anywhere else. Include `STORAGE_OPTIONS = None` when Polars reads S3 directly.
3. **Setup**: imports and display settings. A bare `## Setup` heading is enough here.
4. **Analysis sections**: for each, a markdown cell (what it does, how to read the result) then a code cell. **Never two code cells in a row** — every code cell has a markdown cell immediately before it (tested). Several analyses in one section need a markdown line each.
5. **Results**: small summary tables last.

## Rules

- **Lazy.** `pl.scan_*`, never `pl.read_*`; select only needed columns early; `.collect()` or `sink_*` once, on results that are small (aggregates, counts). See CLAUDE.md "Scale is the top constraint"; check `polars-streaming-check` and `over-vs-join` before relying on streaming or rewriting `.over()`.
- **Concise.** No dead or commented-out code, no empty or duplicate cells, as few words as possible. Functions only for logic worth naming; plain code otherwise.
- **Documented.** Markdown before every code cell; explain how to read each output, not just what the code does.
- **Column names are constants in the parameters cell.** This is an exception to CLAUDE.md's `utils/column_names` rule, because the repo isn't importable on the instance. Define each name once there; never inline a column-name string in analysis cells.
- **No saved outputs** in committed notebooks (clear all outputs, plots included, before committing), and no AWS account IDs, keys or ticket-specific bucket names — use `<bucket>`-style placeholders.
- **Writes** only to the output location named in the parameters cell; say so in the title cell.
- **Plots welcome.** Use matplotlib where a chart clarifies a result (trends, distributions); see the `dataviz` skill for chart design. Import `matplotlib.pyplot as plt` in Setup. Plot collected aggregates (`.to_list()`), never raw rows; keep the table alongside. Give every chart a title and labelled axes, use the same colours for the same series across a notebook, and end the cell with `plt.show()`.
- **Output.** `print(...)` when a cell shows several tables; a bare last expression for a single one.
- **Re-scanning is fine.** Each section may scan the datasets again; don't materialise a joined intermediate just to avoid it.
- **Example values** (job roles, column names) are placeholders — keep them neutral and say in the parameters cell to edit them.

## Writing the `.ipynb`

No notebook editor is available, so write nbformat-4 JSON (a short throwaway script that builds the cell list and `json.dumps` it works well; don't commit the script). Match an existing template's layout: 1-space indent, trailing newline, `conda_python3` kernelspec, `cell-00`, `cell-01`, … ids, and the `parameters` tag on the parameters cell.

## Adding or changing a template

1. Add its name to `EXPECTED_TEMPLATES` in `tests/sagemaker/test_notebook_templates.py` — the tests fail until it exists and meets the rules above. Any other `.ipynb` in the folder is checked against the same rules, registered or not.
2. Write it in `notebooks/sagemaker/`, then list it under **Templates** here (`TestSkillMd` fails until the list and the folder match).

```bash
uv run pytest tests/sagemaker -v
```

The tests check structure only; they don't run the notebooks. Before handing a notebook over, run every code cell locally with `uv run python` against small synthetic parquet files in a temp folder, overriding only the parameters-cell values (the repo environment has Polars; nothing touches AWS). The user then confirms it on the instance against a small dataset.
