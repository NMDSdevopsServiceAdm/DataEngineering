---
name: sagemaker
description: Use when creating or editing a Jupyter notebook for the `data-engineering-notebook` SageMaker instance, or when asked to "compare two outputs", "diff old vs new dataset", "score a rolling-average duration", or "write an ad-hoc notebook". Provides committed notebook templates for common SageMaker tasks, which double as examples of how to structure ad-hoc notebooks in this repo.
---

# SageMaker notebooks

Ad-hoc analysis runs as Jupyter notebooks on the `data-engineering-notebook` SageMaker instance (launch steps in `NOTEBOOKS.md`). The instance has Polars preinstalled (`conda_python3` kernel; the templates need Polars 1.24 or later for `nulls_equal` and were tested on 1.44) but notebooks don't import this repo (`projects/`, `utils/`) — they stay self-contained. The repo is cloned onto the instance only to deliver the templates (see below).

Claude can't reach AWS or the instance. Claude writes the `.ipynb`; the user gets it onto the instance, runs it and stops the instance. Never run a notebook against AWS from here.

## Templates

They live in `notebooks/sagemaker/` — outside `.claude/`, which JupyterLab hides, so they are visible in the cloned repo on the instance.

- `notebooks/sagemaker/sagemaker_setup.ipynb` — the bootstrap notebook: clones the repo on the instance (first run) or updates it (later runs), then copies the latest templates into `~/SageMaker/templates/`. Edit: `BRANCH` (and `REPO_URL`, `CLONE_DIR`, `TEMPLATES_DIR` if they change).
- `notebooks/sagemaker/compare_outputs.ipynb` — diff a source dataset against a compare dataset (e.g. the same output before and after a code change): schema, row counts, row-level and per-column differences on key columns, written to CSV. Edit: `SOURCE_PATH`, `COMPARE_PATH`, `OUTPUT_PATH`, `KEY_COLUMNS`, `SLICE_COLUMN`, `COMPARE_COLUMNS`.
- `notebooks/sagemaker/compare_before_after_filter.ipynb` — compare a dataset before and after a filter (matched on keys), split by whether each row is in the filtered values: rows nulled, trendline over time, value distribution (summary stats + histogram), effect on the final estimates column. Edit: `BEFORE_PATH`, `AFTER_PATH`, `KEY_COLUMNS`, `DATE_COLUMN`, `FILTER_COLUMN`, `FILTERED_VALUES`, `VALUE_COLUMN`, `ESTIMATE_COLUMN`.
- `notebooks/sagemaker/interpolation_durations.ipynb` — score a rolling-average duration on imputed data: jumpiness and accuracy against known values per split and status. Edit: `IMPUTED_DATA_PATH`, `ROLLING_AVERAGE_PERIOD`, `SCORE_FROM_DATE`, the column constants.

- `notebooks/sagemaker/model_metrics_history.ipynb` — metric history for the filled posts models across training runs: run counts, legacy and cross-validated scores in separate sections, a text trend per score, and change against the previous run and a rolling baseline with breach flags. Reads each run's files in the pipeline-resources `models/` folder. Edit: `MODELS_PATH`, `MODEL_NAMES`, `STEP_TOLERANCE`, `DRIFT_TOLERANCE`, `BASELINE_RUNS`, the column and metric constants.

To use one: open it from `~/SageMaker/templates/`, **Save As** into your own folder, edit **only the parameters cell**. Never work inside `~/SageMaker/templates/` or the clone — both are overwritten on every run — and don't edit a committed template for a one-off run.

## Getting the templates onto the instance

Start the instance (`NOTEBOOKS.md`) and open JupyterLab. `sagemaker_setup.ipynb` can't fetch itself, so the **first time only** put it on the instance by hand. In JupyterLab, **File → New → Terminal**, then run the command under "Notebook templates" in `NOTEBOOKS.md` (it uses `--branch main`; use `--branch <ticket-branch>` for a branch that hasn't merged yet).

All templates, including `sagemaker_setup.ipynb`, are now in `~/SageMaker/templates/`. (No git or GitHub access? Download `sagemaker_setup.ipynb` from GitHub and use **Upload files** into `~/SageMaker`.) Then:

1. Run `sagemaker_setup.ipynb` top to bottom at the start of each piece of work. It needs `git` and GitHub access on the instance (already set up). The first run clones the repo; later runs fetch and hard-reset to `BRANCH`, so the clone always matches GitHub and local edits in it are discarded.
2. Open the template you need from `~/SageMaker/templates/` and Save As into your own folder.
3. **Stop the instance when finished** — it is billed while running.

`BRANCH` is `main`. To try a template before it merges, push the branch and set `BRANCH` to it (the notebook only sees pushed commits). A template deleted from the repo isn't removed from `~/SageMaker/templates/`.

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
- **No saved outputs** in committed notebooks (clear all outputs before committing), and no AWS account IDs, keys or ticket-specific bucket names — use `<bucket>`-style placeholders.
- **Writes** only to the output location named in the parameters cell; say so in the title cell.
- Docstrings and comments describe what the code does, not what it replaced.
- **Tables, not plots.** Only Polars is known to be on the instance; don't assume matplotlib or other plotting libraries.
- **Output.** `print(...)` when a cell shows several tables; a bare last expression for a single one.
- **Re-scanning is fine.** Each section may scan the datasets again; don't materialise a joined intermediate just to avoid it.
- **Example values** (job roles, column names) are placeholders — keep them neutral and say in the parameters cell to edit them.

## Writing the `.ipynb`

No notebook editor is available, so write nbformat-4 JSON (a short throwaway script that builds the cell list and `json.dumps` it works well; don't commit the script). Match the existing templates:

- top level: `cells`, `metadata` (`kernelspec` `conda_python3`, `language_info`), `nbformat: 4`, `nbformat_minor: 5`; 1-space indent, trailing newline
- every cell has `cell_type`, `metadata`, `source` (list of lines ending `\n`) and an `id` (`cell-00`, `cell-01`, …)
- code cells also have `execution_count: null` and `outputs: []`; the parameters cell has `metadata: {"tags": ["parameters"]}`

## Adding or changing a template

1. Add its name to `EXPECTED_TEMPLATES` in `tests/sagemaker/test_notebook_templates.py` — the tests fail until it exists and meets the rules above. Any other `.ipynb` in the folder is checked against the same rules, registered or not.
2. Write it in `notebooks/sagemaker/`, then list it under **Templates** here (`TestSkillMd` fails until the list and the folder match).

```bash
uv run pytest tests/sagemaker -v
```

The tests check structure only; they don't run the notebooks. Before handing a notebook over, run every code cell locally with `uv run python` against small synthetic parquet files in a temp folder, overriding only the parameters-cell values (the repo environment has Polars; nothing touches AWS). The user then confirms it on the instance against a small dataset.
