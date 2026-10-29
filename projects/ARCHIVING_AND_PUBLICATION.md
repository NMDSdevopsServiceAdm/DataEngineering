# Archiving and publication

**WORK IN PROGRESS**

**THIS IS A TEMPLATE TO BE UPDATED OVER TIME.**

## Archiving

### Frequency
Our pipeline for estimating filled posts by job role runs four times per month.
The first of which is on or as close as possible to the 1st of each month.
Each run calculates estimates across all time.

### Holding
Each of these runs is archived into a holding bucket.
Each archive is partitioned on run date and a run number. Run number always increases over time.
A single archive consists of two datasets; job role estimates and metadata.
We only archive specific columns in each dataset.

## Publication

### Preparation
The `Publication-Assessment` step function starts automatically when the estimates pipeline succeeds, and prepares the run it has just archived.
Preparation merges the archived job role estimates and metadata, and filters the data to import dates from April six financial years ago onwards, then each quarter up to the start of the previous financial year, then each month for within the previous and current financial years.
The prepared data is stored per run, partitioned on archive date and run number, so a run can still be published after later runs have been archived.
The assessment data is then built from the prepared run, with filters for external benchmarking.
The assessment columns are going to be benchmarked against external data, therefore we only want to include rows that have a value in the external data (all rows receive a filled posts estimate from our pipeline).
The assessment data is aggregated into high level groupings by region, service and job role.
The assessment data is partitioned on archive date and run number, so every assessed run is kept. Rebuilding a run replaces that run's partition only.

### Assessment of readiness
We look at the cumulative percentage change in filled posts per month over time.
We benchmark our trend against external data.
We investigate and resolve irregularities until we are satisfied the outputs are accurate.
Assessing a run and approving it are separate: assessing is the review, approving is our decision that the run is accurate.
We record approval by setting `approved` for the run in the run log with `scripts/update_run_log.sh` (see `scripts/update_run_log_runbook.md`).
We must publish an output on the 15th of each month at 09:30, unless that is outside working time, upon which we will publish as soon as possible afterwards.

### Publishing a run
Several runs can be approved in a month, so choosing the one to publish is a separate decision and a separate run log update.
Select it with `scripts/update_run_log.sh`, setting `selected_for_publication`.
Only an approved run archived since the 1st of the month can be selected, and selecting another run moves the selection.
The script can then start the `Publication-Output` step function with `{"run_number": N}`; otherwise start it by hand with that input.
It builds the public data from that run's prepared data: the publication summary behind the Tableau output and the T0, T1 and T2 data-download tables.
Public data is only built for a selected run, so the step function fails without a `run_number`.

If a second estimates run finishes while `Publication-Assessment` is still running for the previous run, the assessment step may read the other run's prepared data.
Start `Publication-Assessment` again with that run's `run_number` to build its assessment data.

### Long term storage
When a run is selected for publication the archived output used to make it is copied into a long-term storage bucket.
Archived data in the holding bucket that was never selected is removed after about two months.
