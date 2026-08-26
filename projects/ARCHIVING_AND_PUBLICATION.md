# Archiving and publication

## Archiving

### Frequency
Our pipeline for estimating filled posts by job role runs four times per month.
The first of which is on or as close as possible to the 1st of each month.
Each run calculates estimates across all time.

### Holding
Each of these runs is archived into a holding bucket.
Each archive is partioned on run date and a run number. Run number always increases over time.
A single archive consists of three datasets; job role estimates, metadata and current geographies.
We only archive specific columns in each dataset.

## Publication

### Preparation
The archived output from the 1st run of each month is assessed for publication readiness.
Assessing the data for publication requires merging the three archived datasets, creating filters for external benchmarking and splitting the data into two sets; one for publication and one for assessment.
We filter the data to import dates from April 2020 onwards, then each quarter from several years ago up to the start of the previous financial year, then each month for within the previous and current financial years.
The assessment dataset is going to be benchmarked against external data, therefore we only want to include rows that have a value in the external data (all rows receive a filled posts estimate from our pipeline).
Each of those datasets requires aggregating into high level groupings by region, service and job role.

### Assessment of readiness
We look at the cumulative percentage change in filled posts per month over time.
We benchmark our trend against external data.
We investigate and resolve irregularties until we are satisfied the outputs are accurate.
We must publish an output on the 15th of each month at 09:30, unless it's outside working time.

### Long term storage
When the data for publication is signed-off then the archived output it used is copied into a long-term storage bucket.
Archived data in the holding bucket that is more than two months old is removed.
