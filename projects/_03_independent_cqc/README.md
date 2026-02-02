# Independent CQC

This project estimates the number of filled posts in the adult social care sector in England, at the level of each CQC-registered location.

## Processes in this module

- **Merge data** from various sources.
- **Clean data** by:
  - Filtering irrelevant data
  - Removing duplicated data
  - Handling nulls
  - Identifying outliers
- **Impute data** to populate nulls for locations with at least one known ASCWDS or PIR submission.
- **Feature engineering** to prepare the features required for the modelling process.
- **Model** predictions for care homes, non-residential locations and to convert PIR data from 'people' to 'filled posts'.
- **Estimate filled posts** to combine known submissions with imputed and modelled predictions to attribute a filled post estimate to all CQC-registered locations throughout time.
- **Estimate filled posts by job role** breaks the overall filled post estimate down into each individual job role collected in ASCWDS.
- **Validations** for each process using [PyDeequ](https://github.com/awslabs/deequ) to ensure data quality and schema consistency.

The outputs are used to inform policy, workforce planning, and sector-level reporting.
