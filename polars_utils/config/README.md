# PointBlank Validation using YAML configuration

This subdirectory provides a config-driven approach for validating datasets using [pointblank](https://github.com/rich-iannone/pointblank) and YAML-based validation rules.

## Getting Started

### 1. Add Dataset specification

To provide the latest metadata and dataset/report name for a new validated dataset:
- add the dataset details to the [datasets.yml](datasets.yml) config with the relevant name / version details which will determine the S3 path for the dataset and output reports, eg:
```yaml
# existing YAML config
datasets:
    # new dataset
    my_new_dataset_key:
        dataset: dataset_name_in_s3
        domain: CQC_perhaps
        version: 3.0.0
        report_name: data_quality_report_name_of_report
```



### 2. YAML Validation Config Structure
- create a new YAML config file (`.yml`) in the [config](.) directory describing the validation rules
- the filename should correspond to the `dataset` key in [datasets.yml](datasets.yml), eg. `dataset_name_in_s3`
- the config file should follow the structure below, including `null` for `dataset` (as this needs to be overwritten at runtime with the polars Dataframe) along with validation steps, thresholds etc:

```yaml
tbl: null
tbl_name: "friendly name for table in report"
label: "Descriptive label for the validation suite"
brief: "Optional brief description of the validation suite"
thresholds:
    warning: 1  # indicates a single record failure will throw a warning
actions:
    warning: "Text to print on error"  # or python callable
steps:
  # Data quality checks
  - col_vals_gt:
      columns: revenue
      value: 0
      na_pass: true
      brief: "Revenue values must be positive when present"

  # Temporal validation
  - col_vals_expr:
      expr:
        python: |
          pl.col("event_date").str.strptime(pl.Date, "%Y-%m-%d").is_not_null()
      brief: "Event dates must be valid YYYY-MM-DD format"
```

See the [pointblank YAML documentation](https://posit-dev.github.io/pointblank/user-guide/yaml-validation-workflows.html) for more details on available validation steps, or [locations_raw.yml](locations_raw.yml) as an example.

### 3. Include Validation in StepFunction
- Add the validation step as an ECS task to the state machine definition
- Define args as result variables in previous steps and then pass as a list of args to the task call
See `Validate Locations Api Raw` [IngestCQCAPIDelta-StepFunction](../../terraform/pipeline/step-functions/IngestCQCAPIDelta-StepFunction.json) as an example
