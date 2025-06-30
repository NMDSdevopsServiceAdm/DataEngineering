# Ingest Capacity Tracker Care Home Pipeline

This Step Function defines the ingestion, cleaning, and validation process for the Capacity Tracker Care Home dataset.

## Overview

This pipeline is automatically triggered via an [Amazon EventBridge](https://docs.aws.amazon.com/eventbridge/latest/userguide/what-is-amazon-eventbridge.html) rule when a new CSV is added to a monitored S3 location. The pipeline ensures timely ingestion, cleaning, validation, and cataloging of updated care home data without the need for manual intervention.

The pipeline comprises several stages:

1. **Ingest Capacity Tracker Care Home Data**: Imports raw CSV data from S3 and stores it as Parquet files in S3.
2. **Clean the Data**: Applies cleaning transformations and stores the output in a cleaned dataset location.
3. **Validate Cleaned Data**: Runs data quality checks on both raw and cleaned datasets.
4. **Crawler Execution**: Glue crawlers update the metadata catalog after a successful or failed run to ensure downstream accessibility.

## Structure

The Step Function uses a parallel structure with error handling:

- **Parallel Branch**: Handles the ingest-clean-validate sequence.
- **Catch Block**: Catches any errors and sends a failure notification via Lambda before proceeding to crawler updates.
- **Success/Failure Path**: Ensures crawlers are run on both success and failure paths to update the Glue Data Catalog consistently.

## Tasks

| Step | Description | AWS Resource |
|------|-------------|--------------|
| Ingest Capacity Tracker Care Home | Loads raw data into S3 | `glue:startJobRun.sync` |
| Clean Capacity Tracker Care Home data | Cleans raw data and outputs to cleaned S3 location | `glue:startJobRun.sync` |
| Validate cleaned CT Care Home data | Validates cleaned datasets | `glue:startJobRun.sync` |
| Run validation crawler | Updates the Glue catalog for validation reports | `glue:startCrawler` |
| Run CT crawler | Updates the Glue catalog for care home datasets | `glue:startCrawler` |
| Publish error notification | Sends failure info to a Lambda function | `lambda:invoke.waitForTaskToken` |

## Error Handling

If any step within the ingest-clean-validate sequence fails, the pipeline:

1. Invokes a Lambda to notify stakeholders of the error.
2. Still proceeds with Glue crawler runs to ensure metadata consistency.
3. Ends in a `Fail` state if recovery is not possible.

## Outputs

- Cleaned Capacity Tracker Care Home dataset in S3.
- Validation reports for data quality assessment.
- Updated Glue Data Catalog entries for both raw and cleaned datasets.

## Parameters

This pipeline is dynamically configured using parameters:

- `${ingest_ct_care_home_job_name}`
- `${clean_ct_care_home_data_job_name}`
- `${validate_ct_care_home_cleaned_data_job_name}`
- `${dataset_bucket_uri}`
- `${ct_crawler_name}`
- `${data_validation_reports_crawler_name}`
- `${pipeline_failure_lambda_function_arn}`

These are typically injected at runtime by the infrastructure orchestration layer.

## Visual Representation

```{mermaid}
graph TD
    A[Ingest CT Care Home]:::format --> B[Clean CT Care Home]:::format
    B --> C[Validate Cleaned Data]:::format
    C --> D{Success?}:::format
    D -->|Yes| E[Run Validation Crawler]:::format
    E --> F[Run CT Crawler]:::format
    F --> G[Succeed]:::format
    D -->|No| H[Notify via Lambda]:::format
    H --> I[Run Validation Crawler When Failed]:::format
    I --> J[Run CT Crawler When Failed]:::format
    J --> K[Fail]:::format

    classDef format fill:#F5FAFD,stroke:#005EB8,stroke-width:2px;
```
