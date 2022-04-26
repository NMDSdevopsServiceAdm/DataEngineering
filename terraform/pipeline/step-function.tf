resource "aws_sfn_state_machine" "ethnicity-breakdown-state-machine" {
  name     = "${terraform.workspace}-EthnicityBreakdownPipeline"
  role_arn = aws_iam_role.step_function_iam_role.arn
  type     = "STANDARD"


  definition = <<EOF
{
  "Comment": "A description of my state machine",
  "StartAt": "Ingest ASCWDS",
  "States": {
    "Ingest ASCWDS": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": {
        "JobName": "ingest_ascwds_dataset_job",
        "Arguments": {
          "--destination.$": "$.jobs.ingest_ascwds_dataset.destination",
          "--source.$": "$.jobs.ingest_ascwds_dataset.source"
        }
      },
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "Next": "Fail"
        }
      ],
      "Next": "Parallel",
      "ResultPath": null
    },
    "Parallel": {
      "Type": "Parallel",
      "Next": "Estimate 2021 Jobs",
      "ResultPath": null,
      "Branches": [
        {
          "StartAt": "Run ASCWDS Crawler",
          "States": {
            "Run ASCWDS Crawler": {
              "Type": "Task",
              "Parameters": {
                "Name": "data_engineering_ASCWDS"
              },
              "Resource": "arn:aws:states:::aws-sdk:glue:startCrawler",
              "End": true
            }
          }
        },
        {
          "StartAt": "Prepare Locations",
          "States": {
            "Prepare Locations": {
              "Type": "Task",
              "Resource": "arn:aws:states:::glue:startJobRun.sync",
              "Parameters": {
                "JobName": "prepare_locations_job",
                "Arguments": {
                  "--destination.$": "$.jobs.prepare_locations_job.destination",
                  "--pir_source.$": "$.jobs.prepare_locations_job.pir_source",
                  "--workplace_source.$": "$.jobs.prepare_locations_job.workplace_source",
                  "--cqc_provider_source.$": "$.jobs.prepare_locations_job.cqc_provider_source",
                  "--cqc_location_source.$": "$.jobs.prepare_locations_job.cqc_location_source"
                }
              },
              "End": true
            }
          }
        }
      ]
    },
    "Estimate 2021 Jobs": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": {
        "JobName": "estimate_2021_jobs_job",
        "Arguments": {
          "--destination.$": "$.jobs.estimate_2021_jobs_job.destination",
          "--prepared_locations_source.$": "$.jobs.estimate_2021_jobs_job.prepared_locations_source"
        }
      },
      "Next": "Run Data Engineering Crawler",
      "ResultPath": null
    },
    "Run Data Engineering Crawler": {
      "Type": "Task",
      "Parameters": {
        "Name": "data_engineering_DATA_ENGINEERING"
      },
      "Resource": "arn:aws:states:::aws-sdk:glue:startCrawler",
      "Next": "Success"
    },
    "Success": {
      "Type": "Succeed"
    },
    "Fail": {
      "Type": "Fail"
    }
  }
}
EOF
}


resource "aws_iam_role" "step_function_iam_role" {
  name               = "${terraform.workspace}-AWSStepFunction-data-engineering-role"
  assume_role_policy = data.aws_iam_policy_document.step_function_iam_policy.json
}

data "aws_iam_policy_document" "step_function_iam_policy" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

resource "aws_iam_policy" "step_function_iam_policy" {
  name        = "${terraform.workspace}-step_function_iam_policy"
  path        = "/"
  description = "IAM Policy for step functions"

  policy = jsonencode({

    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Effect" : "Allow",
        "Action" : [
          "xray:PutTraceSegments",
          "xray:PutTelemetryRecords",
          "xray:GetSamplingRules",
          "xray:GetSamplingTargets"
        ],
        "Resource" : [
          "*"
        ]
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "glue:StartCrawler",
          "glue:StartJobRun",
          "glue:GetJobRun"
        ],
        "Resource" : "*"
      }

    ]
  })
}

resource "aws_iam_role_policy_attachment" "AWSStepFunctionRole_data_engineering_policy_attachment" {
  policy_arn = aws_iam_policy.step_function_iam_policy.arn
  role       = aws_iam_role.step_function_iam_role.name
}
