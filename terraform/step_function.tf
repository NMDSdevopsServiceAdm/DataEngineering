resource "aws_sfn_state_machine" "master_state_machine" {
  name     = "MasterPipeline"
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
          "--source": "s3://sfc-data-engineering-raw/domain=ASCWDS/dataset=workplace/version=1.0.0/year=2022/month=03/day=18/import_date=20220318/"
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
      "Next": "Parallel"
    },
    "Parallel": {
      "Type": "Parallel",
      "Next": "Estimate 2021 Jobs",
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
                  "--destination": "s3://skillsforcare/step-function-testing/prepare_locations_test",
                  "--pir_source": "s3://sfc-data-engineering/domain=CQC/dataset=pir/version=0.0.3/ year=2020/month=03/day=31/import_date=20200331/",
                  "--workplace_source": "s3://sfc-data-engineering/domain=ASCWDS/dataset=workplace/version=0.0.1/",
                  "--cqc_provider_source": "s3://sfc-data-engineering/domain=CQC/dataset=providers-api/version=1.0.0/year=2022/month=03/day=01/import_date=20220301/",
                  "--cqc_location_source": "s3://sfc-data-engineering/domain=CQC/dataset=locations-api/version=1.0.0/year=2022/month=03/day=01/import_date=20220301/"
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
          "--destination": "s3://skillsforcare/step-function-testing/estimate_2021_jobs_test",
          "--prepared_locations_source": "s3://skillsforcare/step-function-testing/prepare_locations_test"
        }
      },
      "Next": "Run Data Engineering Crawler"
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
  name               = "AWSStepFunction-data-engineering-role"
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
  name        = "step_function_iam_policy"
  path        = "/"
  description = "IAM Policy for step functions"

  policy = jsonencode({

    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Effect" : "Allow",
        "Action" : [
          "glue:StartJobRun"
        ],
        "Resource" : [
          "*"
        ]
      },
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
        "Sid" : "VisualEditor0",
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
