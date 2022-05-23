resource "aws_iam_policy" "glue_job_s3_data_engineering_policy" {
  name        = "${terraform.workspace}-glue_service_data_engineering_policy"
  path        = "/"
  description = "Iam policy for the ${var.script_name} job on workspace: ${terraform.workspace}"

  policy = jsonencode({

    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Effect" : "Allow",
        "Action" : [
          "s3:ListBucket",
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ],
        "Resource" : [
          "arn:aws:s3:::",
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_job_s3_policy_attachment" {
  policy_arn = aws_iam_policy.glue_job_s3_data_engineering_policy.arn
  role       = var.glue_role_arn
}
