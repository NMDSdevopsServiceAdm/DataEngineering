resource "aws_iam_policy" "glue_job_s3_data_engineering_policy" {
  name        = "${terraform.workspace}-${var.script_name}-bucket_access_policy"
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
          "arn:aws:s3:::${var.resource_bucket.bucket_name}",
          "arn:aws:s3:::${var.datasets_bucket.bucket_name}",
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_job_s3_policy_attachment" {
  policy_arn = aws_iam_policy.glue_job_s3_data_engineering_policy.arn
  role       = var.glue_role.name
}
