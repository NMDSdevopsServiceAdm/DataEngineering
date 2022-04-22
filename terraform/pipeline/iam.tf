resource "aws_iam_role" "glue_service_iam_role" {
  name               = "glue_service_iam_role"
  assume_role_policy = data.aws_iam_policy_document.glue_service_assume_role_policy.json
}

data "aws_iam_policy_document" "glue_service_assume_role_policy" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}