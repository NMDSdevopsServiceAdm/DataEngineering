resource "aws_iam_role" "sfc_glue_service_iam_role" {
  name               = "${terraform.workspace}-glue_service_iam_role"
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

resource "aws_iam_role_policy_attachment" "AWSGlueServiceRole_policy_attachment" {
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
  role       = aws_iam_role.sfc_glue_service_iam_role.name
}
