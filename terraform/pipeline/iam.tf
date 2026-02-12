resource "aws_iam_policy" "query_all_in_athena" {
  name        = "${terraform.workspace}-query-all-in-athena"
  path        = "/"
  description = "All read and list privileges for any athena resources"

  policy = templatefile("policy-documents/query-all-in-athena.json", { account_id = data.aws_caller_identity.current.account_id })
}

resource "aws_iam_group" "can_assume_admin_role" {
  count = local.workspace_prefix == "main" ? 1 : 0
  name  = "CanAssumeAdminRole"
}

resource "aws_iam_group_policy" "can_assume_admin_role_access_policy" {
  count  = local.workspace_prefix == "main" ? 1 : 0
  group  = aws_iam_group.can_assume_admin_role[0].name
  policy = data.aws_iam_policy_document.admin_role_access_policy[0].json
}

data "aws_iam_policy_document" "admin_role_access_policy" {
  count = local.workspace_prefix == "main" ? 1 : 0
  statement {
    actions   = ["sts:AssumeRole"]
    resources = [aws_iam_role.admin_role[0].arn]
  }
}

data "aws_iam_policy_document" "admin_role_assume_role_policy" {
  count = local.workspace_prefix == "main" ? 1 : 0
  statement {
    actions = ["sts:AssumeRole"]

    # This is a trust policy against the root user, it does not allow every user access to the role, they still need the `sts:AssumeRole` permission against their user
    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::344210435447:root"]
    }
  }
}

resource "aws_iam_role" "admin_role" {
  count              = local.workspace_prefix == "main" ? 1 : 0
  name               = "Admin"
  assume_role_policy = data.aws_iam_policy_document.admin_role_assume_role_policy[0].json

  tags = {
    HasProductionAccess = true
  }
}

resource "aws_iam_role_policy_attachment" "admin_role_administrator_access_policy_attach" {
  count      = local.workspace_prefix == "main" ? 1 : 0
  role       = aws_iam_role.admin_role[0].name
  policy_arn = "arn:aws:iam::aws:policy/AdministratorAccess"
}
