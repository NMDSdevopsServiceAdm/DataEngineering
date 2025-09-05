variable "lambda_dir" {
  default = "lambdas"
}

variable "AWS_ACCOUNT_ID" {
  type = string
}

variable "CIRCLE_BRANCH" {
  type = string
}

group "all" {
  targets = ["create_dataset_snapshot", "check_dataset_equality", "delta_cqc"]
}

# group "ingest" {
#   targets = ["delta_cqc"]
# }

# target "create_dataset_snapshot" {
#   context = "."
#   dockerfile = "./lambdas/create_dataset_snapshot/Dockerfile"
#   tags = ["${AWS_ACCOUNT_ID}.dkr.ecr.eu-west-2.amazonaws.com/lambda/create-snapshot:${CIRCLE_BRANCH}"]
#   platforms = ["linux/amd64"]
#   no-cache = true
# }

# target "check_dataset_equality" {
#   context = "."
#   dockerfile = "./lambdas/check_dataset_equality/Dockerfile"
#   tags = ["${AWS_ACCOUNT_ID}.dkr.ecr.eu-west-2.amazonaws.com/lambda/check-datasets-equal:${CIRCLE_BRANCH}"]
#   platforms = ["linux/amd64"]
#   no-cache = true
# }

target "delta_cqc" {
  context = "."
  dockerfile = "./projects/_01_ingest/cqc_api/fargate/Dockerfile"
  # tags = ["${AWS_ACCOUNT_ID}.dkr.ecr.eu-west-2.amazonaws.com/fargate/cqc:${CIRCLE_BRANCH}"]
  platforms = ["linux/amd64"]
  no-cache = true
}

