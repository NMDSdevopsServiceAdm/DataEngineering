variable "lambda_dir" {
  default = "lambdas"
}

variable "AWS_ACCOUNT_ID" {
  type = string
}

variable "SANITISED_CIRCLE_BRANCH" {
  type = string
}

group "all" {
  targets = ["delta_cqc", "_03_independent_cqc", "_03_independent_cqc_model"]
}

# group "ingest" {
#   targets = ["delta_cqc"]
# }

target "delta_cqc" {
  context = "."
  dockerfile = "./projects/_01_ingest/cqc_api/fargate/Dockerfile"
  tags = ["${AWS_ACCOUNT_ID}.dkr.ecr.eu-west-2.amazonaws.com/fargate/cqc:${SANITISED_CIRCLE_BRANCH}"]
  platforms = ["linux/amd64"]
  no-cache = true
}

target "_03_independent_cqc" {
  context = "."
  dockerfile = "./projects/_03_independent_cqc/Dockerfile_and_requirements/Dockerfile"
  tags = ["${AWS_ACCOUNT_ID}.dkr.ecr.eu-west-2.amazonaws.com/fargate/03_independent_cqc:${SANITISED_CIRCLE_BRANCH}"]
  platforms = ["linux/amd64"]
  no-cache = true
}

target "_03_independent_cqc_model" {
  context = "."
  dockerfile = "./projects/_03_independent_cqc/_04_model/fargate/Dockerfile"
  tags = ["${AWS_ACCOUNT_ID}.dkr.ecr.eu-west-2.amazonaws.com/fargate/03_independent_cqc_model:${SANITISED_CIRCLE_BRANCH}"]
  platforms = ["linux/amd64"]
  no-cache = true
}
