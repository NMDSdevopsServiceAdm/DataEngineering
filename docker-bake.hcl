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
  targets = ["delta_cqc", "model_retrain", "model_preprocess", "model_predict", "_03_independent_cqc"]
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

target "model_retrain" {
  context = "."
  dockerfile = "./projects/_03_independent_cqc/_05_model/fargate/retraining/Dockerfile"
  tags = ["${AWS_ACCOUNT_ID}.dkr.ecr.eu-west-2.amazonaws.com/fargate/model-retrain:${SANITISED_CIRCLE_BRANCH}"]
  platforms = ["linux/amd64"]
  no-cache = true
}

target "model_preprocess" {
  context = "."
  dockerfile = "./projects/_03_independent_cqc/_05_model/fargate/preprocessing/Dockerfile"
  tags = ["${AWS_ACCOUNT_ID}.dkr.ecr.eu-west-2.amazonaws.com/fargate/preprocessing:${SANITISED_CIRCLE_BRANCH}"]
  platforms = ["linux/amd64"]
  no-cache = true
}

target "model_predict" {
  context = "."
  dockerfile = "./projects/_03_independent_cqc/_06_estimate_filled_posts/fargate/Dockerfile"
  tags = ["${AWS_ACCOUNT_ID}.dkr.ecr.eu-west-2.amazonaws.com/fargate/prediction:${SANITISED_CIRCLE_BRANCH}"]
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
