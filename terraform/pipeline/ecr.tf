#####################################################################################################
# The ECR repositories are created outside of terraform please see:                                 #
# https://skillsforcare.atlassian.net/wiki/spaces/DE/pages/1493762050/Lambda+deployment+via+Docker  #
# for more information on how to create one                                                         #
#####################################################################################################

# Get the latest image digests for this branch
data "aws_ecr_image" "create_dataset_snapshot" {
  repository_name = "lambda/create-snapshot"
  image_tag       = local.workspace_prefix
}

data "aws_ecr_image" "check_datasets_equal" {
  repository_name = "lambda/check-datasets-equal"
  image_tag       = local.workspace_prefix
}