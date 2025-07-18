# ECR Repository to store the Docker image
resource "aws_ecr_repository" "create_dataset_snapshot" {
  name                 = "${local.workspace_prefix}-create-snapshot-lambda-repo"
  image_tag_mutability = "MUTABLE"
  force_delete         = true
  image_scanning_configuration {
    scan_on_push = true
  }
}

# Get the latest image digest
# data "aws_ecr_image" "create_dataset_snapshot" {
#   repository_name = aws_ecr_repository.create_dataset_snapshot.name
#   image_tag       = "latest"
# }
