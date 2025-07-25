# ECR Repositories to store the Docker images
resource "aws_ecr_repository" "create_dataset_snapshot" {
  name                 = "create-snapshot-lambda-repo"
  image_tag_mutability = "MUTABLE"
  force_delete         = true
  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_repository" "check_datasets_equal" {
  name                 = "check-datasets-equal-repo"
  image_tag_mutability = "MUTABLE"
  force_delete         = true
  image_scanning_configuration {
    scan_on_push = true
  }
}

# Get the latest image digests for this branch
data "aws_ecr_image" "create_dataset_snapshot" {
  repository_name = aws_ecr_repository.create_dataset_snapshot.name
  image_tag       = local.workspace_prefix
}

data "aws_ecr_image" "check_datasets_equal" {
  repository_name = aws_ecr_repository.check_datasets_equal.name
  image_tag       = local.workspace_prefix
}