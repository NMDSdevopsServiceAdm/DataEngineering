# ECR Repository to store the Docker image
resource "aws_ecr_repository" "create_dataset_snapshot" {
  name                 = "create-snapshot-lambda-repo"
  image_tag_mutability = "MUTABLE"
  force_delete         = true
  image_scanning_configuration {
    scan_on_push = true
  }
}

# Build and push the Docker image
resource "null_resource" "docker_image" {
  triggers = {
    docker_file = filesha256("../../lambdas/create_dataset_snapshot/Dockerfile")
    source_dir  = sha256(join("", [for f in fileset("../../lambdas/create_dataset_snapshot", "**") : filesha256("../../lambdas/create_dataset_snapshot/${f}")]))
  }
  provisioner "local-exec" {
    command = <<EOF
      aws ecr get-login-password | docker login --username AWS --password-stdin ${data.aws_caller_identity.current.account_id}.dkr.ecr.eu-west-2.amazonaws.com
      cd ../../lambdas/create_dataset_snapshot
      docker build -t ${aws_ecr_repository.create_dataset_snapshot.repository_url}:latest .
      docker push ${aws_ecr_repository.create_dataset_snapshot.repository_url}:latest
    EOF
  }
  depends_on = [aws_ecr_repository.create_dataset_snapshot]
}

# Get the latest image digest
data "aws_ecr_image" "create_dataset_snapshot" {
  repository_name = aws_ecr_repository.create_dataset_snapshot.name
  image_tag       = "latest"
  depends_on      = [null_resource.docker_image]
}
