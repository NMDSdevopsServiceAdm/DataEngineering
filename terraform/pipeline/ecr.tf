# Build and push the Docker image
resource "null_resource" "docker_image" {
  triggers = {
    # docker_file = filesha256("../../lambdas/create_dataset_snapshot/Dockerfile")
    # source_dir  = sha256(join("", [for f in fileset("../../lambdas/create_dataset_snapshot", "**") : filesha256("../../lambdas/create_dataset_snapshot/${f}")]))
  }
  provisioner "local-exec" {
    command = <<EOF
      pwd
    EOF
  }
}