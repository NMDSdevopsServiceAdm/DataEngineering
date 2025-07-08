# Terraform Deployment Guide

## Installing AWS CLI
AWS CLI is a prerequisite of Terraform.
1. [Install AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
2. Request access to the AWS Console
3. Once this access is granted, follow the steps here to [setup your access and secret key](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html)

## Deploying Terraform

1. Set up your AWS crendentials as terraform variables

Copy the file located at `terraform/pipeline/terraform.tfvars.example` and save as `terraform/pipeline/terraform.tfvars`.

```
cp terraform/pipeline/terraform.tfvars.example terraform/pipeline/terraform.tfvars
```
Populate this file with your access key and secret access key.

2. From terminal/command line ensure you're in the Terraform directory
```
% pwd
/Users/username/Projects/skillsforcare/DataEngineering/terraform/pipeline
```
3. Run `terraform plan` to evaluate the planned changes
```
terraform plan
```
4. Check the planned changes to make sure they are correct!
5. Then run `terraform apply` to deploy the changes. Confirm with `yes` when prompted
```
terraform apply
```

## Destroying Terraform
To remove Terraform generated infrastructure first ensure you are in the correct directory working on the correct workspace.

```
cd terraform/pipeline
terraform init
terraform workspace list
```

To switch to a different workspace run:
```
terraform workspace select <workspace_name>
```

Then run:
```
terraform destroy
```

To delete a workspace make sure it is not your current workspace (you can select the default workspace) and run:

```
terraform workspace select default
terraform workspace delete <workspace_name>
```

## CircleCI Deployment
- Automatically deploys infrastructure for each branch.
- Requires manual approval when merging to main
- You can find the full CircleCi configuration inside [.circleci/config.yml](.circleci/config.yml).
