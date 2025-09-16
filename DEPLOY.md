# Terraform Deployment Guide

## Installing AWS Command Line Interface
Amazon Web Services Command Line Interface is a prerequisite of Terraform.
1. [Install Amazon Web Services Command Line Interface](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
2. Request access to the Amazon Web Services Console
3. Once this access is granted, follow the steps here to [setup your access and secret key](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html)

## Resource Dependencies
Some resources have been created outside of terraform. The extra resources you would need to create are:
- An Elastic Container Registry repository called "lambda/create-snapshot"
- An Elastic Container Registry repository called "lambda/check-datasets-equal"


## Deploying Terraform

1. Set up your Amazon Web Services crendentials as terraform variables

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
3. Run `terraform plan -var-file=../non-prod.s3.tfbackend` to evaluate the planned changes
```
terraform plan
```
4. Check the planned changes to make sure they are correct!
5. Then run `terraform apply -var-file=../non-prod.s3.tfbackend` to deploy the changes. Confirm with `yes` when prompted
```
terraform apply -var-file=../non-prod.s3.tfbackend`
```

In the very rare case you need to do a manual production deployment from your own machine, you'll need to rerun `terraform init`, using `../prod.s3.tfbackend` as the value for `-backend-config`. It is then essential that once you have performed this deployment, you rerun `terraform init -backend-config=../non-prod.s3.tfbackend`, otherwise any summary action may be accidentally applied to the production environment if the `allowed_account_ids` entry is accidentally deleted.

## Destroying Terraform
To remove Terraform generated infrastructure first ensure you are in the correct directory working on the correct workspace.

```
cd terraform/pipeline
terraform init -backend-config=../non-prod.s3.tfbackend
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
