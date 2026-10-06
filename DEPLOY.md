# Terraform Deployment Guide

Amazon Web Services Command Line Interface is a prerequisite of Terraform. [See our windows setup guide for instructions on setting up AWS CLI and terraform for the first time](WINDOWS_SETUP.md)

## Deploying Terraform

1. Set an environment variable for HOME (`aws-mfa` reads it to find your `.aws` folder, and Windows may not set it). This only applies to the current terminal, so repeat it in each new one:
```
$Env:HOME = $Env:USERPROFILE
```
2. Provide your MFA token:
```
aws-mfa --mfa-profile prod --token xxxxxx
```
3. Ensure you're in the Terraform directory `cd terraform/pipeline`

4. Set an environment variable for AWS_PROFILE:
```
$Env:AWS_PROFILE="non-prod"
```
5. Initialise terraform:
```
terraform init -backend-config="../non_prod_local.s3.tfbackend"
```
6. Select workspace
```
terraform workspace select <branch name>
```
7. Run `terraform plan` to evaluate the planned changes
```
terraform plan
```
8. Check the planned changes to make sure they are correct!
9. Then run `terraform apply` to deploy the changes. Confirm with `yes` when prompted
```
terraform apply
```

In the very rare case you need to do a manual production deployment from your own machine, you'll need to rerun `terraform init`, using `../prod_local.s3.tfbackend` as the value for `-backend-config`. You will need elevated permissions to do this. It is then essential that once you have performed this deployment, you rerun `terraform init -backend-config="../non_prod.s3.tfbackend"`, otherwise any summary action may be accidentally applied to the production environment if the `allowed_account_ids` entry is accidentally deleted.

## Destroying Terraform
To remove Terraform generated infrastructure first ensure you are in the correct directory working on the correct workspace.

```
cd terraform/pipeline
terraform init -backend-config="../non_prod_local.s3.tfbackend"
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
- Per branch, the image build and `terraform-plan`/`terraform-apply` run one at a time (`serial-group`), so a newer push waits for a running apply. A superseded pipeline's apply is skipped and its `flag-incomplete-deploy` job fails: don't rerun it.
- On main, `check-main-tip` runs first in the prod `plan-approve-and-apply` group and fails if the pipeline's commit is no longer the tip of `main`, so an older pipeline that reaches the group after a newer one can't apply its older plan or sync older code. Don't rerun a pipeline that fails it: the newer commit's pipeline carries those changes.
- Needs **Auto-cancel redundant workflows** OFF in the CircleCI project settings (Project Settings > Advanced), or a running apply is cancelled again.
- Branch names may only use letters, digits, `.`, `_`, `-` and `/` (the `serial-group` key allows nothing else).
