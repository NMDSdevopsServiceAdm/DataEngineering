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

## Merging to main
Merge by adding the `ready-to-merge` label, not the Merge button. The `merge-when-ready` GitHub Action then merges labelled PRs one at a time, in the order they were labelled. For each PR it checks the PR is approved, updates the branch from `main`, waits for the PR's checks and CircleCI workflows, then waits for the deploy of `main`'s latest commit to finish (including its manual `plan-approval`) before merging. It removes the label when done.

- Before labelling, resolve any merge conflicts: GitHub doesn't run workflows for a PR with a conflict, so the label would do nothing.
- When a PR is labelled, the bot comments its place in the queue ("Queued: position 2 of 3"). To see the queue, filter PRs by the `ready-to-merge` label. Run status and logs are under the repo's Actions tab > `merge-when-ready` > the run's "Merge labelled PRs in turn" step.
- To take a PR out of the queue, remove its label (or close it). This works at any point, including while it waits.
- A PR needs at least one approving review and no outstanding request for changes, checked at the start and again just before merging. Otherwise it gets a comment and loses its label: re-add it once approved.
- A PR that can't be merged (failed checks, merge conflict, `main` moving ahead too fast, or a bot error) gets a comment with the reason and loses its label. The next labelled PR carries on. Fix it and re-add the label.
- If `main`'s latest deploy failed or never started, the PR in progress gets a comment and loses its label, and the run fails so nothing merges until it is fixed. The fix itself has to go in with the Merge button. The other labelled PRs keep their labels but nothing restarts them: once `main` is fixed, run the workflow manually (Actions > `merge-when-ready` > Run workflow, with "dry run" unticked).
- After the last merge the run waits for that deploy too, so a failed deploy turns the run red.
- If CircleCI rejects the token or project slug (HTTP 401/403/404), the run fails and no labels are changed. Fix `CIRCLECI_TOKEN`, then run the workflow manually as above.
- A run stops itself after 5h45m (GitHub's job limit is 6h), comments on the PR it was waiting on and removes its label: re-add it to continue.
- Add `merge-dry-run` instead to run every check without merging. It removes the label afterwards.
- The bot runs `main`'s copy of the workflow and script (`pull_request_target`), not the PR's, so changes to `scripts/merge_when_ready.py` or the workflow only take effect once merged, and labelling a PR that edits them tests nothing. Test such changes with a throwaway PR whose base is a branch containing them.
- Needs the `ready-to-merge` and `merge-dry-run` labels to exist, and the `CIRCLECI_TOKEN` secret.
- The Merge button still works, so the label is a team convention.
