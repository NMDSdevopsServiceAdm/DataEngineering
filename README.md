# DataEngineering

## Welcome to the Skills for Care Data Engineering repository.

This repository contains the following:
- Terraform infrustructure as code for pipeline deployment on AWS
- Spark jobs for feature extraction data transformations

Be sure to check out our [Wiki](https://github.com/NMDSdevopsServiceAdm/DataEngineering/wiki) for more info!

<br>

[![<ORG_NAME>](https://circleci.com/gh/NMDSdevopsServiceAdm/DataEngineering.svg?style=shield)](https://app.circleci.com/pipelines/github/NMDSdevopsServiceAdm/DataEngineering)

<br>


# Mission Statement

*"INSERT MISSION STATEMENT*

# Building the project

### Download the following prerequisite installs:
Tool | Windows | Mac/Linux
--- | --- | ---
Python version 3.9.6 | https://www.python.org/downloads/ | https://www.python.org/downloads/
Git | https://github.com/git-guides/install-git | https://github.com/git-guides/install-git
Pyenv | https://github.com/pyenv-win/pyenv-win | https://github.com/pyenv/pyenv
Pipenv | https://www.pythontutorial.net/python-basics/install-pipenv-windows/ | https://pipenv-fork.readthedocs.io/en/latest/install.html
java jdk8 (need to create an oracle account) | https://www.java.com/en/download/ | https://www.java.com/en/download/

## Install Java (MacOS)
This project is using jdk8. We recommend using Brew (https://brew.sh) to install the java development kit. This project is using **jdk8**.
```
brew update
brew install adoptopenjdk8
```


## Clone the project
```
git clone https://github.com/NMDSdevopsServiceAdm/DataEngineering.git
```
## Create virtual environment and install dependencies
```
cd DataEngineering
pipenv install --dev
```

For detailed Windows setup see here: https://github.com/NMDSdevopsServiceAdm/DataEngineering/blob/main/WindowsSetup.md


### Start virtual env
```
pipenv shell
```
### Stop virtual env
```
exit
```
#### IMPORTANT
Do not use `deactivate` or `source deactivate` - this will leave pipenv in a confused state because you will still be in that spawned shell instance but not in an activated virtualenv. 

## Testing
### Run tests
*Make sure you have the virtual environment running (see above).*

Run a specific test file once
```
python -m unittest tests/unit/<test_name.py>
```
Watch a specific test file and auto rerun the tests when there are changes
```
pytest-watch -m unittest tests/unit/<test_name.py>
```

Run all tests once
```
python -m unittest discover tests/unit "test_*.py"
```
Watch all the tests and auto rerun the tests when there are any changes
```
pytest-watch
```
#### Run specific test within test file
```
python -m unittest tests.unit.<glue_job_test_folder>.<test_class>.<specific_test>
```
example:
```
python -m unittest tests.unit.test_prepare_locations.PrepareLocationsTests.test_get_pir_df
```


For verbose output add `-v` to the end of the command.


<br>
<br>

# Infrastructure

So you want to update the platform's infrastructure? We utilise [Terraform](https://learn.hashicorp.com/terraform) as our tool of choice for managing our Infrastructure as Code (IAC). Have a read about IAC [here](https://en.wikipedia.org/wiki/Infrastructure_as_code).

## Our Continuous Depoyment Pipeline 
***The CD part of [CICD](https://www.redhat.com/en/topics/devops/what-is-ci-cd#:~:text=CI%2FCD%20is%20a%20method,continuous%20delivery%2C%20and%20continuous%20deployment.)***

We utilise [CircleCI](https://circleci.com/docs/?utm_source=google&utm_medium=sem&utm_campaign=sem-google-dg--emea-en-brandAuth-maxConv-auth-brand&utm_term=g_p-circleci_c__linux_20220513&utm_content=sem-google-dg--emea-en-brandAuth-maxConv-auth-brand_keyword-text_eta-circleCI_phrase-&gclid=Cj0KCQjwhLKUBhDiARIsAMaTLnGUFcuTVX-Ux2Asd9rfD9z0kiZiIr69Aj-cSPmQAi7xtr6jkYzFVtwaAkj-EALw_wcB) to automate terraform deployments. <br>
When creating a new git branch and pushing to the remote repository a CircleCi workflow will automatically trigger. <br>
One of the steps in this workflow is to deploy terraform. You can find the full CircleCi configuration inside [.circleci/config.yml](.circleci/config.yml).
Once the workflow has completed AWS will contain all the infrastructure required to run the pipeline and all associated glue jobs.<br>

Environments will be torn down when the branch is deleted from GitHub.
So make sure you delete your branch after merging into main (this is good practice anyway!).

> ❗ **When merging with the main branch**: The workflow will run here too. There is a mandatory, manual approval step required here. Please read the output of `terraform plan`. Ensure this is correct, then give your approval. The workflow will complete and the main (production) infrastructure will be updated. The main branch workflows can be found [here](https://app.circleci.com/pipelines/github/NMDSdevopsServiceAdm/DataEngineering?branch=main&filter=all).

<br>

## Continuous Integration
***The CI part of [CICD](https://www.redhat.com/en/topics/devops/what-is-ci-cd#:~:text=CI%2FCD%20is%20a%20method,continuous%20delivery%2C%20and%20continuous%20deployment.)***

When you push to a remote git branch, we run some linting checks for Python and Terraform code as a part of the CircleCi workflow (mentioned above).
If your branch fails either of these checks, you need to run the relevant linter to fix the errors.
Instructions for both Terraform and Python are detailed below.

### Linting Python code

We use [black](https://black.readthedocs.io/en/stable/) to lint our Python code.
Install black using pip
```
pip install black
```
To lint all the Python files, first ensure you're at the root of the repository, then run
```
black .
```

### Linting Terraform code

Install Terraform following [the instructions below](#installing-terraform).

Ensure you are at the root of the repository, then run
```
terraform fmt -recursive
```

## The manual approach
### Installing Terraform
The Terraform docs are an excellent resource for this: https://learn.hashicorp.com/
tutorials/terraform/install-cli <br> Here's the tldr though, just in case.
1. Download binary: https://www.terraform.io/downloads
2. Unzip
3. Make available on path

<br>

### Installing AWS CLI
AWS CLI is a prerequisite of Terraform. Follow these [instructions](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) to install and configure it. 

### Deploying Terraform

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
<br>

### Destroying Terraform

“What goes up must come down.” - Isaac Newton

Nobody wants a bunch of infrastructure left lying around, unused, it adds cognitive load, confusion and possible expense.

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

This will generate a "destruction plan" - closely read through this plan and ensure you want to execute all of the planned changes. Once satisfied, confirm the changes. Terraform will then proceed to tear down all of the running infrastructure in your current workspace. <br>

To delete a workspace make sure it is not your current workspace (you can select the default workspace) and run:

```
terraform workspace select default
terraform workspace delete <workspace_name>
```

# Jupyter Notebooks

>The notebook extends the console-based approach to interactive computing in a qualitatively new direction, providing a web-based application suitable for capturing the whole computation process: developing, documenting, and executing code, as well as communicating the results. The Jupyter notebook combines two components:
>
>**A web application**: a browser-based tool for interactive authoring of documents which combine explanatory text, mathematics, computations and their rich media output.
>
>**Notebook documents**: a representation of all content visible in the web application, including inputs and outputs of the computations, explanatory text, mathematics, images, and rich media representations of objects.

[Source](https://jupyter-notebook.readthedocs.io/en/stable/notebook.html)

----

## Spinning up a notebook
We utilise AWS EMR (Elastic Map Reduce) for our notebook environment. Below are the steps required to get this environment running.

1. Head over to AWS [EMR](https://eu-west-2.console.aws.amazon.com/elasticmapreduce/home?region=eu-west-2)
2. Select *"Clusters"* from the left navigation column.
3. If there isn't a cluster already running, clone a new one from the most recently terminated.
    - Select the most recently terminated cluster.
    - Select *"Clone"* from the top navigation
    - Confirm *"yes"* to *"...including steps..."*
    - Check the hardware configuration found in step 2 is appropriate. We usually utilise 1 m5.xlarge master node and 3 m5.xlarge core nodes. By default we utilise spot pricing for reduced operational costs.
    - Complete the wizzard by clicking *"Create Cluster"*
4. Wait for cluster to finishing building (2 - 10 minutes)
5. Navigate to *"Notebooks"* from the left navigation column.
6. Either create a new notebook, or start a pre-existing one.
7. Wait for notebok to start (1-3 minutes)
8. Select notebook and click *"Open in JupyterLab"* - This will start your interactive notebook session. 
9. Once finished with the notebooks terminate the cluster.
    - Navigate to *"Clusters"* from the left navigation column.
    - Select the running cluster
    - Click *"Terminate"*

### Installing extra python libraries

We run a bash script as an EMR step after the cluster has started which installs any python libraries we need on the cluster using pip.

The script is stored [here][install_python_libs_script] in S3.
You can edit this script to upload extra python libraries and they will be uploaded the next time a cluster is started.

To add extra libraries:
1. Download the script either via the "Download" button [in the console][install_python_libs_script] or using the [aws cli][aws_cli_docs].
```
aws s3 cp s3://aws-emr-resources-344210435447-eu-west-2/bootstrap-scripts/install-python-libraries-for-emr.sh .
```
2. Open the downloaded script and add the following line to the end of the script for each library that needs installing.
```
sudo python3 -m pip install package_name
```
3. Upload the updated script to the same location (s3://aws-emr-resources-344210435447-eu-west-2/bootstrap-scripts/install-python-libraries-for-emr.sh).
Either using console or the aws cli.
```
aws s3 cp ./install-python-libraries-for-emr.sh s3://aws-emr-resources-344210435447-eu-west-2/bootstrap-scripts/install-python-libraries-for-emr.sh
```

The libraries will be installed the next time a new cluster is cloned and started.


## Notebook costs
An EMR cluster is charged per instance minute, for this reason ensure the cluster is terminated when not in use.
The notebooks are free, but require a cluster to run on. 
The AWS EMR costing documentation can be found here: https://aws.amazon.com/emr/pricing/

[install_python_libs_script]: https://s3.console.aws.amazon.com/s3/object/aws-emr-resources-344210435447-eu-west-2?region=eu-west-2&prefix=bootstrap-scripts/install-python-libraries-for-emr.sh
[aws_cli_docs]: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html


## Connect AWS Athena to Tableau
To visualise the data in Tableau we need to connect AWS Athena to Tableau. 

1. Install JDBC driver on your pc, download from [here] (https://docs.aws.amazon.com/athena/latest/ug/connect-with-jdbc.html): version JDBC 4.1 with AWS SDK.
2. This needs saving here: *"C:\Program Files\Tableau\Drivers"*. This will require admin/IT to move from your downloads to this folder location.
3. Open a new Tableau document on the left-hand side and select *"Amazon Athena"* from the *"Connect'"* menu under *"To a server"*.
4. This will open a screen where you need to add credentials into the fields - to get these go to *"F:\ASC-WDS Copy Files\Research & Analysis Team Folders\Analysis Team\c. Ongoing Work\Tanya\Data engineering"* in a file called *"Tableau Athena Access Key"*.
5. Once connected select 'main-data-engineering-database' from the 'Database' dropdown in Tableau. All of the tables should now appear and are now available to analyse. Other databases can be selected.
