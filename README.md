## DataEngineering

### Welcome to the Skills for Care Data Engineering repository.

This repository contains the following:
- Terraform infrustructure as code for pipeline deployment on AWS
- Spark jobs for feature extraction data transformations

Be sure to check out our [Wiki](https://github.com/NMDSdevopsServiceAdm/DataEngineering/wiki) for more info!

<br>

[![<ORG_NAME>](https://circleci.com/gh/NMDSdevopsServiceAdm/DataEngineering.svg?style=shield)](https://app.circleci.com/pipelines/github/NMDSdevopsServiceAdm/DataEngineering)

<br>


## Mission Statement


### About Us

We are Skills for Care’s Workforce Intelligence Team, the experts in adult social care workforce insight.

Skills for Care, as the leading source of adult social care workforce intelligence, helps to create a better-led, skilled and valued adult social care workforce. We provide practical tools and support to help adult social care organisations in England recruit, retain, develop and lead their workforce. We work with employers and related services to ensure dignity and respect are at the heart of service delivery.

We’re commissioned by the Department of Health and Social Care to collect data on adult social care providers and their workforce via the Adult Social Care Workforce Data Set (previously named National Minimum Data Set for Social Care). For over 15 years we’ve turned this data into intelligence and insight that's relied upon by the Government and across our sector.

### About the Data Engineering Project

Our data engineering project aims to convert our data modeling processes into reproducible analytical pipelines in AWS. These pipelines will be more accurate and efficient, allowing us to:
 - model our estimates using all the data available to us
 - provide more frequent updates to our estimates
 - use more complex modelling techniques, such as machine learning or AI modules
 - make our detailed methods available to anyone who wants to explore them


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
### Run specific test within test file
```
python -m unittest tests.unit.<glue_job_test_folder>.<test_class>.<specific_test>
```
example:
```
python -m unittest tests.unit.test_clean_ascwds_workplace_data.MainTests.test_main
```


For verbose output add `-v` to the end of the command.


<br>
<br>

## Documentation
The documentation for the work we do here should be located in one of two places, as detailed in the subsections below

### Docstrings
We have started to implement docstrings within our code, and they are to follow the [Google Docstring Styleguide](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings). Currently the approach is to ensure all functions within jobs have a docstring, then expand that out to classes and modules as desired.
This relaxed approach is because of Sphinx in the section below, which has been setup to automatically and dynamically pick up docstrings from code within the low-level documentation, and thus over time will become more and more populated. This will allow us to control the growth of documentation and visualise it the way we want to.
There is a [discussion and comparison of different Docstring standards on our Confluence](https://skillsforcare.atlassian.net/wiki/spaces/DE/pages/1033994246/Docstrings+StyleGuides+and+Resources)

### Sphinx
For low level, close-to-code documentation and technical pipeline components, you can easily launch our documentation server once you've cloned our repository
- `sphinx-autobuild docs/source/ docs/build/`
*Note that if you already have a copy of our repository you will need to update your pipenv file* - Try doing this with the following before running the above if you have any issues:
- `pipenv sync --dev`

#### Making changes to documentation
Since Sphinx is configured to understand Markdown (.md) files, you can find all of the documentation (excluding the main repo README) within the `docs` directory.
To understand more about how the Sphinx documentation was setup, consult our [Sphinx page on Confluence](https://skillsforcare.atlassian.net/wiki/spaces/DE/pages/1028227086/Sphinx)

### Confluence
For high level project documentation, including content such as handovers, ADRs, signposting, private decision logs and guidance otherwise not available or suitable for our public repo, you can request access to our Confluence here: [Skills For Care Data Engineering Confluence Home](https://skillsforcare.atlassian.net/wiki/spaces/DE/overview?homepageId=1011220675)


## Infrastructure

So you want to update the platform's infrastructure? We utilise [Terraform](https://learn.hashicorp.com/terraform) as our tool of choice for managing our Infrastructure as Code (IAC). Have a read about IAC [here](https://en.wikipedia.org/wiki/Infrastructure_as_code).

### Our Continuous Depoyment Pipeline
***The CD part of [CICD](https://www.redhat.com/en/topics/devops/what-is-ci-cd#:~:text=CI%2FCD%20is%20a%20method,continuous%20delivery%2C%20and%20continuous%20deployment.)***

We utilise [CircleCI](https://circleci.com/docs/?utm_source=google&utm_medium=sem&utm_campaign=sem-google-dg--emea-en-brandAuth-maxConv-auth-brand&utm_term=g_p-circleci_c__linux_20220513&utm_content=sem-google-dg--emea-en-brandAuth-maxConv-auth-brand_keyword-text_eta-circleCI_phrase-&gclid=Cj0KCQjwhLKUBhDiARIsAMaTLnGUFcuTVX-Ux2Asd9rfD9z0kiZiIr69Aj-cSPmQAi7xtr6jkYzFVtwaAkj-EALw_wcB) to automate terraform deployments. <br>
When creating a new git branch and pushing to the remote repository a CircleCi workflow will automatically trigger. <br>
One of the steps in this workflow is to deploy terraform. You can find the full CircleCi configuration inside [.circleci/config.yml](.circleci/config.yml).
Once the workflow has completed AWS will contain all the infrastructure required to run the pipeline and all associated glue jobs.<br>

Environments will be torn down when the branch is deleted from GitHub.
So make sure you delete your branch after merging into main (this is good practice anyway!).

> ❗ **When merging with the main branch**: The workflow will run here too. There is a mandatory, manual approval step required here. Please read the output of `terraform plan`. Ensure this is correct, then give your approval. The workflow will complete and the main (production) infrastructure will be updated. The main branch workflows can be found [here](https://app.circleci.com/pipelines/github/NMDSdevopsServiceAdm/DataEngineering?branch=main&filter=all).

<br>

### Continuous Integration
#***The CI part of [CICD](https://www.redhat.com/en/topics/devops/what-is-ci-cd#:~:text=CI%2FCD%20is%20a%20method,continuous%20delivery%2C%20and%20continuous%20deployment.)***

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

#Install Terraform following [the instructions below](#installing-terraform).

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

### Creating Credentials for AWS CLI
With the AWS CLI installed and configured, you will now need access to the Skills for Care AWS
1. Request access from the team to the AWS Console.
2. Once this access is granted, follow the steps here to [setup your access and secret key](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html).

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

## Jupyter Notebooks

>The notebook extends the console-based approach to interactive computing in a qualitatively new direction, providing a web-based application suitable for capturing the whole computation process: developing, documenting, and executing code, as well as communicating the results. The Jupyter notebook combines two components:
>
>**A web application**: a browser-based tool for interactive authoring of documents which combine explanatory text, mathematics, computations and their rich media output.
>
>**Notebook documents**: a representation of all content visible in the web application, including inputs and outputs of the computations, explanatory text, mathematics, images, and rich media representations of objects.

[Source](https://jupyter-notebook.readthedocs.io/en/stable/notebook.html)

----

### Spinning up a notebook
> ❗ **You will need to request access from the team to AWS Console to complete these steps**, if you've not done this already.

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


## Other Guidance

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


### Notebook costs
An EMR cluster is charged per instance minute, for this reason ensure the cluster is terminated when not in use.
The notebooks are free, but require a cluster to run on.
The AWS EMR costing documentation can be found here: https://aws.amazon.com/emr/pricing/

[install_python_libs_script]: https://s3.console.aws.amazon.com/s3/object/aws-emr-resources-344210435447-eu-west-2?region=eu-west-2&prefix=bootstrap-scripts/install-python-libraries-for-emr.sh
[aws_cli_docs]: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html

### AWS Buckets Versioning

**Versioning** is a feature in AWS which lets us keep multiple variants of the same object in the same bucket. However, instead of just overriding and/or deleting the older version, S3 allows for the preservation, retrieval and archival of all object.

#### Buckets come in three types:
	- Unversioned (default)
	- Versioning-enabled
	- Versioning-suspended

You enable and suspend a bucket's versioning functionality at a bucket level.  It should be noted that once versioning is enabled, it can **never** be returned to an unversioned state.

The versioning state applies to all objects that are in or enter the bucket. If you enable versioning on a bucket, all objects that are in the bucket get a `version ID`. Any editing of these objects will generate a new unique `version ID`.

- Objects in a bucket before you set the version state have a `null` `version ID`.

For more information about versioning see: https://docs.aws.amazon.com/AmazonS3/latest/userguide/Versioning.html

### Activating Versioning on a Bucket

In AWS you can select a Bucket > click "Properties" > here we see "Bucket Versioning", click edit and make the relevant changes needed.

### Versioning by default

Inside our stack we have a terraforming config file ([s3.tf](https://github.com/NMDSdevopsServiceAdm/DataEngineering/tree/main/terraform/modules/s3-bucket)) which lays out and applies certain formatting when creating buckets.

The directory path on your machine for this file:

     /DataEngineering/terraform/modules/s3-bucket/

One thing to be aware of is that and bucket with the `sfc-` will have versioning on by default. If you run in to a situation when creating new buckets in the stack be aware that this prefix is applied automatically and correct accordingly.

### Deleting and Restoring Objects

With versioning enabled, the previous versions of that object exist and can be viewed within the bucket by clicking on the `Show Version` slider. With a versioning enabled bucket we can see every version of the object which was previously hidden. You have the ability to download or view the older version, you can also see previous version and objects that have been deleted.

A new item appears in the list of objects; the **Delete Marker**. This gets added to any object which is deleted; not it does not get added to objects that get updated or over wrote. If you wish to restore a version that was previously deleted, click the check box beside the delete marker and at the top of the screen click "delete".  Deleting the delete marker restores the previous version of the object.
