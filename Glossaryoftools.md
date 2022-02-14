# Amazon Web Services
## S3
Amazon Simple Storage Service (Amazon S3) allows cloud storage for data objects.
Data objects are held within buckets (these are similar to folders within microsoft file explorer).
You can store as much data as you want within S3 and Amazon automatically backs up the data – 
it stores every version of every object in an S3 bucket so you can automatically recover something if it is accidentally deleted.
Administrators can controls who can access the data via using access management tools.

## Glue
AWS Glue is a fully managed ETL (extract, transform, and load) service.
Using Glue, you can categorize your data, clean it, enrich it, and move it between various data stores and data streams. 
AWS Glue consists of a central metadata repository known as the AWS Glue Data Catalog, an ETL engine that automatically generates Python or Scala code, and a scheduler that handles dependency resolution, job monitoring, and retries.
Once an ETL jobs has run, glue also uses crawlers which discover your datasets, discover your file types, extract the schema and store all this information in a centralised metadata catalogue for querying and analysis.
You can schedule glue jobs automatically for use cases such as (but not limited to) calling APIs, automating emails and ETL).

## Athena
Athena is an interactive query service that makes it easy to analyse data in Amazon S3 using standard SQL. 
Athena is useful when initially investigating a dataset. 
You are charged $5 per terabyte scanned by your queries so it is best practice to limit your query to the top 20 rows of data.

## Jupyter Notebook via Amazon EMR
### Amazon EMR
Amazon EMR (elastic map reduce) allows you to spin up a cluster of computers to process big data frameworks, such as Apache Spark, on AWS, to process and analyse vast amounts of data quickly.
When you spin up a cluster, you can choose how many master and worker computers you would like to do the work.
Master computers look at the code you give it and divide it up between the workers, you typically want 1 master and 3 workers machines when spinning up a cluster.
<br>
### Jupyter Notebooks

>The notebook extends the console-based approach to interactive computing in a qualitatively new direction, providing a web-based application suitable for capturing the whole computation process: developing, documenting, and executing code, as well as communicating the results. The Jupyter notebook combines two components:
>
>**A web application**: a browser-based tool for interactive authoring of documents which combine explanatory text, mathematics, computations and their rich media output.
>
>**Notebook documents**: a representation of all content visible in the web application, including inputs and outputs of the computations, explanatory text, mathematics, images, and rich media representations of objects.

[Source](https://jupyter-notebook.readthedocs.io/en/stable/notebook.html)

To use jupyter, you must first connect to a cluster which was created in EMR as these computers will be used to process the data.
Jupyter notebooks over 40 programming languages, including Python, PySpark, R, Julia, and Scala.
Jupyter is not used for programming applications as this is more of a research/development environment however, it can be good to use jupyer if you want to analys1e some data without turning it into a glue job.

# VS Code
VSCode is an IDE (integrated development environment).
We use VSCode as the production/development environment, turning a jupyter notebook into an automated job.
VSCode allows you to write tests for your code to ensure it is working correctly alongside having a debug mode which will let you know if there is an issue with the code itself or if a test fails.

# Circleci

# Use Cases
Tool | Use Case
--- | --- 
S3 | Storing Data
Glue | Job automation, ETL, API connection
Athena | Initial dataset investigation using SQL
Jupyter | Pre-production environment, useful for investigation
VS Code | Production/development environment, turns code into automated jobs
Circleci | Devops/infrastructure

