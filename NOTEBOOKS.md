# Jupyter Notebooks on AWS EMR

## Steps to Launch
1. Go to [AWS EMR Console](https://eu-west-2.console.aws.amazon.com/elasticmapreduce/home?region=eu-west-2)
2. Clone the most recent terminated cluster
3. Wait for cluster to build (5–10 mins)
4. Go to "Workspaces (Notebooks)"
5. Select the notebook you require (or create a new notebook) and Attach cluster
6. The Notebook should automatically load, but select "Quick launch" if not
7. Terminate the cluster when finished

## EMR Python Libraries
We use a bootstrap script to install extra libraries.

Edit it locally:
```bash
aws s3 cp s3://aws-emr-resources-.../install-python-libraries-for-emr.sh .
# Edit the file
aws s3 cp ./install-python-libraries-for-emr.sh s3://aws-emr-resources-.../
```

## Cost awareness
- EMR clusters are billed per instance minute
- Always terminate the cluster after use

## Troubleshooting
- Ensure cluster is active before starting notebooks
- Restart kernel if new libraries aren’t found
