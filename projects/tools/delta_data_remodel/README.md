
# Delta Dataset Remodel

This tool is intended to help you remodel full datasets (where you store the entire snapshot for every timepoint)
into a delta dataset (which stores changes only).

More information can be found here: https://skillsforcare.atlassian.net/wiki/x/PwDiVw

It is currently able to remodel:
- CQC raw providers dataset

## How To Use

I created an EC2 instance, and ran the code from there. 
I used a `m5.xlarge` instance, though a smaller instance would likely be perfectly capable. 

1. On creation of the instance, download the `.pem` file
2. Move the `.pem` file to a directory for ssh files: `mv ~\Downloads\[instance_name].pem .\ssh`
3. Assume the permissions of that file: `chmod 400 .ssh\[instance_name].pem`
4. Upload your file to the instance `scp -i .ssh/[instance_name].pem [local path] ec2-user@ec2-[ec2 public IP with - in place of .].eu-west-2.compute.amazonaws.com:/home/ec2-user[optional /subfolder on ec2 instance]`
5. SSH into the EC2 instance `ssh -i .ssh/[instance_name].pem ec2-user@ec2-[ec2 public IP with - in place of .].eu-west-2.compute.amazonaws.com`
6. Repeat with all other necessary files
7. Use `pip` to install any dependencies
8. Run the script using `python3 [script_name].py`

## How To Test

There are two test files.
- `test_utils.py` contains unit tests for the `utils.py` file.
- `test_cqc_raw_providers_diffs.py` contains non-unit tests. These can be run manually to test that the data is consistent between the delta and full datasets

The tests included in `test_cqc_raw_providers_diffs.py` are:
1. `test_rebuilt_dataset_equality`: test that when you rebuild the full dataset from the delta dataset, the two are equivalent
2. `test_snapshot_equality`: test that each snapshot is equivalent to its full dataset counterpart
3. `evaluate_dataset_changes`: see how many of each column has changed between snapshots (useful for understanding what has happened when all the data changes)
4. `test_delta_matches_changes_api`: check that the changes recorded line up with the cqc changes api
