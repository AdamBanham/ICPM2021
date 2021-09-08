# ICPM2021 - Data Sources
This is a reproduction repository for MIMIC III ETL used in [x].
Before following the steps detailed below to reproduce our data set, ensure access to the MIMIC III data set. Which includes completing the required ethics training to be using this publicly available data set.<br><br>
The MIMIC III dataset and access information can be found at: <br>https://physionet.org/content/mimiciii/1.4/ <br>
If you wanted an in-depth understanding of the data available in this dataset, please refer to the official documentation at: <br>https://mimic.mit.edu/docs/iii/ <br><br>
We assume that you already have access to the data to reproduce the ETL steps for creating two event logs used for an ICPM2021 submission, a movements log and a procedure log. <br>
We also assume that you have an AWS account and are familiar with python, SQL and AWS services. In-depth knowledge of any of these languages are not needed, but some introduction knowledge will be helpful.<br>

## Disclaimer
Note that AWS services are not free. However, AWS does provide a level of freeness below a usage level. Therefore, following these steps to execute the creation of these datasets should not cost more than a dollar in the worst case. <br>
But we do not guarantee that following these steps will not result in a charge to your account.<br>
We recommend using cloud storage and cloud services to use this dataset as it provides universal access across many different types of personal computers available today.<br>

# Step Zero - Get Access 
Johnson [1] has set up access on two cloud services providers, Google and AWS. You do not need to download the files locally then reupload them to a cloud service to access these datasets. However, ensure that you follow the steps below to request access to the data set already on the cloud service. <br>
Follow steps for AWS : <br> 
(1) https://mimic.mit.edu/docs/gettingstarted/cloud/link/ <br> 
(2) https://mimic.mit.edu/docs/gettingstarted/cloud/request/ <br>

We will be using Amazon web services (AWS) for the rest of ETL steps.
Using the web interface for AWS console management, you can check if you have access by using a cloud shell terminal and running the following line of code: <br>

```aws s3 ls s3://mimic-iii-physionet ```

Which should provide the following output back: <br>

![cloudshell example](data/in/cloudshell.png)

# Step One - Create Queryable Interface
Now that we have access to the data, we recommend that you create a copy of the MIMIC-III data set in your local region if you are not located near us-east-1 (North Virginia).<br>
Before doing so, we require that you set up the AWS CLI on your local computer for this step and for future steps using python scripts.<br>
For more information on the AWS CLI see : <br> https://github.com/aws/aws-cli/tree/v2 <br>

We assume that you have a python 3.7+ environment and have installed pipenv to recreate our development environment for the rest of these steps.
For more information on pipenv see: <br> https://pipenv.pypa.io/en/latest/ <br>

To install the development environment on your local computer, open a terminal and navigate to the root folder and then run the following commands: <br>
```
pipenv install
pipenv shell
```
![pipenv install example](data/in/pipenv_install.png)
<br>
You should now have activated a virtual environment matching the original development environment used to generate data sets.
After setting up the development environment and configuring AWS CLI for the region which is closest to you, data/steps/01/copy.py will create a copy of MIMIC-III data set in an s3 bucket of that region.
Run the following command to begin the migration from us-east-1 to your desired region:<br>

```
python "./data/steps/migrate.py" 
```
OR if you are not in the pipenv shell:<br>
```
pipenv run "python" "./data/steps/migrate.py"
```

After running this script successfully, an output file will be made, ```data/out/migrate_out.json```. This file is used in future steps and is hashed to managed states between scripts.
Now that we have a local region copy of the mimic-iii dataset, we follow the example presented by AWS at: <br>https://aws.amazon.com/blogs/big-data/perform-biomedical-informatics-without-a-database-using-mimic-iii-data-and-amazon-athena/<br>.
The difference in compute times (1557s/157s), and cost ($2.97/$0.05) are notable in the example.
However, we have modified their cloud formation template and created a script to handle the launching of these resources.
The cloud formation file can be found [here](data/in/athena_template.yaml), for inspection. 
This cloud stack will create an Athena database and optimised table for each csv in the mimic-iii data set.


Run the following commands to create a cloud formation stack, which will deploy all the needed resources to run our ETL scripts over Athena.

```
python "./data/steps/construct.py"
```
OR if you are not in the pipenv shell:<br>
```
pipenv run "python" "./data/steps/construct.py"
```
After running this command, if the stack was constructed, an output file will be made at ```data/out/construct_out.json```.<br>
This file is used in the future steps and is hashed to manage states between scripts.<br>
Once you have finished with our reproduction steps, you can run the following command to remove the resources allocation.<br>

```
python "./data/steps/remove_stack.py"
```
OR if you are not in the pipenv shell:<br>
```
pipenv run "python" "./data/steps/remove_stack.py"
```
# Step Two - Running Queries
Now that we have a queryable interface on AWS, we can run the next group of python scripts to extract and perform some of the transformation steps.
These scripts will require internet access to request Athena to run queries. They will download the outcome of queries into ```mimiciii/out/```.
The first step is to collect exogenous data points of interest for this study.
Run the following command to query for data points and create a local copy.
Ensure that you run these commands from the root directory (where [this readme](readme.md) is located).

```
python ./mimiciii/steps/exogenous.py
```
OR if you are not in the pipenv shell:<br>
```
pipenv run "python" "./mimiciii/steps/exogenous.py"
```

This script will query mimiciii in for a collection of patient observations, then record a single csv for each patient in ```mimiciii/out/exogenous/```.
A record of patients and their identifiers from mimic-iii extracted by this script can found be found in ```mimiciii/out/exogenous/patient_universe.csv```.
Depending on your internet connection, the script will roughly take ~10minutes.

The next step is to collect the control flow perspective for the movements event log. 
This event log captures events to be the movement or requested movement of a patient between ICU wards.
Again this script will query Athena in partitions and output a single csv containing all events for this event log, ```mimiciii/out/movements/controlflow_events.csv```.
Depending on your internet connection, the script will roughly take 10~20minutes.

```
python ./mimiciii/steps/movements.py
```
OR if you are not in the pipenv shell:<br>
```
pipenv run "python" "./mimiciii/steps/movements.py"
```

Finally, the last step for extraction is to collect the control flow and patient universe for the procedures log.
This event log captures procedures that occurred within a single patient admission for patients that presented with 'RESPIRATORY FAILURE' at admission.
Again this script will query Athena in partitions and output a single csv containing all events for this event log, ```mimiciii/out/procedures/controlflow_events.csv```.
Depending on your internet connection, the script will roughly take ~one minute, as this script is querying a very small portion of the MIMIC-III data set.
Run the following commands to complete this step.

```
python ./mimiciii/steps/procedures.py
```
OR if you are not in the pipenv shell:<br>
```
pipenv run "python" "./mimiciii/steps/procedures.py"
```

# Step Three - Transformation of data to XES

The next step in this reproduction data set is to instantiate our xPM framework with the given linking, slicing and transformation functions in our evaluation in [x].
We have included our original code base, which performed this task but acknowledge that in its given state, it is rather inelegant.
Future implementations will hopefully have more time to produce a general framework in a python module to simplify these steps or in a Plugin for ProM.

## Procedures log
To create the procedures log, described in [x], use the following commands. 

```
python process/steps/make.py -log procedures -threaded true (threads are used to reduce computation time)
or
python process/steps/make.py -log procedures (single thread used)
```
OR if you are not in the pipenv shell:<br>
```
pipenv run "python" "process/steps/make.py" -log procedures -threaded true
or
pipenv run "python" "process/steps/make.py" -log procedures
```

This command will produce three tubular versions of an event log, an endogenous log, an exogenous+endogenous log and an exogenous log.
To ensure that the same tubular version which was used in [x] was produced, this [readme.md](process/out/procedures/readme.md) contains checksums for each log.
Checksums for each log are displayed at the end of the script. Please compare these checksums to ensure that the same log is produced.

The computation time of this log is less than one minute.
## Movements log
To create the movements log, described in [x], use the following commands. 

```
python process/steps/make.py -log movements -threaded true (threads are used to reduce computation time)
or
python process/steps/make.py -log movements (single thread used)
```
OR if you are not in the pipenv shell:<br>
```
pipenv run "python" "process/steps/make.py" -log movements -threaded true
or
pipenv run "python" "process/steps/make.py" -log movements
```

This command will produce three tubular versions of an event log, an endogenous log, an exogenous+endogenous log and an exogenous log.
To ensure that the same tubular version which was used in [x] was produced, this [readme.md](process/out/movements/readme.md) contains checksums for each log.
Checksums for each log are displayed at the end of the script. Please compare these checksums to ensure that the same log is produced.

The computation time of this log is roughly ~6 hours on a single thread and ~10 minutes with 15 concurrent threads.

# References

[1] Johnson, A., Pollard, T., & Mark, R. (2016). MIMIC-III Clinical Database (version 1.4). PhysioNet. https://doi.org/10.13026/C2XW26.<br>
[x] 