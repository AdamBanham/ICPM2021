# ICPM2021 - Data Sources
A reproduction repository for MIMIC III ETL

Ensure that you have access to MIMIC III dataset and have completed the required ethics training to be using this publicly available dataset.<br><br>
The MIMIC III dataset and access information can be found at: <br>https://physionet.org/content/mimiciii/1.4/ <br>
If you wanted an in-depth understanding of the data available in this dataset, please refer to the official documentation at: <br>https://mimic.mit.edu/docs/iii/ <br><br>
We assume that you already have access to the data to reproduce the ETL steps for creating two datasets used for a ICPM2021 submission, a movements log and a procedure log. <br>
We also assume that you have an AWS account and are familiar with python, SQL and AWS services. In-depth knowledge of any of these languages are not needed, but some introduction knowledge will be helpful.<br>

## Disclaimer
Note that AWS services are not free. However, AWS does provide a level of freeness below a usage level. Therefore, following these steps to execute the creation of these datasets should not cost more than a dollar in the worst case. <br>
But we do not guarantee that following these steps will not result in a charge to your account.<br>
We recommend using cloud storage and cloud services to use this dataset as it provides universal access across many different types of personal computers available today.<br>

# Step Zero - Get Access 
Johnson has set up access on two cloud services providers, Google and AWS. To access these datasets so that you don't need to download the files locally then reupload them to a cloud service, ensure that you follow the steps below to request access to the dataset already on the cloud service. <br>
Follow steps for AWS : <br> 
(1) https://mimic.mit.edu/docs/gettingstarted/cloud/link/ <br> 
(2) https://mimic.mit.edu/docs/gettingstarted/cloud/request/ <br>

Using the web interface for aws console management, you can check if you have access by using a cloudshell terminal and running the following line of code: <br>

```aws s3 ls s3://mimic-iii-physionet ```

Which should provide the following output back: <br>

![cloudshell example](data/in/cloudshell.png)

# Step One - Create Queryable Interface


aws cli - https://github.com/aws/aws-cli/tree/v2



# Step Two - Running Queries


# Step Three - Transformation of data to XES