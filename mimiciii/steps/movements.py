from tqdm import tqdm 
import boto3 
from time import sleep 
from glob import glob 
from os import remove as remove_file
from os import path
import pandas as pd
from ults import check_hash_in_map,get_query_s3_location,update_query_dbname,BATCH_SIZE
import json
from more_itertools import chunked


CONTROLFLOW_SQL_SWAP = "##SUBJECTS##"
MOVEMENTS_PATIENT_UNIVERSE_SQL = "mimiciii/in/movements_patient_universe.SQL"
MOVEMENTS_CONTROFLOW_SQL = "mimiciii/in/movements_controlflow.SQL"

MOVEMENTS_LOG_CONTROLFLOW_OUT_DIR = "mimiciii/out/movements/"
MOVEMENTS_LOG_PATIENTS_OUT = "patient_universe.csv"
MOVEMENTS_LOG_CONTROLFLOW_OUT_CSV = "controlflow_events.csv"

def run_query(sql,filename,res_bucket:str,res_out:str):
    # connect to aws athena
    client = boto3.client("athena")
    # send request
    tqdm.write("starting query...")
    res = client.start_query_execution(
        QueryString=sql,
        ResultConfiguration={
            'OutputLocation' : f"s3://{res_bucket}/{res_out}"
        }
    )
    # wait for completion
    id = res['QueryExecutionId']
    status = client.get_query_execution(
        QueryExecutionId=id
    )['QueryExecution']['Status']['State']
    while status not in ['SUCCEEDED','FAILED','CANCELLED']:
        tqdm.write(f"waiting for query to complete ({status})...")
        sleep(2.5)
        status = client.get_query_execution(
            QueryExecutionId=id
        )['QueryExecution']['Status']['State']
    tqdm.write(f"query final state was :: {status}")
    # collect result, copy to local, remove from remote
    if status == 'SUCCEEDED':
        tqdm.write("downloading query...")
        s3 = boto3.resource("s3")
        s3.meta.client.download_file(res_bucket,f'{res_out}/{id}.csv',f'{filename}')
        return pd.read_csv(f"{filename}")
    else:
        raise SystemError(f"query failed :: {status}")
        return []

if __name__ == "__main__": 
    # find bucket and temp folder
    res_bucket, res_out = get_query_s3_location()
    tqdm.write(f"Save location for athena queries on s3 will be : s3://{res_bucket}/{res_out}")
    #load in athena statements 
    tqdm.write(f"loading query statements...")
    subset_patients_sql = update_query_dbname(open(MOVEMENTS_PATIENT_UNIVERSE_SQL).read())
    controlflow_patients_sql = update_query_dbname(open(MOVEMENTS_CONTROFLOW_SQL).read())
    # get patients
    tqdm.write(f"getting patient universe...")
    patients = run_query(subset_patients_sql,MOVEMENTS_LOG_CONTROLFLOW_OUT_DIR+MOVEMENTS_LOG_PATIENTS_OUT,res_bucket, res_out)
    subject_ids = tuple(patients.subject_id.values)
    controflow_events_df = pd.DataFrame()
    # find controlflow events in batches
    tqdm.write(f"finding event universe...")
    for subset in tqdm(chunked(subject_ids,BATCH_SIZE),desc="collecting controlflow events", total= len(list(chunked(subject_ids,BATCH_SIZE)))):
        temp_df = run_query(
            controlflow_patients_sql.replace(CONTROLFLOW_SQL_SWAP,str(tuple(set(subset)))),
            MOVEMENTS_LOG_CONTROLFLOW_OUT_DIR+"temp.csv",
            res_bucket,
            res_out
        )
        controflow_events_df = pd.concat([controflow_events_df,temp_df])
    #save out controlfow events
    remove_file(MOVEMENTS_LOG_CONTROLFLOW_OUT_DIR+"temp.csv")
    tqdm.write(f"saving event universe...")
    controflow_events_df.to_csv(MOVEMENTS_LOG_CONTROLFLOW_OUT_DIR+MOVEMENTS_LOG_CONTROLFLOW_OUT_CSV,index=False)
    tqdm.write(f"finished...")