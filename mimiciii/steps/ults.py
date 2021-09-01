import hashlib
from typing import Dict,Tuple
import json
from copy import deepcopy,copy
import boto3 
from time import sleep 
from tqdm import tqdm 
import pandas as pd
from os import path

CONSTRUCT_PARAMS = "data/out/construct_out.json"
MIGRATE_OUT = "data/out/migrate_out.json"

QUERY_DBNAME_REPLACE = "##dbname##"

def update_query_dbname(query:str) -> str:
    construct_params = json.load(open(CONSTRUCT_PARAMS,'r'))
    return query.replace(QUERY_DBNAME_REPLACE, construct_params["databasename"])

def get_query_s3_location() -> Tuple[str,str]:
    tqdm.write("checking for previous steps...")
    if (path.exists(CONSTRUCT_PARAMS) and path.exists(MIGRATE_OUT)):
        construct_params = json.load(open(CONSTRUCT_PARAMS,'r'))
        migrate_params = json.load(open(MIGRATE_OUT,'r'))
        if (check_hash_in_map(construct_params) and check_hash_in_map(migrate_params)):
            res_bucket = migrate_params["location"][5:].split("/")[0]
            res_out = migrate_params["location"][5:].split("/")[1] + f"/{construct_params['databasename']}/temp"
        else:
            raise AssertionError("Cannot verify contents of previous steps...")
    else:
        raise NotImplementedError("Need previous outcomes of data steps to continue.")
    return res_bucket,res_out

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

def check_hash_in_map(map:Dict[str,str]) -> bool:
    """
    Checks that contents of a map match its hash
    """
    verified = False
    if( "hash" not in map):
        raise NotImplementedError("Hash not include in map.")
    temp = deepcopy(map)
    hash = copy(temp["hash"])
    del temp["hash"]
    new_hash = hashlib.md5(json.dumps(temp).encode()).hexdigest()
    verified = hash == new_hash
    return verified