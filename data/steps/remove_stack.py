import boto3 
from os import path
import os
import json
from time import sleep
from ults import check_hash_in_map

def stack_exists(name,client):
    try:
        data = client.describe_stacks(StackName = name)
    except client.exceptions.ClientError:
        return False
    return True

STACK_PARAMS = "./data/out/construct_out.json"

if __name__ == "__main__":
    print("checking for previous construction...")
    if (path.exists(STACK_PARAMS)):
        print("found previous build...")
        print("verfying contents...")
        params = json.load(open(STACK_PARAMS,'r'))
        if (check_hash_in_map(params)):
            print("verfied contents...")
            print("requesting stack deletion...")
            client = boto3.client('cloudformation')
            client.delete_stack(
                StackName=params["stackname"]
            )
            print("waiting for deletion...")
            sleep(2)
            while(stack_exists(params["stackname"],client)):
                print("waiting for deletion...")
                sleep(2)
            print("stack deleted...")
            os.remove(STACK_PARAMS)
            print("finished...")
        else:
            print("Unable to verify previous build, cancelling...")
    else:
        print("Unable to find previous build...")