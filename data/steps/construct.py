import boto3
from time import sleep
import json
import hashlib

LOCATION_KEY = "###LOCATION###"
DATABASE_NAME = "###DBNAME###"
TEMPLATE_LOCATION = "data/in/athena_template.yaml"


try :
    from migrate import OUTPUT_PATH as migrate_out
    MIGRATE_STEP = json.load(open(migrate_out,'r'))
except Exception:
    print("unable to find outcome from migrate step")
    raise NotImplementedError("Please run the migrate step before constructing a data base")

if __name__ == "__main__":
    #load in template
    print("Loading template...")
    template = open(TEMPLATE_LOCATION,'r').read()
    template = template.replace(LOCATION_KEY,MIGRATE_STEP['location'])
    print("Cloud formation template ready...")
    #get user input
    print("Beginning to construct database...")
    stack_name = input("Name for cloud formation stack:")
    database_name = input("Name for database:")
    template = template.replace(DATABASE_NAME,database_name)
    tags = []
    add_tags = input("Add tags to stack and resources made? [Y/n]")
    while add_tags not in ['Y','n']:
        print("Options are: [Y]es or [n]o")
        add_tags = input("Add tags to stack and resources made? [Y/n]")

    if add_tags == 'Y':
        add_tags = True
    else:
        add_tags = False
    while add_tags:
        key = input("Tag Key:")
        value = input("Tag Value:")
        tags.append({
            'Key' : key,
            'Value' : value
        })
        add_more = input("Add more tags? [Y/n]")
        while add_more not in ['Y','n']:
            print("Options are: [Y]es or [n]o")
            add_more = input("Add more tags? [Y/n]")
        if add_more == 'Y':
            add_tags = True
        else:
            add_tags = False
    print("constructing database...")
    #build cloud formation
    client = boto3.client('cloudformation')
    try :
        response = client.create_stack(
            StackName=stack_name,
            TemplateBody=template,
            Tags=tags
        )
        udid = response['StackId']
        print(f"[{udid}] stack construction started...")
        #check progress of build
        response = client.describe_stacks(
            StackName=stack_name,
        )
        stack = None
        for stacker in response['Stacks']:
            if stacker["StackId"] == udid:
                stack = stacker
                break 
        while stack['StackStatus'] != 'CREATE_COMPLETE':
            print(f"[{udid}] constructing...")
            sleep(2)
            response = client.describe_stacks(
                StackName=stack_name,
            )
            stack = None
            for stacker in response['Stacks']:
                if stacker["StackId"] == udid:
                    stack = stacker
                    break 
        print(f"[{udid}] stack construction completed...")
    except client.exceptions.AlreadyExistsException:
        print("Failed to create stack, as stackname is already in use.")

    print("Finished...")
