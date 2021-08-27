import boto3
import subprocess
import json
import hashlib
import sys 

OUTPUT_PATH = "data/out/migrate_out.json"

if __name__ == "__main__":
    # get user input
    print("Beginning Migration...")
    bucket = input("Target Bucket Name :")
    region = input("Target Region :")
    location = f"s3://{bucket}/data/"
    print(f"Using the following s3 location as a migration target: {location}")
    #check bucket exits
    exists = False
    s3 = boto3.client('s3')
    try :
        s3.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={
                'LocationConstraint' : region
            }
        )
        exists = True
    except s3.exceptions.BucketAlreadyExists:
        exists = True
    except s3.exceptions.BucketAlreadyOwnedByYou:
        exists = True
    except s3.exceptions.ClientError as e:
        print("Unable to check if a bucket exits, likely an issue with AWS permission for your account or input.")
        raise(e)
    # start migrate
    if (exists):
        push = subprocess.call([ 
            "aws",
            "s3",
            "sync",
            "s3://mimic-iii-physionet",
            location,
            "--source-region",
            "us-east-1",
        ])

        print(f"Mirgation completed :: {push}")
        print("Saving location...")
        # save out user input
        out = {
            'location' : location
        }
        # add hash
        hash = hashlib.md5(json.dumps(out).encode()).hexdigest()
        out['hash'] = hash 
        # save output
        json.dump(out,open(OUTPUT_PATH,'w'),indent=2)
    else:
        print("unable to find migration target, stopping process...")
    print("Finished...")
