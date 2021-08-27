import boto3
import subprocess
import json
import hashlib

try :
    from migrate import OUTPUT_PATH as migrate_out
    MIGRATE_STEP = json.load(open(migrate_out,'r'))
except Exception:
    print("unable to find outcome from migrate step")
    raise NotImplementedError("Please run the migrate step before constructing a data base")

if __name__ == "__main__":
    print("Beginning to construct database...")


    print("Finished...")
