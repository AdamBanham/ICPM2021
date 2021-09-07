from tqdm import tqdm 
from os import remove as remove_file
import pandas as pd
from ults import get_query_s3_location,update_query_dbname,BATCH_SIZE,run_query
from more_itertools import chunked


CONTROLFLOW_SQL_SWAP = "##SUBJECTS##"
MOVEMENTS_PATIENT_UNIVERSE_SQL = "mimiciii/in/movements_patient_universe.SQL"
MOVEMENTS_CONTROFLOW_SQL = "mimiciii/in/movements_controlflow.SQL"

MOVEMENTS_LOG_CONTROLFLOW_OUT_DIR = "mimiciii/out/movements/"
MOVEMENTS_LOG_PATIENTS_OUT = "patient_universe.csv"
MOVEMENTS_LOG_CONTROLFLOW_OUT_CSV = "controlflow_events.csv"

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