from tqdm import tqdm
from glob import glob
from os import remove as remove_file
from ults import run_query,get_query_s3_location,update_query_dbname,BATCH_SIZE
from more_itertools import chunked

EXOGENOUS_DIR = "mimiciii/out/exogenous/"

EXOGENOUS_DATAPOINTS_SQL = "mimiciii/in/exogenous_datapoints.SQL"
EXOGENOUS_DATAPOINTS_SWAP = "##SUBJECT_ID##"
EXOGENOUS_PATIENTS_SQL = "mimiciii/in/exogenous_patient_universe.SQL"

EXOGENOUS_FILENAME = "PATIENT_{ID}.csv"

if __name__ == "__main__":
    #check for previous steps
    res_bucket, res_out = get_query_s3_location()
    #load in athena statements
    patients_sql = update_query_dbname(open(EXOGENOUS_PATIENTS_SQL).read())
    exo_datapoints_sql = update_query_dbname(open(EXOGENOUS_DATAPOINTS_SQL).read())
    # clear patients folder before filling
    tqdm.write("clearing cache...")
    for file in list(glob(EXOGENOUS_DIR+"/PATIENT_*.csv")):
        remove_file(file)
    # get patient universe
    tqdm.write("getting patient universe...")
    patients = run_query(
        patients_sql,
        EXOGENOUS_DIR+"patient_universe.csv",
        res_bucket,
        res_out
    )
    patients = list(chunked(patients.subject_id.values,BATCH_SIZE))
    tqdm.write("collected patient universe...")
    # begin pulling data from chartevents
    counted = 0
    tqdm.write("getting exogenous universe...")
    for patients in tqdm(patients,desc="collecting exogenous",total=len(patients)):
        if len(patients) < 1:
            continue
        tqdm.write("starting bulk query...")
        bulk_data = run_query(
            exo_datapoints_sql.replace(EXOGENOUS_DATAPOINTS_SWAP,str(tuple(set(patients)))),
            EXOGENOUS_DIR+"TEMP.CSV",
            res_bucket,
            res_out
            )
        for patient,data in bulk_data.groupby("patient"):
            tqdm.write(f"saving patient -- {patient} ...")
            data.to_csv(f"{EXOGENOUS_DIR}{EXOGENOUS_FILENAME.format(ID=patient)}",index=False)
            counted += 1
    tqdm.write(f"total number of patients sampled :: {counted}")
    tqdm.write("Finished...")