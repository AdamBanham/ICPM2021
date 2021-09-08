import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy.signal import stft
from os import remove as remove_file
from glob import glob
from tqdm import tqdm
from more_itertools import grouper,windowed
from joblib import Parallel,delayed
import json
from typing import Tuple
import sys

from ults import get_hash_checksum

import hashlib
def md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def get_filenames(pattern):
    for file in glob(pattern):
        yield file

def load_data(filename):
    return pd.read_csv(filename)

def find_slicing_events(controlflow, stream):
    # find slicing events of stream, such at least two events exists at the end and start of stream
    event_times = [] 
    # find nearest events to streams
    nearestidx = [
        tuple([value,np.searchsorted(stream,value, side='left')])
        for idx,value
        in enumerate(controlflow)
    ]
    # update ends 
    starts = [
        real
        for real,sortidx
        in nearestidx
        if sortidx == 0
    ]
    ends = [
        real
        for real,sortidx
        in nearestidx
        if sortidx == len(stream)
    ]
    # find any slicing events
    slicers = [
        real
        for real,sortidx
        in nearestidx
        if real not in starts and real not in ends
    ]
    # check for edge cases
    # # no events slices the start of the stream
    if len(starts) < 1:
        if len(slicers) > 1:
            starts = [slicers[0]]
            slicers = slicers[1:]
        else:
            raise NotImplementedError("Unable to handle case where no starting event can be found before stream and no events slice stream")
    # # no events slice the end of the stream
    if len(ends) < 1:
        if len(slicers) > 1:
            ends = [slicers[-1]]
            slicers = slicers[0:-1]
        else:
            raise NotImplementedError("Unable to handle case where no ending event can be found after stream and no events are left in slice section")
    # combine events and return
    event_times = [tuple(["START",np.max(starts)])] + [ tuple(["SLICE",val]) for val in slicers] + [tuple(["END",np.min(ends)])]
    return event_times 

def order_events(controlflow):
    controlflow_time = controlflow.time_complete.dropna().values
    # controlflow_time = np.concatenate((controlflow_time,
    #     controlflow.time_start.dropna().values)
    # )
    try:
        controlflow_time = [ pd.Timestamp(val).tz_convert(None) for val in controlflow_time ]
    except TypeError:
        controlflow_time = [ pd.Timestamp(val) for val in controlflow_time ]
    controlflow_time.sort()
    trace_start = controlflow_time[0]
    controlflow_time = [0] + [
        (value - trace_start).total_seconds()/60
        for value 
        in controlflow_time[1:]
    ]
    return controlflow_time,trace_start

def plot_scatters(exo_time,exo_data,slices,label,format="--"):
    # find nearest exo_time and exo_data for slices
    nearest = [
        np.searchsorted(exo_time,val,side="left")-1
        for val 
        in slices
    ]
    nearest = [
        val if val < len(exo_time) else len(exo_time) - 1 
        for val 
        in nearest
    ]
    nearest = [
        val if val >= 0 else 0
        for val 
        in nearest
    ]
    # create x and y for plots
    ## create min and max
    y_data = pd.Series(exo_data).dropna()
    y = [np.min(y_data),np.max(y_data)]
    y = [y[0] - (.1 * y[0]),y[1] + (.1 * y[1])]
    tqdm.write(f"plot scatter {label}")
    for idx,val in zip(nearest,slices):
        x = [exo_time[idx],exo_time[idx]]
        tqdm.write(f"x :: {x}")
        tqdm.write(f"y :: {y}")
        plt.plot(
            x,
            y,
            format,
            label=f"{label} : {np.round(val - x[0],1)}"
        )

def create_derviative(times:list,values:list) -> Tuple[list,list]:
    dtime = []
    dvalues = []
    if (len(times) < 2):
        return [], []
    else:
        for timepair,valuepair in zip(windowed(times,2),windowed(values,2)):
            if (None in timepair or None in valuepair):
                continue
            dtime.append(timepair[-1])
            dvalues.append(
                (valuepair[1] - valuepair[0]) / (timepair[1] - timepair[0])
            )
    return dtime,dvalues

STREAM_DICT_SWAPPER = {
    "spo2" : "O21",
    "o2 saturation pulseoxymetry" : "O22",
    "respiratory rate" : "RR1",
    "heart rate" : "HR1",
    "hr alarm [low]" : "HR2",
    "hr alarm [high]" : "HR3",
    "arterial bp mean" : "ABP1",
    "arterial bp [systolic]" : "ABP2"
}

def convert_stream_name(stream_name:str,suffix=None) -> str:
    if stream_name in STREAM_DICT_SWAPPER.keys():
        stream_name = STREAM_DICT_SWAPPER[stream_name]
    if (not suffix == None):
        return stream_name +"_"+suffix
    else:
        return stream_name

def create_agg_statements(slice_start,slice_end,exo_points,exo_time,stream_name,debug=False):
    if (debug):
        tqdm.write(f"created exogenous aggerates for {slice_start} to {slice_end}")
    # create segement 
    segment_time = pd.Series(exo_time)
    segment_data = pd.Series(exo_points)
    # exo features
    rounder = lambda x: np.round(x,1)
    data = segment_data[(segment_time <= slice_end[1]) & (segment_time >= slice_start[1])]
    inter_segement = segment_data[(segment_time <= slice_end[1]) & (segment_time >= slice_start[1])].values.tolist()
    exo_min_inter = rounder(np.min(data.dropna()))
    exo_max_inter = rounder(np.max(data.dropna()))
    exo_mean_inter = rounder(np.mean(data.dropna()))
    _,_,exo_stft_inter = stft(data, 1, nperseg=3 if len(data) > 2 else 1)
    exo_stft_inter = rounder(np.sum(np.abs(exo_stft_inter)))
    data = segment_data[(segment_time <= slice_end[1])]
    prev_segement = data.values.tolist()
    exo_min_prev = rounder(np.min(data.dropna()))
    exo_max_prev = rounder(np.max(data.dropna()))
    exo_mean_prev = rounder(np.mean(data.dropna()))
    _,_,exo_stft_prev = stft(data, 1, nperseg=3 if len(data) > 2 else 1)
    exo_stft_prev = rounder(np.sum(np.abs(exo_stft_prev)))
    # create dataframe
    exo_aggs = pd.DataFrame([
        [
            inter_segement, exo_min_inter, exo_max_inter, exo_mean_inter,exo_stft_inter,
            prev_segement, exo_min_prev, exo_max_prev, exo_mean_prev, exo_stft_prev
        ]
    ],columns=[
        f"{stream_name.replace(' ','')}_i_signal",
        f"{stream_name.replace(' ','')}_i_min",
        f"{stream_name.replace(' ','')}_i_max",
        f"{stream_name.replace(' ','')}_i_mean",
        f"{stream_name.replace(' ','')}_i_stft",
        f"{stream_name.replace(' ','')}_p_signal",
        f"{stream_name.replace(' ','')}_p_min",
        f"{stream_name.replace(' ','')}_p_max",
        f"{stream_name.replace(' ','')}_p_mean",
        f"{stream_name.replace(' ','')}_p_stft"
    ])
    if (debug):
        tqdm.write(f"(inter) min : {exo_aggs.values[0,0]}")
        tqdm.write(f"(inter) max : {exo_aggs.values[0,1]}")
        tqdm.write(f"(inter) mean : {exo_aggs.values[0,2]}")
        tqdm.write(f"(inter) stft : {exo_aggs.values[0,3]}")
        tqdm.write(f"(prev) min : {exo_aggs.values[0,4]}")
        tqdm.write(f"(prev) max : {exo_aggs.values[0,5]}")
        tqdm.write(f"(prev) mean : {exo_aggs.values[0,6]}")
        tqdm.write(f"(prev) stft : {exo_aggs.values[0,7]}")
    return list(exo_aggs.values[0,:]), list(exo_aggs.columns)

def threaded_work(filenames,cache_num,plots=False,debug=False):
    # load in controlflow events
    controlflow = pd.read_csv(TARGET_CONTROLFLOW)
    controlflow["event_id"] = range(1,len(controlflow.index)+1)
    controlflow["trace_patient"] = controlflow["trace_patient"].astype(str)
    endo_controlfow = pd.DataFrame([],columns=controlflow.columns, dtype="object")
    exo_controlflow = pd.DataFrame([],columns=controlflow.columns, dtype="object")
    exoOnly_controflow = pd.DataFrame([], columns=TARGET_EXO_COLS, dtype="object")
    new_controlflow = controlflow.copy()
    # get all control flow events for these patients
    patients = [ filename.split("_")[-1].split(".")[0] for filename in filenames if filename != None ]
    new_controlflow = new_controlflow[new_controlflow.trace_patient.isin(patients)]
    # loop through patients
    for filename in [ file for file in filenames if file != None]:
        # get patient info
        try :
            patient = filename.split("_")[-1].split(".")[0]
        except Exception as e:
            tqdm.write(f"cannot find {filename} :: {e}")
            continue
        patient_flow = new_controlflow[new_controlflow.trace_patient == patient]
        # make sure to only consider a single trace when slicing
        for key,patientflow_group in patient_flow.groupby("trace_concept"):
            # create time stream for control flow
            patientflow_time,trace_start = order_events(patientflow_group.copy())
            patientflow_group = patientflow_group[patientflow_group.time_complete.isna() == False]
            # load exo data to consider
            try :
                exo_data = load_data(filename)
            except Exception as e:
                tqdm.write(f"cannot find {filename} :: {e}")

            try :
                exo_data["starttime"] = [
                    pd.Timestamp(val).tz_convert(None)
                    for val 
                    in exo_data["starttime"]
                ]
            except TypeError:
                exo_data["starttime"] = [
                    pd.Timestamp(val)
                    for val 
                    in exo_data["starttime"]
                ]
            exo_data["value"] = pd.to_numeric(exo_data["value"],'coerce','float')
            if exo_data.shape[0] > 0:
                # add this groupset to endo log
                endo_controlfow = endo_controlfow.append(
                    patientflow_group.copy()
                )
                exo_controlflow = exo_controlflow.append(
                    patientflow_group.copy()
                )
                exoOnly_controflow = exoOnly_controflow.append(
                    patientflow_group[TARGET_EXO_COLS].copy()
                )
            # for each exo stream find slicing events and attach
            for stream_name in exo_data.label.unique():
                # filter streams such that they are after the trace start
                filtered_exo_data = exo_data[exo_data.starttime > trace_start]
                exo_points = list(filtered_exo_data[filtered_exo_data.label == stream_name].value.values)
                exo_time = list(filtered_exo_data[filtered_exo_data.label == stream_name].starttime.values)
                if len(exo_time) < 2:
                    if (debug):
                        tqdm.write(f"{patient} -- {key} -- {stream_name} :: not enough exo time points to be considered")
                    continue
                #  convert to relative time
                exo_time = [
                    (pd.Timestamp(value) - trace_start).total_seconds()/60
                    for value 
                    in exo_time
                ]
                exo_time = pd.Series(exo_time)
                # find slicing events
                try:
                    event_slices = find_slicing_events(patientflow_time,exo_time)
                except Exception as e:
                    if (debug):
                        tqdm.write(f"{patient} -- {key} -- {stream_name} :: error occured while slicing :: {e}")
                    continue
                if (debug):
                    tqdm.write(f"{patient} -- {key} -- {stream_name} :: is being finalised")
                # create plots if required
                if (plots):
                    fig = plt.figure(figsize=(8,5))
                    plt.plot(
                        exo_time,
                        exo_points
                    )
                    # PLOT START, SLICES AND ENDS 
                    for slice_type in ["START","SLICE","END"]:
                        slicing_events = [
                            val[1]
                            for val 
                            in event_slices
                            if val[0] == slice_type
                        ]
                        plot_scatters(exo_time,exo_points,slicing_events,slice_type)
                    # prettier the plot and save out for visual confirmation
                    plt.title(f"{patient} -- {key} -- {stream_name}")
                    plt.xlabel("relative minutes since start of trace")
                    plt.xlim([-50,np.max(controlflow_time)+50])
                    plt.legend()
                    fig.savefig(f"./out/exo/{patient}_{key}_{stream_name.replace(' ','_')}.png",format="png",dpi=100)
                    plt.close(fig)
                    del fig
                # CREATE agg statements 
                # d1_exo_time,d1_exo_points = create_derviative(exo_time,exo_points)
                for start,end in zip(event_slices[:-1],event_slices[1:]):
                    # add zeroth derivation
                    values, columns = create_agg_statements(start,end, exo_points, exo_time, convert_stream_name(stream_name.lower(),""))
                    # find attached event 
                    idxs = patientflow_group.index[pd.Series(patientflow_time) == end[1]]
                    event_id = patientflow_group.loc[idxs].event_id
                    if (debug):
                        tqdm.write(f"{values}")
                        tqdm.write(f"{len(values)}")
                        tqdm.write(f"{columns}")
                        tqdm.write(f"{len(columns)}")
                        tqdm.write(f"attaching to event :: {event_id.values}")
                    # add to copy of controlflow events
                    for val,col in zip(values,columns):
                        # check that col exists
                        if col not in exo_controlflow.columns:
                            exo_controlflow[col] = np.nan 
                            exoOnly_controflow[col] = np.nan
                        if "_signal" in col:
                            exo_controlflow.loc[event_id.index,col] = json.dumps(val) 
                            exoOnly_controflow.loc[event_id.index,col] = json.dumps(val)
                        else: 
                            exo_controlflow.loc[event_id.index,col] = val
                            exoOnly_controflow.loc[event_id.index,col] = val
                    # # add first derivation
                    # if (len(d1_exo_time) == 0):
                    #     continue
                    # values, columns = create_agg_statements(start,end, d1_exo_points, d1_exo_time, convert_stream_name(stream_name.lower(),"D1"))
                    # # find attached event 
                    # idxs = patientflow_group.index[pd.Series(patientflow_time) == end[1]]
                    # event_id = patientflow_group.loc[idxs].event_id
                    # if (debug):
                    #     tqdm.write(f"{values}")
                    #     tqdm.write(f"{len(values)}")
                    #     tqdm.write(f"{columns}")
                    #     tqdm.write(f"{len(columns)}")
                    #     tqdm.write(f"attaching to event :: {event_id.values}")
                    # # add to copy of controlflow events
                    # for val,col in zip(values,columns):
                    #     # check that col exists
                    #     if col not in exo_controlflow.columns:
                    #         exo_controlflow[col] = np.nan 
                    #         exoOnly_controflow[col] = np.nan
                    #     if "_signal" in col:
                    #         pass
                    #         # new_controlflow.loc[event_id.index,col] = json.dumps(val) 
                    #     else: 
                    #         exo_controlflow.loc[event_id.index,col] = val
                    #         exoOnly_controflow.loc[event_id.index,col] = val
    tqdm.write(f"caching out current controlflow events |{cache_num}|...")
    exo_cols = [
            col for col in exo_controlflow.columns if "_min" in col
            ] + [
            col for col in exo_controlflow.columns if "_mean" in col
            ] + [
            col for col in exo_controlflow.columns if "_max" in col
            ] + [
            col for col in exo_controlflow.columns if "_stft" in col
            ]
    for col,df in zip(exo_cols,[exo_controlflow,exoOnly_controflow]):
        df[col] = df[col].values.astype(float)
        df[col] = [ 
            np.round(val,3)
            for val 
            in df[col].values
        ]
    new_controlflow.to_csv(f"{CACHE_DIR}endo/{cache_num}.csv",index=False)
    exo_controlflow.to_csv(f"{CACHE_DIR}exo/{cache_num}.csv",index=False)
    exoOnly_controflow.to_csv(f"{CACHE_DIR}exoonly/{cache_num}.csv",index=False)

def threaded_workflow(debug:bool=False):
    # create grouper
    patient_universe = pd.read_csv(TARGET_PATIENT_LIST)
    exogenous = [ 
        file 
        for file 
        in get_filenames(TARGET_PATTERN) 
        if 
            int(file.split('_')[1].replace(".csv","")) 
            in 
            patient_universe.subject_id.values
    ]
    filenames = list(grouper(exogenous,THREAD_GROUPS))
    total_batchs = len(filenames)
    # create threadpool
    tqdm.write("starting work...")
    with Parallel(n_jobs=NUM_THREADS,verbose=-1) as pool:
        # begin workers
        pool(delayed(threaded_work)(filegroup,group,False,debug) for group,filegroup in enumerate(tqdm(filenames,desc="thread batchs",ncols=150,total=total_batchs)) )
    # collected cached csvs and recompose
    fileset = [glob(CACHE_DIR+"endo/*.csv"),glob(CACHE_DIR+"exo/*.csv"),glob(CACHE_DIR+"exoonly/*.csv")]
    swaps = ["endo","endo+exo","exo"]
    dfs = [pd.DataFrame(),pd.DataFrame(),pd.DataFrame()]
    for df_key,files,swap in zip(range(len(fileset)),fileset,swaps):
        for file in tqdm(files,desc="merging results",ncols=150):
            dfs[df_key] = pd.concat([dfs[df_key],pd.read_csv(file)],ignore_index=True)
        # ensure that col types are the same
        ## checksum is getting non-matching cases
        exo_cols = [
            col for col in dfs[df_key].columns if "_min" in col
            ] + [
            col for col in dfs[df_key].columns if "_mean" in col
            ] + [
            col for col in dfs[df_key].columns if "_max" in col
            ] + [
            col for col in dfs[df_key].columns if "_stft" in col
            ]
        for col in exo_cols:
            dfs[df_key][col] = dfs[df_key][col].values.astype(float)
            dfs[df_key][col] = [ 
                np.round(val,3)
                for val 
                in dfs[df_key][col].values
            ]
        # save out file
        dfs[df_key] = dfs[df_key].sort_values(by=["trace_concept","time_complete"])
        dfs[df_key].to_csv(TARGET_OUTPUT.replace("endo",swap),index=False)
        #create hashsum of file
        print(f"{swap} checksum :: {get_hash_checksum(TARGET_OUTPUT.replace('endo',swap))}")
    tqdm.write("outcome saved to hard drive...")
    # perform checksum if samplesize is in testcases
    
def single_threaded_workflow(plots=False,debug=False):
    # GET CONTROL FLOW EVENTS
    controlflow = pd.read_csv(TARGET_CONTROLFLOW)
    controlflow["event_id"] = range(1,len(controlflow.index)+1)
    controlflow["trace_patient"] = controlflow["trace_patient"].astype(str)
    endo_controlfow = pd.DataFrame([],columns=controlflow.columns, dtype="object")
    exo_controlflow = pd.DataFrame([],columns=controlflow.columns, dtype="object")
    exoOnly_controflow = pd.DataFrame([], columns=TARGET_EXO_COLS, dtype="object")
    #setup patient universe
    patient_universe = pd.read_csv(TARGET_PATIENT_LIST)
    patient_count = patient_universe.shape[0]
    exogenous = [ 
        file 
        for file 
        in get_filenames(TARGET_PATTERN) 
        if 
            int(file.split('_')[1].replace(".csv","")) 
            in 
            patient_universe.subject_id.values
    ]
    # GET INDIVIDUAL PATIENT DATA
    for num,filename in tqdm(enumerate(exogenous),total=patient_count,desc="Handling Patient Charts"):
        # get all control flow events for this patient
        patient = filename.split("_")[-1].split(".")[0]
        controlflow_events = controlflow[controlflow.trace_patient == patient]
        # make sure to only consider a single trace when slicing
        for key,controlflow_group in controlflow_events.groupby("trace_concept"):
            # create time stream for control flow
            controlflow_time,trace_start = order_events(controlflow_group.copy())
            controlflow_group = controlflow_group[controlflow_group.time_complete.isna() == False]
            # load exo data to consider
            exo_data = load_data(filename)
            try : 
                exo_data["starttime"] = [
                    pd.Timestamp(val).tz_convert(None)
                    for val 
                    in exo_data["starttime"]
                ]
            except TypeError:
                exo_data["starttime"] = [
                    pd.Timestamp(val)
                    for val 
                    in exo_data["starttime"]
                ]
            exo_data["value"] = pd.to_numeric(exo_data["value"],'coerce','float')
            if exo_data.shape[0] > 0:
                # add this groupset to endo log
                endo_controlfow = endo_controlfow.append(
                    controlflow_group.copy()
                )
                exo_controlflow = exo_controlflow.append(
                    controlflow_group.copy()
                )
                exoOnly_controflow = exoOnly_controflow.append(
                    controlflow_group[TARGET_EXO_COLS].copy()
                )
            # for each exo stream find slicing events and attach
            for stream_name in exo_data.label.unique():
                # filter streams such that they are after the trace start
                filtered_exo_data = exo_data[exo_data.starttime > trace_start]
                exo_points = list(filtered_exo_data[filtered_exo_data.label == stream_name].value.values)
                exo_time = list(filtered_exo_data[filtered_exo_data.label == stream_name].starttime.values)
                if len(exo_time) < 2:
                    if (debug):
                        tqdm.write(f"{patient} -- {key} -- {stream_name} :: not enough exo time points to be considered")
                    continue
                #  convert to relative time
                exo_time = [
                    (pd.Timestamp(value) - trace_start).total_seconds()/60
                    for value 
                    in exo_time
                ]
                exo_time = pd.Series(exo_time)
                try:
                    event_slices = find_slicing_events(controlflow_time,exo_time)
                except Exception as e:
                    if (debug):
                        tqdm.write(f"{patient} -- {key} -- {stream_name} :: error occured while slicing :: {e}")
                    continue
                if (debug):
                    tqdm.write(f"{patient} -- {key} -- {stream_name} :: is being finalised")
                # CREATE agg statements 
                # d1_exo_time,d1_exo_points = create_derviative(exo_time,exo_points)
                for start,end in zip(event_slices[:-1],event_slices[1:]):
                    values, columns = create_agg_statements(start,end, exo_points, exo_time, convert_stream_name(stream_name.lower(),""))
                    # find attached event 
                    idxs = controlflow_group.index[pd.Series(controlflow_time) == end[1]]
                    event_id = controlflow_group.loc[idxs].event_id
                    if (debug):
                        tqdm.write(f"{values}")
                        tqdm.write(f"{len(values)}")
                        tqdm.write(f"{columns}")
                        tqdm.write(f"{len(columns)}")
                        tqdm.write(f"attaching to event :: {event_id.values}")
                    # add to copy of controlflow events
                    for val,col in zip(values,columns):
                        # check that col exists
                        if col not in exo_controlflow.columns:
                            exo_controlflow[col] = np.nan 
                            exoOnly_controflow[col] = np.nan
                        if "_signal" in col:
                            exo_controlflow.loc[event_id.index,col] = json.dumps(val) 
                            exoOnly_controflow.loc[event_id.index,col] = json.dumps(val) 
                        else: 
                            exo_controlflow.loc[event_id.index,col] = val
                            exoOnly_controflow.loc[event_id.index,col] = val
                if (plots):
                    fig = plt.figure(figsize=(8,5))
                    plt.plot(
                        exo_time,
                        exo_points
                    )
                    # PLOT START, SLICES AND ENDS 
                    for slice_type in ["START","SLICE","END"]:
                        slicing_events = [
                            val[1]
                            for val 
                            in event_slices
                            if val[0] == slice_type
                        ]
                        plot_scatters(exo_time,exo_points,slicing_events,slice_type)
                    # prettier the plot and save out for visual confirmation
                    plt.title(f"{patient} -- {key} -- {stream_name}")
                    plt.xlabel("relative minutes since start of trace")
                    plt.xlim([-50,np.max(controlflow_time)+50])
                    plt.legend()
                    fig.savefig(f"./out/exo/{patient}_{key}_{stream_name.replace(' ','_')}.png",format="png",dpi=100)
                    plt.close(fig)
                    del fig
        if (num % 50) == 0:
            tqdm.write("caching out current controlflow events...")
            endo_controlfow.to_csv(TARGET_OUTPUT,index=False)
            exo_controlflow.to_csv(TARGET_OUTPUT.replace("endo","endo+exo"),index=False)
            exoOnly_controflow.to_csv(TARGET_OUTPUT.replace("endo","exo"),index=False)
    # exo column
    exo_cols = [
            col for col in exo_controlflow.columns if "_min" in col
            ] + [
            col for col in exo_controlflow.columns if "_mean" in col
            ] + [
            col for col in exo_controlflow.columns if "_max" in col
            ] + [
            col for col in exo_controlflow.columns if "_stft" in col
            ]
    for col in exo_cols:
        exo_controlflow[col] = exo_controlflow[col].values.astype(float)
        exo_controlflow[col] = [ 
            np.round(val,decimals=3)
            for val 
            in exo_controlflow[col].values
        ]
        exoOnly_controflow[col] = exoOnly_controflow[col].values.astype(float)
        exoOnly_controflow[col] = [ 
            np.round(val,decimals=3)
            for val 
            in exoOnly_controflow[col].values
        ]
    #sort values by trace_concept and event's complete time
    endo_controlfow = endo_controlfow.sort_values(by=["trace_concept","time_complete"])
    exo_controlflow = exo_controlflow.sort_values(by=["trace_concept","time_complete"])
    exoOnly_controflow = exoOnly_controflow.sort_values(by=["trace_concept","time_complete"])
    #save out
    endo_controlfow.to_csv(TARGET_OUTPUT,index=False)
    exo_controlflow.to_csv(TARGET_OUTPUT.replace("endo","endo+exo"),index=False)
    exoOnly_controflow.to_csv(TARGET_OUTPUT.replace("endo","exo"),index=False)
    #shout checksum
    print(f"endo checksum :: {get_hash_checksum(TARGET_OUTPUT)}")
    print(f"endo+exo checksum :: {get_hash_checksum(TARGET_OUTPUT.replace('endo','endo+exo'))}")
    print(f"exo checksum :: {get_hash_checksum(TARGET_OUTPUT.replace('endo','exo'))}")

CACHE_DIR = "process/out/cache/"

MOVEMENT_CONTROLFLOW_CSV = "mimiciii/out/movements/controlflow_events.csv"
MOVEMENT_OUTPUT = "process/out/movements/movement_log_endo.csv"
MOVEMENT_EXOONLY_COLS = ["trace_concept","event_name","time_start","time_complete","event_id"]
MOVEMENTS_PATIENT_UNIVERSE = "mimiciii/out/movements/patient_universe.csv"

PROCEDURE_CONTROLFLOW_CSV = "mimiciii/out/procedures/controlflow_events.csv"
PROCEDURE_OUTPUT = "process/out/procedures/procedures_log_endo.csv"
PROCEDURE_EXOONLY_COLS = ["trace_concept","event_name","time_start","time_complete","event_id"]
PROCEDURE_PATIENT_UNIVERSE = "mimiciii/out/procedures/patient_universe.csv"

EXOGENOUS_DATASET_PATTERN = "mimiciii/out/exogenous/PATIENT_[0-9]*.csv"

TARGET_CONTROLFLOW = MOVEMENT_CONTROLFLOW_CSV
TARGET_PATTERN = EXOGENOUS_DATASET_PATTERN
TARGET_OUTPUT = MOVEMENT_OUTPUT
TARGET_EXO_COLS = MOVEMENT_EXOONLY_COLS
TARGET_PATIENT_LIST = MOVEMENTS_PATIENT_UNIVERSE


THREADED_WORKFLOW = False
THREAD_GROUPS = 25
NUM_THREADS = -2


if __name__ == "__main__":
    # handle args
    args = sys.argv[1:]
    arg_sets = grouper(args,2)
    arg_dict = dict( (set[0],set[1]) for set in arg_sets if len(set) == 2)
    print("args ::" + str(arg_dict))
    #clearing cache
    print("clearing cache...")
    for file in glob(CACHE_DIR+"**/*.csv"):
        remove_file(file)
    print("cache cleared...")
    print("beginning set up...")
    #check if have a log to create
    try :
        if arg_dict['-log'] == 'movements':
            TARGET_CONTROLFLOW = MOVEMENT_CONTROLFLOW_CSV
            TARGET_PATTERN = EXOGENOUS_DATASET_PATTERN
            TARGET_OUTPUT = MOVEMENT_OUTPUT
            TARGET_EXO_COLS = MOVEMENT_EXOONLY_COLS
            TARGET_PATIENT_LIST = MOVEMENTS_PATIENT_UNIVERSE
        elif arg_dict['-log'] == 'procedures':
            TARGET_CONTROLFLOW = PROCEDURE_CONTROLFLOW_CSV
            TARGET_PATTERN = EXOGENOUS_DATASET_PATTERN
            TARGET_OUTPUT = PROCEDURE_OUTPUT
            TARGET_EXO_COLS = PROCEDURE_EXOONLY_COLS
            TARGET_PATIENT_LIST = PROCEDURE_PATIENT_UNIVERSE
        else:
            raise ValueError
    except ValueError:
        print("Unknown option for -log: possible options are ['movements','procedures']")
        sys.exit(1)
    except KeyError:
        print("Missing -log option: possible options are -log ['movements','procedures']")
        sys.exit(1)

    try :
        if arg_dict['-threaded'] == 'true':
            THREADED_WORKFLOW = True
        elif arg_dict['-threaded'] == 'false':
            THREADED_WORKFLOW = False
        else:
            raise ValueError
    except KeyError:
        print("-threaded option not specified, single thread workflow will be used")
    except ValueError:
        print("Unknown option for -threaded: possible options are ['true','false']")
        sys.exit(1)

    debug = False

    try : 
        if arg_dict['-debug'] == 'on':
            debug = True 
        else :
            raise ValueError
    except KeyError:
        pass 
    except ValueError:
        print("Unknown option for -debug: possible options are ['on']")
        sys.exit(1)

    print("set up completed...")
    if (THREADED_WORKFLOW):
        # threadpool to speed up computation
        threaded_workflow(debug=debug)
    else :
        # run workflow that uses doesn't use threads
        single_threaded_workflow(debug=debug)