WITH PATIENT AS (
SELECT "subject_id" AS "trace_patient", "gender" as "trace_gender", "expire_flag" as "trace_dead" 
FROM ##dbname##."patients"
where "subject_id" IN ##SUBJECTS##
),

PATIENT_TRACES AS ( 
SELECT * 
FROM  ##dbname##."admissions"
JOIN PATIENT ON PATIENT."trace_patient" = "subject_id"
WHERE "subject_id" IN (SELECT "trace_patient" FROM PATIENT )
),

PATIENT_ADMISSIONS AS (
SELECT "admittime" AS "time_complete", 'hos_admission' AS "event_name",
"admission_location" AS "data_location",
"admission_type" AS "data_type", 
"trace_patient",
"trace_gender",
"trace_dead",
"hadm_id" AS "trace_concept",
"diagnosis" AS "trace_diagnosis"
FROM PATIENT_TRACES
WHERE "admittime" IS NOT NULL
),

PATIENT_DISCHARGES AS (
SELECT "dischtime" AS "time_complete", 'hos_discharge' AS "event_name",
"discharge_location" AS "data_location",
"admission_type" AS "data_type", 
"trace_patient",
"trace_gender",
"trace_dead",
"hadm_id" AS "trace_concept",
"diagnosis" AS "trace_diagnosis"
FROM PATIENT_TRACES
WHERE "dischtime" IS NOT NULL
AND "deathtime" IS NULL
),

PATIENT_DEATH AS (
SELECT "dischtime" AS "time_complete", 'hos_death' AS "event_name",
"admission_location" AS "data_location",
"admission_type" AS "data_type", 
"trace_patient",
"trace_gender",
"trace_dead",
"hadm_id" AS "trace_concept",
"diagnosis" AS "trace_diagnosis"
FROM PATIENT_TRACES
WHERE "dischtime" IS NOT NULL
AND "deathtime" IS NOT NULL
),

ADMISSION_TRANSFERS AS (
SELECT * 
FROM ##dbname##."transfers"
JOIN PATIENT ON PATIENT."trace_patient" = "subject_id"
WHERE "hadm_id" IN (SELECT "hadm_id" FROM PATIENT_TRACES)
),

PATIENT_WARDS_KNOWN AS (
SELECT date_add('minute', cast(round(rand() * -10) as bigint),"outtime") AS "time_complete", "curr_careunit" AS "event_name",
cast("curr_wardid" as varchar) AS "data_location",
"eventtype" AS "data_type", 
"trace_patient",
"trace_gender",
"trace_dead",
"hadm_id" AS "trace_concept",
NULL AS "trace_diagnosis",
"intime" as "time_start"
FROM ADMISSION_TRANSFERS
WHERE "eventtype" != 'discharge'
AND "curr_careunit" IS NOT NULL
),

LIKELY_PATIENT_OUTCARE AS (
SELECT "prev_careunit", "prev_wardid", COUNT(*) as "num_of_obs"
FROM ##dbname##."transfers"
WHERE "eventtype" = 'discharge'
AND "prev_careunit" IS NULL
group by "prev_careunit","prev_wardid"
order by "num_of_obs" DESC
LIMIT 7
),

PATIENT_WARDS_UNKNOWN AS (
SELECT date_add('minute', cast(round(rand() * -10) as bigint),"outtime") AS "time_complete", 'WARD' AS "event_name",
cast("curr_wardid" as varchar) AS "data_location",
"eventtype" AS "data_type", 
"trace_patient",
"trace_gender",
"trace_dead",
"hadm_id" AS "trace_concept",
NULL AS "trace_diagnosis",
"intime" as "time_start"
FROM ADMISSION_TRANSFERS
WHERE "eventtype" != 'discharge'
AND "curr_careunit" IS NULL
AND "curr_wardid" NOT IN (SELECT "prev_wardid" FROM LIKELY_PATIENT_OUTCARE)
),

PATIENT_WARDS_OUTCARE AS (
SELECT date_add('second', cast(round(rand() * - 60) as bigint),"outtime") AS "time_complete", 'OUTPATIENTCARE' AS "event_name",
cast("curr_wardid" as varchar) AS "data_location",
"eventtype" AS "data_type", 
"trace_patient",
"trace_gender",
"trace_dead",
"hadm_id" AS "trace_concept",
NULL AS "trace_diagnosis",
"intime" as "time_start"
FROM ADMISSION_TRANSFERS
WHERE "eventtype" != 'discharge'
AND "curr_careunit" IS NULL
AND "curr_wardid" IN (SELECT "prev_wardid" FROM LIKELY_PATIENT_OUTCARE)
),

ED_WARD_STAYS AS (
SELECT "edouttime" AS "time_complete", 'ED_WARD' AS "event_name",
"admission_location" AS "ED",
"admission_type" AS "data_type", 
"trace_patient",
"trace_gender",
"trace_dead",
"hadm_id" AS "trace_concept",
"diagnosis" AS "trace_diagnosis",
"edregtime" as "time_start", 
NULL as "data_measure", 
NULL as "data_instance", 
NULL as "data_diagnosis"
FROM PATIENT_TRACES
WHERE "edregtime" IS NOT NULL
AND "edouttime" IS NOT NULL
AND "edouttime" > "edregtime"
),

PART_1_EVENTS_JOIN AS (
SELECT *, NULL as "time_start", NULL as "data_measure", NULL as "data_instance", NULL as "data_diagnosis"
FROM PATIENT_ADMISSIONS
UNION ALL 
SELECT *, NULL as "time_start", NULL as "data_measure", NULL as "data_instance", NULL as "data_diagnosis"
FROM PATIENT_DISCHARGES
UNION ALL
SELECT *, NULL as "time_start", NULL as "data_measure", NULL as "data_instance", NULL as "data_diagnosis"
FROM PATIENT_DEATH
UNION ALL 
SELECT *, NULL as "data_measure", NULL as "data_instance", NULL as "data_diagnosis"
FROM PATIENT_WARDS_KNOWN
UNION ALL 
SELECT *, NULL as "data_measure", NULL as "data_instance", NULL as "data_diagnosis"
FROM PATIENT_WARDS_UNKNOWN
UNION ALL 
SELECT *, NULL as "data_measure", NULL as "data_instance", NULL as "data_diagnosis"
FROM PATIENT_WARDS_OUTCARE
UNION ALL 
SELECT *
FROM ED_WARD_STAYS
),

CALLOUT_EVENTS AS (
SELECT 
"outcometime" AS "time_complete",
'callout_' || (
    CASE 
        WHEN "curr_careunit" IS NOT NULL THEN lower("curr_careunit")
        ELSE 'other'
    END 
 ) || '_' || lower(substr("acknowledge_status",1,5)) as "event_name",
"curr_careunit" || '_' || cast( "curr_wardid" as varchar) AS "data_location",
"callout_service" AS "data_type",
p."trace_patient",
p."trace_gender",
p."trace_dead",
"hadm_id" AS "trace_concept",
null AS "trace_diagnosis",
"createtime" as "time_start",
NULL as "data_measure",
NULL as "data_instance",
(
    (
        CASE
            WHEN "request_tele" > 0 THEN 'tele,'
            ELSE ''
        END 
    ) || 
    (
        CASE
            WHEN "request_resp" > 0 THEN 'resp,'
            ELSE ''
        END 
    ) || 
    (
        CASE
            WHEN "request_cdiff" > 0  THEN 'cdiff,'
            ELSE ''
        END 
    ) || 
    (
        CASE
            WHEN "request_mrsa" > 0  THEN 'mrsa,'
            ELSE ''
        END 
    ) || 
    (
        CASE
            WHEN "request_vre" > 0 THEN 'vre,'
            ELSE ''
        END 
    )
) as "data_diagnosis",
"callout_outcome" as "data_outcome"
FROM ##dbname##."callout"
LEFT JOIN PATIENT as p ON p.trace_patient = CALLOUT.subject_id
WHERE "hadm_id" IN (SELECT "trace_concept" FROM PATIENT_ADMISSIONS)
AND "createtime" < "outcometime"),

EVENTS_JOIN AS (
SELECT *
FROM CALLOUT_EVENTS
UNION ALL 
SELECT *, NULL AS "data_outcome"
FROM PART_1_EVENTS_JOIN
)

SELECT 
"trace_concept",
"trace_patient",
"trace_gender",
"trace_dead",
"trace_diagnosis",
"event_name",
"time_start",
"time_complete",
"data_diagnosis",
"data_measure"
"data_location",
"data_type",
"data_instance",
"data_outcome"
FROM EVENTS_JOIN
ORDER BY "trace_concept","time_complete"
