WITH SELECTION AS (
  SELECT D_ITEMS.ITEMID, D_ITEMS.LABEL, PROCEDUREEVENTS_MV.*, ADMISSIONS.admission_type, ADMISSIONS.admission_location, ADMISSIONS.diagnosis
  FROM  ##dbname##.PROCEDUREEVENTS_MV
  INNER JOIN  ##dbname##.D_ITEMS
  ON PROCEDUREEVENTS_MV.ITEMID = D_ITEMS.ITEMID
  INNER JOIN  ##dbname##.ADMISSIONS
  ON PROCEDUREEVENTS_MV.HADM_ID = ADMISSIONS.HADM_ID
  WHERE PROCEDUREEVENTS_MV.HADM_ID IN (SELECT HADM_ID FROM  ##dbname##.ADMISSIONS WHERE DIAGNOSIS IN ('RESPIRATORY FAILURE'))
  AND D_ITEMS.LABEL NOT LIKE '%Gauge%'
  AND D_ITEMS.LABEL NOT LIKE '%OR %'
),

KEEPERS AS (
  SELECT LABEL,COUNT(*) AS PRO_NO
  FROM SELECTION
  GROUP BY LABEL
  ORDER BY COUNT(*) DESC
  LIMIT 50
),

EVENTS AS (
  SELECT *
  FROM SELECTION
  WHERE LABEL IN (SELECT LABEL FROM KEEPERS)
)

SELECT 
  concat(CAST(HADM_ID AS VARCHAR),'-',CAST(ICUSTAY_ID AS VARCHAR)) AS trace_concept,
  starttime as "time_start",
  endtime as "time_complete",
  label as "event_name",
  subject_id as "trace_patient",
  admission_type as "trace_admission_type",
  admission_location as "trace_admission_location",
  diagnosis as "trace_diagonsis",
  ORDERCATEGORYNAME as "data_procedure_cat",
  STATUSDESCRIPTION as "data_procedure_status"
FROM EVENTS
ORDER BY trace_concept