with TARGET AS (
SELECT
  *
FROM 
  ##dbname##."chartevents"
WHERE
  "subject_id" in ##SUBJECT_ID##
),


TEMP AS (
SELECT 
dit."label" as "label", 
chrt.value as "value",
chrt.charttime as "starttime",
chrt.storetime as "fintime",
chrt.subject_id as "patient"
FROM TARGET as chrt
left join ##dbname##."d_items" as dit Using("itemid")
where 
  lower(dit.label) IN ('spo2','respiratory rate','o2 saturation pulseoxymetry','hr alarm [low]','hr alarm [high]','heart rate','arterial bp mean','arterial bp [systolic]')
order by "label","starttime" ASC
)

SELECT 
TEMP.*
FROM
TEMP