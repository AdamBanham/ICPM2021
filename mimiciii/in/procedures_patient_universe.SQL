with patients as (
  SELECT
  "hadm_id", "subject_id"
  FROM  ##dbname##."admissions"
  WHERE "hadm_id" IN (SELECT "hadm_id" FROM  ##dbname##."admissions" WHERE "diagnosis" IN ('RESPIRATORY FAILURE'))
)

SELECT
  count(*) as "obs",
  "subject_id",
  "hadm_id"
FROM 
  ##dbname##."chartevents"
group by "subject_id", "hadm_id"
having count(*) > 5000
AND "subject_id" in ( select "subject_id" from patients ) 
AND "hadm_id" in  ( select "hadm_id" from patients )
order by "obs" ASC