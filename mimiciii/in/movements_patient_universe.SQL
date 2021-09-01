SELECT
  count(*) as "obs",
  "subject_id"
FROM 
  ##dbname##."chartevents"
group by "subject_id"
having count(*) > 5000
order by "obs" ASC