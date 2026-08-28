/*-- Falls das job_starten_flag gesetzt ist, 
--liefert die Abfrage einen Wert zurück, ansonsten nichts. 
--*/
SELECT
  job_starten_flag
FROM
  `{{project_id}}.is_maint_schema.dwh_ta_k_ilv_abr_ilv`
WHERE
  (quellsystem = 'MAXIMO')
  AND (ilv_teilschritt = 'FILL')
  AND (job_starten_flag = 0)
;