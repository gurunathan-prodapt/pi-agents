-- Target Dialect: BigQuery SQL
-- Converted from: d_abgl_kunde_woech.sql
-- Performs weekly customer master data address validation against reference system.
SELECT 
  CASE 
    WHEN src.adresse != ref.adresse THEN CONCAT('ABWEICHUNG: Kunde ', src.kunden_id, ' hat abweichende Adresse.')
    ELSE 'OK'
  END AS status_msg
FROM 
  `@gcp_project.@bq_dataset.kunde_stammdaten` AS src
LEFT JOIN 
  `@gcp_project.@bq_dataset.referenz_stammdaten` AS ref
ON 
  src.kunden_id = ref.kunden_id
WHERE 
  src.stichtag = @stichtag;