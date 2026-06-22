-- Migrated BigQuery SQL for job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_templ.ksh
-- Original source: d_ausd_v_ta_cntrct_templ.sql

-- This SQL script contains the core transformation logic.
-- It is designed to be executed by an Apache Airflow DAG in BigQuery.

-- 1. Declare processing date variable (v_datum)
-- This DECLARE block would typically be run as a separate task
-- to determine the v_datum value and pass it to subsequent tasks.
-- For a monolithic script, it could be part of a single query.
-- In the Airflow DAG, v_datum will be passed via XCom.
DECLARE v_datum DATE DEFAULT (
  SELECT COALESCE(MAX(DATE(m.timecreated)), DATE '1900-01-01')
  FROM `project.dataset.dwtk_meldungen` m -- TODO: Replace project.dataset with actual project and dataset names
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

-- 2. Truncate the target table
-- This statement should be executed as a distinct step before insertion.
TRUNCATE TABLE `project.dataset.sof_ta_cntrct_templ`; -- TODO: Replace project.dataset with actual project and dataset names

-- 3. Insert transformed data
INSERT INTO `project.dataset.sof_ta_cntrct_templ` -- TODO: Replace project.dataset with actual project and dataset names
(
  cntrct_template_id,
  cds_description_id,
  cds_description
)
SELECT
  ct.cntrct_template_id,
  ct.cds_description_id,
  cd.cds_description
FROM `project.dataset.cds_ta_cntrct_template` ct -- TODO: Replace project.dataset with actual project and dataset names
JOIN `project.dataset.cds_ta_care_description` cd -- TODO: Replace project.dataset with actual project and dataset names
  ON ct.cds_description_id = cd.cds_description_id
WHERE ct.insert_at <= v_datum
  AND (ct.modified_at IS NULL OR ct.modified_at > v_datum)
  AND ct.valid_from <= v_datum
  AND (ct.valid_to IS NULL OR ct.valid_to > v_datum)
  AND ct.is_production = 1
  AND cd.language = 1;