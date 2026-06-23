-- BigQuery Stored Procedure for d_ausd_v_ta_cntrct_templ.sql
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_cntrct_templ.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_templ.ksh

CREATE OR REPLACE PROCEDURE `project.dataset.proc_d_ausd_v_ta_cntrct_templ`()
BEGIN
  -- Declare variable for snapshot date
  DECLARE v_datum DATE;

  -- Determine snapshot date (equivalent to Oracle's SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101'))
  SET v_datum = COALESCE(
    (SELECT MAX(m.timecreated)
     FROM `project.dataset.isbert_dwtk_meldungen` AS m
     WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'),
    PARSE_DATE('%Y%m%d', '19000101')
  );

  -- Truncate target table (equivalent to Oracle's TRUNCATE TABLE)
  TRUNCATE TABLE `project.dataset.sof_ta_cntrct_templ`;

  -- Insert data into the target table
  INSERT INTO `project.dataset.sof_ta_cntrct_templ`
    (CNTRCT_TEMPLATE_ID, CDS_DESCRIPTION_ID, CDS_DESCRIPTION)
  SELECT
    ct.cntrct_template_id,
    ct.cds_description_id,
    cd.cds_description
  FROM `project.dataset.cds_ta_cntrct_template` AS ct
  JOIN `project.dataset.cds_ta_care_description` AS cd
    ON ct.cds_description_id = cd.cds_description_id
  WHERE ct.insert_at <= v_datum
    AND (ct.modified_at IS NULL OR ct.modified_at > v_datum)
    AND ct.valid_from <= v_datum
    AND (ct.valid_to IS NULL OR ct.valid_to > v_datum)
    AND ct.is_production = 1
    AND cd.language = 1;

END;