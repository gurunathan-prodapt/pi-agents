CREATE OR REPLACE PROCEDURE dwh_bert_dataset.d_ausd_bp_ta_msisdn_transform()
BEGIN
  -- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_msisdn.sql
  -- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_msisdn.ksh

  DECLARE v_datum STRING;

  -- Derive v_datum from dwtk_meldungen, equivalent to Oracle's SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101')
  SET v_datum = (
    SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
    FROM dwh_bert_dataset.dwtk_meldungen AS m
    WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
  );

  -- Truncate the target table, equivalent to Oracle's TRUNCATE TABLE sof$ta_msisdn
  TRUNCATE TABLE dwh_bert_dataset.sof_ta_msisdn;

  -- Insert data into the target table with the latest valid MSISDN records,
  -- equivalent to Oracle's INSERT INTO sof$ta_msisdn SELECT ...
  INSERT INTO dwh_bert_dataset.sof_ta_msisdn (
    BPR_INSTANCE_ID,
    MSISDN,
    CALLNUMBER_ROLE_ID,
    VALID_TO
  )
  SELECT
    cn1.bpri_com_id AS bpr_instance_id,
    cn1.msisdn,
    cn1.callnumber_role_id,
    COALESCE(cn1.valid_to, DATE '4712-12-31') AS valid_to
  FROM
    (
      SELECT
        cn.*,
        MAX(COALESCE(cn.valid_to, DATE '4712-12-31')) OVER (PARTITION BY cn.bpri_com_id) AS max_valid_to
      FROM
        dwh_bert_dataset.sof_ta_msisdn_his AS cn
    ) AS cn1
  WHERE
    COALESCE(cn1.valid_to, DATE '4712-12-31') = cn1.max_valid_to;

END;