-- Migrated from legacy source d_ausd_v_ta_action_assoc.sql
-- Original job: k_ausd_v_ta_action_assoc.ksh
BEGIN
  DECLARE v_datum STRING;
  SET v_datum = (
    SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
    FROM `my_gcp_project.my_dataset.dwtk_meldungen` m
    WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
  );

  -- Truncate existing data in the target table
  TRUNCATE TABLE `my_gcp_project.my_dataset.sof_ta_action_assoc`;

  -- Insert new data based on filtered source records
  INSERT INTO `my_gcp_project.my_dataset.sof_ta_action_assoc`(cntrct_id, rv_action_id)
  SELECT
    ac.cntrct_id,
    ac.rv_action_id
  FROM `my_gcp_project.my_dataset.cds_ta_action_assoc` ac
  WHERE DATE(ac.insert_at) <= PARSE_DATE('%Y%m%d', v_datum)
    AND DATE(ac.valid_from) <= PARSE_DATE('%Y%m%d', v_datum)
    AND ac.is_production = 1
    AND (ac.modified_at IS NULL OR DATE(ac.modified_at) > PARSE_DATE('%Y%m%d', v_datum))
    AND (ac.valid_to IS NULL OR DATE(ac.valid_to) > PARSE_DATE('%Y%m%d', v_datum));
END;