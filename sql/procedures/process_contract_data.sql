-- BigQuery Stored Procedure for core contract data processing
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_cntrct_evn.ksh
--                vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_cntrct_evn.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_evn.ksh

CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset_id.process_contract_data`(
  IN p_stichtag STRING, -- This parameter is passed but not explicitly used in the translated SQL.
  IN p_wiederanlaufWert INT64
)
BEGIN
  -- BigQuery equivalent for Oracle's TRUNCATE TABLE or conditional DELETE/INSERT
  -- Based on the description for p_wiederanlaufWert in r_ausd_bp_ta_cntrct_evn.ksh:
  -- "if this value is set, only contracts with DWH_VERTRAG_ID > Wiederanlaufwert
  -- are written to the FOS table (entries concerning values >= this value are deleted)"
  -- Assuming DWH_VERTRAG_ID maps to cntrct_id in sof_ta_bpr_evn and sof_ta_cntrct_evn.

  IF p_wiederanlaufWert IS NULL OR p_wiederanlaufWert = 0 THEN
    -- Full refresh: truncate table and insert all data
    TRUNCATE TABLE `your_project_id.your_dataset_id.sof_ta_cntrct_evn`;

    INSERT `your_project_id.your_dataset_id.sof_ta_cntrct_evn` (
      cntrct_id,
      evn
    )
    SELECT
      bpr.cntrct_id,
      SUM(CASE bpr.bpr_id
            WHEN 32 THEN 1
            WHEN 2839 THEN 10
            WHEN 2506 THEN 2
            WHEN 2840 THEN 20
            WHEN 3055 THEN 3
            WHEN 3056 THEN 30
            WHEN 3821 THEN 4 -- 'Standard-Plus EVN'
            ELSE 0
          END) AS evn
    FROM `your_project_id.your_dataset_id.sof_ta_bpr_evn` AS bpr
    GROUP BY
      bpr.cntrct_id;
  ELSE
    -- Incremental processing: delete existing data for contracts >= p_wiederanlaufWert
    -- then insert new/updated data for contracts > p_wiederanlaufWert.
    -- This assumes cntrct_id is the DWH_VERTRAG_ID mentioned in the source script's usage.
    DELETE FROM `your_project_id.your_dataset_id.sof_ta_cntrct_evn`
    WHERE cntrct_id >= p_wiederanlaufWert;

    INSERT `your_project_id.your_dataset_id.sof_ta_cntrct_evn` (
      cntrct_id,
      evn
    )
    SELECT
      bpr.cntrct_id,
      SUM(CASE bpr.bpr_id
            WHEN 32 THEN 1
            WHEN 2839 THEN 10
            WHEN 2506 THEN 2
            WHEN 2840 THEN 20
            WHEN 3055 THEN 3
            WHEN 3056 THEN 30
            WHEN 3821 THEN 4 -- 'Standard-Plus EVN'
            ELSE 0
          END) AS evn
    FROM `your_project_id.your_dataset_id.sof_ta_bpr_evn` AS bpr
    WHERE bpr.cntrct_id > p_wiederanlaufWert
    GROUP BY
      bpr.cntrct_id;
  END IF;

END;