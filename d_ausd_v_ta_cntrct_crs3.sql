-- BigQuery SQL for DW.BERT_AUSD_V_TA_CNTRCT_CRS3
-- Replaces legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_cntrct_crs3.sql

-- Define v_datum variable based on logic from original script
DECLARE v_datum STRING;

SET v_datum = (
  SELECT
    IFNULL(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101')
  FROM
    `isbert_schema_target.dwtk_meldungen` AS m
  WHERE
    m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

-- Truncate target table before insert
TRUNCATE TABLE `sof_dataset_target.ta_cntrct_crs3`;

-- Insert data into the target table with twinbill information
INSERT INTO `sof_dataset_target.ta_cntrct_crs3`(
        cntrct_id,
        obj_version,
        contract_number,
        cntrct_template_id,
        cntrct_validity_id,
        valid_from,
        com_per_ext_rea_cv,
        billcycle_id,
        vo_code,
        cntrct_start_date,
        cntrct_st,
        cntrct_parent,
        cntrct_ty,
        cost_centre,
        cost_centre_user,
        commitment_reference_date,
        order_number,
        rv_num,
        twinbill,
        twin_vertrag_id)
   SELECT
        c.cntrct_id,
        c.obj_version,
        c.contract_number,
        c.cntrct_template_id,
        c.cntrct_validity_id,
        c.valid_from,
        c.com_per_ext_rea_cv,
        c.billcycle_id,
        c.vo_code,
        c.cntrct_start_date,
        c.cntrct_st,
        c.cntrct_parent,
        c.cntrct_ty,
        c.cost_centre,
        c.cost_centre_user,
        c.commitment_reference_date,
        c.order_number,
        c.RV_NUM,
        CASE WHEN ctb.cntrct_id IS NOT NULL
             THEN 'TB'
             END AS TWINBILL,
        ctb.cntrct_id AS TWIN_VERTRAG_ID
   FROM
        `sof_dataset_target.ta_cntrct_crs2` AS c
   LEFT JOIN
        `sof_dataset_target.ta_cntrct_crs2` AS ctb
     ON
        c.cntrct_id = ctb.cntrct_parent
    AND
        ctb.cntrct_ty = 20        -- Mobilfunkzusatzvertrag
   WHERE
        c.cntrct_ty NOT IN (10, 20)   -- keine RV, Mobilfunkzusatzvertraege

   UNION DISTINCT

   SELECT
        ctb.cntrct_id,
        ctb.obj_version,
        ctb.contract_number,
        ctb.cntrct_template_id,
        ctb.cntrct_validity_id,
        ctb.valid_from,
        ctb.com_per_ext_rea_cv,
        ctb.billcycle_id,
        ctb.vo_code,
        ctb.cntrct_start_date,
        ctb.cntrct_st,
        ctb.cntrct_parent,
        ctb.cntrct_ty,
        ctb.cost_centre,
        ctb.cost_centre_user,
        ctb.commitment_reference_date,
        ctb.order_number,
        c.RV_NUM,
        'TB' AS TWINBILL,
        c.cntrct_id AS TWIN_VERTRAG_ID
   FROM
        `sof_dataset_target.ta_cntrct_crs2` AS c
   JOIN
        `sof_dataset_target.ta_cntrct_crs2` AS ctb
     ON
        c.cntrct_id = ctb.cntrct_parent
   WHERE
        ctb.cntrct_ty = 20       -- Mobilfunkzusatzvertrag
   AND
        c.cntrct_ty NOT IN (10,20);