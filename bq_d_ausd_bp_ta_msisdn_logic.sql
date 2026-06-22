-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_msisdn.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_msisdn.ksh

-- This script contains the core data extraction and transformation logic
-- translated from the original Oracle SQL script d_ausd_bp_ta_msisdn.sql.
-- It is intended to be embedded or called from the main BigQuery Stored Procedure.

-- Assumed context: This script will be executed within a BigQuery Stored Procedure
-- that handles variable declarations, error handling, and logging.

-- Step 01: Truncate the target table `sof_ta_msisdn`
TRUNCATE TABLE `my-gcp-project.isbert_dataset.sof_ta_msisdn`;

-- Step 02: Insert valid MSISDNs into the target table
INSERT INTO `my-gcp-project.isbert_dataset.sof_ta_msisdn`
(
  BPR_INSTANCE_ID,
  MSISDN,
  CALLNUMBER_ROLE_ID,
  VALID_TO
)
SELECT
    cn1.bpri_com_id,
    cn1.msisdn,
    cn1.callnumber_role_id,
    COALESCE(cn1.valid_to, PARSE_DATE('%Y%m%d', '47121231')) AS valid_to
FROM
    (
        SELECT
            cn.bpri_com_id,
            cn.msisdn,
            cn.callnumber_role_id,
            cn.valid_to,
            MAX(COALESCE(cn.valid_to, PARSE_DATE('%Y%m%d', '47121231'))) OVER (PARTITION BY cn.bpri_com_id) AS max_valid_to
        FROM
            `my-gcp-project.isbert_dataset.sof_ta_msisdn_his` AS cn
    ) AS cn1
WHERE
    COALESCE(cn1.valid_to, PARSE_DATE('%Y%m%d', '47121231')) = cn1.max_valid_to;