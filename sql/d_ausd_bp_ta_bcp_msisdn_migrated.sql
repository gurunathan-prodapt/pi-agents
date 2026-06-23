-- Migration of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_bcp_msisdn.sql
-- Core data processing logic for BigQuery.
-- This script is designed to be executed within the r_ausd_bp_ta_bcp_msisdn stored procedure.
-- Oracle-specific syntax and hints have been removed.

-- Truncate the target table before insertion
TRUNCATE TABLE `dataset.sof_ta_bcp_msisdn`;

-- Anreicherung der Daten mit den MSISDN des BCP-Vertrages
INSERT INTO `dataset.sof_ta_bcp_msisdn`
(
  CNTRCT_ID,
  BPR_ID,
  CNTRCT_ID_REF,
  TN_TEL_MSISDN
)
SELECT
    DISTINCT
    bp.cntrct_id,
    bp.bpr_id,
    bp.cntrct_id_ref,
    rn.tn_tel_msisdn
FROM
    `dataset.sof_ta_bpr_bcp` AS bp
INNER JOIN
    `dataset.sof_ta_rn_vertrag` AS rn
ON
    bp.cntrct_id_ref = rn.cntrct_id;