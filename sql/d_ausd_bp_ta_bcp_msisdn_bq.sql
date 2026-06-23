-- Migrated from Oracle SQL script: d_ausd_bp_ta_bcp_msisdn.sql
-- Part of BigQuery migration for job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_msisdn.ksh

-- This script performs the core data transformation for PoolBasisprodukt.
-- It assumes variables are passed or set in the calling environment (e.g., a stored procedure).

-- Step01: Truncate the target table
-- The original uses isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_bcp_msisdn REUSE STORAGE');
-- This translates to a direct TRUNCATE TABLE statement in BigQuery.
TRUNCATE TABLE `my-gcp-project.my_dataset.sof_ta_bcp_msisdn`;

-- Step11c: Anreicherung der Daten mit den MSISDN des BCP-Vertrages.
-- Enriches data with MSISDNs of BCP contracts.
-- Original Oracle hints /*+ full(bp) parallel(bp,4) full(rn) parallel(rn,4) */ are removed as BigQuery optimizes automatically.
INSERT INTO `my-gcp-project.my_dataset.sof_ta_bcp_msisdn`
(
  CNTRCT_ID,
  BPR_ID,
  CNTRCT_ID_REF,
  TN_TEL_MSISDN
)
SELECT
  DISTINCT -- The original Oracle query explicitly included `SELECT distinct`.
  bp.cntrct_id,
  bp.bpr_id,
  bp.cntrct_id_ref,
  rn.tn_tel_msisdn
FROM
  `my-gcp-project.my_dataset.sof_ta_bpr_bcp` AS bp
INNER JOIN -- Original was implicit join by WHERE clause
  `my-gcp-project.my_dataset.sof_ta_rn_vertrag` AS rn
ON
  bp.cntrct_id_ref = rn.cntrct_id;

-- COMMIT is not needed in BigQuery as DML operations are atomic.