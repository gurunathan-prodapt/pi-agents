-- BigQuery SQL script generated from legacy source vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_da_vda_tk.ksh
-- This script performs data transformation for basis product data related to telephone numbers.

TRUNCATE TABLE `sof$ta_rn_da_vda_tk`;

INSERT INTO `sof$ta_rn_da_vda_tk`
(
  CNTRCT_ID,
  DA_RN_MSISDN,
  DA_RN_STATUS,
  DA_RN_VALID_TO,
  VDA_RN_MSISDN,
  VDA_RN_STATUS,
  VDA_RN_VALID_TO,
  TK_RN_MSISDN,
  TK_RN_STATUS,
  TK_RN_VALID_TO
)
SELECT
  cntrct_id,
  DA_RN_msisdn,
  DA_RN_status,
  DA_RN_valid_to,
  VDA_RN_msisdn,
  VDA_RN_status,
  VDA_RN_valid_to,
  TK_RN_msisdn,
  TK_RN_status,
  TK_RN_valid_to
FROM `sof$ta_rn_einzeln` rp
WHERE DA_RN_msisdn IS NOT NULL
   OR VDA_RN_msisdn IS NOT NULL
   OR TK_RN_msisdn IS NOT NULL;