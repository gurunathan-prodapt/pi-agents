-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_rn_da_vda_tk.sql
-- Job: DW.BERT_AUSD_BP_TA_RN_DA_VDA_TK
DECLARE v_datum STRING DEFAULT (
  SELECT COALESCE(
    FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated)),
    '19000101'
  )
  FROM `your_metadata_dataset.dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

TRUNCATE TABLE `your_dataset.sof_ta_rn_da_vda_tk`;

INSERT INTO `your_dataset.sof_ta_rn_da_vda_tk`
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
FROM `your_dataset.sof_ta_rn_einzeln` rp
WHERE DA_RN_msisdn IS NOT NULL
   OR VDA_RN_msisdn IS NOT NULL
   OR TK_RN_msisdn IS NOT NULL;