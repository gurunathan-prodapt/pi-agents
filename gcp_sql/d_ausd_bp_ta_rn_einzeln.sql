DECLARE v_datum STRING DEFAULT (
  SELECT COALESCE(MAX(FORMAT_DATE('%Y%m%d', DATE(timecreated))), '19000101')
  FROM `{{ var.value.gcp_project_id }}.{{ var.value.bq_dataset }}.dwtk_meldungen`
  WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
);

TRUNCATE TABLE `{{ var.value.gcp_project_id }}.{{ var.value.bq_dataset }}.sof_ta_rn_einzeln`;

INSERT INTO `{{ var.value.gcp_project_id }}.{{ var.value.bq_dataset }}.sof_ta_rn_einzeln` (
  CNTRCT_ID,
  TN_MULTI_SINGLE,
  TN_TEL_MSISDN,
  TN_TEL_STATUS,
  TN_TEL_VALID_TO,
  TN_FAX_MSISDN,
  TN_FAX_STATUS,
  TN_FAX_VALID_TO,
  TN_DAT_MSISDN,
  TN_DAT_STATUS,
  TN_DAT_VALID_TO,
  TC_MULTI_SINGLE,
  TC_TEL_MSISDN,
  TC_TEL_STATUS,
  TC_TEL_VALID_TO,
  TC_FAX_MSISDN,
  TC_FAX_STATUS,
  TC_FAX_VALID_TO,
  TC_DAT_MSISDN,
  TC_DAT_STATUS,
  TC_DAT_VALID_TO,
  TB_MULTI_SINGLE,
  TB_TEL_MSISDN,
  TB_TEL_STATUS,
  TB_TEL_VALID_TO,
  TB_FAX_MSISDN,
  TB_FAX_STATUS,
  TB_FAX_VALID_TO,
  TB_DAT_MSISDN,
  TB_DAT_STATUS,
  TB_DAT_VALID_TO,
  DA_RN_MSISDN,
  DA_RN_STATUS,
  DA_RN_VALID_TO,
  VDA_RN_MSISDN,
  VDA_RN_STATUS,
  VDA_RN_VALID_TO,
  TK_RN_MSISDN,
  TK_RN_STATUS,
  TK_RN_VALID_TO,
  MS_RN_1_MSISDN,
  MS_RN_1_STATUS,
  MS_RN_1_VALID_TO,
  MS_RN_2_MSISDN,
  MS_RN_2_STATUS,
  MS_RN_2_VALID_TO
)
SELECT
  bp.cntrct_id,
  CASE
    WHEN bp.bpr_id = 31 THEN
      CASE
        WHEN ms.callnumber_role_id = 2 THEN 'Multinumbering'
        WHEN ms.callnumber_role_id = 1 THEN 'Singlenumbering'
        ELSE NULL
      END
    ELSE NULL
  END AS TN_MULTI_SINGLE,
  CASE
    WHEN bp.bpr_id = 31 THEN
      CASE
        WHEN ms.callnumber_role_id IN (1, 2) THEN ms.msisdn
        ELSE NULL
      END
    ELSE NULL
  END AS TN_TEL_MSISDN,
  CASE
    WHEN bp.bpr_id = 31 THEN
      CASE
        WHEN ms.callnumber_role_id IN (1, 2) THEN IF(ms.valid_to <= PARSE_DATE('%Y%m%d', v_datum), 'L', 'A')
        ELSE NULL
      END
    ELSE NULL
  END AS TN_TEL_STATUS,
  CASE
    WHEN bp.bpr_id = 31 THEN
      CASE
        WHEN ms.callnumber_role_id IN (1, 2) THEN ms.valid_to
        ELSE NULL
      END
    ELSE NULL
  END AS TN_TEL_VALID_TO,
  CASE
    WHEN bp.bpr_id = 31 THEN
      CASE
        WHEN ms.callnumber_role_id = 3 THEN ms.msisdn
        ELSE NULL
      END
    ELSE NULL
  END AS TN_FAX_MSISDN,
  CASE
    WHEN bp.bpr_id = 31 THEN
      CASE
        WHEN ms.callnumber_role_id = 3 THEN IF(ms.valid_to <= PARSE_DATE('%Y%m%d', v_datum), 'L', 'A')
        ELSE NULL
      END
    ELSE NULL
  END AS TN_FAX_STATUS,
  CASE
    WHEN bp.bpr_id = 31 THEN
      CASE
        WHEN ms.callnumber_role_id = 3 THEN ms.valid_to
        ELSE NULL
      END
    ELSE NULL
  END AS TN_FAX_VALID_TO,
  CASE
    WHEN bp.bpr_id = 31 THEN
      CASE
        WHEN ms.callnumber_role_id = 5 THEN ms.msisdn
        ELSE NULL
      END
    ELSE NULL
  END AS TN_DAT_MSISDN,
  CASE
    WHEN bp.bpr_id = 31 THEN
      CASE
        WHEN ms.callnumber_role_id = 5 THEN IF(ms.valid_to <= PARSE_DATE('%Y%m%d', v_datum), 'L', 'A')
        ELSE NULL
      END
    ELSE NULL
  END AS TN_DAT_STATUS,
  CASE
    WHEN bp.bpr_id = 31 THEN
      CASE
        WHEN ms.callnumber_role_id = 5 THEN ms.valid_to
        ELSE NULL
      END
    ELSE NULL
  END AS TN_DAT_VALID_TO,
  CASE
    WHEN bp.bpr_id = 2759 THEN
      CASE
        WHEN ms.callnumber_role_id = 2 THEN 'Multinumbering'
        WHEN ms.callnumber_role_id = 1 THEN 'Singlenumbering'
        ELSE NULL
      END
    ELSE NULL
  END AS TC_MULTI_SINGLE,
  CASE
    WHEN bp.bpr_id = 2759 THEN
      CASE
        WHEN ms.callnumber_role_id = 2 THEN ms.msisdn
        WHEN ms.callnumber_role_id = 1 THEN ms.msisdn
        ELSE NULL
      END
    ELSE NULL
  END AS TC_TEL_MSISDN,
  CASE
    WHEN bp.bpr_id = 2759 THEN
      CASE
        WHEN ms.callnumber_role_id IN (1, 2) THEN IF(ms.valid_to <= PARSE_DATE('%Y%m%d', v_datum), 'L', 'A')
        ELSE NULL
      END
    ELSE NULL
  END AS TC_TEL_STATUS,
  CASE
    WHEN bp.bpr_id = 2759 THEN
      CASE
        WHEN ms.callnumber_role_id IN (1, 2) THEN ms.valid_to
        ELSE NULL
      END
    ELSE NULL
  END AS TC_TEL_VALID_TO,
  CASE
    WHEN bp.bpr_id = 2759 THEN
      CASE
        WHEN ms.callnumber_role_id = 3 THEN ms.msisdn
        ELSE NULL
      END
    ELSE NULL
  END AS TC_FAX_MSISDN,
  CASE
    WHEN bp.bpr_id = 2759 THEN
      CASE
        WHEN ms.callnumber_role_id = 3 THEN IF(ms.valid_to <= PARSE_DATE('%Y%m%d', v_datum), 'L', 'A')
        ELSE NULL
      END
    ELSE NULL
  END AS TC_FAX_STATUS,
  CASE
    WHEN bp.bpr_id = 2759 THEN
      CASE
        WHEN ms.callnumber_role_id = 3 THEN ms.valid_to
        ELSE NULL
      END
    ELSE NULL
  END AS TC_FAX_VALID_TO,
  CASE
    WHEN bp.bpr_id = 2759 THEN
      CASE
        WHEN ms.callnumber_role_id = 5 THEN ms.msisdn
        ELSE NULL
      END
    ELSE NULL
  END AS TC_DAT_MSISDN,
  CASE
    WHEN bp.bpr_id = 2759 THEN
      CASE
        WHEN ms.callnumber_role_id = 5 THEN IF(ms.valid_to <= PARSE_DATE('%Y%m%d', v_datum), 'L', 'A')
        ELSE NULL
      END
    ELSE NULL
  END AS TC_DAT_STATUS,
  CASE
    WHEN bp.bpr_id = 2759 THEN
      CASE
        WHEN ms.callnumber_role_id = 5 THEN ms.valid_to
        ELSE NULL
      END
    ELSE NULL
  END AS TC_DAT_VALID_TO,
  CASE
    WHEN bp.bpr_id = 2800 THEN
      CASE
        WHEN ms.callnumber_role_id = 2 THEN 'Multinumbering'
        WHEN ms.callnumber_role_id = 1 THEN 'Singlenumbering'
        ELSE NULL
      END
    ELSE NULL
  END AS TB_MULTI_SINGLE,
  CASE
    WHEN bp.bpr_id = 2800 THEN
      CASE
        WHEN ms.callnumber_role_id = 2 THEN ms.msisdn
        WHEN ms.callnumber_role_id = 1 THEN ms.msisdn
        ELSE NULL
      END
    ELSE NULL
  END AS TB_TEL_MSISDN,
  CASE
    WHEN bp.bpr_id = 2800 THEN
      CASE
        WHEN ms.callnumber_role_id IN (1, 2) THEN IF(ms.valid_to <= PARSE_DATE('%Y%m%d', v_datum), 'L', 'A')
        ELSE NULL
      END
    ELSE NULL
  END AS TB_TEL_STATUS,
  CASE
    WHEN bp.bpr_id = 2800 THEN
      CASE
        WHEN ms.callnumber_role_id IN (1, 2) THEN ms.valid_to
        ELSE NULL
      END
    ELSE NULL
  END AS TB_TEL_VALID_TO,
  CASE
    WHEN bp.bpr_id = 2800 THEN
      CASE
        WHEN ms.callnumber_role_id = 3 THEN ms.msisdn
        ELSE NULL
      END
    ELSE NULL
  END AS TB_FAX_MSISDN,
  CASE
    WHEN bp.bpr_id = 2800 THEN
      CASE
        WHEN ms.callnumber_role_id = 3 THEN IF(ms.valid_to <= PARSE_DATE('%Y%m%d', v_datum), 'L', 'A')
        ELSE NULL
      END
    ELSE NULL
  END AS TB_FAX_STATUS,
  CASE
    WHEN bp.bpr_id = 2800 THEN
      CASE
        WHEN ms.callnumber_role_id = 3 THEN ms.valid_to
        ELSE NULL
      END
    ELSE NULL
  END AS TB_FAX_VALID_TO,
  CASE
    WHEN bp.bpr_id = 2800 THEN
      CASE
        WHEN ms.callnumber_role_id = 5 THEN ms.msisdn
        ELSE NULL
      END
    ELSE NULL
  END AS TB_DAT_MSISDN,
  CASE
    WHEN bp.bpr_id = 2800 THEN
      CASE
        WHEN ms.callnumber_role_id = 5 THEN IF(ms.valid_to <= PARSE_DATE('%Y%m%d', v_datum), 'L', 'A')
        ELSE NULL
      END
    ELSE NULL
  END AS TB_DAT_STATUS,
  CASE
    WHEN bp.bpr_id = 2800 THEN
      CASE
        WHEN ms.callnumber_role_id = 5 THEN ms.valid_to
        ELSE NULL
      END
    ELSE NULL
  END AS TB_DAT_VALID_TO,
  CASE
    WHEN bp.bpr_id = 2835 THEN
      CASE
        WHEN ms.callnumber_role_id = 7 THEN ms.msisdn
        ELSE NULL
      END
    ELSE NULL
  END AS DA_RN_MSISDN,
  CASE
    WHEN bp.bpr_id = 2835 THEN
      CASE
        WHEN ms.callnumber_role_id = 7 THEN IF(ms.valid_to <= PARSE_DATE('%Y%m%d', v_datum), 'L', 'A')
        ELSE NULL
      END
    ELSE NULL
  END AS DA_RN_STATUS,
  CASE
    WHEN bp.bpr_id = 2835 THEN
      CASE
        WHEN ms.callnumber_role_id = 7 THEN ms.valid_to
        ELSE NULL
      END
    ELSE NULL
  END AS DA_RN_VALID_TO,
  CASE
    WHEN bp.bpr_id = 2836 THEN
      CASE
        WHEN ms.callnumber_role_id = 8 THEN ms.msisdn
        ELSE NULL
      END
    ELSE NULL
  END AS VDA_RN_MSISDN,
  CASE
    WHEN bp.bpr_id = 2836 THEN
      CASE
        WHEN ms.callnumber_role_id = 8 THEN IF(ms.valid_to <= PARSE_DATE('%Y%m%d', v_datum), 'L', 'A')
        ELSE NULL
      END
    ELSE NULL
  END AS VDA_RN_STATUS,
  CASE
    WHEN bp.bpr_id = 2836 THEN
      CASE
        WHEN ms.callnumber_role_id = 8 THEN ms.valid_to
        ELSE NULL
      END
    ELSE NULL
  END AS VDA_RN_VALID_TO,
  CASE
    WHEN bp.bpr_id = 2837 THEN
      CASE
        WHEN ms.callnumber_role_id = 9 THEN ms.msisdn
        ELSE NULL
      END
    ELSE NULL
  END AS TK_RN_MSISDN,
  CASE
    WHEN bp.bpr_id = 2837 THEN
      CASE
        WHEN ms.callnumber_role_id = 9 THEN IF(ms.valid_to <= PARSE_DATE('%Y%m%d', v_datum), 'L', 'A')
        ELSE NULL
      END
    ELSE NULL
  END AS TK_RN_STATUS,
  CASE
    WHEN bp.bpr_id = 2837 THEN
      CASE
        WHEN ms.callnumber_role_id = 9 THEN ms.valid_to
        ELSE NULL
      END
    ELSE NULL
  END AS TK_RN_VALID_TO,
  CASE
    WHEN bp.bpr_id = 3848 AND ms.callnumber_role_id = 12 AND bp.slave_number = 1 THEN ms.msisdn
    ELSE NULL
  END AS MS_RN_1_MSISDN,
  CASE
    WHEN bp.bpr_id = 3848 AND ms.callnumber_role_id = 12 AND bp.slave_number = 1 THEN IF(ms.valid_to <= PARSE_DATE('%Y%m%d', v_datum), 'L', 'A')
    ELSE NULL
  END AS MS_RN_1_STATUS,
  CASE
    WHEN bp.bpr_id = 3848 AND ms.callnumber_role_id = 12 AND bp.slave_number = 1 THEN ms.valid_to
    ELSE NULL
  END AS MS_RN_1_VALID_TO,
  CASE
    WHEN bp.bpr_id = 3848 AND ms.callnumber_role_id = 12 AND bp.slave_number = 2 THEN ms.msisdn
    ELSE NULL
  END AS MS_RN_2_MSISDN,
  CASE
    WHEN bp.bpr_id = 3848 AND ms.callnumber_role_id = 12 AND bp.slave_number = 2 THEN IF(ms.valid_to <= PARSE_DATE('%Y%m%d', v_datum), 'L', 'A')
    ELSE NULL
  END AS MS_RN_2_STATUS,
  CASE
    WHEN bp.bpr_id = 3848 AND ms.callnumber_role_id = 12 AND bp.slave_number = 2 THEN ms.valid_to
    ELSE NULL
  END AS MS_RN_2_VALID_TO
FROM `{{ var.value.gcp_project_id }}.{{ var.value.bq_dataset }}.sof_ta_bpr_basis` bp
JOIN `{{ var.value.gcp_project_id }}.{{ var.value.bq_dataset }}.sof_ta_msisdn` ms
  ON bp.bpr_instance_id = ms.bpr_instance_id
WHERE bp.bpr_id IN (31, 2759, 2800, 2835, 2836, 2837, 3848)
  AND ms.callnumber_role_id IN (1, 2, 3, 5, 7, 8, 9, 12);