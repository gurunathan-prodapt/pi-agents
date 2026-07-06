from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryGetDataOperator
from airflow.utils.dates import days_ago

PROJECT_ID = "{{ var.value.PROJECT_ID }}"
DATASET_NAME = "{{ var.value.DATASET_NAME }}"
MELDUNGEN_DATASET = "{{ var.value.MELDUNGEN_DATASET }}"

default_args = {
    "owner": "data-platform",
    "retries": 1,
}

with DAG(
    dag_id="dw_bert_ausd_bp_ta_p_basisprod",
    default_args=default_args,
    start_date=days_ago(1),
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
    tags=["migration", "bigquery", "basisprod"],
) as dag:

    start = EmptyOperator(task_id="start")

    get_v_datum = BigQueryGetDataOperator(
        task_id="get_v_datum",
        dataset_id=MELDUNGEN_DATASET,
        table_id="dwtk_meldungen",
        selected_fields=["timecreated"],
        max_results=1,
        gcp_conn_id="google_cloud_default",
    )

    truncate_target = BigQueryInsertJobOperator(
        task_id="truncate_target",
        configuration={
            "query": {
                "query": "TRUNCATE TABLE `{{ var.value.PROJECT_ID }}.{{ var.value.DATASET_NAME }}.sof_ta_p_basisprod`",
                "useLegacySql": False,
            }
        },
        gcp_conn_id="google_cloud_default",
    )

    load_target = BigQueryInsertJobOperator(
        task_id="load_target",
        configuration={
            "query": {
                "query": """INSERT INTO `{{ var.value.PROJECT_ID }}.{{ var.value.DATASET_NAME }}.sof_ta_p_basisprod` (
  CNTRCT_ID, EVN, TNV_ICCID, TNV_MCC, TNV_MNC, TNV_HLR, TNV_SI, TNV_ICC_STAT, TNV_ICC_VALID, 
  TC_ICCID, TC_MCC, TC_MNC, TC_HLR, TC_SI, TC_ICC_STAT, TC_ICC_VALID, 
  TB_ICCID, TB_MCC, TB_MNC, TB_HLR, TB_SI, TB_ICC_STAT, TB_ICC_VALID, 
  MS1_ICCID, MS1_MCC, MS1_MNC, MS1_HLR, MS1_SI, MS1_STAT, MS1_VALID, 
  MS2_ICCID, MS2_MCC, MS2_MNC, MS2_HLR, MS2_SI, MS2_STAT, MS2_VALID,
  TNV_E_ID, TNV_CARD_TYPE_NAME, TC_E_ID, TC_CARD_TYPE_NAME, TB_E_ID, TB_CARD_TYPE_NAME, 
  MS1_E_ID, MS1_CARD_TYPE_NAME, MS2_E_ID, MS2_CARD_TYPE_NAME, 
  TNV_MULTI_SINGLE, TC_MULTI_SINGLE, TB_MULTI_SINGLE, TNV_MSISDN, TNV_MS_STAT, TNV_MS_VALID, 
  TNV_DAT_MSISDN, TNV_DAT_STAT, TNV_DAT_VALID, TNV_FAX_MSISDN, TNV_FAX_STAT, TNV_FAX_VALID, 
  TC_MSISDN, TC_MS_STAT, TC_MS_VALID, TC_DAT_MSISDN, TC_DAT_STAT, TC_DAT_VALID, 
  TC_FAX_MSISDN, TC_FAX_STAT, TC_FAX_VALID, TB_MSISDN, TB_MS_STAT, TB_MS_VALID, 
  TB_DAT_MSISDN, TB_DAT_STAT, TB_DAT_VALID, TB_FAX_MSISDN, TB_FAX_STAT, TB_FAX_VALID, 
  MS1_MSISDN, MS1_MS_STAT, MS1_MS_VALID, MS2_MSISDN, MS2_MS_STAT, MS2_MS_VALID, 
  DA_MSISDN, DA_MS_STAT, DA_MS_VALID, VDA_MSISDN, VDA_MS_STAT, VDA_MS_VALID, 
  TK_MSISDN, TK_MS_STAT, TK_MS_VALID, BCP_VERTRAG, BCP_ICCID, BCP_HLR, APN, BCP_TN_TEL, 
  DATA_OPTION_REIN, VOICE_OPTION_REIN, MIX_OPTION, MULTI_OPTION, ROAMING_OPTION, SONSTIGE_OPTION,
  MS3_ICCID, MS3_E_ID, MS3_CARD_TYPE_NAME, MS3_MCC, MS3_MNC, MS3_HLR, MS3_SI, MS3_STAT, MS3_VALID,
  MS4_ICCID, MS4_E_ID, MS4_CARD_TYPE_NAME, MS4_MCC, MS4_MNC, MS4_HLR, MS4_SI, MS4_STAT, MS4_VALID,
  MS5_ICCID, MS5_E_ID, MS5_CARD_TYPE_NAME, MS5_MCC, MS5_MNC, MS5_HLR, MS5_SI, MS5_STAT, MS5_VALID,
  MS6_ICCID, MS6_E_ID, MS6_CARD_TYPE_NAME, MS6_MCC, MS6_MNC, MS6_HLR, MS6_SI, MS6_STAT, MS6_VALID,
  MS7_ICCID, MS7_E_ID, MS7_CARD_TYPE_NAME, MS7_MCC, MS7_MNC, MS7_HLR, MS7_SI, MS7_STAT, MS7_VALID,
  MS8_ICCID, MS8_E_ID, MS8_CARD_TYPE_NAME, MS8_MCC, MS8_MNC, MS8_HLR, MS8_SI, MS8_STAT, MS8_VALID,
  MS9_ICCID, MS9_E_ID, MS9_CARD_TYPE_NAME, MS9_MCC, MS9_MNC, MS9_HLR, MS9_SI, MS9_STAT, MS9_VALID,
  MS10_ICCID, MS10_E_ID, MS10_CARD_TYPE_NAME, MS10_MCC, MS10_MNC, MS10_HLR, MS10_SI, MS10_STAT, MS10_VALID
)
SELECT
  cn.cntrct_id, ev.evn, icc.tn_iccid, icc.tn_imsi_mcc, icc.tn_imsi_mnc, icc.tn_imsi_hlr, icc.tn_imsi_si, icc.tn_status, icc.tn_valid_to,
  icc.tc_iccid, icc.tc_imsi_mcc, icc.tc_imsi_mnc, icc.tc_imsi_hlr, icc.tc_imsi_si, icc.tc_status, icc.tc_valid_to,
  icc.tb_iccid, icc.tb_imsi_mcc, icc.tb_imsi_mnc, icc.tb_imsi_hlr, icc.tb_imsi_si, icc.tb_status, icc.tb_valid_to,
  icc.ms1_iccid, icc.ms1_imsi_mcc, icc.ms1_imsi_mnc, icc.ms1_imsi_hlr, icc.ms1_imsi_si, icc.ms1_status, icc.ms1_valid_to,
  icc.ms2_iccid, icc.ms2_imsi_mcc, icc.ms2_imsi_mnc, icc.ms2_imsi_hlr, icc.ms2_imsi_si, icc.ms2_status, icc.ms2_valid_to,
  icc.tn_e_id, icc.tn_card_type_name, icc.tc_e_id, icc.tc_card_type_name, icc.tb_e_id, icc.tb_card_type_name,
  icc.ms1_e_id, icc.ms1_card_type_name, icc.ms2_e_id, icc.ms2_card_type_name,
  msi.tn_multi_single, msi.tc_multi_single, msi.tb_multi_single,
  msi.tn_tel_msisdn, msi.tn_tel_status, msi.tn_tel_valid_to,
  msi.tn_dat_msisdn, msi.tn_dat_status, msi.tn_dat_valid_to,
  msi.tn_fax_msisdn, msi.tn_fax_status, msi.tn_fax_valid_to,
  msi.tc_tel_msisdn, msi.tc_tel_status, msi.tc_tel_valid_to,
  msi.tc_dat_msisdn, msi.tc_dat_status, msi.tc_dat_valid_to,
  msi.tc_fax_msisdn, msi.tc_fax_status, msi.tc_fax_valid_to,
  msi.tb_tel_msisdn, msi.tb_tel_status, msi.tb_tel_valid_to,
  msi.tb_dat_msisdn, msi.tb_dat_status, msi.tb_dat_valid_to,
  msi.tb_fax_msisdn, msi.tb_fax_status, msi.tb_fax_valid_to,
  msi.ms_rn_1_msisdn, msi.ms_rn_1_status, msi.ms_rn_1_valid_to,
  msi.ms_rn_2_msisdn, msi.ms_rn_2_status, msi.ms_rn_2_valid_to,
  msd.da_rn_msisdn, msd.da_rn_status, msd.da_rn_valid_to,
  msd.vda_rn_msisdn, msd.vda_rn_status, msd.vda_rn_valid_to,
  msd.tk_rn_msisdn, msd.tk_rn_status, msd.tk_rn_valid_to,
  bccm.cntrct_id_ref, bccm.tn_iccid, bccm.tn_imsi_hlr,
  IF(av.apn IS NULL, av.apn, CONCAT(av.apn, ',', av.apn_cntrct)),
  bccm.tn_tel_msisdn, opt.data_option_rein, opt.voice_option_rein, opt.mix_option, opt.multi_option, opt.roaming_option, opt.sonstige_option,
  icc.ms3_iccid, icc.ms3_e_id, icc.ms3_card_type_name, icc.ms3_imsi_mcc, icc.ms3_imsi_mnc, icc.ms3_imsi_hlr, icc.ms3_imsi_si, icc.ms3_status, icc.ms3_valid_to,
  icc.ms4_iccid, icc.ms4_e_id, icc.ms4_card_type_name, icc.ms4_imsi_mcc, icc.ms4_imsi_mnc, icc.ms4_imsi_hlr, icc.ms4_imsi_si, icc.ms4_status, icc.ms4_valid_to,
  icc.ms5_iccid, icc.ms5_e_id, icc.ms5_card_type_name, icc.ms5_imsi_mcc, icc.ms5_imsi_mnc, icc.ms5_imsi_hlr, icc.ms5_imsi_si, icc.ms5_status, icc.ms5_valid_to,
  icc.ms6_iccid, icc.ms6_e_id, icc.ms6_card_type_name, icc.ms6_imsi_mcc, icc.ms6_imsi_mnc, icc.ms6_imsi_hlr, icc.ms6_imsi_si, icc.ms6_status, icc.ms6_valid_to,
  icc.ms7_iccid, icc.ms7_e_id, icc.ms7_card_type_name, icc.ms7_imsi_mcc, icc.ms7_imsi_mnc, icc.ms7_imsi_hlr, icc.ms7_imsi_si, icc.ms7_status, icc.ms7_valid_to,
  icc.ms8_iccid, icc.ms8_e_id, icc.ms8_card_type_name, icc.ms8_imsi_mcc, icc.ms8_imsi_mnc, icc.ms8_imsi_hlr, icc.ms8_imsi_si, icc.ms8_status, icc.ms8_valid_to,
  icc.ms9_iccid, icc.ms9_e_id, icc.ms9_card_type_name, icc.ms9_imsi_mcc, icc.ms9_imsi_mnc, icc.ms9_imsi_hlr, icc.ms9_imsi_si, icc.ms9_status, icc.ms9_valid_to,
  icc.ms10_iccid, icc.ms10_e_id, icc.ms10_card_type_name, icc.ms10_imsi_mcc, icc.ms10_imsi_mnc, icc.ms10_imsi_hlr, icc.ms10_imsi_si, icc.ms10_status, icc.ms10_valid_to
FROM `{{ var.value.PROJECT_ID }}.{{ var.value.DATASET_NAME }}.sof_ta_cntrct_dist` cn
LEFT JOIN `{{ var.value.PROJECT_ID }}.{{ var.value.DATASET_NAME }}.sof_ta_cntrct_evn` ev ON cn.cntrct_id = ev.cntrct_id
LEFT JOIN `{{ var.value.PROJECT_ID }}.{{ var.value.DATASET_NAME }}.sof_ta_iccid_vertrag` icc ON cn.cntrct_id = icc.cntrct_id
LEFT JOIN `{{ var.value.PROJECT_ID }}.{{ var.value.DATASET_NAME }}.sof_ta_rn_vertrag` msi ON cn.cntrct_id = msi.cntrct_id
LEFT JOIN `{{ var.value.PROJECT_ID }}.{{ var.value.DATASET_NAME }}.sof_ta_tarifoption` opt ON cn.cntrct_id = opt.cntrct_id
LEFT JOIN `{{ var.value.PROJECT_ID }}.{{ var.value.DATASET_NAME }}.sof_ta_apn_vertrag` av ON cn.cntrct_id = av.cntrct_id
LEFT JOIN (
  SELECT BC.CNTRCT_ID, BC.CNTRCT_ID_REF, BC.TN_ICCID, BC.TN_IMSI_HLR, BCM.TN_TEL_MSISDN
  FROM `{{ var.value.PROJECT_ID }}.{{ var.value.DATASET_NAME }}.SOF_TA_BCP_ICCID` BC
  JOIN `{{ var.value.PROJECT_ID }}.{{ var.value.DATASET_NAME }}.SOF_TA_BCP_MSISDN` BCM ON BC.CNTRCT_ID = BCM.CNTRCT_ID AND BC.CNTRCT_ID_REF = BCM.CNTRCT_ID_REF
) bccm ON cn.cntrct_id = bccm.cntrct_id
LEFT JOIN `{{ var.value.PROJECT_ID }}.{{ var.value.DATASET_NAME }}.sof_ta_rn_da_vda_tk` msd ON cn.cntrct_id = msd.cntrct_id
WHERE cn.cntrct_id > CAST('{{ var.value.get("wiederanlaufWert", 0) }}' AS INT64);""",
                "useLegacySql": False,
            }
        },
        gcp_conn_id="google_cloud_default",
    )

    end = EmptyOperator(task_id="end")

    start >> get_v_datum >> truncate_target >> load_target >> end