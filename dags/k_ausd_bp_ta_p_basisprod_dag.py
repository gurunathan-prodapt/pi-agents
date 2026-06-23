# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
This module contains the Airflow DAG for the k_ausd_bp_ta_p_basisprod.ksh migration.

It replaces the legacy KornShell script by orchestrating BigQuery SQL execution.
"""

from __future__ import annotations

import pendulum

from airflow.configuration import conf
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryOperator
from airflow.utils.trigger_rule import TriggerRule

from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the BigQuery project and dataset from Airflow configuration or environment variables
# Ensure these are configured in your Airflow environment.
# Example: AIRFLOW__WEBSERVER__DEFAULT_PROJECT_ID or set via Airflow Variables/Connections
# You can set these in Airflow UI as Variables or use Airflow Connections.
# For simplicity, default to placeholders here.
GCP_PROJECT_ID = conf.get("core", "dbt_project_id", fallback="your-gcp-project-id")
BQ_DATASET_ID = "isbert_reporting" # This should be replaced with the actual BigQuery dataset name where tables reside

def _validate_and_format_stichtag(**kwargs):
    """
    Validates the stichtag parameter and formats it to YYYY-MM-DD.
    Pushes formatted dates to XCom for use by downstream tasks.
    """
    stichtag_str = kwargs["params"].get("stichtag")
    if not stichtag_str:
        raise ValueError("Parameter 'stichtag' is required.")

    try:
        # Assuming input format 'DDMMYYYY'
        stichtag_dt = datetime.strptime(stichtag_str, "%d%m%Y")
        kwargs["ti"].xcom_push(key="formatted_stichtag", value=stichtag_dt.strftime("%Y-%m-%d"))
        kwargs["ti"].xcom_push(key="stichtag_nodash", value=stichtag_dt.strftime("%Y%m%d"))
        
        # Logic from original ksh's gestern.ksh is a bit ambiguous.
        # Assuming p_datum_heute is 'today' relative to stichtag, and p_datum_gestern is 'stichtag'.
        # This can be adjusted based on exact business logic of gestern.ksh.
        kwargs["ti"].xcom_push(key="today_date_nodash", value=(stichtag_dt + timedelta(days=1)).strftime("%Y%m%d"))
        kwargs["ti"].xcom_push(key="yesterday_date_nodash", value=stichtag_dt.strftime("%Y%m%d")) 
    except ValueError:
        raise ValueError(f"Invalid stichtag format: {stichtag_str}. Expected 'DDMMYYYY'.")

with DAG(
    dag_id="k_ausd_bp_ta_p_basisprod_dag",
    default_args=default_args,
    description="Migrated Airflow DAG for k_ausd_bp_ta_p_basisprod.ksh to process basis product data.",
    schedule=None,  # This DAG is intended to be triggered manually or via an external system, or define a specific schedule_interval
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["isbert", "basisprod", "bigquery"],
    params={
        "job_kennung": {
            "type": "string",
            "title": "Job Identifier (j)",
            "description": "Unique identifier for the job.",
            "default": "DEFAULT_JOB",
        },
        "eintrags_nr": {
            "type": "string",
            "title": "Entry Number (f)",
            "description": "Entry number for the process.",
            "default": "0",
        },
        "stichtag": {
            "type": "string",
            "title": "Reference Date (s) DDMMYYYY",
            "description": "The reference date for data processing in DDMMYYYY format.",
            "pattern": "^(0[1-9]|[12][0-9]|3[01])(0[1-9]|1[0-2])(19|20)\\d{2}$",
            "examples": ["01012023"],
            "default": datetime.now().strftime("%d%m%Y"), # Default to today's date in DDMMYYYY format
        },
        "wiederanlauf_wert": {
            "type": "integer",
            "title": "Restart Value (l)",
            "description": "Value to resume processing from (e.g., for restart scenarios).",
            "default": 0,
        },
    }
) as dag:
    validate_params_task = PythonOperator(
        task_id="validate_and_format_stichtag",
        python_callable=_validate_and_format_stichtag,
        provide_context=True, # Allows access to task instance (ti) and DAG parameters (params)
    )

    truncate_target_table = BigQueryOperator(
        task_id="truncate_sof_ta_p_basisprod",
        gcp_conn_id="google_cloud_default",  # Ensure this connection is configured in Airflow
        sql=f"""
            TRUNCATE TABLE `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.SOF_TA_P_BASISPROD`;
        """,
        use_legacy_sql=False,
    )

    execute_basisprod_sql = BigQueryOperator(
        task_id="execute_basisprodukt_sql",
        gcp_conn_id="google_cloud_default", # Ensure this connection is configured in Airflow
        sql="""
            -- Parameters pulled from XCom by previous task or directly from DAG params
            {% set stichtag = ti.xcom_pull(task_ids='validate_and_format_stichtag', key='formatted_stichtag') %}
            {% set today_date_nodash = ti.xcom_pull(task_ids='validate_and_format_stichtag', key='today_date_nodash') %}
            {% set yesterday_date_nodash = ti.xcom_pull(task_ids='validate_and_format_stichtag', key='yesterday_date_nodash') %}
            {% set job_kennung = params.job_kennung %}
            {% set eintrags_nr = params.eintrags_nr %}
            {% set wiederanlauf_wert = params.wiederanlauf_wert %}

            INSERT INTO `{{ params.gcp_project_id }}.{{ params.bq_dataset_id }}.SOF_TA_P_BASISPROD`
            (CNTRCT_ID, EVN, TNV_ICCID, TNV_MCC, TNV_MNC, TNV_HLR, TNV_SI, TNV_ICC_STAT, TNV_ICC_VALID, TC_ICCID, TC_MCC, TC_MNC, TC_HLR, TC_SI, TC_ICC_STAT, TC_ICC_VALID, TB_ICCID, TB_MCC, TB_MNC, TB_HLR, TB_SI, TB_ICC_STAT, TB_ICC_VALID, MS1_ICCID, MS1_MCC, MS1_MNC, MS1_HLR, MS1_SI, MS1_STAT, MS1_VALID, MS2_ICCID, MS2_MCC, MS2_MNC, MS2_HLR, MS2_SI, MS2_STAT, MS2_VALID, TNV_E_ID, TNV_CARD_TYPE_NAME, TC_E_ID, TC_CARD_TYPE_NAME, TB_E_ID, TB_CARD_TYPE_NAME, MS1_E_ID, MS1_CARD_TYPE_NAME, MS2_E_ID, MS2_CARD_TYPE_NAME, TNV_MULTI_SINGLE, TC_MULTI_SINGLE, TB_MULTI_SINGLE, TNV_MSISDN, TNV_MS_STAT, TNV_MS_VALID, TNV_DAT_MSISDN, TNV_DAT_STAT, TNV_DAT_VALID, TNV_FAX_MSISDN, TNV_FAX_STAT, TNV_FAX_VALID, TC_MSISDN, TC_MS_STAT, TC_MS_VALID, TC_DAT_MSISDN, TC_DAT_STAT, TC_DAT_VALID, TC_FAX_MSISDN, TC_FAX_STAT, TC_FAX_VALID, TB_MSISDN, TB_MS_STAT, TB_MS_VALID, TB_DAT_MSISDN, TB_DAT_STAT, TB_DAT_VALID, TB_FAX_MSISDN, TB_FAX_STAT, TB_FAX_VALID, MS1_MSISDN, MS1_MS_STAT, MS1_MS_VALID, MS2_MSISDN, MS2_MS_STAT, MS2_MS_VALID, DA_MSISDN, DA_MS_STAT, DA_MS_VALID, VDA_MSISDN, VDA_MS_STAT, VDA_MS_VALID, TK_MSISDN, TK_MS_STAT, TK_MS_VALID, BCP_VERTRAG, BCP_ICCID, BCP_HLR, APN, BCP_TN_TEL, DATA_OPTION_REIN, VOICE_OPTION_REIN, MIX_OPTION, MULTI_OPTION, ROAMING_OPTION, SONSTIGE_OPTION, MS3_ICCID, MS3_E_ID, MS3_CARD_TYPE_NAME, MS3_MCC, MS3_MNC, MS3_HLR, MS3_SI, MS3_STAT, MS3_VALID, MS4_ICCID, MS4_E_ID, MS4_CARD_TYPE_NAME, MS4_MCC, MS4_MNC, MS4_HLR, MS4_SI, MS4_STAT, MS4_VALID, MS5_ICCID, MS5_E_ID, MS5_CARD_TYPE_NAME, MS5_MCC, MS5_MNC, MS5_HLR, MS5_SI, MS5_STAT, MS5_VALID, MS6_ICCID, MS6_E_ID, MS6_CARD_TYPE_NAME, MS6_MCC, MS6_MNC, MS6_HLR, MS6_SI, MS6_STAT, MS6_VALID, MS7_ICCID, MS7_E_ID, MS7_CARD_TYPE_NAME, MS7_MCC, MS7_MNC, MS7_HLR, MS7_SI, MS7_STAT, MS7_VALID, MS8_ICCID, MS8_E_ID, MS8_CARD_TYPE_NAME, MS8_MCC, MS8_MNC, MS8_HLR, MS8_SI, MS8_STAT, MS8_VALID, MS9_ICCID, MS9_E_ID, MS9_CARD_TYPE_NAME, MS9_MCC, MS9_MNC, MS9_HLR, MS9_SI, MS9_STAT, MS9_VALID, MS10_ICCID, MS10_E_ID, MS10_CARD_TYPE_NAME, MS10_MCC, MS10_MNC, MS10_HLR, MS10_SI, MS10_STAT, MS10_VALID)
            SELECT
                cn.cntrct_id,
                ev.evn,
                icc.tn_iccid AS tnv_iccid,
                icc.tn_imsi_mcc AS tnv_mcc,
                icc.tn_imsi_mnc AS tnv_mnc,
                icc.tn_imsi_hlr AS tnv_hlr,
                icc.tn_imsi_si AS tnv_si,
                icc.tn_status AS tnv_icc_stat,
                icc.tn_valid_to AS tnv_icc_valid,
                icc.tc_iccid AS tc_iccid,
                icc.tc_imsi_mcc AS tc_mcc,
                icc.tc_imsi_mnc AS tc_mnc,
                icc.tc_imsi_hlr AS tc_hlr,
                icc.tc_imsi_si AS tc_si,
                icc.tc_status AS tc_icc_stat,
                icc.tc_valid_to AS tc_icc_valid,
                icc.tb_iccid AS tb_iccid,
                icc.tb_imsi_mcc AS tb_mcc,
                icc.tb_imsi_mnc AS tb_mnc,
                icc.tb_imsi_hlr AS tb_hlr,
                icc.tb_imsi_si AS tb_si,
                icc.tb_status AS tb_icc_stat,
                icc.tb_valid_to AS tb_icc_valid,
                icc.ms1_iccid AS ms1_iccid,
                icc.ms1_imsi_mcc AS ms1_mcc,
                icc.ms1_imsi_mnc AS ms1_mnc,
                icc.ms1_imsi_hlr AS ms1_hlr,
                icc.ms1_imsi_si AS ms1_si,
                icc.ms1_status AS ms1_stat,
                icc.ms1_valid_to AS ms1_valid,
                icc.ms2_iccid AS ms2_iccid,
                icc.ms2_imsi_mcc AS ms2_mcc,
                icc.ms2_imsi_mnc AS ms2_mnc,
                icc.ms2_imsi_hlr AS ms2_hlr,
                icc.ms2_imsi_si AS ms2_si,
                icc.ms2_status AS ms2_stat,
                icc.ms2_valid_to AS ms2_valid,
                icc.tn_e_id AS tnv_e_id,
                icc.tn_card_type_name AS tnv_card_type_name,
                icc.tc_e_id AS tc_e_id,
                icc.tc_card_type_name AS tc_card_type_name,
                icc.tb_e_id AS tb_e_id,
                icc.tb_card_type_name AS tb_card_type_name,
                icc.ms1_e_id AS ms1_e_id,
                icc.ms1_card_type_name AS ms1_card_type_name,
                icc.ms2_e_id AS ms2_e_id,
                icc.ms2_card_type_name AS ms2_card_type_name,
                msi.tn_multi_single AS tnv_multi_single,
                msi.tc_multi_single AS tc_multi_single,
                msi.tb_multi_single AS tb_multi_single,
                msi.tn_tel_msisdn AS tnv_msisdn,
                msi.tn_tel_status AS tnv_ms_stat,
                msi.tn_tel_valid_to AS tnv_ms_valid,
                msi.tn_dat_msisdn AS tnv_dat_msisdn,
                msi.tn_dat_status AS tnv_dat_stat,
                msi.tn_dat_valid_to AS tnv_dat_valid,
                msi.tn_fax_msisdn AS tnv_fax_msisdn,
                msi.tn_fax_status AS tnv_fax_stat,
                msi.tn_fax_valid_to AS tnv_fax_valid,
                msi.tc_tel_msisdn AS tc_msisdn,
                msi.tc_tel_status AS tc_ms_stat,
                msi.tc_tel_valid_to AS tc_ms_valid,
                msi.tc_dat_msisdn AS tc_dat_msisdn,
                msi.tc_dat_status AS tc_dat_stat,
                msi.tc_dat_valid_to AS tc_dat_valid,
                msi.tc_fax_msisdn AS tc_fax_msisdn,
                msi.tc_fax_status AS tc_fax_stat,
                msi.tc_fax_valid_to AS tc_fax_valid,
                msi.tb_tel_msisdn AS tb_msisdn,
                msi.tb_tel_status AS tb_ms_stat,
                msi.tb_tel_valid_to AS tb_ms_valid,
                msi.tb_dat_msisdn AS tb_dat_msisdn,
                msi.tb_dat_status AS tb_dat_stat,
                msi.tb_dat_valid_to AS tb_dat_valid,
                msi.tb_fax_msisdn AS tb_fax_msisdn,
                msi.tb_fax_status AS tb_fax_stat,
                msi.tb_fax_valid_to AS tb_fax_valid,
                msi.ms_rn_1_msisdn AS ms1_msisdn,
                msi.ms_rn_1_status AS ms1_ms_stat,
                msi.ms_rn_1_valid_to AS ms1_ms_valid,
                msi.ms_rn_2_msisdn AS ms2_msisdn,
                msi.ms_rn_2_status AS ms2_ms_stat,
                msi.ms_rn_2_valid_to AS ms2_ms_valid,
                msd.da_rn_msisdn AS da_msisdn,
                msd.da_rn_status AS da_ms_stat,
                msd.da_rn_valid_to AS da_ms_valid,
                msd.vda_rn_msisdn AS vda_msisdn,
                msd.vda_rn_status AS vda_ms_stat,
                msd.vda_rn_valid_to AS vda_ms_valid,
                msd.tk_rn_msisdn AS tk_msisdn,
                msd.tk_rn_status AS tk_ms_stat,
                msd.tk_rn_valid_to AS tk_ms_valid,
                bccm.cntrct_id_ref AS BCP_VERTRAG,
                bccm.tn_iccid AS BCP_ICCID,
                bccm.tn_imsi_hlr AS BCP_HLR,
                CASE
                    WHEN av.apn IS NULL THEN av.apn
                    ELSE CONCAT(av.apn, ',', av.apn_cntrct)
                END AS apn,
                bccm.tn_tel_msisdn AS bcp_tn_tel,
                opt.data_option_rein AS data_option_rein,
                opt.voice_option_rein AS voice_option_rein,
                opt.mix_option AS mix_option,
                opt.multi_option AS multi_option,
                opt.roaming_option AS roaming_option,
                opt.sonstige_option AS sonstige_option,
                icc.ms3_iccid AS ms3_iccid,
                icc.ms3_e_id AS ms3_e_id,
                icc.ms3_card_type_name AS ms3_card_type_name,
                icc.ms3_imsi_mcc AS ms3_mcc,
                icc.ms3_imsi_mnc AS ms3_mnc,
                icc.ms3_imsi_hlr AS ms3_hlr,
                icc.ms3_imsi_si AS ms3_si,
                icc.ms3_status AS ms3_stat,
                icc.ms3_valid_to AS ms3_valid,
                icc.ms4_iccid AS ms4_iccid,
                icc.ms4_e_id AS ms4_e_id,
                icc.ms4_card_type_name AS ms4_card_type_name,
                icc.ms4_imsi_mcc AS ms4_mcc,
                icc.ms4_imsi_mnc AS ms4_mnc,
                icc.ms4_imsi_hlr AS ms4_hlr,
                icc.ms4_imsi_si AS ms4_si,
                icc.ms4_status AS ms4_stat,
                icc.ms4_valid_to AS ms4_valid,
                icc.ms5_iccid AS ms5_iccid,
                icc.ms5_e_id AS ms5_e_id,
                icc.ms5_card_type_name AS ms5_card_type_name,
                icc.ms5_imsi_mcc AS ms5_mcc,
                icc.ms5_imsi_mnc AS ms5_mnc,
                icc.ms5_imsi_hlr AS ms5_hlr,
                icc.ms5_imsi_si AS ms5_si,
                icc.ms5_status AS ms5_stat,
                icc.ms5_valid_to AS ms5_valid,
                icc.ms6_iccid AS ms6_iccid,
                icc.ms6_e_id AS ms6_e_id,
                icc.ms6_card_type_name AS ms6_card_type_name,
                icc.ms6_imsi_mcc AS ms6_mcc,
                icc.ms6_imsi_mnc AS ms6_mnc,
                icc.ms6_imsi_hlr AS ms6_hlr,
                icc.ms6_imsi_si AS ms6_si,
                icc.ms6_status AS ms6_stat,
                icc.ms6_valid_to AS ms6_valid,
                icc.ms7_iccid AS ms7_iccid,
                icc.ms7_e_id AS ms7_e_id,
                icc.ms7_card_type_name AS ms7_card_type_name,
                icc.ms7_imsi_mcc AS ms7_mcc,
                icc.ms7_imsi_mnc AS ms7_mnc,
                icc.ms7_imsi_hlr AS ms7_hlr,
                icc.ms7_imsi_si AS ms7_si,
                icc.ms7_status AS ms7_stat,
                icc.ms7_valid_to AS ms7_valid,
                icc.ms8_iccid AS ms8_iccid,
                icc.ms8_e_id AS ms8_e_id,
                icc.ms8_card_type_name AS ms8_card_type_name,
                icc.ms8_imsi_mcc AS ms8_mcc,
                icc.ms8_imsi_mnc AS ms8_mnc,
                icc.ms8_imsi_hlr AS ms8_hlr,
                icc.ms8_imsi_si AS ms8_si,
                icc.ms8_status AS ms8_stat,
                icc.ms8_valid_to AS ms8_valid,
                icc.ms9_iccid AS ms9_iccid,
                icc.ms9_e_id AS ms9_e_id,
                icc.ms9_card_type_name AS ms9_card_type_name,
                icc.ms9_imsi_mcc AS ms9_mcc,
                icc.ms9_imsi_mnc AS ms9_mnc,
                icc.ms9_imsi_hlr AS ms9_hlr,
                icc.ms9_imsi_si AS ms9_si,
                icc.ms9_status AS ms9_stat,
                icc.ms9_valid_to AS ms9_valid,
                icc.ms10_iccid AS ms10_iccid,
                icc.ms10_e_id AS ms10_e_id,
                icc.ms10_card_type_name AS ms10_card_type_name,
                icc.ms10_imsi_mcc AS ms10_mcc,
                icc.ms10_imsi_mnc AS ms10_mnc,
                icc.ms10_imsi_hlr AS ms10_hlr,
                icc.ms10_imsi_si AS ms10_si,
                icc.ms10_status AS ms10_stat,
                icc.ms10_valid_to AS ms10_valid
            FROM
                `{{ params.gcp_project_id }}.{{ params.bq_dataset_id }}.SOF_TA_CNTRCT_DIST` cn
            LEFT OUTER JOIN
                (
                    SELECT
                        BC.CNTRCT_ID,
                        BC.CNTRCT_ID_REF,
                        BC.TN_ICCID,
                        BC.TN_IMSI_HLR,
                        BCM.TN_TEL_MSISDN
                    FROM
                        `{{ params.gcp_project_id }}.{{ params.bq_dataset_id }}.SOF_TA_BCP_ICCID` BC
                    INNER JOIN
                        `{{ params.gcp_project_id }}.{{ params.bq_dataset_id }}.SOF_TA_BCP_MSISDN` BCM
                    ON
                        BC.CNTRCT_ID = BCM.CNTRCT_ID AND BC.CNTRCT_ID_REF = BCM.CNTRCT_ID_REF
                ) bccm
                ON cn.cntrct_id = bccm.cntrct_id
            LEFT OUTER JOIN
                `{{ params.gcp_project_id }}.{{ params.bq_dataset_id }}.SOF_TA_CNTRCT_EVN` ev
                ON cn.cntrct_id = ev.cntrct_id
            LEFT OUTER JOIN
                `{{ params.gcp_project_id }}.{{ params.bq_dataset_id }}.SOF_TA_ICCID_VERTRAG` icc
                ON cn.cntrct_id = icc.cntrct_id
            LEFT OUTER JOIN
                `{{ params.gcp_project_id }}.{{ params.bq_dataset_id }}.SOF_TA_RN_VERTRAG` msi
                ON cn.cntrct_id = msi.cntrct_id
            LEFT OUTER JOIN
                `{{ params.gcp_project_id }}.{{ params.bq_dataset_id }}.SOF_TA_RN_DA_VDA_TK` msd
                ON cn.cntrct_id = msd.cntrct_id
            LEFT OUTER JOIN
                `{{ params.gcp_project_id }}.{{ params.bq_dataset_id }}.SOF_TA_TARIFOPTION` opt
                ON cn.cntrct_id = opt.cntrct_id
            LEFT OUTER JOIN
                `{{ params.gcp_project_id }}.{{ params.bq_dataset_id }}.SOF_TA_APN_VERTRAG` av
                ON cn.cntrct_id = av.cntrct_id
            ;
        """,
        use_legacy_sql=False,
        params={
            "gcp_project_id": GCP_PROJECT_ID,
            "bq_dataset_id": BQ_DATASET_ID,
        }
    )

    # Define task dependencies
    validate_params_task >> truncate_target_table >> execute_basisprod_sql

# Note: The `d_ausd_bp_ta_p_basisprod.sql` script also queries `isbert_schema.dwtk_meldungen`
# for `v_datum`. This logic is currently not directly integrated into the BigQueryOperator
# as the ksh script passes `p_Stichtag` explicitly. If `dwtk_meldungen` is meant to override
# `p_Stichtag` for some reason, additional PythonOperator logic would be needed.
# For this migration, `p_Stichtag` (passed as `stichtag` DAG param) is used as the primary
# reference date.
#
# Also, the SQL script outputs to a `tmpFile` for record count. This is typically handled
# by Airflow's BigQueryOperator by logging job statistics. If a specific record count
# is needed for a downstream task, a PythonOperator could fetch the job result.