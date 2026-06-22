# Airflow DAG for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_rechempf.ksh
# Replaces legacy job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_rechempf.ksh

from __future__ import annotations

import pendulum

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator


@dag(
    dag_id="isbert_r_ausd_rechempf_dag",
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["isbert", "rechempf", "bigquery"],
    params={
        "stichtag_ddmmyyyy": Param(
            type="string",
            title="Processing Date (DDMMYYYY)",
            description="The processing date in DDMMYYYY format. Defaults to today if not provided.",
            default="",
        ),
        "restart_value": Param(
            type="integer",
            title="Restart Value",
            description="Restart value for incremental processing. Defaults to 0.",
            default=0,
        ),
        "project_id": Param(
            type="string",
            title="GCP Project ID",
            description="The Google Cloud Project ID for BigQuery operations.",
            default="your-gcp-project-id", # TODO: Replace with actual project ID or use Airflow connection/variable
        ),
        "isbert_dwh_dataset": Param(
            type="string",
            title="ISBERT DWH Dataset",
            description="BigQuery dataset for ISBERT DWH tables.",
            default="isbert_dwh",
        ),
        "carmen_source_dataset": Param(
            type="string",
            title="Carmen Source Dataset",
            description="BigQuery dataset for Carmen source tables.",
            default="carmen_source",
        ),
        "fos_source_dataset": Param(
            type="string",
            title="FOS Source Dataset",
            description="BigQuery dataset for FOS source tables.",
            default="fos_source",
        ),
        "dwh_source_dataset": Param(
            type="string",
            title="DWH Source Dataset",
            description="BigQuery dataset for DWH source tables.",
            default="dwh_source",
        ),
        "fos_target_dataset": Param(
            type="string",
            title="FOS Target Dataset",
            description="BigQuery dataset for FOS target tables (created by this job).",
            default="fos_target",
        ),
    },
)
def r_ausd_rechempf_etl_dag():
    @task
    def parse_params_and_setup(**kwargs):
        """
        Parses DAG parameters, sets default values, and calculates dates.
        Replaces shell script's getopts, h_alis_date.ksh, and gestern.ksh.
        """
        stichtag_ddmmyyyy = kwargs["params"].get("stichtag_ddmmyyyy")
        restart_value = kwargs["params"].get("restart_value")

        today = pendulum.today("UTC")
        yesterday = today.subtract(days=1)

        # Default stichtag to today if not provided
        if not stichtag_ddmmyyyy:
            stichtag_ddmmyyyy = today.strftime("%d%m%Y")
        
        # Format stichtag for BigQuery (YYYYMMDD)
        stichtag_date = pendulum.datetime.strptime(stichtag_ddmmyyyy, "%d%m%Y")
        stichtag_yyyymmdd = stichtag_date.strftime("%Y%m%d")

        today_yyyymmdd = today.strftime("%Y%m%d")
        yesterday_yyyymmdd = yesterday.strftime("%Y%m%d")

        kwargs["ti"].xcom_push(key="stichtag_yyyymmdd", value=stichtag_yyyymmdd)
        kwargs["ti"].xcom_push(key="today_yyyymmdd", value=today_yyyymmdd)
        kwargs["ti"].xcom_push(key="yesterday_yyyymmdd", value=yesterday_yyyymmdd)
        kwargs["ti"].xcom_push(key="restart_value", value=restart_value)

        kwargs["ti"].xcom_push(key="project_id", value=kwargs["params"]["project_id"])
        kwargs["ti"].xcom_push(key="isbert_dwh_dataset", value=kwargs["params"]["isbert_dwh_dataset"])
        kwargs["ti"].xcom_push(key="carmen_source_dataset", value=kwargs["params"]["carmen_source_dataset"])
        kwargs["ti"].xcom_push(key="fos_source_dataset", value=kwargs["params"]["fos_source_dataset"])
        kwargs["ti"].xcom_push(key="dwh_source_dataset", value=kwargs["params"]["dwh_source_dataset"])
        kwargs["ti"].xcom_push(key="fos_target_dataset", value=kwargs["params"]["fos_target_dataset"])

    @task
    def log_status_task(**kwargs):
        """
        Logs job completion status. Replaces DWMSG_SetzeStatusOK.
        """
        print(f"Job completed successfully for Stichtag: {kwargs['ti'].xcom_pull(key='stichtag_yyyymmdd')}")
        # In a real-world scenario, you might want to push metrics to monitoring systems
        # or update a job status table.

    start_etl = parse_params_and_setup()

    execute_bq_load = BigQueryExecuteQueryOperator(
        task_id="execute_bq_load_main_script",
        gcp_conn_id="google_cloud_default", # Ensure this connection is configured in Airflow
        project_id="{{ ti.xcom_pull(key='project_id') }}",
        sql="""
            -- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_rechempf.sql
            -- This script performs a full reload into target tables based on the Stichtag.

            DECLARE stichtag_yyyymmdd STRING DEFAULT @stichtag_yyyymmdd;
            DECLARE today_yyyymmdd STRING DEFAULT @today_yyyymmdd;
            DECLARE yesterday_yyyymmdd STRING DEFAULT @yesterday_yyyymmdd;
            DECLARE restart_value INT64 DEFAULT @restart_value;

            DECLARE stichtag_date DATE DEFAULT PARSE_DATE('%Y%m%d', stichtag_yyyymmdd);

            -- ========================= Step00 ==================================
            -- Original Oracle script used `isbert_schema.dwtk_meldungen` to derive v_datum,
            -- but the shell script overrides this with p_stichtag.
            -- This is now handled by the Airflow DAG's parameter parsing.

            -- ========================= Step01 ==================================
            -- Checking for existence of event tables. Not needed in BigQuery CREATE OR REPLACE.

            -- ========================= Step02 ==================================
            -- Truncating temporary tables. Replaced by CREATE OR REPLACE TABLE.

            -- ========================= Step03 ==================================
            -- Create temporary means of payment table sof$ta_means_of_pay
            CREATE OR REPLACE TABLE
                `{{ ti.xcom_pull(key='project_id') }}.{{ ti.xcom_pull(key='fos_target_dataset') }}.sof_ta_means_of_pay`
            AS
            SELECT
                mop.BP_ID,
                mop.MEANS_OF_PAYMENT_ID,
                mop.OBJ_VERSION,
                mop.INSERT_AT,
                mop.MOP_TY,
                mop.ACCOUNT_INT_BP_ID,
                mop.ACCOUNT_INT_MOP_ID,
                mop.BANK_ID_ACC,
                mop.ACCOUNT_NUMBER_ACC,
                mop.BANK_INTERNATIONAL_ID,
                mop.MANDATE_VAR_CV,
                mop.MANDATE_ST,
                mop.MOP_ST,
                mop.CHECK_ST,
                mop.STATUS_REASON,
                mop.IBAN,
                mop.MANDATE_REFERENCE_NO,
                mop.MANDATE_MIGRATED,
                mop.MANDATE_CITY,
                mop.MANDATE_DATE,
                mop.VALID_FROM,
                mop.VALID_TO,
                mop.INSERT_BY,
                mop.MODIFIED_AT,
                mop.MODIFIED_BY,
                mop.MODIFY_REASON,
                mop.IS_IN_ARCHIVE,
                mop.ROW_VERSION,
                mop.REDUNDANT_RB_DOMAIN_PATH,
                mop.REDUNDANT_RB_PROC_PATH,
                mop.IS_PRODUCTION,
                mop.RB_PARTITION_ID
            FROM
                `{{ ti.xcom_pull(key='project_id') }}.{{ ti.xcom_pull(key='carmen_source_dataset') }}.ta_means_of_payment` AS mop
            WHERE
                (mop.insert_at <= stichtag_date
                    AND (mop.modified_at IS NULL OR mop.modified_at > stichtag_date))
                AND (mop.valid_from <= stichtag_date
                    AND (mop.valid_to IS NULL OR mop.valid_to > stichtag_date))
                AND mop.is_production = 1;


            -- Create temporary bank table sof$ta_bank
            CREATE OR REPLACE TABLE
                `{{ ti.xcom_pull(key='project_id') }}.{{ ti.xcom_pull(key='fos_target_dataset') }}.sof_ta_bank`
            AS
            SELECT
                ba.BANK_ID,
                ba.INSERT_AT,
                ba.COUNTRY_CODE,
                ba.BANK_SORT_NAME,
                ba.BANK_NAME,
                ba.INSERT_BY,
                ba.MODIFIED_AT,
                ba.MODIFIED_BY,
                ba.MODIFY_REASON,
                ba.IS_IN_ARCHIVE,
                ba.ROW_VERSION,
                CAST(NULL AS STRING) AS BIC, -- Original was NULL BIC
                CAST(NULL AS STRING) AS BANK_INTERNATIONAL_ID -- Original was NULL BANK_INTERNATIONAL_ID
            FROM
                `{{ ti.xcom_pull(key='project_id') }}.{{ ti.xcom_pull(key='carmen_source_dataset') }}.ta_bank` AS ba
            WHERE
                (ba.insert_at <= stichtag_date
                    AND (ba.MODIFIED_AT IS NULL OR ba.MODIFIED_AT > stichtag_date))
            UNION ALL
            SELECT
                -99999 AS BANK_ID,
                bi.INSERT_AT,
                bi.COUNTRY_CODE,
                CAST(NULL AS STRING) AS BANK_SORT_NAME,
                bi.BANK_NAME,
                bi.INSERT_BY,
                bi.MODIFIED_AT,
                bi.MODIFIED_BY,
                bi.MODIFY_REASON,
                bi.IS_IN_ARCHIVE,
                bi.ROW_VERSION,
                bi.BIC,
                bi.BANK_INTERNATIONAL_ID
            FROM
                `{{ ti.xcom_pull(key='project_id') }}.{{ ti.xcom_pull(key='carmen_source_dataset') }}.ta_bank_international` AS bi
            WHERE
                (bi.insert_at <= stichtag_date
                    AND (bi.MODIFIED_AT IS NULL OR bi.MODIFIED_AT > stichtag_date));

            -- ========================= Step04 ==================================
            -- Create table sof$ta_bank_verb
            CREATE OR REPLACE TABLE
                `{{ ti.xcom_pull(key='project_id') }}.{{ ti.xcom_pull(key='fos_target_dataset') }}.sof_ta_bank_verb`
            AS
            SELECT
                mp.MEANS_OF_PAYMENT_ID,
                mp.BP_ID,
                mp.ACCOUNT_NUMBER_ACC,
                ba.BANK_NAME,
                ba.BANK_SORT_NAME,
                mp.IBAN,
                ba.BIC
            FROM
                `{{ ti.xcom_pull(key='project_id') }}.{{ ti.xcom_pull(key='fos_target_dataset') }}.sof_ta_means_of_pay` AS mp
            JOIN
                `{{ ti.xcom_pull(key='project_id') }}.{{ ti.xcom_pull(key='fos_target_dataset') }}.sof_ta_bank` AS ba
            ON
                MP.BANK_ID_ACC = BA.BANK_ID
                OR mp.BANK_INTERNATIONAL_ID = ba.BANK_INTERNATIONAL_ID;

            -- Create table sof$ta_bank_zuord
            CREATE OR REPLACE TABLE
                `{{ ti.xcom_pull(key='project_id') }}.{{ ti.xcom_pull(key='fos_target_dataset') }}.sof_ta_bank_zuord`
            AS
            SELECT
                za.inv_def_mopref_id,
                ba.account_number_acc,
                ba.bank_name,
                ba.bank_sort_name,
                ba.iban,
                ba.bic
            FROM
                `{{ ti.xcom_pull(key='project_id') }}.{{ ti.xcom_pull(key='fos_target_dataset') }}.sof_ta_bank_verb` AS ba
            JOIN
                `{{ ti.xcom_pull(key='project_id') }}.{{ ti.xcom_pull(key='fos_source_dataset') }}.ta_e_regulierer` AS za
            ON
                za.means_of_payment_id = ba.means_of_payment_id
                AND za.mop_bp_id = ba.bp_id;

            -- ========================= Step05 ==================================
            -- Create table sof$ta_p_rech_empf (final target table)
            CREATE OR REPLACE TABLE
                `{{ ti.xcom_pull(key='project_id') }}.{{ ti.xcom_pull(key='fos_target_dataset') }}.sof_ta_p_rech_empf`
            AS
            SELECT
                '0' AS KUNDENKONTO,
                re.inv_def_invrec_id AS RECHDEF_ID,
                '0' AS DPPS_KONTONUMMER,
                CASE
                    WHEN (re.corp_unit IS NULL AND bp.organisation_name IS NULL)
                    THEN
                        CASE
                            WHEN (re.surname_s IS NULL)
                            THEN CONCAT(bp.first_name, ' ', bp.surname)
                            ELSE CONCAT(re.first_name_g, ' ', re.surname_s)
                        END
                    ELSE
                        CASE
                            WHEN (re.corp_unit IS NULL)
                            THEN bp.organisation_name
                            ELSE re.corp_unit
                        END
                END AS RECHNUNGSEMPFAENGER,
                'C' AS QUELLE,
                CASE
                    WHEN (re.surname_s IS NULL)
                    THEN bp.title
                    ELSE ''
                END AS AKAD_TITEL,
                CASE
                    WHEN (re.corp_unit IS NULL)
                    THEN bp.organisation_name
                    ELSE re.corp_unit
                END AS FIRMA,
                CASE
                    WHEN (re.first_name_g IS NULL)
                    THEN bp.first_name
                    ELSE re.first_name_g
                END AS VORNAME,
                CASE
                    WHEN (re.surname_s IS NULL)
                    THEN bp.surname
                    ELSE re.surname_s
                END AS NACHNAME,
                re.for_the_attention_of AS ZUSATZ_1,
                re.address_attachment AS ZUSATZ_2,
                CASE
                    WHEN (re.street IS NULL)
                    THEN
                        CASE
                            WHEN (re.pobox IS NULL)
                            THEN ''
                            ELSE CONCAT('Postfach ', re.pobox)
                        END
                    ELSE CONCAT(re.street, ' ', re.house_nr)
                END AS STRASSE,
                re.zip_code AS PLZ,
                re.city AS WOHNORT,
                re.land_sd AS LAND,
                ba.bank_name AS BANKNAME,
                ba.account_number_acc AS BANK_KONTONUMMER,
                ba.bank_sort_name AS BLZ,
                re.address_attachment_org AS ORGANISATIONSEINHEIT,
                bp.sales_tax_freed AS MWST_KENNZEICHEN,
                bp.tm_customerid AS KUN_NR_RECH_EMPF,
                ba.iban,
                ba.bic
            FROM
                `{{ ti.xcom_pull(key='project_id') }}.{{ ti.xcom_pull(key='fos_target_dataset') }}.sof_ta_bank_zuord` AS ba
            JOIN
                `{{ ti.xcom_pull(key='project_id') }}.{{ ti.xcom_pull(key='fos_source_dataset') }}.ta_e_reach_re` AS re
            ON
                re.inv_def_mopref_id = ba.inv_def_mopref_id
            JOIN
                `{{ ti.xcom_pull(key='project_id') }}.{{ ti.xcom_pull(key='fos_source_dataset') }}.ta_e_business_re` AS bp
            ON
                re.bp_id = bp.bp_id;

            -- ========================= Step06 ==================================
            -- Create table sof$ta_p_d1_vpn (final target table)
            CREATE OR REPLACE TABLE
                `{{ ti.xcom_pull(key='project_id') }}.{{ ti.xcom_pull(key='fos_target_dataset') }}.sof_ta_p_d1_vpn`
            AS
            SELECT
                bp.vertrags_id,
                bp.vpn_id
            FROM
                `{{ ti.xcom_pull(key='project_id') }}.{{ ti.xcom_pull(key='dwh_source_dataset') }}.vi_s_ibasisprodukt` AS bp
            WHERE
                bp.vpn_id IS NOT NULL
                AND bp.basisprodukt_id IN (2828, 2831);

            -- ========================= Step07 ==================================
            -- Deletion of temporary intermediate tables. Not needed with CREATE OR REPLACE.
            -- The tables sof_ta_means_of_pay, sof_ta_bank, sof_ta_bank_verb, sof_ta_bank_zuord
            -- are effectively temporary and will be replaced on each run.
            """,
        use_legacy_sql=False,
        params={
            "stichtag_yyyymmdd": "{{ ti.xcom_pull(key='stichtag_yyyymmdd') }}",
            "today_yyyymmdd": "{{ ti.xcom_pull(key='today_yyyymmdd') }}",
            "yesterday_yyyymmdd": "{{ ti.xcom_pull(key='yesterday_yyyymmdd') }}",
            "restart_value": "{{ ti.xcom_pull(key='restart_value') }}",
        },
    )

    log_completion = log_status_task()

    start_etl >> execute_bq_load >> log_completion

r_ausd_rechempf_etl_dag()