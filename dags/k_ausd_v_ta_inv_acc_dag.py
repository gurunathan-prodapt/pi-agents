# Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_inv_acc.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_inv_acc.ksh

from __future__ import annotations

import pendulum
import logging

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.trigger_rule import TriggerRule

# Import custom utilities
from utils.error_handling import log_error
from utils.parameter_handling import validate_parameter

log = logging.getLogger(__name__)

# Define BigQuery project and dataset for consistency
BQ_PROJECT_ID = "gcp-project-id" # Placeholder: Replace with actual GCP Project ID
BQ_DATASET_ID = "isrpt_isbert_prod" # Placeholder: Replace with actual BigQuery Dataset ID

def _parse_and_validate_parameters(ti, **kwargs):
    """
    Parses and validates p_JobKennung and p_EintragsNr from DAG run configuration.
    """
    dag_run_conf = kwargs["dag_run"].conf if kwargs["dag_run"] else {}
    p_job_kennung = dag_run_conf.get("p_job_kennung")
    p_eintrags_nr = dag_run_conf.get("p_eintrags_nr")

    if not validate_parameter("Jobkennung", p_job_kennung):
        log_error(0, "E", 193, "Jobkennung parameter is missing or empty.")
        raise ValueError("Missing or empty parameter: p_job_kennung")

    if not validate_parameter("EintragsNr", p_eintrags_nr):
        log_error(0, "E", 193, "EintragsNr parameter is missing or empty.")
        raise ValueError("Missing or empty parameter: p_eintrags_nr")

    ti.xcom_push(key="p_job_kennung", value=p_job_kennung)
    ti.xcom_push(key="p_eintrags_nr", value=p_eintrags_nr)
    log.info(f"Parameters successfully validated: p_job_kennung={p_job_kennung}, p_eintrags_nr={p_eintrags_nr}")

def _log_record_count(**kwargs):
    """
    Retrieves and logs the number of processed records.
    Since BigQueryExecuteQueryOperator for INSERT/TRUNCATE does not directly return
    the count of affected rows to XCom, we log a placeholder message.
    In a real scenario, consider adding a subsequent BigQuery task to COUNT(*)
    or parsing job statistics from BigQuery logs.
    """
    log.info("---------- ENDE Datenverarbeitung ----------")
    log.info("Processed records: [Count not directly available from INSERT statement, check BigQuery job statistics for affected rows or add a COUNT task]")


with DAG(
    dag_id="k_ausd_v_ta_inv_acc_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["isbert", "aufbereitung"],
    doc_md="""
    ### Airflow DAG for k_ausd_v_ta_inv_acc.ksh Migration
    This DAG migrates the functionality of the legacy KornShell script
    `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_inv_acc.ksh`
    to Google Cloud Composer (Airflow) and BigQuery.

    The original script orchestrates the execution of `d_ausd_v_ta_inv_acc.sql`,
    handles parameter validation, and logs job status.

    **Inputs (via DAG run configuration):**
    - `p_job_kennung` (string): Job identifier.
    - `p_eintrags_nr` (string): Entry number.

    **Outputs:**
    - Populates `gcp-project-id.isrpt_isbert_prod.sof_ta_inv_acc` table in BigQuery.
    - Logs execution status and (simulated) record counts to Airflow logs.
    """,
) as dag:
    start_task = PythonOperator(
        task_id="start_job",
        python_callable=lambda: log.info("Starting k_ausd_v_ta_inv_acc DAG"),
    )

    parse_and_validate_params = PythonOperator(
        task_id="parse_and_validate_parameters",
        python_callable=_parse_and_validate_parameters,
        provide_context=True,
    )

    truncate_target_table = BigQueryExecuteQueryOperator(
        task_id="truncate_target_table",
        project_id=BQ_PROJECT_ID,
        sql=f"TRUNCATE TABLE `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.sof_ta_inv_acc`;",
        use_legacy_sql=False,
        gcp_conn_id="google_cloud_default",
    )

    insert_data_task = BigQueryExecuteQueryOperator(
        task_id="insert_data_task",
        project_id=BQ_PROJECT_ID,
        sql="""
            -- Legacy source: vobs/dw_source/isrpt/isbert/aufbereitung/sql/d_ausd_v_ta_inv_acc.sql
            -- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_inv_acc.ksh

            INSERT INTO `{{ params.project_id }}.{{ params.dataset_id }}.sof_ta_inv_acc` (
                   cntrct_id,
                   inv_definition_id,
                   inv_pay_ty_cv,
                   inv_media_cv,
                   billcycle_id,
                   sales_tax_freed,
                   account_reference,
                   rechn_inh_konfig_text
            )
              SELECT
                   ia.cntrct_id,
                   id.inv_definition_id,
                   id.inv_pay_ty_cv,
                   id.inv_media_cv,
                   id.billcycle_id,
                   id.sales_tax_freed,
                   ar.account_reference,
                   id.rechn_inh_konfig_text
              FROM
                    `{{ params.project_id }}.{{ params.dataset_id }}.sof_ta_inv_assign`   ia,
                    `{{ params.project_id }}.{{ params.dataset_id }}.sof_ta_inv_def`      id,
                    `{{ params.project_id }}.{{ params.dataset_id }}.sof_ta_acc_ref`      ar
              WHERE
                    ia.inv_definition_id = id.inv_definition_id
              AND   id.acc_ref_id        = ar.acc_ref_id;
        """,
        use_legacy_sql=False,
        gcp_conn_id="google_cloud_default",
        params={"project_id": BQ_PROJECT_ID, "dataset_id": BQ_DATASET_ID},
    )

    log_record_count = PythonOperator(
        task_id="log_record_count",
        python_callable=_log_record_count,
        provide_context=True,
    )

    end_task = PythonOperator(
        task_id="end_job",
        python_callable=lambda: log.info("Finished k_ausd_v_ta_inv_acc DAG"),
        trigger_rule=TriggerRule.ALL_DONE, # Ensure this runs even if upstream fails
    )

    start_task >> parse_and_validate_params >> truncate_target_table >> insert_data_task >> log_record_count >> end_task