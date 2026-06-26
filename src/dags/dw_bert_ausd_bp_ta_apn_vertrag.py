# Legacy Orchestration Files:
#   - DW.BERT_AUSD_BP_TA_APN_VERTRAG.xml (UC4 Job Definition)
#   - r_ausd_bp_ta_apn_vertrag.ksh (Rahmenskript)
#   - k_ausd_bp_ta_apn_vertrag.ksh (Kontrollskript)
# Legacy Job: ausd_bp_ta_apn_vertrag
#
# Replaces: Shell-based wrappers and UC4 scheduling with native Airflow.

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.models import Variable

# Read environment variables/Airflow variables for project and dataset
GCP_PROJECT = Variable.get("gcp_project", default_var="your_project")
GCP_DATASET = Variable.get("gcp_dataset", default_var="your_dataset")
GCP_LOCATION = Variable.get("gcp_location", default_var="EU")

default_args = {
    "owner": "data-migration-team",
    "depends_on_past": False,
    "start_date": datetime(2026, 4, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dw_bert_ausd_bp_ta_apn_vertrag",
    default_args=default_args,
    description="Orchestration for BERT APN and Contract Reference aggregation (ausd_bp_ta_apn_vertrag)",
    schedule_interval=None,  # Triggered after staging loads are complete
    catchup=False,
    tags=["bert", "warehouse", "credit-scoring", "ausd_bp_ta_apn_vertrag"],
) as dag:

    # Execute the BigQuery transformation using Option B (strict functional equivalence)
    aggregate_apn_data = BigQueryExecuteQueryOperator(
        task_id="aggregate_apn_contract_refs",
        sql=f"""
            CREATE TEMP FUNCTION aggregate_limited(arr ARRAY<STRING>, delimiter STRING, max_len INT64)
            RETURNS STRING
            LANGUAGE js AS r\"\"\"
              if (!arr) return null;
              let result = "";
              for (let i = 0; i < arr.length; i++) {
                let item = arr[i];
                if (!item) continue;
                let next_str = result ? result + delimiter + item : item;
                if (next_str.length <= max_len) {
                  result = next_str;
                } else {
                  continue;
                }
              }
              return result;
            \"\"\";

            CREATE OR REPLACE TABLE `{GCP_PROJECT}.{GCP_DATASET}.sof_ta_apn_vertrag` AS
            SELECT
              cntrct_id,
              aggregate_limited(ARRAY_AGG(access_point_name ORDER BY access_point_name), ', ', 100) AS apn,
              aggregate_limited(ARRAY_AGG(cntrct_id_ref ORDER BY cntrct_id_ref), ', ', 100) AS cntrct_ref
            FROM
              `{GCP_PROJECT}.{GCP_DATASET}.sof_ta_bpr_apn`
            GROUP BY
              cntrct_id;
        """,
        use_legacy_sql=False,
        location=GCP_LOCATION
    )

    aggregate_apn_data