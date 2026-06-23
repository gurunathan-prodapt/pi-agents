#
# Airflow DAG for k_ausd_v_ta_barrier_zusgf.ksh migration
# Replaces: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_barrier_zusgf.ksh
#

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryOperator

# --- Configuration Variables ---
# IMPORTANT: Replace these with your actual GCP Project ID and BigQuery dataset names.
PROJECT_ID = "your-gcp-project-id"  # e.g., "my-gcp-project"
SOURCE_DATASET = "source_dataset"   # e.g., "raw_zone"
TARGET_DATASET = "target_dataset"   # e.g., "transformed_zone"

# --- SQL Definitions ---
# DDL for creating the target table if it doesn't exist
CREATE_TABLE_DDL = f"""
CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{TARGET_DATASET}.sof_ta_barrier_zusgf`
(
    cntrct_id                  INT64,
    sperrart_alle              STRING,
    sperrgrund_alle            STRING,
    stilllegungszeitraum_alle  STRING,
    sperrgrund_zusgf           INT64
);
"""

# SQL for truncating the target table and inserting transformed data
TRANSFORMATION_SQL = f"""
TRUNCATE TABLE `{PROJECT_ID}.{TARGET_DATASET}.sof_ta_barrier_zusgf`;

INSERT INTO `{PROJECT_ID}.{TARGET_DATASET}.sof_ta_barrier_zusgf`
  (cntrct_id,
   sperrart_alle,
   sperrgrund_alle,
   stilllegungszeitraum_alle,
   sperrgrund_zusgf)
WITH barrier_src AS (
  SELECT DISTINCT
    CAST(cntrct_id AS INT64) AS cntrct_id,
    REPLACE(REPLACE(sperrart, 'Rufnummern', ''), ' ', '') AS sperrart,
    sperrgrund,
    CASE
      WHEN ist_stillegung = 1 THEN
        CASE
          WHEN sperr_ende IS NULL THEN
            CONCAT('ab ', FORMAT_DATE('%d.%m.%Y', DATE(sperr_beginn)))
          ELSE
            CONCAT(
              FORMAT_DATE('%d.%m.%Y', DATE(sperr_beginn)),
              ' - ',
              FORMAT_DATE('%d.%m.%Y', DATE(sperr_ende))
            )
        END
      ELSE NULL
    END AS stilllegungszeitraum_alle,
    CASE
      WHEN barrier_reason_cv = 2 THEN 2
      ELSE 3
    END AS sperrgrund_zusgf
  FROM `{PROJECT_ID}.{SOURCE_DATASET}.sof_ta_barrier`
),
agg AS (
  SELECT
    cntrct_id,
    STRING_AGG(sperrart, ',' ORDER BY sperrart) AS sperrart_alle,
    STRING_AGG(sperrgrund, ',' ORDER BY sperrart) AS sperrgrund_alle,
    STRING_AGG(stilllegungszeitraum_alle, ', ' ORDER BY sperrart) AS stilllegungszeitraum_alle,
    CASE
      WHEN COUNTIF(sperrgrund_zusgf != 2) > 0 THEN 3
      ELSE 2
    END AS sperrgrund_zusgf
  FROM barrier_src
  GROUP BY cntrct_id
)
SELECT
  cntrct_id,
  sperrart_alle,
  sperrgrund_alle,
  stilllegungszeitraum_alle,
  sperrgrund_zusgf
FROM agg;
"""

with DAG(
    dag_id="k_ausd_v_ta_barrier_zusgf_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # Define your schedule here, e.g., "@daily", "0 0 * * *"
    catchup=False,
    tags=["isrpt", "isbert", "data_transformation", "barrier_data"],
    description="Airflow DAG to migrate k_ausd_v_ta_barrier_zusgf.ksh, processing barrier data in BigQuery.",
) as dag:
    # Task to ensure the target table exists.
    # This task will create the table only if it does not already exist.
    create_target_table = BigQueryOperator(
        task_id="create_target_table",
        sql=CREATE_TABLE_DDL,
        use_legacy_sql=False,
        project_id=PROJECT_ID,
        gcp_conn_id="google_cloud_default",  # Ensure this connection is configured in Airflow
    )

    # Task to execute the main data transformation logic.
    # It first truncates the table and then inserts the processed data.
    load_transformed_data = BigQueryOperator(
        task_id="load_transformed_data",
        sql=TRANSFORMATION_SQL,
        use_legacy_sql=False,
        project_id=PROJECT_ID,
        gcp_conn_id="google_cloud_default",
    )

    # Define task dependencies
    # The transformation task depends on the target table being present.
    create_target_table >> load_transformed_data