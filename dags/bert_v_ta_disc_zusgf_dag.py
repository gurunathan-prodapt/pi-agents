#
# Apache Airflow DAG for BERT_V_TA_DISC_ZUSGF.
# This DAG orchestrates the reconciliation process for the ta_disc_zusgf table,
# replacing the legacy KornShell script r_ausd_v_ta_disc_zusgf.ksh.
#

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.trigger_rule import TriggerRule

# Define project and dataset IDs. These should ideally come from Airflow Variables or environment configurations.
PROJECT_ID = "your-gcp-project-id"  # TODO: Replace with your actual GCP Project ID
DATASET_ID = "your-bigquery-dataset-id" # TODO: Replace with your actual BigQuery Dataset ID

with DAG(
    dag_id="bert_v_ta_disc_zusgf",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # Set your desired schedule, e.g., "0 3 * * *" for daily at 3 AM UTC
    catchup=False,
    tags=["isbert", "reconciliation", "bigquery"],
    description="Orchestrates the reconciliation for ta_disc_zusgf table.",
) as dag:
    # Task to execute the BigQuery DDL for the ta_disc_zusgf table.
    # This task is typically run once or on schema changes, not every DAG run.
    # It's included here for completeness but might be commented out or run manually in production.
    create_ta_disc_zusgf_table = BigQueryExecuteQueryOperator(
        task_id="create_ta_disc_zusgf_table",
        sql="""
            -- BigQuery DDL for the ta_disc_zusgf table.
            -- Replaces legacy table definition related to job BERT_V_TA_DISC_ZUSGF.
            --
            -- IMPORTANT: This schema is a placeholder. The actual schema must be
            -- derived from a detailed analysis of the source system's `ta_disc_zusgf`
            -- table definition and usage.
            --

            CREATE TABLE IF NOT EXISTS `""" + f"{PROJECT_ID}.{DATASET_ID}.ta_disc_zusgf" + """`
            (
                -- Placeholder columns. Replace with actual schema from source.
                id                  STRING OPTIONS(description="Unique identifier"),
                description         STRING OPTIONS(description="Description of the discount or reconciliation item"),
                amount              NUMERIC OPTIONS(description="Amount associated with the item"),
                currency_code       STRING OPTIONS(description="Currency of the amount (e.g., 'EUR', 'USD')"),
                transaction_date    DATE OPTIONS(description="Date of the transaction or reconciliation"),
                status              STRING OPTIONS(description="Status of the item (e.g., 'ACTIVE', 'RECONCILED')"),
                load_timestamp      TIMESTAMP OPTIONS(description="Timestamp when the record was loaded into BigQuery")
            )
            PARTITION BY transaction_date
            CLUSTER BY status, currency_code
            OPTIONS(
                description="This table stores reconciled discount data, replacing the legacy ta_disc_zusgf table.",
                labels=[('source_system', 'isbert'), ('job_name', 'bert_v_ta_disc_zusgf')]
            );
        """,
        use_legacy_sql=False,
        gcp_conn_id="google_cloud_default", # Ensure you have a BigQuery connection defined in Airflow
        # If this task should only run once, consider a separate DAG or manual trigger.
        # For daily runs, it ensures the table exists, but schema changes need careful management.
    )

    # Task to execute the core reconciliation logic.
    reconcile_ta_disc_zusgf = BigQueryExecuteQueryOperator(
        task_id="reconcile_ta_disc_zusgf",
        sql="""
            -- BigQuery SQL for the core reconciliation logic of BERT_V_TA_DISC_ZUSGF.
            -- This script replaces the functionality previously found in
            -- vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_disc_zusgf.ksh.
            --
            -- IMPORTANT: This is a placeholder script. The actual transformation logic
            -- must be derived from a detailed analysis of the original k_ausd_v_ta_disc_zusgf.ksh
            -- script and its interactions with the ta_disc_zusgf table.
            --
            -- A typical reconciliation process might involve:
            -- 1. Loading new or updated data into a staging table.
            -- 2. Comparing the staging data with the existing ta_disc_zusgf table.
            -- 3. Inserting new records, updating existing records, or marking records as reconciled/inactive.
            -- 4. Handling discrepancies and logging.
            --

            MERGE INTO `""" + f"{PROJECT_ID}.{DATASET_ID}.ta_disc_zusgf" + """` AS target
            USING (
                -- Placeholder for the source data. This could be a staging table,
                -- a subquery processing raw data, or another source system.
                -- Replace this with the actual source for reconciliation.
                SELECT
                    'NEW_ID_1' as id,
                    'New Discount Item 1' as description,
                    100.00 as amount,
                    'EUR' as currency_code,
                    CURRENT_DATE() as transaction_date,
                    'ACTIVE' as status
                UNION ALL
                SELECT
                    'EXISTING_ID_2' as id,
                    'Updated Discount Item 2' as description,
                    150.00 as amount,
                    'USD' as currency_code,
                    DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) as transaction_date,
                    'RECONCILED' as status
                -- Add more placeholder data or replace with actual source query
            ) AS source
            ON target.id = source.id
            WHEN MATCHED THEN
                -- Update existing records if there are changes
                UPDATE SET
                    description = source.description,
                    amount = source.amount,
                    currency_code = source.currency_code,
                    transaction_date = source.transaction_date,
                    status = source.status,
                    load_timestamp = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN
                -- Insert new records
                INSERT (id, description, amount, currency_code, transaction_date, status, load_timestamp)
                VALUES (source.id, source.description, source.amount, source.currency_code, source.transaction_date, source.status, CURRENT_TIMESTAMP());
        """,
        use_legacy_sql=False,
        gcp_conn_id="google_cloud_default",
        # Ensure that the DDL task runs before the DML task
    )

    # Define task dependencies
    # The DDL task for table creation should ideally be run as a separate
    # one-off or schema management process. For a daily DAG, we'll assume the
    # table exists or create it idempotently.
    # If `create_ta_disc_zusgf_table` is intended to be run every time,
    # then `reconcile_ta_disc_zusgf` depends on it.
    create_ta_disc_zusgf_table >> reconcile_ta_disc_zusgf