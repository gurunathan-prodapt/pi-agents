# This Airflow DAG replaces the legacy job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_action_assoc.ksh
# and its associated SQL script vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_action_assoc.sql.

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

with DAG(
    dag_id="d_ausd_v_ta_action_assoc",
    schedule=None,  # Replace with appropriate schedule, e.g., "0 3 * * *" for daily at 3 AM UTC
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["isbert", "etl"],
    params={
        # p_EintragsNr was unused in the original SQL and is omitted here.
        # If needed for future extensions, it can be added and passed via Jinja templating.
    },
) as dag:
    process_ta_action_assoc = BigQueryExecuteQueryOperator(
        task_id="process_ta_action_assoc",
        gcp_conn_id="google_cloud_default", # Ensure this connection is configured in Airflow
        sql="""
            -- Declare v_datum from dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_action_assoc.sql
            DECLARE v_datum STRING;

            SET v_datum = (
                SELECT
                    COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
                FROM
                    `isbert_schema.dwtk_meldungen` m
                WHERE
                    m.job_kennung = 'BERT_DROP_TEMP_TABLE'
            );

            -- Truncate target table: sof_ta_action_assoc
            -- Replaces isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_action_assoc')
            TRUNCATE TABLE `sof_ta_action_assoc`;

            -- Insert data into sof_ta_action_assoc
            -- Original: INSERT INTO sof$ta_action_assoc ... FROM cds$ta_action_assoc &v_carmen ac
            INSERT INTO `sof_ta_action_assoc` (
                cntrct_id,
                rv_action_id
            )
            SELECT
                ac.cntrct_id,
                ac.rv_action_id
            FROM
                `cds_ta_action_assoc` ac -- Assumes cds$ta_action_assoc is migrated to cds_ta_action_assoc in BigQuery
            WHERE
                DATE(ac.insert_at)      <= PARSE_DATE('%Y%m%d', v_datum)
                AND DATE(ac.valid_from)     <= PARSE_DATE('%Y%m%d', v_datum)
                AND ac.is_production   = 1
                AND ( ac.modified_at IS NULL OR DATE(ac.modified_at) > PARSE_DATE('%Y%m%d', v_datum) )
                AND ( ac.valid_to    IS NULL OR DATE(ac.valid_to)    > PARSE_DATE('%Y%m%d', v_datum) );
        """,
        use_legacy_sql=False,
    )