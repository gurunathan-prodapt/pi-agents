# Apache Airflow DAG for r_ausd_v_ta_c_bfc
# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_c_bfc.ksh
# This DAG orchestrates the BigQuery transformation steps for the binding period cache.

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

# Define BigQuery project and dataset IDs
# These can be set as Airflow Variables or passed as DAG params
BIGQUERY_PROJECT_ID = '{{ project_id }}'  # Placeholder, replace with your GCP Project ID
BIGQUERY_DATASET_ID = '{{ dataset_id }}'  # Placeholder, replace with your BigQuery Dataset ID

# Parameters for the BigQuery SQL scripts
# v_max_update corresponds to the Oracle DEFINE v_max_update = 1000000
# v_bfc_procedure is replaced by CURRENT_DATE() in BQ SQL, but can be
# made a configurable parameter if dynamic dating is required.
DAG_PARAMS = {
    "v_max_update": 1000000,
    # "v_bfc_procedure_date": "2023-01-01" # Example if you want to fix the bfc_procedure date
}

with DAG(
    dag_id="r_ausd_v_ta_c_bfc_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # Set your desired schedule (e.g., "@daily", "0 3 * * *")
    catchup=False,
    tags=["bigquery", "etl"],
    params=DAG_PARAMS,
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
    },
) as dag:
    # 1. Create/Update Target Cache Table DDL
    create_ta_c_bfc_table = BigQueryExecuteQueryOperator(
        task_id="create_ta_c_bfc_table",
        sql="""
            -- BigQuery DDL for the target cache table ta_c_bfc
            CREATE TABLE IF NOT EXISTS `{}.{}.ta_c_bfc` (
              cntrct_id STRING NOT NULL,
              bindefrist DATE,
              bfc_age INT64,
              bfc_count INT64,
              bfc_procedure DATE,
              commitment_reference_date DATE,
              cntrct_validity_id STRING,
              load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
            );
        """.format(BIGQUERY_PROJECT_ID, BIGQUERY_DATASET_ID),
        use_legacy_sql=False,
        gcp_conn_id="google_cloud_default",
    )

    # 2. Create/Update Staging Table DDL
    create_ta_c_bfc_akt_table = BigQueryExecuteQueryOperator(
        task_id="create_ta_c_bfc_akt_table",
        sql="""
            -- BigQuery DDL for the staging table ta_c_bfc_akt
            CREATE TABLE IF NOT EXISTS `{}.{}.ta_c_bfc_akt` (
              cntrct_id STRING NOT NULL,
              bindefrist DATE,
              bfc_age INT64,
              bfc_count INT64,
              bfc_procedure DATE,
              commitment_reference_date DATE,
              cntrct_validity_id STRING,
              load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
            );
        """.format(BIGQUERY_PROJECT_ID, BIGQUERY_DATASET_ID),
        use_legacy_sql=False,
        gcp_conn_id="google_cloud_default",
    )

    # 3. Create/Update bfc_get_bindefrist UDF
    create_bfc_get_bindefrist_udf = BigQueryExecuteQueryOperator(
        task_id="create_bfc_get_bindefrist_udf",
        sql="""
            -- BigQuery UDF for bfc_get_bindefrist
            CREATE OR REPLACE FUNCTION `{}.{}.bfc_get_bindefrist`(
                cntrct_id STRING,
                commitment_reference_date DATE,
                cntrct_validity_id STRING
            )
            RETURNS DATE
            LANGUAGE SQL
            AS (
                -- IMPORTANT: The actual business logic from Oracle's Cds$vr_Bindefrist.GetBindeFrist
                -- needs to be thoroughly analyzed and re-implemented here. This is a placeholder
                -- that mimics the signature and returns a dummy value.
                IF(commitment_reference_date IS NULL,
                    NULL,
                    -- Replace '9999-12-31' with the actual calculated date based on the re-implemented
                    -- Cds$vr_Bindefrist.GetBindeFrist logic.
                    DATE '9999-12-31'
                )
            );
        """.format(BIGQUERY_PROJECT_ID, BIGQUERY_DATASET_ID),
        use_legacy_sql=False,
        gcp_conn_id="google_cloud_default",
    )

    # 4. Step 1: Build Staging Table
    step1_build_staging = BigQueryExecuteQueryOperator(
        task_id="step1_build_staging",
        sql="""
            -- BigQuery SQL for Step 1: Build Staging Table ta_c_bfc_akt
            TRUNCATE TABLE `{}.{}.ta_c_bfc_akt`;

            INSERT INTO `{}.{}.ta_c_bfc_akt` (
                cntrct_id,
                commitment_reference_date,
                cntrct_validity_id,
                bfc_age,
                bfc_count
            )
            SELECT
                c.cntrct_id,
                MAX(c.commitment_reference_date) AS commitment_reference_date,
                MAX(c.cntrct_validity_id) AS cntrct_validity_id,
                MAX(
                    GREATEST(
                        IFNULL(c.bfc_age, PARSE_DATE('%Y%m%d', '19000101')),
                        IFNULL(b.bfc_age, PARSE_DATE('%Y%m%d', '19000101')),
                        IFNULL(v.bfc_age, PARSE_DATE('%Y%m%d', '19000101')),
                        IFNULL(p_fi.bfc_age, PARSE_DATE('%Y%m%d', '19000101')),
                        IFNULL(p_fo.bfc_age, PARSE_DATE('%Y%m%d', '19000101')),
                        IFNULL(p_fi_n.bfc_age, PARSE_DATE('%Y%m%d', '19000101')),
                        IFNULL(p_fo_n.bfc_age, PARSE_DATE('%Y%m%d', '19000101'))
                    )
                ) AS bfc_age,
                COUNT(1) AS bfc_count
            FROM
                `{}.{}.sof$ta_cntrct_crs` AS c
            LEFT JOIN
                `{}.{}.sof$ta_barrier` AS b
                ON c.cntrct_id = b.cntrct_id
            LEFT JOIN
                `{}.{}.sof$ta_cntrct_valid` AS v
                ON c.cntrct_validity_id = v.cntrct_validity_id
            LEFT JOIN
                `{}.{}.sof$ta_period` AS p_fi
                ON v.first_period_id = p_fi.period_id
            LEFT JOIN
                `{}.{}.sof$ta_period` AS p_fo
                ON v.following_period_id = p_fo.period_id
            LEFT JOIN
                `{}.{}.sof$ta_period` AS p_fi_n
                ON v.first_notice_period_id = p_fi_n.period_id
            LEFT JOIN
                `{}.{}.sof$ta_period` AS p_fo_n
                ON v.follow_notice_period_id = p_fo_n.period_id
            GROUP BY
                c.cntrct_id;
        """.format(
            BIGQUERY_PROJECT_ID, BIGQUERY_DATASET_ID,
            BIGQUERY_PROJECT_ID, BIGQUERY_DATASET_ID,
            BIGQUERY_PROJECT_ID, BIGQUERY_DATASET_ID,
            BIGQUERY_PROJECT_ID, BIGQUERY_DATASET_ID,
            BIGQUERY_PROJECT_ID, BIGQUERY_DATASET_ID,
            BIGQUERY_PROJECT_ID, BIGQUERY_DATASET_ID,
            BIGQUERY_PROJECT_ID, BIGQUERY_DATASET_ID,
            BIGQUERY_PROJECT_ID, BIGQUERY_DATASET_ID
        ),
        use_legacy_sql=False,
        gcp_conn_id="google_cloud_default",
    )

    # 5. Step 2: Initial Load if Target Table is Empty
    step2_initial_load = BigQueryExecuteQueryOperator(
        task_id="step2_initial_load",
        sql="""
            -- BigQuery SQL for Step 2: Initial Population of ta_c_bfc
            INSERT INTO `{}.{}.ta_c_bfc` (
                cntrct_id,
                bfc_age,
                bfc_count,
                bfc_procedure,
                commitment_reference_date,
                cntrct_validity_id
            )
            SELECT
                akt.cntrct_id,
                akt.bfc_age,
                akt.bfc_count,
                PARSE_DATE('%Y%m%d', '19000101') AS bfc_procedure,
                akt.commitment_reference_date,
                akt.cntrct_validity_id
            FROM
                `{}.{}.ta_c_bfc_akt` AS akt
            WHERE
                (SELECT COUNT(1) FROM `{}.{}.ta_c_bfc`) = 0;
        """.format(
            BIGQUERY_PROJECT_ID, BIGQUERY_DATASET_ID,
            BIGQUERY_PROJECT_ID, BIGQUERY_DATASET_ID,
            BIGQUERY_PROJECT_ID, BIGQUERY_DATASET_ID
        ),
        use_legacy_sql=False,
        gcp_conn_id="google_cloud_default",
    )

    # 6. Step 3: Merge Changed Rows
    step3_merge_changed_rows = BigQueryExecuteQueryOperator(
        task_id="step3_merge_changed_rows",
        sql="""
            -- BigQuery SQL for Step 3: Merge Changed Rows into ta_c_bfc
            MERGE INTO `{}.{}.ta_c_bfc` AS D
            USING `{}.{}.ta_c_bfc_akt` AS S
            ON (
                D.cntrct_id = S.cntrct_id
            )
            WHEN MATCHED AND (
                   D.bfc_age < S.bfc_age
                OR D.bfc_count <> S.bfc_count
            ) THEN
                UPDATE SET
                    bindefrist = `{}.{}.bfc_get_bindefrist`(S.cntrct_id, S.commitment_reference_date, S.cntrct_validity_id),
                    bfc_age = S.bfc_age,
                    bfc_count = S.bfc_count,
                    bfc_procedure = CURRENT_DATE(),
                    commitment_reference_date = S.commitment_reference_date,
                    cntrct_validity_id = S.cntrct_validity_id,
                    load_ts = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN
                INSERT (
                    cntrct_id,
                    bindefrist,
                    bfc_age,
                    bfc_count,
                    bfc_procedure,
                    commitment_reference_date,
                    cntrct_validity_id,
                    load_ts
                )
                VALUES (
                    S.cntrct_id,
                    `{}.{}.bfc_get_bindefrist`(S.cntrct_id, S.commitment_reference_date, S.cntrct_validity_id),
                    S.bfc_age,
                    S.bfc_count,
                    CURRENT_DATE(),
                    S.commitment_reference_date,
                    S.cntrct_validity_id,
                    CURRENT_TIMESTAMP()
                );
        """.format(
            BIGQUERY_PROJECT_ID, BIGQUERY_DATASET_ID,
            BIGQUERY_PROJECT_ID, BIGQUERY_DATASET_ID,
            BIGQUERY_PROJECT_ID, BIGQUERY_DATASET_ID,
            BIGQUERY_PROJECT_ID, BIGQUERY_DATASET_ID
        ),
        use_legacy_sql=False,
        gcp_conn_id="google_cloud_default",
    )

    # 7. Step 4: Recalculate Stale Rows
    step4_recalculate_stale_rows = BigQueryExecuteQueryOperator(
        task_id="step4_recalculate_stale_rows",
        sql="""
            -- BigQuery SQL for Step 4: Recalculate Stale Rows in ta_c_bfc
            UPDATE `{}.{}.ta_c_bfc`
            SET
                bindefrist = `{}.{}.bfc_get_bindefrist`(
                    cntrct_id,
                    commitment_reference_date,
                    cntrct_validity_id
                ),
                bfc_procedure = CURRENT_DATE(),
                load_ts = CURRENT_TIMESTAMP()
            WHERE
                bfc_procedure < CURRENT_DATE()
            QUALIFY
                ROW_NUMBER() OVER(ORDER BY cntrct_id) <= {{ params.v_max_update }};
        """.format(
            BIGQUERY_PROJECT_ID, BIGQUERY_DATASET_ID,
            BIGQUERY_PROJECT_ID, BIGQUERY_DATASET_ID
        ),
        use_legacy_sql=False,
        gcp_conn_id="google_cloud_default",
    )

    # 8. Cleanup Staging Table
    cleanup_staging_table = BigQueryExecuteQueryOperator(
        task_id="cleanup_staging_table",
        sql="""
            -- BigQuery SQL for Cleanup: Truncate Staging Table ta_c_bfc_akt
            TRUNCATE TABLE `{}.{}.ta_c_bfc_akt`;
        """.format(BIGQUERY_PROJECT_ID, BIGQUERY_DATASET_ID),
        use_legacy_sql=False,
        gcp_conn_id="google_cloud_default",
    )

    # Define task dependencies
    (
        [create_ta_c_bfc_table, create_ta_c_bfc_akt_table]
        >> create_bfc_get_bindefrist_udf
        >> step1_build_staging
        >> step2_initial_load
        >> step3_merge_changed_rows
        >> step4_recalculate_stale_rows
        >> cleanup_staging_table
    )