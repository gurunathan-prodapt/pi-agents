# MIGRATION_NOTES.md for k_ausd_bp_ta_bcp_iccid.ksh

## 1. Summary

This document details the migration of the ETL job orchestrated by `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_iccid.ksh`. The original job, a KornShell script, managed parameter parsing, date validation, and executed a core Oracle PL/SQL script (`d_ausd_bp_ta_bcp_iccid.sql`) to truncate and load the `SOF$TA_BCP_ICCID` table.

The job has been re-platformed to Google Cloud Platform (GCP).
*   **Orchestration**: Migrated from KornShell to **Cloud Composer (Apache Airflow)**.
*   **Data Storage & Transformation**: Migrated from Oracle Database to **Google BigQuery**.

The core logic now resides in a BigQuery SQL script, executed by an Airflow DAG, which also handles parameter processing and date calculations previously managed by various KornShell utilities.

## 2. Generated Artifacts

The following files were generated as part of this migration:

*   **`src/ddl/bigquery/isbert_schema/dwtk_meldungen.sql`**
    *   **Role**: BigQuery Data Definition Language (DDL) script to create the `isbert_schema.dwtk_meldungen` table. This table is a BigQuery equivalent of the original Oracle `isbert_schema.dwtk_meldungen` table, used for deriving `v_datum`.
*   **`src/ddl/bigquery/sof/ta_bpr_bcp.sql`**
    *   **Role**: BigQuery DDL script to create the `sof.ta_bpr_bcp` table. This table is a BigQuery equivalent of the original Oracle `sof$ta_bpr_bcp` table, serving as a source for the transformation.
*   **`src/ddl/bigquery/sof/ta_iccid_vertrag.sql`**
    *   **Role**: BigQuery DDL script to create the `sof.ta_iccid_vertrag` table. This table is a BigQuery equivalent of the original Oracle `sof$ta_iccid_vertrag` table, serving as a source for the transformation.
*   **`src/ddl/bigquery/sof/ta_bcp_iccid.sql`**
    *   **Role**: BigQuery DDL script to create the `sof.ta_bcp_iccid` table. This table is the BigQuery equivalent of the original Oracle `SOF$TA_BCP_ICCID` table, serving as the target for the transformation.
*   **`src/sql/bigquery/d_ausd_bp_ta_bcp_iccid.bqsql`**
    *   **Role**: BigQuery SQL script containing the core data transformation logic. It replaces the original Oracle PL/SQL script (`d_ausd_bp_ta_bcp_iccid.sql`), performing the `TRUNCATE` and `INSERT INTO ... SELECT DISTINCT` operations to populate `sof.ta_bcp_iccid`.
*   **`src/dags/k_ausd_bp_ta_bcp_iccid_dag.py`**
    *   **Role**: Apache Airflow DAG (Directed Acyclic Graph) written in Python. This DAG orchestrates the entire job, including parameter validation, date calculations (replacing `gestern.ksh`), and executing the `d_ausd_bp_ta_bcp_iccid.bqsql` script using the `BigQueryOperator`. It also incorporates Airflow's native logging and error handling.

## 3. Key Design Decisions

*   **Cloud-Native Re-platforming**: The entire job was re-platformed to Google Cloud Platform to leverage its scalable, managed services for data warehousing and orchestration.
*   **Airflow for Orchestration**: Apache Airflow (via Cloud Composer) was chosen to replace the KornShell orchestration. This provides robust scheduling, monitoring, dependency management, and error handling capabilities, which are superior to custom shell scripting. It also centralizes job definitions and execution logs.
*   **BigQuery for Data Transformation**: Google BigQuery was selected as the target data warehouse. The Oracle PL/SQL transformation logic was translated into standard BigQuery SQL to capitalize on BigQuery's columnar storage, serverless architecture, and high-performance query engine. This eliminates the need for managing an Oracle database instance for this ETL.
*   **Python for Utility Logic**: All utility shell scripts (e.g., `gestern.ksh` for date calculations, `h_alis_parameter.ksh` for parameter parsing, `f_alis_msgerr.ksh` for error handling) were re-implemented in Python directly within the Airflow DAG. This consolidates the job's logic into a single, maintainable Python codebase and leverages Airflow's native features.
*   **Direct BigQuery Interaction**: The `h_alis_sqlplus.ksh` utility, which executed SQL*Plus scripts, was replaced by Airflow's `BigQueryOperator`. This operator directly interfaces with BigQuery, streamlining the execution of SQL transformations.
*   **Preservation of Truncate/Insert Pattern**: The original job's pattern of truncating the target table before inserting new data was maintained in the BigQuery SQL transformation, ensuring similar behavior.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**:
    *   Ensure the `isbert_schema` and `sof` BigQuery datasets exist in your GCP project. If not, create them.
2.  **BigQuery Table Creation**:
    *   Execute the DDL scripts:
        *   `src/ddl/bigquery/isbert_schema/dwtk_meldungen.sql`
        *   `src/ddl/bigquery/sof/ta_bpr_bcp.sql`
        *   `src/ddl/bigquery/sof/ta_iccid_vertrag.sql`
        *   `src/ddl/bigquery/sof/ta_bcp_iccid.sql`
    *   **Important**: Verify and adjust the inferred column data types in these DDLs against the actual Oracle source schema to ensure data integrity.
3.  **Data Ingestion**:
    *   Ensure that the source data from Oracle tables (`isbert_schema.dwtk_meldungen`, `sof$ta_bpr_bcp`, `sof$ta_iccid_vertrag`) is regularly ingested and available in their respective BigQuery tables. This is a prerequisite for the job to run successfully.
4.  **IAM Permissions**:
    *   Grant the Airflow service account (used by your Cloud Composer environment) the necessary BigQuery roles:
        *   `BigQuery Data Editor` on the `sof` dataset (for `TRUNCATE` and `INSERT` into `sof.ta_bcp_iccid`).
        *   `BigQuery Data Viewer` on the `isbert_schema` and `sof` datasets (for `SELECT` from source tables).
        *   `BigQuery Job User` for running BigQuery queries.
5.  **Airflow Deployment**:
    *   Upload the BigQuery SQL script (`src/sql/bigquery/d_ausd_bp_ta_bcp_iccid.bqsql`) to a Google Cloud Storage (GCS) bucket accessible by your Airflow environment. A common practice is to place it in a `dags/sql/bigquery/` subdirectory within your Airflow GCS bucket.
    *   Upload the Airflow DAG file (`src/dags/k_ausd_bp_ta_bcp_iccid_dag.py`) to the DAGs folder in your Airflow GCS bucket.
6.  **Airflow Connection Configuration**:
    *   Ensure a BigQuery connection is configured in Airflow. The DAG defaults to `google_cloud_default`, which typically works out-of-the-box with Cloud Composer. If a custom connection is required, configure it in the Airflow UI.
7.  **Airflow Variables/Parameters**:
    *   Review the DAG's default parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`). If these need to be dynamically set or overridden, configure them as Airflow Variables or pass them during manual DAG triggers.
    *   Investigate and configure the `v_carmen` value (`@pcrs1`) if it represents a dynamic or sensitive configuration. If it's a static identifier, it can remain hardcoded in the SQL.
8.  **Scheduling**:
    *   Define the desired schedule for the `k_ausd_bp_ta_bcp_iccid_dag` in the Airflow UI, or configure external systems to trigger the DAG as needed. The DAG is currently set to `schedule=None`, indicating it's intended for manual or external triggers.

## 5. Known Gaps & Unresolved References

*   **`v_carmen = "@pcrs1"`**: The exact nature and necessity of this variable and its value (`@pcrs1`) remain unclear. Further investigation is required to determine if it represents a database link, an external system identifier, or a configuration value, and how it should be handled in the GCP environment.
*   **Schema Data Type Verification**: The DDLs for BigQuery tables (`isbert_schema.dwtk_meldungen`, `sof.ta_bpr_bcp`, `sof.ta_iccid_vertrag`, `sof.ta_bcp_iccid`) have inferred data types. These *must* be manually verified against the actual Oracle source schema to prevent data truncation, type conversion errors, or unexpected behavior.
*   **`p_wiederanlaufWert` Usage**: The original script excerpt initializes `p_wiederanlaufWert` but does not show its usage. Its purpose and impact on the overall job flow need to be confirmed to ensure complete migration.
*   **`tmpFile` and `v_records`**: The original script used a temporary file to store `v_records`. While the BigQuery transformation directly handles data, the exact purpose of `v_records` and its subsequent use in the original script (beyond the provided excerpt) is not fully clear. If `v_records` was used for logging or downstream processing, an equivalent mechanism (e.g., Airflow XComs or BigQuery query results) might be needed.
*   **Missing Complexity/Automation Data**: The inferred "Medium" complexity and "Semi-Auto" automation bucket for the source job were based on limited information. These classifications should be validated manually.

## 6. Validation

To ensure the migrated job functions correctly, perform the following validation steps:

1.  **BigQuery SQL Validation (Unit Test)**:
    *   Manually execute the `src/sql/bigquery/d_ausd_bp_ta_bcp_iccid.bqsql` script in the BigQuery console.
    *   Ensure that the source tables (`isbert_schema.dwtk_meldungen`, `sof.ta_bpr_bcp`, `sof.ta_iccid_vertrag`) contain representative test data.
    *   Verify that the `sof.ta_bcp_iccid` table is truncated and then populated with the expected data, matching the output of the original Oracle PL/SQL script.
2.  **Airflow DAG Validation (Integration Test)**:
    *   Trigger the `k_ausd_bp_ta_bcp_iccid_dag` manually from the Airflow UI.
    *   Monitor the DAG run in the Airflow UI, checking task statuses and logs for any errors or warnings.
    *   After the DAG completes successfully, query the `sof.ta_bcp_iccid` table in BigQuery.
    *   **Passing Criteria**:
        *   The Airflow DAG completes successfully without any failed tasks.
        *   The `sof.ta_bcp_iccid` table in BigQuery is truncated and loaded with the correct data.
        *   The row count and data content of `sof.ta_bcp_iccid` in BigQuery match the expected output from the original Oracle job for the same input data.
        *   All parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`) are correctly processed and reflected in logs or downstream behavior if applicable.

## 7. Rollback Procedure

In case of critical issues or unexpected behavior after go-live, follow these steps to roll back to the original system:

1.  **Deactivate Airflow DAG**:
    *   In the Airflow UI, set the `k_ausd_bp_ta_bcp_iccid_dag` to "Off" or delete it to prevent further execution.
2.  **Restore BigQuery Target Table (if necessary)**:
    *   If the `sof.ta_bcp_iccid` table was corrupted or incorrectly updated, use BigQuery's time travel feature or restore from a snapshot/backup to revert it to a known good state prior to the problematic run.
3.  **Re-enable Original Job**:
    *   Re-enable the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_iccid.ksh` job in its legacy scheduling system (e.g., cron, enterprise scheduler).
4.  **Verify Legacy Job Execution**:
    *   Monitor the re-enabled legacy job to ensure it runs successfully and produces the expected output in the Oracle `SOF$TA_BCP_ICCID` table.
5.  **Data Consistency Check**:
    *   Perform a data consistency check between the legacy Oracle `SOF$TA_BCP_ICCID` table and the BigQuery `sof.ta_bcp_iccid` table (if it was not restored) to understand any discrepancies that occurred during the migration attempt.