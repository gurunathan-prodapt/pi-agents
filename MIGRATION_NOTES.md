# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the `k_ausd_v_ta_p_vertrag.ksh` KornShell script and its associated `d_ausd_v_ta_p_vertrag.sql` Oracle SQL*Plus script. The original job orchestrated the processing of contract data, performing transformations, and managing temporary tables within an Oracle database.

The migration targets Google Cloud Platform, specifically:
*   **Google BigQuery**: For all persistent and temporary data storage and SQL-based data processing.
*   **Cloud Composer (Apache Airflow)**: For orchestrating the data pipeline, replacing the KornShell script's control flow.

The primary goal was to re-implement the existing workflow with equivalent functionality and data integrity on a cloud-native, scalable, and managed platform.

## 2. Generated artifacts

The migration produced the following artifacts:

*   **`d_ausd_v_ta_p_vertrag.sql.bqsql`**
    *   **Role**: This file contains the core data transformation and manipulation logic, translated from Oracle SQL*Plus to BigQuery Standard SQL. It includes the logic for determining the reference date (`v_datum`), truncating the target table (`sof$ta_p_vertrag`), inserting processed contract data, and truncating various temporary tables. This script is designed to be executed directly within BigQuery.

*   **`dags/k_ausd_v_ta_p_vertrag_dag.py`**
    *   **Role**: This is an Apache Airflow DAG (Directed Acyclic Graph) written in Python. It replaces the orchestration functionality of the original `k_ausd_v_ta_p_vertrag.ksh` KornShell script. The DAG defines a single task (`execute_contract_data_processing`) that uses the `BigQueryExecuteQueryOperator` to run the BigQuery SQL script (`d_ausd_v_ta_p_vertrag.sql.bqsql`) as an atomic operation. It also handles parameter passing and integrates with Airflow's native scheduling and monitoring capabilities.

## 3. Key design decisions

Several key design decisions were made during this migration:

*   **Orchestration Layer Transition (KornShell to Airflow)**: The original KornShell script, responsible for environment setup, parameter parsing, SQL execution, and error handling, was replaced by an Apache Airflow DAG. This decision leverages Cloud Composer's managed Airflow service for robust scheduling, monitoring, logging, and dependency management, aligning with cloud-native best practices. Python's flexibility also allows for easier integration with GCP services.
*   **Data Processing Engine (Oracle SQL*Plus to BigQuery SQL)**: The core data transformation logic, originally in Oracle SQL*Plus, was translated to BigQuery Standard SQL. This move capitalizes on BigQuery's serverless, highly scalable, and cost-effective query engine, eliminating the need for managing traditional database instances.
*   **Handling Oracle-Specific SQL Constructs**:
    *   **Outer Join Syntax**: Oracle's proprietary `(+)` outer join syntax was replaced with the standard `LEFT JOIN` clause, which is fully supported in BigQuery.
    *   **Variable Definition**: Oracle `DEFINE` and `COLUMN ... NEW_VALUE` for `v_datum` were translated to BigQuery `DECLARE` and `SET` statements, allowing for variable usage within BigQuery scripting.
    *   **Date Functions**: Oracle's `NVL` and `TO_CHAR` for date formatting were replaced with BigQuery's `IFNULL` and `FORMAT_DATE` functions.
    *   **SQL*Plus Commands**: Commands like `WHENEVER SQLERROR`, `SET TIMING ON`, `SPOOL`, `START`, and `COMMIT` were removed as they are specific to SQL*Plus and their functionalities are either handled by Airflow (logging, error handling) or BigQuery's transactional model.
    *   **Hints**: Oracle `/*+ parallel(...) */` hints were removed, relying on BigQuery's automatic query optimization capabilities.
*   **PL/SQL Procedure Calls to Explicit BigQuery DDL**: The original script used a PL/SQL procedure (`isbert_schema.DWPA_UTIL_SKRIPT.runstatement`) to truncate multiple temporary tables. This was replaced by explicit `TRUNCATE TABLE` statements directly within the BigQuery SQL script. This simplifies the migration by avoiding the need to port complex PL/SQL logic, assuming the procedure's primary function was indeed truncation.
*   **Parameterization**: Command-line parameters from the KornShell script (`JobKennung`, `EintragsNr`) were mapped to Airflow DAG parameters, allowing for flexible execution and configuration via Airflow's UI or API.
*   **Temporary File Handling**: The KornShell script's use of temporary files for record counts was deemed unnecessary in the migrated solution, as Airflow's logging and BigQuery's query results provide sufficient operational insights.

## 4. Manual steps before go-live

Before the migrated job can be deployed and run in production, the following manual steps must be completed:

1.  **BigQuery Dataset and Table Creation**:
    *   Ensure the `isbert_schema` BigQuery dataset exists.
    *   Ensure all source tables (`isbert_schema.dwtk_meldungen`, `sof$ta_vertrag_tmp`) and the target table (`sof$ta_p_vertrag`) exist in BigQuery with their correct schemas (column names, data types).
    *   All temporary tables listed for truncation (`sof$ta_disc_zusgf`, `sof$ta_discount`, etc.) must also exist in BigQuery with appropriate schemas.

2.  **Data Ingestion Pipelines**:
    *   **Critical Dependency**: Establish and validate robust data ingestion pipelines from the source Oracle database (including `isbert_schema` and any tables accessed via `@pcrs1` Carmen DB link) into the corresponding BigQuery tables. This includes `isbert_schema.dwtk_meldungen` and `sof$ta_vertrag_tmp`, which are direct inputs to this job.
    *   Verify that these pipelines are populating the BigQuery tables with up-to-date and accurate data before this job runs.

3.  **IAM Permissions**:
    *   The Google Cloud service account associated with the Cloud Composer environment (or the specific Airflow connection used) must have the necessary BigQuery permissions. This typically includes:
        *   `BigQuery Data Editor` on the datasets containing `sof$ta_p_vertrag` and all temporary tables.
        *   `BigQuery Data Viewer` on the datasets containing `isbert_schema.dwtk_meldungen` and `sof$ta_vertrag_tmp`.
        *   `BigQuery Job User` to run BigQuery queries.

4.  **Airflow DAG Deployment**:
    *   Upload the `dags/k_ausd_v_ta_p_vertrag_dag.py` file to the DAGs folder of your Cloud Composer environment.

5.  **Scheduling Configuration**:
    *   Define the appropriate `schedule_interval` for the `k_ausd_v_ta_p_vertrag_dag` within the DAG definition or via the Airflow UI, as it is currently set to `None`.

6.  **Parameter Definition**:
    *   Determine how the `JobKennung` and `EintragsNr` parameters will be provided at runtime. If they are dynamic, ensure the upstream process triggering this DAG passes them via `dag_run.conf`. If static, update the default values in the DAG definition.

## 5. Known gaps & unresolved references

The following items have been identified as potential gaps or require further investigation/resolution:

*   **Upstream Data Lineage for `sof$ta_vertrag_tmp`**: The exact process that populates `sof$ta_vertrag_tmp` in the legacy Oracle system is not fully detailed. It is crucial to ensure this upstream process is also migrated or properly integrated to continuously feed data into the BigQuery `sof$ta_vertrag_tmp` table.
*   **Carmen DB Integration Details**: The original script referenced `@pcrs1` (Carmen DB) via a database link. The specific method for ingesting data from Carmen DB into BigQuery (e.g., CDC, batch export, Dataflow) needs to be fully defined and implemented to ensure data availability and consistency.
*   **PL/SQL `DWPA_UTIL_SKRIPT.runstatement` Complexity**: The migration assumes `DWPA_UTIL_SKRIPT.runstatement` primarily performs `TRUNCATE TABLE` operations. If this PL/SQL procedure contains additional complex logic (e.g., logging, auditing, conditional logic, or data archiving), that logic has not been explicitly migrated and would represent a functional gap. A review of the original PL/SQL source code is recommended.
*   **Custom Error Handling/Reporting**: The original KornShell script likely included custom error handling and reporting mechanisms (e.g., via `f_alis_msgerr.ksh`). While Airflow provides native logging and alerting, any specific custom reporting requirements (e.g., sending emails to specific distribution lists with custom formats) would need to be re-implemented in the Airflow DAG.
*   **Potential for BigQuery Optimization (B4 Item)**: The `d_ausd_v_ta_p_vertrag.sql` script was flagged as a potential B4 (Redesign) item due to Oracle-specific features. While a direct translation has been performed, a full redesign might yield more optimized BigQuery SQL, especially concerning partitioning, clustering, or more efficient join strategies if the data volumes are very large. This could be a follow-up optimization task.

## 6. Validation

To validate the successful migration and operation of the `k_ausd_v_ta_p_vertrag_dag`:

1.  **Trigger the Airflow DAG**:
    *   Manually trigger the `k_ausd_v_ta_p_vertrag_dag` from the Cloud Composer UI.
    *   Alternatively, if a schedule is defined, wait for the scheduled run.
    *   Ensure any required `dag_run.conf` parameters (`JobKennung`, `EintragsNr`) are provided if not using defaults.

2.  **Monitor DAG Execution**:
    *   Observe the DAG run in the Airflow UI.
    *   Verify that the `execute_contract_data_processing` task completes successfully (green status).
    *   Check the task logs for any errors or warnings.

3.  **Verify BigQuery Output**:
    *   **Target Table Population**: Query the `sof$ta_p_vertrag` table in BigQuery to confirm that data has been inserted.
    *   **Row Count Comparison**: Compare the row count of `sof$ta_p_vertrag` in BigQuery with the row count from the legacy Oracle system for the same processing period and input data.
    *   **Data Content Validation**: Perform spot checks or a full data comparison (e.g., using checksums or row-by-row comparison for a sample) between the BigQuery `sof$ta_p_vertrag` table and the Oracle `sof$ta_p_vertrag` table.
    *   **Temporary Table Truncation**: Verify that all temporary tables listed in the `TRUNCATE TABLE` statements (e.g., `sof$ta_disc_zusgf`, `sof$ta_discount`, etc.) are empty after the DAG run.

4.  **Performance Check**:
    *   Review the BigQuery job execution details (available in the BigQuery UI or logs) to assess query duration and resources consumed. Ensure performance is within acceptable limits.

**"Passing" Criteria**:
*   The Airflow DAG completes successfully without any task failures.
*   The `sof$ta_p_vertrag` table in BigQuery is populated with data.
*   The row count and a significant sample of data in BigQuery's `sof$ta_p_vertrag` match the expected output from the legacy Oracle system.
*   All specified temporary tables in BigQuery are successfully truncated.
*   The job completes within an acceptable time frame.

## 7. Rollback procedure

In case of issues during or after deployment, the following rollback procedure can be followed:

1.  **Deactivate/Delete Airflow DAG**:
    *   In the Cloud Composer UI, set the `k_ausd_v_ta_p_vertrag_dag` to "Off" or delete it from the DAGs folder to prevent further runs.

2.  **Revert BigQuery Target Table**:
    *   If the `sof$ta_p_vertrag` table was modified incorrectly, restore it to its state prior to the problematic run. This can be done by:
        *   Restoring from a BigQuery table snapshot if one was taken before the run.
        *   Using BigQuery's time travel feature to query data from before the problematic job.
        *   If no snapshot or time travel is feasible, re-running the *original* legacy job (if still operational) to overwrite the BigQuery table with correct data (assuming the legacy job can write to BigQuery, which is unlikely).
        *   *Best practice*: Ensure a backup or snapshot of `sof$ta_p_vertrag` is taken before the first production run of the migrated DAG.

3.  **Re-enable Legacy System**:
    *   If the legacy Oracle system and its associated KornShell script were decommissioned, they must be reactivated.
    *   Ensure the original `k_ausd_v_ta_p_vertrag.ksh` script can run successfully and populate the Oracle `sof$ta_p_vertrag` table.

4.  **Pause/Revert Data Ingestion**:
    *   If the issue is related to upstream data ingestion into BigQuery, pause or revert those pipelines to prevent further incorrect data from flowing into BigQuery.

5.  **Communication**:
    *   Inform relevant stakeholders about the rollback and the status of the data processing.

This procedure ensures that data processing can continue using the original system while the issues with the migrated job are investigated and resolved.