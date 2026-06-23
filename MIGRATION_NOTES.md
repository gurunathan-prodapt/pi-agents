# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_austausch.ksh`. This script, along with its core logic (implied to be in `k_ausd_austausch.ksh`), is responsible for preparing a time-based snapshot extraction of contract cache data for the BERT report's "Forderungsscoring" (claims scoring).

The job has been migrated from a legacy KornShell environment to **Google Cloud's BigQuery**. The orchestration and data transformation logic are now implemented as BigQuery Stored Procedures, leveraging BigQuery for data storage and processing.

## 2. Generated Artifacts

The migration process generated the following artifacts:

*   **`bigquery_ddl/bert_reporting_tables.sql`**:
    *   **Role**: Defines the necessary BigQuery schema (`bert_reporting`) and tables. This includes the `job_audit_log` table for structured logging, and placeholder DDL for the various source tables (`sof_ta_p_...`) and target FOS tables (`rpt_ta_s_d1_...`) that are involved in the data flow. These DDLs are based on inferred schemas from the source system's usage.
*   **`bigquery_sp/k_ausd_austausch_sp.sql`**:
    *   **Role**: This BigQuery Stored Procedure encapsulates the core data extraction, transformation, and loading (ETL) logic that was originally contained or implied within `k_ausd_austausch.ksh`. It performs the `DELETE` (for restart logic) and `INSERT INTO ... SELECT FROM` operations on the target FOS tables.
*   **`bigquery_sp/r_ausd_austausch_sp.sql`**:
    *   **Role**: This BigQuery Stored Procedure serves as the orchestrator, replacing `r_ausd_austausch.ksh`. It handles parameter parsing, validation, defaulting, environment setup (conceptual), error handling, and invokes the `k_ausd_austausch_sp` for the core data processing. It also manages logging to the `job_audit_log` table.
*   **`python_orchestration/r_ausd_austausch_dag.py` (Optional/Conceptual)**:
    *   **Role**: (If external orchestration is chosen) A Python script or Cloud Composer (Airflow) DAG that wraps the execution of `r_ausd_austausch_sp`. This would be responsible for external parameter handling, advanced scheduling, and potentially integrating with other GCP services.

## 3. Key Design Decisions

*   **BigQuery Stored Procedures for Orchestration and Transformation**:
    *   **Why**: This approach leverages BigQuery's native capabilities for SQL-based data processing and control flow. It reduces external dependencies, simplifies deployment within GCP, and provides better performance for data-intensive operations compared to shell scripting.
    *   **Trade-offs**: Shell-specific features (e.g., `getopts`, `trap` for error handling, custom logging) required re-implementation using BigQuery SQL constructs (e.g., `IF`, `EXCEPTION WHEN ERROR`) and dedicated audit tables.
*   **Modularization into Wrapper and Core Stored Procedures**:
    *   **Why**: The original architecture separated `r_ausd_austausch.ksh` (wrapper) from `k_ausd_austausch.ksh` (core logic). This modularity is preserved by creating `r_ausd_austausch_sp` to handle orchestration and `k_ausd_austausch_sp` for the core data manipulation. This promotes reusability and clearer separation of concerns.
*   **Centralized Audit Logging to BigQuery Table**:
    *   **Why**: The custom shell-based logging (`DWMSG_*` functions) is replaced by structured `INSERT` statements into a dedicated `bert_reporting.job_audit_log` table. This provides queryable, centralized logging within BigQuery, which can be easily integrated with Cloud Logging and Monitoring for alerts and dashboards.
*   **BigQuery-Native Parameter Handling**:
    *   **Why**: Command-line parameter parsing (`getopts`) is replaced by direct input parameters to the BigQuery Stored Procedures. This provides a clear, type-safe interface for passing execution-specific values like `Stichtag` and `Wiederanlaufwert`.
*   **Direct Translation of Restart Logic**:
    *   **Why**: The inferred `DELETE` then `INSERT` pattern for restart logic was directly translated into BigQuery SQL to maintain functional parity with the legacy system.
    *   **Trade-offs**: For very large tables, this pattern can be inefficient. More optimized strategies like `MERGE` statements or leveraging BigQuery's partitioning/clustering capabilities could offer better performance but would require a redesign of the core logic. This is noted as a potential performance gap.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**:
    *   Ensure the `bert_reporting` BigQuery dataset exists in your target GCP project. If not, create it using the `CREATE SCHEMA IF NOT EXISTS bert_reporting;` statement from `bigquery_ddl/bert_reporting_tables.sql` or via the GCP Console.
2.  **Table DDL Deployment**:
    *   Execute the `bigquery_ddl/bert_reporting_tables.sql` script to create the `job_audit_log` table and the placeholder source (`sof_ta_p_...`) and target (`rpt_ta_s_d1_...`) tables within the `bert_reporting` dataset.
    *   **Important**: The DDL for source tables (`sof_ta_p_...`) are placeholders. Ensure the actual source tables (or views pointing to them) with correct schemas and data types are present and populated in BigQuery.
3.  **Stored Procedure Deployment**:
    *   Deploy `bigquery_sp/k_ausd_austausch_sp.sql` and `bigquery_sp/r_ausd_austausch_sp.sql` to the `bert_reporting` dataset. This can be done via the BigQuery UI, `bq` command-line tool, or CI/CD pipelines.
4.  **IAM Permissions Configuration**:
    *   **Service Account**: Identify or create a dedicated GCP service account that will execute the BigQuery Stored Procedures.
    *   **BigQuery Permissions**: Grant this service account the necessary BigQuery roles:
        *   `BigQuery Data Editor` (`roles/bigquery.dataEditor`) on the `bert_reporting` dataset to allow reading from source tables, writing to target tables, and inserting into the `job_audit_log`.
        *   `BigQuery Job User` (`roles/bigquery.jobUser`) to allow running BigQuery jobs.
    *   **Cloud Logging Permissions**: Ensure the service account has permissions to write logs to Cloud Logging if any external orchestration or custom logging is implemented beyond the `job_audit_log` table.
5.  **Scheduling Configuration**:
    *   **Cloud Scheduler (for direct BQSP execution)**: Create a Cloud Scheduler job that triggers the execution of `r_ausd_austausch_sp` at the desired frequency. This can be done by calling a Cloud Function that executes the SP, or directly via `bq query --use_legacy_sql=false --destination_table=... --run_as_service_account=... 'CALL bert_reporting.r_ausd_austausch_sp(...)'`.
    *   **Cloud Composer (for Python orchestration)**: If a Cloud Composer DAG (`python_orchestration/r_ausd_austausch_dag.py`) is used, deploy the DAG to your Cloud Composer environment. Ensure the Airflow service account has the necessary BigQuery permissions.
6.  **Initial Data Load**:
    *   Ensure that the source tables (`bert_reporting.sof_ta_p_...`) are populated with the initial dataset required for the first run of the migrated job. This might involve other migration jobs or data ingestion pipelines.

## 5. Known Gaps & Unresolved References

The following items were identified as gaps or unresolved references during the migration design and require follow-up:

*   **Content of `k_ausd_austausch.ksh` is Unknown**: The actual data transformation logic within the core script (`k_ausd_austausch.ksh`) was not available for detailed analysis. The current BigQuery Stored Procedure (`k_ausd_austausch_sp`) is based on inferred logic from comments in the wrapper script. A thorough review and validation against the actual `k_ausd_austausch.ksh` script is critical to ensure functional parity. Complex logic might necessitate a different BigQuery implementation or even a Dataflow/Spark job.
*   **Specifics of `.dw_init` and `BERT_DIR_ROOT`**: The exact contents and configurations defined by `$HOME/.dw_init` and the full path resolution of `${BERT_DIR_ROOT}` are not fully known. These need to be identified and mapped to appropriate BigQuery project/dataset names, GCS paths, or environmental configurations within the target GCP environment.
*   **Custom Logging Framework Details**: While a `job_audit_log` table replaces the `DWMSG_*` functions for basic status logging, any specific custom reporting or detailed diagnostic information previously captured by the legacy logging framework needs to be explicitly mapped and implemented in the BigQuery solution or Cloud Logging.
*   **Performance of Restart Logic**: The current `DELETE` then `INSERT` pattern for restart logic can be inefficient for very large tables in BigQuery. If performance becomes an issue, consider optimizing this with BigQuery's `MERGE` statement, or by leveraging partitioning and clustering keys on the target table (`rpt_ta_s_d1_vertrag`) to minimize data scanned and modified.
*   **Data Volume and Latency Requirements**: The scale of the "contract cache" and "FOS table" and the required data refresh frequency were not explicitly defined. These factors will influence whether the current BigQuery Stored Procedure approach is sufficient or if a more robust orchestration solution (e.g., Cloud Composer with more advanced error handling and monitoring) is required.

## 6. Validation

Validation of the migrated job involves functional and data integrity checks.

**How to Run Tests**:

1.  **Execute the Stored Procedure**:
    *   Use the BigQuery UI or `bq` command-line tool to call the main orchestrating Stored Procedure:
        ```sql
        CALL `bert_reporting`.`r_ausd_austausch_sp`(
            p_stichtag_string => 'DDMMYYYY', -- e.g., '01012023'
            p_wiederanlaufWert => 0          -- or a specific restart value
        );
        ```
    *   Test with various combinations of `p_stichtag_string` (valid, missing, invalid format) and `p_wiederanlaufWert` (0, positive integer).
2.  **External Orchestration (if applicable)**:
    *   If using a Cloud Composer DAG, trigger the DAG manually or allow it to run on its schedule.
3.  **Monitor Logs**:
    *   Query the `bert_reporting.job_audit_log` table for entries related to the job execution.
    *   Check Cloud Logging for any errors or warnings generated during the BigQuery job execution.

**What "Passing" Means**:

*   **Successful Execution**: The `job_audit_log` table shows an entry for the executed job with `status = 'SUCCESS'` and no `error_details`. Cloud Logging should not show any unhandled errors.
*   **Data Accuracy and Completeness**:
    *   **Row Counts**: Compare the number of rows in the target `rpt_ta_s_d1_vertrag` (and other `rpt_ta_s_d1_...` tables) for the given `Stichtag` with the expected output from the legacy system.
    *   **Data Integrity**: Sample specific records and verify that column values in the BigQuery target tables match the corresponding values in the legacy system's output for the same `Stichtag`.
    *   **Filtering Logic**: Confirm that the `Stichtag`, `Gueltig_von`, `Gueltig_bis`, `LADEDATUM`, and `DWH_VERTRAG_ID` filters are correctly applied, resulting in the expected subset of data.
    *   **Restart Logic**: When `p_wiederanlaufWert` is provided, verify that only records with `DWH_VERTRAG_ID >= Wiederanlaufwert` were deleted and subsequently re-inserted or updated, and that records below this threshold remain untouched.
*   **Performance**: The job completes within acceptable timeframes, consistent with or better than the legacy system's performance.

## 7. Rollback Procedure

In the event of critical issues or unexpected behavior after go-live, the following rollback procedure should be followed:

1.  **Immediate Action (If issues detected post-go-live)**:
    *   **Stop New Executions**: Immediately halt any scheduled or manual executions of the migrated BigQuery Stored Procedure (`r_ausd_austausch_sp`) or its orchestrator.
    *   **Revert to Legacy System**: Re-enable and resume running the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_austausch.ksh` script in the legacy environment.
    *   **Data Remediation (if necessary)**:
        *   If the BigQuery target tables (`rpt_ta_s_d1_...`) were corrupted or incorrectly populated by the migrated job, delete or truncate the affected partitions/data for the problematic `Stichtag`.
        *   Re-run the legacy `r_ausd_austausch.ksh` job to re-populate the target tables in the legacy system, ensuring data consistency.
        *   If the legacy system cannot directly populate BigQuery, a one-time data transfer from the legacy source to BigQuery might be required to restore a known good state.

2.  **Long-Term Rollback (If migration is fundamentally flawed)**:
    *   **Deactivate Migrated Components**: Deactivate or delete the BigQuery Stored Procedures (`r_ausd_austausch_sp`, `k_ausd_austausch_sp`) and any associated external orchestration (e.g., Cloud Composer DAGs, Cloud Scheduler jobs).
    *   **Continue Legacy Operations**: Continue operating the job exclusively on the legacy KornShell environment.
    *   **Re-evaluate Migration Strategy**: Conduct a post-mortem analysis to understand the root cause of the migration failure and re-evaluate the migration strategy, addressing the identified gaps and risks before attempting another migration.