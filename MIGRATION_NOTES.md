# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell orchestration script `r_ausd_bp_ta_bcp_iccid.ksh` from its legacy environment to Google BigQuery. The original script served as a wrapper for an ETL job ("Bereitstellung Basisprodukte BERT") responsible for extracting cutoff-date-based contract cache data for credit scoring.

The migration involved refactoring the shell script's logic into a BigQuery Stored Procedure (`project.dataset.ausd_bp_ta_ibcp_ccid`) which handles parameter parsing, logging, and invokes a separate BigQuery Stored Procedure (`project.dataset.k_ausd_bp_ta_bcp_iccid`) containing the core data transformation logic. File-based logging has been replaced with a centralized BigQuery logging table (`project.dataset.dwmsg_log`).

## 2. Generated artifacts

The migration process generated the following BigQuery-native artifacts:

*   **`dwmsg_job_sequence_table.sql`**
    *   **Role:** DDL script to create the `project.dataset.dwmsg_job_sequence` table. This table is intended to manage job-specific sequence numbers, primarily for logging purposes, though its usage is not fully implemented in the generated procedures.
*   **`dwmsg_log_table.sql`**
    *   **Role:** DDL script to create the `project.dataset.dwmsg_log` table. This table serves as the centralized logging mechanism for job executions, capturing start/end times, status, messages, parameters, and error details. It replaces the file-based logging of the original KornShell script.
*   **`k_ausd_bp_ta_bcp_iccid_procedure.sql`**
    *   **Role:** BigQuery Stored Procedure (`project.dataset.k_ausd_bp_ta_bcp_iccid`). This procedure is a placeholder for the core ETL logic previously contained within the `k_ausd_bp_ta_bcp_iccid.ksh` kernel script. It includes sample `DELETE` and `INSERT` statements based on the design document's understanding of the original data transformation, interacting with `project.dataset.fos_tabelle` and `project.dataset.vertrag_cache`.
*   **`r_ausd_bp_ta_bcp_iccid_procedure.sql`**
    *   **Role:** BigQuery Stored Procedure (`project.dataset.ausd_bp_ta_ibcp_ccid`). This is the main orchestrator procedure, replacing the original `r_ausd_bp_ta_bcp_iccid.ksh`. It handles input parameter parsing (`p_stichtag_str`, `p_wiederanlaufwert_str`), applies default values, manages logging to `dwmsg_log`, and invokes the `project.dataset.k_ausd_bp_ta_bcp_iccid` procedure. It also implements BigQuery-native error handling.
*   **`procedure_parameters.json`**
    *   **Role:** A JSON configuration file describing the input parameters for the main BigQuery Stored Procedure (`project.dataset.ausd_bp_ta_ibcp_ccid`). It specifies parameter names, types, descriptions, optionality, default value hints, and provides an invocation example. This file is useful for external orchestration tools.

## 3. Key design decisions

*   **Migration to BigQuery Stored Procedures:** The entire KornShell orchestration logic and the delegated kernel logic were migrated to BigQuery Stored Procedures.
    *   **Why:** This approach leverages BigQuery's native capabilities for data processing, eliminates dependencies on a shell environment, and integrates seamlessly with the Google Cloud ecosystem. It allows for direct execution within BigQuery, simplifying deployment and management.
    *   **Trade-offs:** Requires rewriting shell-specific constructs (e.g., `getopts`, `trap`, file I/O) into BigQuery SQL equivalents. Introduces a dependency on BigQuery's SQL dialect and features.
*   **Separation of Orchestration and Core Logic:** The original wrapper (`r_ausd_bp_ta_bcp_iccid.ksh`) and kernel (`k_ausd_bp_ta_bcp_iccid.ksh`) scripts were translated into two distinct BigQuery Stored Procedures (`ausd_bp_ta_ibcp_ccid` and `k_ausd_bp_ta_bcp_iccid`).
    *   **Why:** Maintains the modularity of the original design, promoting reusability and clearer separation of concerns. The orchestrator handles environment setup and logging, while the kernel focuses solely on data transformation.
*   **Centralized BigQuery Logging:** File-based logging was replaced with inserts/updates to a dedicated BigQuery table (`project.dataset.dwmsg_log`).
    *   **Why:** Provides a centralized, queryable, and scalable logging solution. Logs are immediately available for analysis, monitoring, and auditing within BigQuery, eliminating the need to access file systems.
    *   **Trade-offs:** Requires DDL for the log table and `INSERT`/`UPDATE` statements within the procedures, adding minor overhead compared to simple file appends.
*   **Native BigQuery Parameter Handling:** Shell parameter parsing (`getopts`) was replaced by BigQuery Stored Procedure input parameters.
    *   **Why:** Offers type safety, clear definition of inputs, and direct integration with BigQuery's execution model.
*   **BigQuery-Native Error Handling:** Shell `trap` statements were replaced with BigQuery's `EXCEPTION WHEN ERROR THEN ... END` blocks.
    *   **Why:** Provides structured error handling within the BigQuery SQL context, allowing for graceful failure, logging of error details, and propagation of errors to calling processes.
*   **BigQuery Date Functions:** Custom shell date utilities were replaced with BigQuery's built-in date and time functions (e.g., `CURRENT_DATE()`, `PARSE_DATE()`).
    *   **Why:** Simplifies date manipulation, improves performance, and reduces code complexity by leveraging optimized native functions.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (`project.dataset`) exists. If not, create it:
        ```sql
        CREATE SCHEMA IF NOT EXISTS project.dataset;
        ```
2.  **Source and Target Table Creation/Verification:**
    *   Verify that the source table `project.dataset.vertrag_cache` and target table `project.dataset.fos_tabelle` exist in BigQuery with schemas compatible with the `k_ausd_bp_ta_bcp_iccid` procedure's `SELECT` and `INSERT` statements.
    *   If `fos_tabelle` does not exist, create its DDL. The `k_ausd_bp_ta_bcp_iccid` procedure assumes columns like `dwh_vertrag_id`, `gueltig_von`, `gueltig_bis`, `ladedatum`, and `stichtag_wert`.
3.  **Deploy DDL for Logging Tables:**
    *   Execute `dwmsg_log_table.sql` to create the `project.dataset.dwmsg_log` table.
    *   Execute `dwmsg_job_sequence_table.sql` to create the `project.dataset.dwmsg_job_sequence` table.
4.  **Deploy BigQuery Stored Procedures:**
    *   Execute `k_ausd_bp_ta_bcp_iccid_procedure.sql` to create the kernel procedure.
    *   Execute `r_ausd_bp_ta_bcp_iccid_procedure.sql` to create the main orchestrator procedure (`project.dataset.ausd_bp_ta_ibcp_ccid`).
5.  **IAM Permissions Configuration:**
    *   The service account or user running the BigQuery procedures must have the following IAM roles/permissions:
        *   `BigQuery Data Editor` on `project.dataset` (or specific tables `dwmsg_log`, `fos_tabelle`) to `INSERT`, `UPDATE`, `DELETE` data.
        *   `BigQuery Data Viewer` on `project.dataset.vertrag_cache` to `SELECT` data.
        *   `BigQuery Job User` to run BigQuery jobs.
        *   `bigquery.routines.execute` permission on both `project.dataset.ausd_bp_ta_ibcp_ccid` and `project.dataset.k_ausd_bp_ta_bcp_iccid` procedures.
6.  **Scheduling Configuration:**
    *   Configure a scheduling mechanism to invoke the `project.dataset.ausd_bp_ta_ibcp_ccid` procedure. Options include:
        *   **Cloud Composer (Airflow):** Create a DAG that calls the BigQuery procedure, potentially using the `procedure_parameters.json` for configuration.
        *   **BigQuery Scheduled Queries:** If the job is simple and doesn't require complex dependencies, a BigQuery scheduled query can be configured to `CALL` the procedure.
        *   **Cloud Scheduler + Cloud Functions/Workflows:** A Cloud Scheduler job can trigger a Cloud Function or Workflow that, in turn, executes the BigQuery procedure.
7.  **Initial `dwmsg_job_sequence` Entry (if used):**
    *   If the `dwmsg_job_sequence` table is intended to manage job-specific sequence numbers (e.g., for `run_id` generation or other unique identifiers), an initial entry for `job_name = 'r_ausd_bp_ta_bcp_iccid'` might be required. (Note: The generated `r_ausd_bp_ta_bcp_iccid_procedure.sql` currently uses `GENERATE_UUID()` for `run_id` and `log_id` and does not interact with `dwmsg_job_sequence`).

## 5. Known gaps & unresolved references

The following items were identified as gaps or require further follow-up:

*   **Full Kernel Script Logic (`k_ausd_bp_ta_bcp_iccid.ksh`):** The `k_ausd_bp_ta_bcp_iccid_procedure.sql` is a placeholder. Its full transformation logic, including all source tables, target tables, and complex business rules, needs to be thoroughly analyzed and implemented. The current version is based on assumptions from the wrapper script's comments.
*   **Utility Script Functionality:** The full functionality of legacy utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) needs to be completely mapped. While parameter parsing and date functions are largely covered, the comprehensive error handling framework of `f_alis_msgerr.ksh` might have nuances not fully replicated by BigQuery's `EXCEPTION WHEN ERROR` blocks.
*   **Shell Traps:** The exact behavior of the original shell `trap` statements (e.g., for specific signals or cleanup actions) might not be perfectly replicated by BigQuery's `EXCEPTION WHEN ERROR`. Further analysis is needed if specific signal handling was critical.
*   **Environment Sourcing (`. $HOME/.dw_init`):** The contents and implications of the `.dw_init` script are not fully known. Any environment variables, paths, or configurations set by this script will need to be translated into BigQuery procedure parameters, configuration tables, or managed by the orchestration layer (e.g., Airflow variables).
*   **"AL??" Comments:** The commented-out lines like `#AL?? FOSHoleLadedatum "DWH\$TA_C_VERTRAG" v_ladedatum` in the original script indicate potential alternative or legacy logic. It should be confirmed whether this logic is still relevant or can be safely ignored in the BigQuery migration.
*   **Absence of Lineage Edges:** The automated lineage extraction did not identify direct dependencies for the `r_ausd_bp_ta_bcp_iccid.ksh` wrapper. This implies that all interactions (e.g., sourcing helper scripts, invoking the kernel script) were inferred manually. A thorough manual verification of all dependencies is crucial.
*   **`dwmsg_job_sequence` Table Usage:** While the `dwmsg_job_sequence` table was generated, the `r_ausd_bp_ta_bcp_iccid_procedure.sql` does not currently interact with it (e.g., to get a sequence number for `run_id`). The current procedure uses `GENERATE_UUID()`. If the sequence table was intended for a specific purpose (e.g., sequential job IDs), this functionality needs to be explicitly added.

## 6. Validation

To validate the successful migration and functionality of the BigQuery procedures:

1.  **Execute the Main Procedure:**
    *   Call `project.dataset.ausd_bp_ta_ibcp_ccid` with various parameter combinations:
        *   `CALL project.dataset.ausd_bp_ta_ibcp_ccid(NULL, NULL);` (to test default values)
        *   `CALL project.dataset.ausd_bp_ta_ibcp_ccid('YYYY-MM-DD', NULL);` (specific stichtag, default restart)
        *   `CALL project.dataset.ausd_bp_ta_ibcp_ccid(NULL, '12345');` (default stichtag, specific restart)
        *   `CALL project.dataset.ausd_bp_ta_ibcp_ccid('YYYY-MM-DD', '12345');` (specific stichtag and restart)
        *   Test with invalid date formats or non-numeric restart values to verify error handling.
2.  **Monitor `dwmsg_log` Table:**
    *   After each execution, query `project.dataset.dwmsg_log` to check the status and details of the job run.
    *   **Passing Criteria:**
        *   A new entry with the `job_name = 'r_ausd_bp_ta_bcp_iccid'` and a unique `run_id` should be present.
        *   The `status` column should be `'SUCCESS'` for successful runs.
        *   `start_time` and `end_time` should be populated.
        *   `message` should indicate successful completion.
        *   `parameters` JSON should correctly reflect the parsed input values.
        *   For failed runs, `status` should be `'FAILED'`, `message` should indicate failure, and `error_details` should contain relevant error information.
3.  **Verify Data in `fos_tabelle`:**
    *   Query `project.dataset.fos_tabelle` to confirm that data has been inserted and/or deleted correctly based on the `p_stichtag` and `p_wiederanlaufwert` parameters.
    *   **Passing Criteria:**
        *   Records with `dwh_vertrag_id` less than `p_wiederanlaufwert` (if `p_wiederanlaufwert` > 0) should remain unchanged.
        *   Records with `dwh_vertrag_id` greater than or equal to `p_wiederanlaufwert` should reflect the new data from `vertrag_cache` based on the `p_stichtag` criteria.
        *   No unexpected data loss or corruption in `fos_tabelle`.
        *   The `stichtag_wert` column in `fos_tabelle` should match the `p_stichtag` used for the run.
4.  **Performance Check:**
    *   Compare the execution time of the BigQuery procedures with the legacy KornShell script to ensure performance is acceptable or improved.

## 7. Rollback procedure

In case of issues or unexpected behavior after go-live, the following rollback procedure can be followed:

1.  **Immediate Action (Stop New Runs):**
    *   Disable or delete the new BigQuery scheduled query, Cloud Composer DAG, or any other orchestration mechanism triggering the `project.dataset.ausd_bp_ta_ibcp_ccid` procedure.
    *   Re-enable the legacy scheduler for `r_ausd_bp_ta_bcp_iccid.ksh` in the original environment.
2.  **Data Rollback (if `fos_tabelle` was affected):**
    *   If the `project.dataset.fos_tabelle` was corrupted or incorrectly updated by the BigQuery job, use BigQuery's time travel feature to restore the table to a state before the erroneous job run.
    *   Identify the timestamp of the last known good state (e.g., from `dwmsg_log` or by querying `fos_tabelle` history).
    *   Execute a `CREATE TABLE AS SELECT` or `INSERT INTO` statement using `FOR SYSTEM_TIME AS OF` to restore the data:
        ```sql
        CREATE OR REPLACE TABLE project.dataset.fos_tabelle AS
        SELECT * FROM project.dataset.fos_tabelle
        FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR); -- Adjust interval as needed
        ```
        *Alternatively, if a full table restore is not desired, specific `DELETE` and `INSERT` statements can be crafted to revert only the affected changes.*
3.  **Procedure Rollback:**
    *   Drop the newly deployed BigQuery Stored Procedures:
        ```sql
        DROP PROCEDURE IF EXISTS project.dataset.ausd_bp_ta_ibcp_ccid;
        DROP PROCEDURE IF EXISTS project.dataset.k_ausd_bp_ta_bcp_iccid;
        ```
    *   If previous versions of these procedures existed and were managed via version control, they can be redeployed.
4.  **Logging Table (Optional):**
    *   The `dwmsg_log` table can be retained for audit purposes, or its entries related to the failed migration can be marked/filtered. Dropping it is generally not recommended unless it's causing issues.
5.  **Verify Legacy System:**
    *   Confirm that the original `r_ausd_bp_ta_bcp_iccid.ksh` job is running successfully in its legacy environment.