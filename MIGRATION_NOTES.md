# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the legacy KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vertrag_tmp.ksh` and its associated SQL logic (`d_ausd_v_ta_vertrag_tmp.sql`). The job's primary function is to orchestrate data preparation for the `ta_vertrag_tmp` table, handling environment setup, parameter parsing, basic error management, and job status tracking.

The migration target platform is **Google BigQuery**. The KornShell orchestration logic has been re-implemented as a BigQuery Stored Procedure, and the core SQL data transformation logic has been translated to BigQuery Standard SQL. All involved data tables have been defined as BigQuery tables, and legacy job management/logging mechanisms have been replaced with dedicated BigQuery logging tables.

## 2. Generated artifacts

The following files were generated as part of this migration:

*   **`bigquery_ddl/sof_ta_vertrag_tmp.sql`**: BigQuery DDL script for the target table `sof_ta_vertrag_tmp`. This table stores the prepared contract data.
*   **`bigquery_ddl/via.sql`**: BigQuery DDL script for the target table `via`. This is a placeholder DDL; its full schema needs to be confirmed based on its usage in other SQL files.
*   **`bigquery_ddl/dwtk_meldungen.sql`**: BigQuery DDL script for the source table `dwtk_meldungen`. Used for determining the `v_datum` parameter.
*   **`bigquery_ddl/sof_ta_cntrct_crs3.sql`**: BigQuery DDL script for the primary source table `sof_ta_cntrct_crs3`. Contains core contract data.
*   **`bigquery_ddl/job_table.sql`**: BigQuery DDL script for a new `job_table`. This table centralizes job status, active job management, and overall job tracking, replacing the implicit logic from the legacy KornShell script.
*   **`bigquery_ddl/error_log.sql`**: BigQuery DDL script for a new `error_log` table. This table captures detailed error messages and context, replacing the functionality of `f_alis_msgerr.ksh` and `DWMSG_MeldeFehler`.
*   **`bigquery_ddl/job_result_log.sql`**: BigQuery DDL script for a new `job_result_log` table. This table stores metrics like processed record counts, replacing the use of temporary files in the legacy script.
*   **`bigquery_stored_procedures/r_ausd_vertrag_control.sql`**: The main BigQuery Stored Procedure. This procedure encapsulates the entire migrated logic, including parameter validation, job status management, the translated data transformation SQL from `d_ausd_v_ta_vertrag_tmp.sql`, and result logging. It replaces the `k_ausd_v_ta_vertrag_tmp.ksh` script.
*   **`bigquery_stored_procedures/log_error.sql`**: A helper BigQuery Stored Procedure for logging errors consistently into the `error_log` table and updating the `job_table` status.
*   **`bigquery_ddl/sof_ta_bp_ref.sql`**: BigQuery DDL for the `sof_ta_bp_ref` lookup table.
*   **`bigquery_ddl/sof_ta_inv_acc.sql`**: BigQuery DDL for the `sof_ta_inv_acc` lookup table.
*   **`bigquery_ddl/dwh_vi_s_rd_segment.sql`**: BigQuery DDL for the `dwh_vi_s_rd_segment` lookup table (originally a view).
*   **`bigquery_ddl/sof_ta_notice.sql`**: BigQuery DDL for the `sof_ta_notice` lookup table.
*   **`bigquery_ddl/sof_ta_barrier_zusgf.sql`**: BigQuery DDL for the `sof_ta_barrier_zusgf` lookup table.
*   **`bigquery_ddl/sof_ta_cntrct_templ.sql`**: BigQuery DDL for the `sof_ta_cntrct_templ` lookup table.
*   **`bigquery_ddl/sof_ta_cntrct_valid.sql`**: BigQuery DDL for the `sof_ta_cntrct_valid` lookup table.
*   **`bigquery_ddl/sof_ta_period.sql`**: BigQuery DDL for the `sof_ta_period` lookup table.
*   **`bigquery_ddl/sof_ta_vvl_upgrade.sql`**: BigQuery DDL for the `sof_ta_vvl_upgrade` lookup table.
*   **`bigquery_ddl/sof_ta_apn_ve.sql`**: BigQuery DDL for the `sof_ta_apn_ve` lookup table.
*   **`bigquery_ddl/sof_ta_action_assoc.sql`**: BigQuery DDL for the `sof_ta_action_assoc` lookup table.
*   **`bigquery_ddl/sof_vi_c_bfc.sql`**: BigQuery DDL for the `sof_vi_c_bfc` lookup table (originally a view).

## 3. Key design decisions

*   **Orchestration Re-platforming**: The KornShell script's control flow, parameter handling, and job management logic were re-implemented as a **BigQuery Stored Procedure (`r_ausd_vertrag_control`)**. This decision eliminates the need for external shell environments, `sqlplus` clients, and custom utility scripts, allowing for native execution within BigQuery and leveraging its built-in capabilities for scalability and performance.
*   **SQL Logic Consolidation**: The data transformation logic from `d_ausd_v_ta_vertrag_tmp.sql` was directly translated and embedded within the `r_ausd_vertrag_control` BigQuery Stored Procedure. This consolidates the entire job's logic into a single, manageable BigQuery object, simplifying deployment and maintenance.
*   **Centralized Job Management and Logging**: Instead of relying on temporary files, implicit job status updates, and custom shell-based error logging, dedicated BigQuery tables (`job_table`, `error_log`, `job_result_log`) were introduced. This provides a structured, queryable, and scalable mechanism for tracking job execution, errors, and results directly within the data warehouse.
*   **SQL Dialect Translation**: The original SQL (presumed Oracle PL/SQL) was translated to BigQuery Standard SQL. This was a necessary step to adapt the logic to the target platform, involving adjustments to data types, function syntax, and query constructs.
*   **"Retire" Bucket Consideration**: While the original job was categorized for "retire", the migration proceeded with a re-platforming to BigQuery. This implies a decision was made that the functionality provided by the job is still required, and a direct replacement on the new platform was preferred over full decommissioning. This trade-off ensures business continuity for the data preparation process.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**:
    *   Create the target BigQuery dataset, e.g., `your-gcp-project-id.your_bigquery_dataset`. This dataset will house all tables and stored procedures.
2.  **DDL Execution (Table Creation)**:
    *   Execute all DDL scripts located in the `bigquery_ddl/` directory. This will create the target tables (`sof_ta_vertrag_tmp`, `via`), auxiliary logging tables (`job_table`, `error_log`, `job_result_log`), and all necessary source/lookup tables (`dwtk_meldungen`, `sof_ta_cntrct_crs3`, `sof_ta_bp_ref`, etc.).
    *   **Crucial**: Replace `project.dataset` placeholders in all DDLs with your actual GCP project ID and BigQuery dataset ID.
3.  **Stored Procedure Creation**:
    *   Execute the stored procedure scripts located in the `bigquery_stored_procedures/` directory (`log_error.sql` and `r_ausd_vertrag_control.sql`).
    *   **Crucial**: Replace `project.dataset` placeholders in the stored procedures with your actual GCP project ID and BigQuery dataset ID.
4.  **Data Ingestion**:
    *   Ensure all source and lookup tables (e.g., `dwtk_meldungen`, `sof_ta_cntrct_crs3`, `sof_ta_bp_ref`, `sof_ta_inv_acc`, etc.) are populated with current and historical data in BigQuery. This is a critical prerequisite for the stored procedure to function correctly.
5.  **IAM Permissions**:
    *   Grant the necessary BigQuery permissions to the service account or user that will execute the stored procedure. This typically includes:
        *   `BigQuery Data Editor` role on the target dataset (for `INSERT`, `UPDATE`, `TRUNCATE` operations).
        *   `BigQuery Data Viewer` role on all source/lookup datasets/tables.
        *   `BigQuery Job User` role (to run BigQuery jobs, including stored procedures).
6.  **Scheduling Configuration**:
    *   Update the upstream orchestrator (e.g., Cloud Composer/Airflow, Scheduled Queries in BigQuery, or a re-platformed UC4 job) to invoke the new BigQuery Stored Procedure `r_ausd_vertrag_control`.
    *   Ensure the orchestrator passes the required parameters (`p_JobKennung`, `p_EintragsNr`) to the stored procedure.
    *   Decommission or reconfigure the legacy UC4 job and `r_ausd_v_ta_vertrag_tmp.ksh` script to prevent duplicate processing.

## 5. Known gaps & unresolved references

*   **`VIA` Table Schema**: The DDL for the `VIA` table (`bigquery_ddl/via.sql`) is a placeholder. Its actual schema (column names and data types) needs to be fully defined based on its usage in other parts of the legacy system or business requirements.
*   **Completeness of Source Table DDLs**: The DDLs for source and lookup tables (e.g., `dwtk_meldungen`, `sof_ta_cntrct_crs3`, and all `sof_ta_*` tables) are inferred from their usage in `d_ausd_v_ta_vertrag_tmp.sql`. It is possible that these tables contain additional columns in the legacy system that are not referenced by this specific job. A comprehensive schema review of the original source tables is recommended to ensure all relevant columns are migrated.
*   **Full Functionality of Legacy Utility Scripts**: The exact, intricate logic within the legacy KornShell utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, especially the `starteSQLSkript` function) was inferred during migration. While core functionalities have been replicated, any highly specific or complex business logic embedded within these utilities might require further review to ensure 100% functional parity.
*   **`v_datum` Logic Confirmation**: The calculation of `v_datum` using `MAX(m.timecreated)` from `dwtk_meldungen` with a default of `'19000101'` is a direct translation. The business logic behind this specific date calculation and the choice of default should be confirmed.
*   **Upstream Orchestration**: The migration of the upstream `r_ausd_v_ta_vertrag_tmp.ksh` script and its UC4 orchestrator is outside the scope of this document. These components must be migrated or reconfigured to correctly invoke the new BigQuery Stored Procedure.
*   **`project.dataset` Placeholders**: All generated code uses `project.dataset` as placeholders. These must be replaced with the actual GCP project ID and BigQuery dataset ID before deployment.

## 6. Validation

Validation should cover functional correctness, data integrity, and performance.

### How to run tests:

1.  **Environment Setup**:
    *   Ensure all BigQuery DDLs and Stored Procedures are deployed to a dedicated test BigQuery dataset.
    *   Load a representative sample of historical and current data into the BigQuery source/lookup tables (e.g., `dwtk_meldungen`, `sof_ta_cntrct_crs3`, etc.) in the test environment. This data should ideally mirror a production snapshot.
2.  **Execution**:
    *   Manually execute the `r_ausd_vertrag_control` stored procedure in BigQuery, providing test values for `p_JobKennung` and `p_EintragsNr`.
    *   Test with various scenarios:
        *   Valid parameters.
        *   Missing/empty parameters (to verify error handling).
        *   Scenarios that would trigger different branches of the `CASE` statements in the SQL logic.
3.  **Automated Testing (Optional but Recommended)**:
    *   If using Cloud Composer/Airflow, create a test DAG to invoke the BigQuery Stored Procedure with predefined parameters and assert outcomes.

### What "passing" means:

*   **Successful Deployment**: All DDLs and stored procedures are created in BigQuery without syntax errors.
*   **Functional Correctness**:
    *   The `r_ausd_vertrag_control` stored procedure executes successfully for valid input parameters without raising unhandled exceptions.
    *   For invalid or missing parameters, the procedure correctly logs an error to `error_log` and updates `job_table` with a 'FAILED' status.
*   **Data Integrity**:
    *   The data generated in the target tables (`sof_ta_vertrag_tmp`, `via`) in BigQuery is **identical** to the data produced by the legacy `k_ausd_v_ta_vertrag_tmp.ksh` job when run against the same source data. This requires a byte-by-byte or row-by-row comparison.
    *   Record counts in `sof_ta_vertrag_tmp` and `via` match the legacy output.
*   **Logging and Auditing**:
    *   The `job_table` is correctly updated with 'RUNNING', 'COMPLETED', or 'FAILED' statuses, along with start/end times and processed record counts.
    *   The `job_result_log` table accurately records the number of processed records for `sof_ta_vertrag_tmp`.
    *   The `error_log` table captures any errors with appropriate details (job_kennung, error_message, timestamp, etc.).
*   **Performance**: The execution time of the BigQuery Stored Procedure is comparable to or better than the legacy KornShell script's execution time.

## 7. Rollback procedure

In the event of critical issues detected after go-live, the following rollback procedure should be followed:

1.  **Immediate Action (Traffic Diversion)**:
    *   **Revert Orchestration**: Immediately disable or reconfigure the new BigQuery-based scheduler (e.g., Cloud Composer DAG, Scheduled Query) and reactivate the legacy UC4 job and `r_ausd_v_ta_vertrag_tmp.ksh` script. Ensure the legacy job is pointing to the original source and target systems.
2.  **Data Rollback**:
    *   **Target Tables**: If the `sof_ta_vertrag_tmp` or `via` tables in BigQuery were corrupted or incorrectly populated by the migrated job, restore them from the most recent valid backup or by re-running the legacy job to overwrite the incorrect data.
    *   **Logging Tables**: The `job_table`, `error_log`, and `job_result_log` tables are append-only or update-only for job status. While not typically "rolled back" in the same way as data tables, their entries can be marked or filtered if they contain misleading information from a failed run.
3.  **Code Rollback**:
    *   **BigQuery Stored Procedures**: Drop the `r_ausd_vertrag_control` and `log_error` stored procedures from the BigQuery dataset.
    *   **BigQuery Tables**: If necessary, drop the newly created `job_table`, `error_log`, `job_result_log` tables. The target tables (`sof_ta_vertrag_tmp`, `via`) and source/lookup tables should only be dropped if they were exclusively created for this migration and are not used by other processes.
4.  **Root Cause Analysis**:
    *   Analyze the logs (`error_log`, BigQuery audit logs, Cloud Logging) to identify the root cause of the failure.
    *   Address the identified issues in the BigQuery DDLs, stored procedures, or data ingestion processes.
    *   Re-test thoroughly in a non-production environment before attempting another deployment.