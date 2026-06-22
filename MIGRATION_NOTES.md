# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the KornShell orchestration script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_evn.ksh` and its associated core logic (implicitly `k_ausd_bp_ta_cntrct_evn.ksh`).

The original job was responsible for orchestrating the initial provisioning of selected base products (contract events) for the BERT system. It extracted a snapshot of contract cache data from the Data Warehouse (DWH), handled date determination (cutoff date/Stichtag), and managed restart values for incremental processing, making the data available for demand scoring (Forderungsscoring).

The job has been migrated to **Google Cloud Platform (GCP)**, specifically to **BigQuery Stored Procedures** for the core logic and orchestration, with **Cloud Composer (Airflow) or Google Cloud Workflows** as the target scheduling and execution platform.

## 2. Generated Artifacts

The migration process generated the following BigQuery DDL and Stored Procedure files:

*   **`target/bigquery/ddl/job_log.sql`**
    *   **Role**: Defines the BigQuery table `project.dataset.job_log`. This table serves as a centralized, structured audit log for all job executions, replacing the file-based logging and custom `DWMSG_*` functions of the original KornShell script. It captures job details, timestamps, log levels, messages, and specific job parameters like `stichtag` and `restart_value`.
*   **`target/bigquery/ddl/job_control.sql`**
    *   **Role**: Defines the BigQuery table `project.dataset.job_control`. This table tracks the overall status and lifecycle of each job run, replacing the implicit job status management of the original script. It records job numbers, start/end times, and final status (OK/ERROR).
*   **`target/bigquery/ddl/dwh_ta_c_vertrag_source_placeholder.sql`**
    *   **Role**: Defines a placeholder BigQuery table `project.dataset.dwh_ta_c_vertrag_source`. This DDL represents the expected schema for the source contract data from the legacy DWH (`DWH$TA_C_VERTRAG`). It is crucial for the core processing logic and must be populated with actual DWH data before the job can run effectively.
*   **`target/bigquery/ddl/fos_target_table_placeholder.sql`**
    *   **Role**: Defines a placeholder BigQuery table `project.dataset.fos_target_table`. This table is the target for the processed contract event data, making it available for the downstream demand scoring (FOS) system. Its schema reflects the expected output of the data transformation.
*   **`target/bigquery/procedures/ausd_bp_ta_cntrct_evn_core.sql`**
    *   **Role**: Defines the BigQuery Stored Procedure `project.dataset.ausd_bp_ta_cntrct_evn_core`. This procedure encapsulates the core data extraction, transformation, and loading logic previously contained within `k_ausd_bp_ta_cntrct_evn.ksh`. It takes parameters like `p_stichtag` and `p_wiederanlaufWert` to filter and process data from the DWH source into the FOS target table.
*   **`target/bigquery/procedures/ausd_bp_ta_cntrct_evn_wrapper.sql`**
    *   **Role**: Defines the BigQuery Stored Procedure `project.dataset.ausd_bp_ta_cntrct_evn_wrapper`. This procedure replaces the orchestration logic of the original `r_ausd_bp_ta_cntrct_evn.ksh`. It handles parameter parsing, defaulting, validation, logging to `job_log` and `job_control`, and then calls the `ausd_bp_ta_cntrct_evn_core` procedure to execute the main data processing.

## 3. Key Design Decisions

The migration strategy involved several key design decisions to leverage BigQuery's capabilities and GCP's managed services:

*   **Orchestration Layer Migration**: The KornShell wrapper script (`r_ausd_bp_ta_cntrct_evn.ksh`) was migrated to a BigQuery Stored Procedure (`ausd_bp_ta_cntrct_evn_wrapper`).
    *   **Why**: This centralizes the job's execution within the BigQuery environment, allowing for native SQL parameter handling, logging, and error management. It also prepares the job for orchestration by Cloud Composer or Workflows, integrating seamlessly with GCP's data ecosystem.
    *   **Trade-offs**: Requires a separate orchestration tool (e.g., Cloud Composer) to trigger the BigQuery Stored Procedure, adding a layer of infrastructure management compared to a standalone shell script.
*   **Core Logic Encapsulation**: The core data processing logic (originally in `k_ausd_bp_ta_cntrct_evn.ksh`) was encapsulated into a separate BigQuery Stored Procedure (`ausd_bp_ta_cntrct_evn_core`).
    *   **Why**: Promotes modularity, reusability, and allows for pure SQL-based data transformations, leveraging BigQuery's performance and scalability. It separates orchestration concerns from data manipulation.
    *   **Trade-offs**: Requires careful translation of potentially complex shell-based data manipulation (if any) or external tool calls into BigQuery SQL.
*   **Structured Logging and Job Control**: File-based logging and custom job status updates were replaced by dedicated BigQuery tables (`job_log` and `job_control`).
    *   **Why**: Provides a queryable, centralized, and structured audit trail of all job executions. This significantly improves monitoring, debugging, and historical analysis compared to parsing log files.
    *   **Trade-offs**: Requires DDL creation and management for these new tables.
*   **Native Parameter Handling**: Shell `getopts` and variable assignments were replaced by BigQuery Stored Procedure input parameters and SQL logic for defaulting and validation.
    *   **Why**: Leverages BigQuery's native parameter capabilities, ensuring type safety and clear definition of inputs.
*   **Robust Error Handling**: Shell `trap` commands were replaced by BigQuery's `EXCEPTION WHEN ERROR` blocks.
    *   **Why**: Provides structured error handling within the SQL context, allowing for graceful failure, logging of error messages, and updating job status in `job_control`.
*   **Date and Utility Functions**: Custom shell date utilities were replaced by BigQuery's built-in date functions (`CURRENT_DATE()`, `PARSE_DATE()`).
    *   **Why**: Simplifies logic and relies on optimized, native BigQuery functions.
*   **Data Source and Target**: The DWH source (`DWH$TA_C_VERTRAG`) and FOS target tables are represented as BigQuery tables (`dwh_ta_c_vertrag_source_placeholder`, `fos_target_table_placeholder`).
    *   **Why**: Ensures all data processing occurs within BigQuery, benefiting from its performance and integration.
    *   **Trade-offs**: Requires a separate data ingestion pipeline to bring data from the legacy DWH into BigQuery.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **GCP Project and Dataset Setup**:
    *   Ensure the target GCP project (`project`) and BigQuery dataset (`dataset`) exist. If not, create them.
2.  **BigQuery Table Creation**:
    *   Execute the DDL scripts to create the necessary tables:
        *   `target/bigquery/ddl/job_log.sql`
        *   `target/bigquery/ddl/job_control.sql`
        *   `target/bigquery/ddl/dwh_ta_c_vertrag_source_placeholder.sql` (This is a placeholder; its schema must be finalized based on the actual DWH source.)
        *   `target/bigquery/ddl/fos_target_table_placeholder.sql` (This is a placeholder; its schema must be finalized based on the actual FOS requirements.)
3.  **BigQuery Stored Procedure Deployment**:
    *   Execute the SQL scripts to create or replace the stored procedures:
        *   `target/bigquery/procedures/ausd_bp_ta_cntrct_evn_core.sql`
        *   `target/bigquery/procedures/ausd_bp_ta_cntrct_evn_wrapper.sql`
4.  **Data Ingestion for Source Table**:
    *   Establish and configure a data ingestion pipeline to populate `project.dataset.dwh_ta_c_vertrag_source` with the actual data from the legacy `DWH$TA_C_VERTRAG` table. This might involve tools like Cloud Data Fusion, Dataflow, or custom ETL. The schema of `dwh_ta_c_vertrag_source` must accurately reflect the source.
5.  **IAM/Permissions Configuration**:
    *   Grant the service account used by Cloud Composer/Workflows (or any other orchestrator) the necessary BigQuery permissions:
        *   `BigQuery Data Editor` on `project.dataset` to read from source tables, write to `job_log`, `job_control`, and `fos_target_table`, and execute stored procedures.
        *   `BigQuery Job User` to run BigQuery jobs.
6.  **Scheduling Configuration**:
    *   Develop and deploy the Cloud Composer (Airflow) DAG or Google Cloud Workflow definition that will:
        *   Trigger the `project.dataset.ausd_bp_ta_cntrct_evn_wrapper` stored procedure.
        *   Pass the required parameters (`p_stichtag_str`, `p_wiederanlaufWert_input`) to the wrapper procedure.
        *   Configure the desired schedule (e.g., daily, weekly).
7.  **Secrets Management**:
    *   If any external connections or sensitive parameters are introduced (e.g., for data ingestion), ensure they are securely managed using Secret Manager. (Not directly applicable for the current BigQuery SPs, but relevant for the overall pipeline).

## 5. Known Gaps & Unresolved References

The following items were identified as unresolved or requiring further investigation during the migration design phase:

*   **`k_ausd_bp_ta_cntrct_evn.ksh` Logic (B4 Item)**: The actual business logic within the invoked core script (`k_ausd_bp_ta_cntrct_evn.ksh`) was *not available* during this migration. The `ausd_bp_ta_cntrct_evn_core` stored procedure currently contains a *placeholder* implementation based on assumptions from the wrapper script's purpose. **This is a critical gap and requires a full analysis and translation of the original `k_ausd_bp_ta_cntrct_evn.ksh` into BigQuery SQL.**
*   **DWH Source Table Schema**: The exact schema of `DWH$TA_C_VERTRAG` (or its BigQuery equivalent `dwh_ta_c_vertrag_source`) is unknown. The placeholder DDL includes example columns, but the full, accurate schema is essential for correct data extraction and transformation.
*   **`maxladedatum` Logic**: The commented-out logic in the original script regarding `MIN(sysdate,maxladedatum)` suggests a more complex date determination based on a `maxladedatum` for `DWH$TA_C_VERTRAG`. This logic needs to be fully understood and implemented in BigQuery if it's still a requirement.
*   **Utility Script Logic**: The exact functions performed by the sourced utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) were not fully detailed. While basic replacements are proposed, any complex or unique logic within them would need careful porting.
*   **Job Control `job_nr` Generation**: The current `job_control` table uses a simple `MAX(job_nr) + 1` for generating job numbers. For high-concurrency or highly critical production environments, a more robust mechanism (e.g., a dedicated sequence table, UUIDs, or BigQuery's native `GENERATE_UUID()`) might be considered to ensure uniqueness and prevent race conditions.
*   **FOS Integration Mechanism**: The exact mechanism by which the target FOS system consumes the data from the `fos_target_table` in BigQuery needs to be clearly defined and implemented. This could involve BigQuery APIs, exports to Cloud Storage, or direct table access for the FOS system.
*   **Idempotency of Core Logic**: The example `DELETE` statement in `ausd_bp_ta_cntrct_evn_core` is commented out. The original `k_ausd_bp_ta_cntrct_evn.ksh`'s handling of restarts and idempotency (e.g., whether it deletes and re-inserts, or performs upserts) needs to be fully understood and replicated in the BigQuery core procedure.

## 6. Validation

To validate the successful migration and functionality of the BigQuery job:

1.  **Prerequisites**: Ensure all manual setup steps (Section 4) are completed, especially the population of `dwh_ta_c_vertrag_source` with representative test data.
2.  **Manual Execution (for initial testing)**:
    *   Execute the wrapper stored procedure directly in BigQuery:
        ```sql
        -- Test with default stichtag and restart value
        CALL `project.dataset.ausd_bp_ta_cntrct_evn_wrapper`(NULL, NULL);

        -- Test with specific stichtag and default restart value
        CALL `project.dataset.ausd_bp_ta_cntrct_evn_wrapper`('01012023', NULL);

        -- Test with specific stichtag and restart value
        CALL `project.dataset.ausd_bp_ta_cntrct_evn_wrapper`('15032023', 1000);

        -- Test with invalid stichtag (should error)
        -- CALL `project.dataset.ausd_bp_ta_cntrct_evn_wrapper`('INVALID_DATE', NULL);
        ```
3.  **Orchestrated Execution**:
    *   Deploy the Cloud Composer DAG or Cloud Workflow.
    *   Trigger the DAG/Workflow manually or wait for its scheduled execution.
4.  **Verification of "Passing"**:
    *   **Job Control Table**: Query `project.dataset.job_control` to verify:
        *   A new entry exists for each execution.
        *   The `status` column for the latest run is `'OK'`.
        *   `start_ts` and `end_ts` are populated correctly.
        *   `error_message` is `NULL` (for successful runs).
    *   **Job Log Table**: Query `project.dataset.job_log` for the `job_nr` from the `job_control` table:
        *   Verify that expected `INFO` messages are present, indicating job start, core processing completion, and overall success.
        *   Confirm there are no `ERROR` level messages.
        *   Check that `stichtag` and `restart_value` parameters are logged correctly.
    *   **Target Data Verification**: Query `project.dataset.fos_target_table`:
        *   Verify that data has been inserted/updated as expected.
        *   Crucially, confirm that the data reflects the `p_stichtag` and `p_wiederanlaufWert` parameters used in the execution (e.g., `DWH_VERTRAG_ID` > `p_wiederanlaufWert`, `STICH_TAG` matches `p_stichtag`, and validity dates are respected).
        *   Perform data quality checks: row counts, specific data values, and schema adherence.
    *   **Error Scenarios**: Test with invalid parameters or by simulating upstream data issues (if possible) to ensure error handling and logging work as expected (i.e., `job_control` shows `ERROR` status, `job_log` contains `ERROR` messages).

## 7. Rollback Procedure

In case of critical issues or unexpected behavior after go-live, follow this rollback procedure:

1.  **Immediate Action**:
    *   **Disable Orchestration**: Immediately disable or pause the Cloud Composer DAG or Cloud Workflow that triggers the BigQuery job. This prevents further execution of the problematic migrated job.
    *   **Notify Stakeholders**: Inform relevant teams (e.g., data consumers, operations) about the rollback.
2.  **Revert Code (if applicable)**:
    *   **BigQuery Stored Procedures**: If changes were made to the BigQuery Stored Procedures (`ausd_bp_ta_cntrct_evn_wrapper`, `ausd_bp_ta_cntrct_evn_core`) that are causing issues, redeploy the previous, stable versions of these procedures.
    *   **BigQuery Tables**: If any DDL changes were made to `job_log`, `job_control`, or `fos_target_table` that are causing issues, revert them. This might involve dropping and recreating tables (losing data) or using BigQuery's time travel feature for schema changes if within the time travel window.
3.  **Data Rollback**:
    *   **Target Table (`fos_target_table`)**:
        *   **Option A (BigQuery Time Travel)**: If the issue is detected quickly and within BigQuery's time travel window (default 7 days), use `FOR SYSTEM_TIME AS OF` to query the table's state before the problematic run and restore it.
        *   **Option B (Backup/Snapshot)**: If a backup or snapshot of `fos_target_table` was taken before the go-live, restore the table from that backup.
        *   **Option C (Re-run with previous data)**: If the job is idempotent and the source data is stable, it might be possible to clear the `fos_target_table` and re-run the *previous successful version* of the job (if available) or the original legacy job to repopulate it.
    *   **Audit Tables (`job_log`, `job_control`)**: Typically, these tables are append-only. No rollback is usually required for them, but problematic entries can be marked or filtered out for reporting.
4.  **Re-enable Original Job**:
    *   If the original `r_ausd_bp_ta_cntrct_evn.ksh` job and its scheduling mechanism are still operational, re-enable them to ensure business continuity while the migrated job issues are resolved.
5.  **Post-Rollback Analysis**:
    *   Investigate the root cause of the issue with the migrated job.
    *   Apply necessary fixes to the BigQuery DDL, Stored Procedures, or orchestration.
    *   Thoroughly re-test in a non-production environment before attempting another go-live.