# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell orchestration script `r_ausd_bp_ta_rn_einzeln.ksh` and its associated kernel script `k_ausd_bp_ta_rn_einzeln.ksh`. The original job was responsible for the initial provisioning of selected basic products (e.g., FAX, Data24) for the BERT system, creating a snapshot extraction of contract cache data from the Data Warehouse (DWH) for a downstream Forderungsscoring (FOS) system.

The migration target platform is Google Cloud BigQuery, leveraging BigQuery Stored Procedures for both the orchestration and core business logic. External scheduling and orchestration will be handled by Cloud Composer (Apache Airflow). This transition aims to modernize the ETL process, improve scalability, enhance logging and error handling capabilities, and integrate the job within the Google Cloud ecosystem.

## 2. Generated artifacts

The migration process has generated the following BigQuery DDLs, Stored Procedures, and an Airflow DAG:

*   **`sql/ddl/create_job_control_table.sql`**
    *   **Role:** Defines the schema for the `job_control` table. This table tracks the execution status, parameters, and outcomes of each job run, replacing the ad-hoc job status tracking of the original shell script.
*   **`sql/ddl/create_job_log_table.sql`**
    *   **Role:** Defines the schema for the `job_log` table. This table stores general informational, warning, and error messages generated during job execution, providing a structured and queryable log history.
*   **`sql/ddl/create_job_error_log_table.sql`**
    *   **Role:** Defines the schema for the `job_error_log` table. This table captures detailed error information, including error codes, messages, and stack traces, for robust error analysis and debugging.
*   **`sql/ddl/create_processing_audit_table.sql`**
    *   **Role:** Defines the schema for the `processing_audit` table. This table records summary metrics of data processing, such as records selected, deleted, and inserted, providing auditable insights into data transformations.
*   **`sql/ddl/create_contract_cache_source_table.sql`**
    *   **Role:** Defines the schema for `contract_cache_source`, the BigQuery equivalent of the original `DWH$TA_C_VERTRAG` table. This will serve as the primary data source for the migrated job.
*   **`sql/ddl/create_fos_target_table.sql`**
    *   **Role:** Defines the schema for `fos_target_table`, the BigQuery target table for the Forderungsscoring (FOS) system. Processed contract data will be inserted into this table.
*   **`sql/sp/k_ausd_bp_ta_rn_einzeln.sql`**
    *   **Role:** BigQuery Stored Procedure encapsulating the core business logic previously found in `k_ausd_bp_ta_rn_einzeln.ksh`. It handles data selection, filtering, and insertion into the target table, including the restart/resume mechanism.
*   **`sql/sp/r_ausd_bp_ta_rn_einzeln.sql`**
    *   **Role:** BigQuery Stored Procedure replacing the `r_ausd_bp_ta_rn_einzeln.ksh` orchestration script. It manages parameter parsing, defaulting, job control logging, error handling, and invokes the `k_ausd_bp_ta_rn_einzeln` stored procedure.
*   **`python/airflow/r_ausd_bp_ta_rn_einzeln_dag.py`**
    *   **Role:** An Apache Airflow DAG definition for Cloud Composer. This DAG is responsible for scheduling and triggering the `project.dataset.ausd_bp_ta_rn_einzeln` BigQuery Stored Procedure, passing necessary parameters.

## 3. Key design decisions

1.  **BigQuery Stored Procedures for Core Logic and Orchestration**:
    *   **Why**: The original KornShell script primarily performed orchestration and invoked a kernel script that likely contained SQL-like data manipulation. BigQuery Stored Procedures (BQSPs) offer a native, scalable, and performant way to execute complex SQL logic directly within BigQuery. This avoids the overhead and complexity of external processing engines (like Dataflow/Spark) for what is essentially a SQL-centric ETL task. It also simplifies the migration by directly translating shell-orchestrated SQL into BQ-native SQL.
    *   **Trade-offs**: While powerful for SQL, BQSPs are less suited for complex file system operations, external API calls, or highly procedural logic that doesn't map well to SQL. The `semi_auto` automation bucket for the original script suggests potential complexities that might require further refinement if the kernel script had non-SQL elements.

2.  **Dedicated BigQuery Tables for Logging and Job Control**:
    *   **Why**: The original script relied on shell functions (`DWMSG_*`) and file-based logs. Migrating to structured BigQuery tables (`job_control`, `job_log`, `job_error_log`, `processing_audit`) provides a centralized, queryable, and scalable logging solution. This significantly improves observability, debugging, and auditing capabilities compared to parsing text files.
    *   **Trade-offs**: Requires explicit `INSERT` and `UPDATE` statements within the BQSPs, adding verbosity compared to implicit logging in some frameworks.

3.  **Cloud Composer (Airflow) for External Orchestration**:
    *   **Why**: Airflow provides robust scheduling, dependency management, monitoring, and alerting capabilities, which are essential for production ETL pipelines. It replaces the original script's implicit scheduling and parameter passing mechanisms, offering a more enterprise-grade solution for managing job execution.
    *   **Trade-offs**: Introduces an additional component (Airflow environment) to manage and maintain.

4.  **Direct Parameter Mapping and Internal Defaulting**:
    *   **Why**: The original script's command-line parameters (`-s`, `-l`) are directly mapped to input parameters of the main BigQuery Stored Procedure. Defaulting logic (e.g., `p_wiederanlaufWert` to `0`, `p_stichtag` to `CURRENT_DATE()`) is implemented using BigQuery's `IFNULL` and `FORMAT_DATE` functions, mirroring the original script's behavior.
    *   **Trade-offs**: Requires careful handling of date formats (e.g., `DDMMYYYY` string to `DATE` conversion) within BigQuery SQL.

5.  **BigQuery's `BEGIN...EXCEPTION WHEN ERROR...END` for Error Handling**:
    *   **Why**: This BigQuery SQL construct provides a structured way to catch and handle errors within stored procedures, similar to `trap` in shell scripts. It allows for graceful error logging to `job_error_log` and `job_control` tables, ensuring that job failures are properly recorded and visible.
    *   **Trade-offs**: Requires explicit error handling blocks around critical operations; unhandled errors will still cause the procedure to fail.

6.  **Data Source and Target Migration to BigQuery Tables**:
    *   **Why**: `DWH$TA_C_VERTRAG` and the FOS target table are directly migrated to BigQuery tables (`contract_cache_source`, `fos_target_table`). This leverages BigQuery's columnar storage and distributed processing for efficient data access and manipulation.
    *   **Trade-offs**: Requires a one-time historical data load and ongoing data ingestion strategy for `contract_cache_source` if it's an active DWH table.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **Google Cloud Project and Dataset Setup**:
    *   Ensure the target Google Cloud Project (`project`) is correctly set up.
    *   Create the BigQuery dataset (`dataset`) where all tables and stored procedures will reside. Replace `project.dataset` placeholders in the generated code with your actual project ID and dataset ID.

2.  **IAM Permissions Configuration**:
    *   **BigQuery Service Account**: A service account (e.g., used by Cloud Composer or directly for BigQuery API calls) must have the following BigQuery roles:
        *   `BigQuery Data Editor` (to create/update/delete tables and insert/update data in `job_control`, `job_log`, `job_error_log`, `processing_audit`, `fos_target_table`).
        *   `BigQuery Job User` (to run queries and stored procedures).
        *   `BigQuery Data Viewer` (to read from `contract_cache_source`).
    *   **Cloud Composer Service Account**: If using Cloud Composer, its service account will need the above BigQuery permissions.

3.  **BigQuery DDL Execution**:
    *   Execute the DDL scripts to create the necessary tables in BigQuery:
        *   `sql/ddl/create_job_control_table.sql`
        *   `sql/ddl/create_job_log_table.sql`
        *   `sql/ddl/create_job_error_log_table.sql`
        *   `sql/ddl/create_processing_audit_table.sql`
        *   `sql/ddl/create_contract_cache_source_table.sql`
        *   `sql/ddl/create_fos_target_table.sql`

4.  **Initial Data Load for `contract_cache_source`**:
    *   Populate the `project.dataset.contract_cache_source` table with historical and current data from the original `DWH$TA_C_VERTRAG` table. This might involve a one-time bulk load using BigQuery Data Transfer Service, `bq load` command, or a custom Dataflow job.

5.  **BigQuery Stored Procedure Deployment**:
    *   Execute the `CREATE OR REPLACE PROCEDURE` statements for both stored procedures in BigQuery:
        *   `sql/sp/k_ausd_bp_ta_rn_einzeln.sql`
        *   `sql/sp/r_ausd_bp_ta_rn_einzeln.sql`

6.  **Cloud Composer (Airflow) Setup**:
    *   Ensure a Cloud Composer environment is provisioned and running.
    *   **GCP Connection**: Verify that the `google_cloud_default` connection (or your custom GCP connection) is correctly configured in Airflow, pointing to the appropriate GCP project and credentials.
    *   **DAG Deployment**: Upload the `python/airflow/r_ausd_bp_ta_rn_einzeln_dag.py` file to the DAGs folder of your Cloud Composer environment.
    *   **Schedule Configuration**: Configure the desired schedule for the DAG in Airflow. The example DAG uses `schedule=None`, meaning it needs to be triggered manually or via an external trigger. Adjust `schedule` to `@daily`, `0 5 * * *`, or as required.
    *   **Parameterization**: Review and adjust the `p_stichtag` and `p_wiederanlaufwert` parameters in the Airflow DAG. The example uses `{{ ds_nodash }}` for `p_stichtag` (YYYYMMDD format) and `0` for `p_wiederanlaufwert`. Ensure these match the expected input format and business logic.

## 5. Known gaps & unresolved references

1.  **Full Scope of `k_ausd_bp_ta_rn_einzeln.ksh` Logic**:
    *   **Description**: The detailed business logic of the original `k_ausd_bp_ta_rn_einzeln.ksh` was not fully available during this migration design. The generated `k_ausd_bp_ta_rn_einzeln.sql` stored procedure provides a plausible implementation based on common ETL patterns (filtering, deletion, insertion, restart logic).
    *   **Impact**: If the original kernel script contained complex non-SQL logic (e.g., extensive file I/O, external system calls, or highly procedural transformations not easily expressed in SQL), the current BigQuery Stored Procedure might be incomplete or require significant refactoring.
    *   **Follow-up**: A thorough review and detailed analysis of the original `k_ausd_bp_ta_rn_einzeln.ksh` is required to ensure the BigQuery stored procedure accurately replicates all business rules and functionalities. This is a critical B4 item.

2.  **`FOSHoleLadedatum` Functionality**:
    *   **Description**: The original KornShell script had a commented-out line `FOSHoleLadedatum "DWH\$TA_C_VERTRAG" v_ladedatum`, suggesting a mechanism to dynamically determine the `Stichtag` based on the maximum load date of the source table. The current BigQuery procedure defaults `p_stichtag` to `CURRENT_DATE()` if not provided.
    *   **Impact**: If the business requirement is to use a dynamic `Stichtag` derived from the source data's `ladedatum`, the current implementation might not fully match the original intent.
    *   **Follow-up**: Clarify the exact business rule for `Stichtag` determination. If dynamic derivation is needed, the `r_ausd_bp_ta_rn_einzeln` stored procedure will need to query `contract_cache_source` to determine the maximum `ladedatum` and use that as the effective `Stichtag` when `p_stichtag` is not provided.

3.  **Performance Optimization**:
    *   **Description**: While BigQuery is highly performant, the initial migration focuses on functional equivalence. The generated SQL might not be fully optimized for BigQuery's columnar storage and distributed query execution.
    *   **Impact**: Suboptimal performance could lead to higher costs or longer execution times.
    *   **Follow-up**: Post-migration, performance testing and optimization (e.g., partitioning, clustering, query tuning, materializing intermediate results) will be necessary.

4.  **Error Handling Granularity**:
    *   **Description**: The current error handling captures general errors at the procedure level. While sufficient for basic logging, it might not provide granular recovery points or specific error codes for every possible failure scenario within the complex `k_ausd_bp_ta_rn_einzeln` logic.
    *   **Impact**: Debugging specific data-related errors might require more detailed logging or conditional error handling within the kernel procedure.
    *   **Follow-up**: As the `k_ausd_bp_ta_rn_einzeln` logic is fully detailed, consider adding more specific `EXCEPTION WHEN ERROR` blocks or `IF` conditions to handle known data quality issues or business rule violations.

## 6. Validation

Validation ensures that the migrated job functions correctly and produces accurate results.

**How to run the tests:**

1.  **Unit Testing BigQuery Stored Procedures**:
    *   **`k_ausd_bp_ta_rn_einzeln`**: Call this procedure directly from the BigQuery console or a client tool with various `p_job_id`, `p_stichtag`, and `p_wiederanlaufwert` combinations.
        *   Test with `p_wiederanlaufwert = 0` (full run).
        *   Test with `p_wiederanlaufwert > 0` (restart scenario).
        *   Test with `p_stichtag` values that select different subsets of data.
        *   Test with `contract_cache_source` containing no data, some data, and data that should be filtered out.
        *   Verify the contents of `fos_target_table`, `job_log`, `job_error_log`, and `processing_audit` after each call.
    *   **`r_ausd_bp_ta_rn_einzeln`**: Call this procedure directly with and without `p_stichtag` and `p_wiederanlaufwert` parameters.
        *   Verify parameter defaulting logic.
        *   Verify `job_control` entries are created and updated correctly.
        *   Verify `job_log` and `job_error_log` entries are accurate.
        *   Verify the `k_ausd_bp_ta_rn_einzeln` is invoked and its results are reflected.

2.  **Integration Testing with Airflow**:
    *   Deploy the `r_ausd_bp_ta_rn_einzeln_dag.py` to Cloud Composer.
    *   Trigger the DAG manually with different `p_stichtag` and `p_wiederanlaufwert` values (e.g., by modifying the DAG or using Airflow's "Trigger DAG with config" feature).
    *   Monitor the DAG run status in the Airflow UI.
    *   Inspect BigQuery tables (`job_control`, `job_log`, `job_error_log`, `processing_audit`, `fos_target_table`) for expected outcomes.

3.  **Data Validation**:
    *   **Comparison with Source**: For a given `Stichtag`, compare the data in `project.dataset.fos_target_table` with the expected output derived from `project.dataset.contract_cache_source` based on the migration logic.
    *   **Record Counts**: Verify that `source_records_selected`, `target_records_deleted`, and `target_records_inserted` in `processing_audit` match the actual counts in the tables.
    *   **Data Integrity**: Check for data type mismatches, null values where not expected, and referential integrity if applicable.
    *   **Business Logic Validation**: Engage business users or subject matter experts to confirm that the output data meets business requirements for the FOS system.

**What "passing" means:**

*   **Airflow DAG Status**: The Airflow DAG run completes successfully (green status).
*   **Job Control Status**: The `status` column in `project.dataset.job_control` for the corresponding `job_id` is 'SUCCESS'.
*   **Error Logs**: The `project.dataset.job_error_log` table contains no entries for successful job runs. For failed runs, it should contain accurate and detailed error information.
*   **Processing Audit**: The `project.dataset.processing_audit` table accurately reflects the number of records processed (selected, deleted, inserted) for each run.
*   **Target Data Accuracy**: The `project.dataset.fos_target_table` contains the correct and expected data, matching the business logic and source data for the given `Stichtag` and `Wiederanlaufwert`. This is the ultimate measure of success.
*   **Performance**: The job completes within acceptable timeframes and resource consumption limits.

## 7. Rollback procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure can be executed:

1.  **Stop New Job Execution**:
    *   **Airflow**: Pause or delete the `r_ausd_bp_ta_rn_einzeln_bq_orchestration` DAG in Cloud Composer to prevent any further execution of the migrated job.

2.  **Revert BigQuery Stored Procedures (if necessary)**:
    *   If the `CREATE OR REPLACE PROCEDURE` statements were used to modify existing procedures and a previous version needs to be restored, execute the DDL for the previous stable versions of `project.dataset.ausd_bp_ta_rn_einzeln` and `project.dataset.k_ausd_bp_ta_rn_einzeln`. If the procedures were newly created, this step might involve deleting them.

3.  **Data Rollback (Critical)**:
    *   **`fos_target_table`**: This is the most critical step. If the `project.dataset.fos_target_table` was populated or modified by the migrated job, and the data is incorrect or corrupted, it must be restored.
        *   **Option A (Backup)**: Restore `fos_target_table` from a point-in-time snapshot or a backup taken just before the migration. BigQuery's time travel feature can be used to query data from a previous state if within the time travel window.
        *   **Option B (Re-processing by Original System)**: If the FOS system can tolerate a temporary data gap or re-processing, the original `r_ausd_bp_ta_rn_einzeln.ksh` job can be re-enabled to re-generate the data in its original target. This assumes the original target system is still operational and accessible.
        *   **Option C (Manual Correction)**: For minor issues, manual data correction in `fos_target_table` might be considered, but this is generally not recommended for large-scale data issues.

4.  **Re-enable Original System**:
    *   Ensure the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_einzeln.ksh` script and its associated scheduling mechanism are fully re-enabled and operational.

5.  **Cleanup (Optional)**:
    *   If the rollback is permanent, consider deleting the newly created BigQuery tables (`job_control`, `job_log`, `job_error_log`, `processing_audit`, `contract_cache_source`, `fos_target_table`) and stored procedures to avoid incurring unnecessary costs. However, retaining logging tables for post-mortem analysis might be beneficial.