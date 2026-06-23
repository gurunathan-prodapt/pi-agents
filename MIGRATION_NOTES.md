# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell control script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh` and its dependent SQL script `d_ausd_bp_ta_bpr_apn.sql`. The original job orchestrated data processing, including parameter validation, SQL execution against an Oracle database, error handling, and optional file-based post-processing.

The migration targets Google Cloud Platform, specifically:
*   **BigQuery**: For data storage, processing logic (via Stored Procedures), and logging.
*   **Cloud Composer (Apache Airflow)**: For job orchestration and scheduling.

The core functionality of parameter handling, date validation, SQL execution, and error reporting has been translated into BigQuery Stored Procedures. The commented-out file post-processing logic has also been translated into BigQuery SQL, demonstrating its equivalent implementation.

## 2. Generated Artifacts

The migration produced the following BigQuery DDLs, Stored Procedures, and an Airflow DAG:

### 2.1 BigQuery DDLs (Table Schemas)

*   **`sql/ddl/dwtk_meldungen.sql`**:
    *   **Role**: Defines the schema for the `dwtk_meldungen` table in BigQuery, which replaces the legacy Oracle `DWTK_MELDUNGEN` table. This table serves as a source for the main processing logic.
*   **`sql/ddl/sof_ta_bpr_instance.sql`**:
    *   **Role**: Defines the schema for the `sof_ta_bpr_instance` table in BigQuery, replacing the legacy Oracle `SOF$TA_BPR_INSTANCE` table. This table is a source for the main processing logic.
*   **`sql/ddl/sof_ta_apn_carmen.sql`**:
    *   **Role**: Defines the schema for the `sof_ta_apn_carmen` table in BigQuery. This table is implicitly used in the join within `d_ausd_bp_ta_bpr_apn.sql` and is assumed to be a source for `ACCESS_POINT_NAME`.
*   **`sql/ddl/sof_ta_bpr_apn.sql`**:
    *   **Role**: Defines the schema for the `sof_ta_bpr_apn` table in BigQuery, which is the primary target table for the main processing logic, replacing the legacy Oracle `SOF$TA_BPR_APN` table.
*   **`sql/ddl/job_log.sql`**:
    *   **Role**: Defines the schema for a BigQuery audit log table. This table captures job execution details, status, record counts, and messages, replacing the functionality of `FOSJobErzeugeEintrag`.
*   **`sql/ddl/error_log.sql`**:
    *   **Role**: Defines the schema for a BigQuery error log table. This table records detailed error information, replacing the functionality of `f_alis_msgerr.ksh`.

### 2.2 BigQuery Stored Procedures

*   **`sql/stored_procedures/sp_d_ausd_bp_ta_bpr_apn.sql`**:
    *   **Role**: A BigQuery Stored Procedure that encapsulates the core data transformation logic previously found in `d_ausd_bp_ta_bpr_apn.sql`. It reads from `dwtk_meldungen`, `sof_ta_bpr_instance`, and `sof_ta_apn_carmen`, and inserts into `sof_ta_bpr_apn`. It also returns the number of records processed.
*   **`sql/stored_procedures/sp_k_ausd_bp_ta_bpr_apn.sql`**:
    *   **Role**: The main orchestration BigQuery Stored Procedure. It replaces the `k_ausd_bp_ta_bpr_apn.ksh` KornShell script. This procedure handles parameter validation, date derivation, calls `sp_d_ausd_bp_ta_bpr_apn`, and manages logging to `job_log` and `error_log` tables.

### 2.3 BigQuery Post-processing SQL (Optional)

*   **`sql/post_processing/cibasisprodukt_processor.sql`**:
    *   **Role**: A BigQuery SQL script that translates the commented-out `sed`, `sort`, and `join` operations from the original KornShell script. It assumes input tables `cibasis_data24`, `cibasis_data96`, and `cibasis_fax` and produces a final `cibasisprodukt` table. This script also includes DDLs for the assumed input tables.

### 2.4 Cloud Composer (Airflow) DAG

*   **`dags/k_ausd_bp_ta_bpr_apn_dag.py`**:
    *   **Role**: An Apache Airflow DAG that orchestrates the execution of the BigQuery Stored Procedures. It defines the sequence of tasks, passes parameters to `sp_k_ausd_bp_ta_bpr_apn`, and optionally executes the post-processing SQL.

## 3. Key Design Decisions

*   **Consolidation into BigQuery Stored Procedures**: The control flow, parameter handling, and SQL execution logic of the original KornShell script were migrated into a single BigQuery Stored Procedure (`sp_k_ausd_bp_ta_bpr_apn`). This centralizes the logic within the data warehouse, leveraging BigQuery's native capabilities for data processing and reducing the need for external shell scripting.
*   **Native BigQuery SQL for Core Logic**: The `d_ausd_bp_ta_bpr_apn.sql` logic was directly translated into a BigQuery Stored Procedure (`sp_d_ausd_bp_ta_bpr_apn`). This minimizes syntax changes and leverages BigQuery's optimized query engine.
*   **BigQuery for Logging and Error Handling**: Instead of file-based logs and custom shell error functions, dedicated BigQuery tables (`job_log`, `error_log`) are used. This provides structured, queryable logs and integrates error reporting directly into the BigQuery environment using `RAISE` and `EXCEPTION WHEN ERROR` constructs.
*   **Parameter Passing via Stored Procedure Arguments**: Command-line arguments from the KornShell script are mapped directly to `IN` parameters of the BigQuery Stored Procedures, ensuring type safety and clear interface definition.
*   **Native BigQuery Functions for Utilities**: Shell utility scripts (e.g., `gestern.ksh` for date derivation, `h_alis_date.ksh` for date validation) are replaced by equivalent BigQuery SQL functions (`CURRENT_DATE()`, `DATE_SUB()`, `SAFE.PARSE_DATE()`), eliminating external dependencies.
*   **Table-based Post-processing**: The commented-out file-based `sed`, `sort`, `join` operations are translated into standard BigQuery SQL queries using temporary tables and `REPLACE`, `DISTINCT`, `JOIN` clauses. This shifts from filesystem manipulation to efficient, scalable, and auditable table operations within BigQuery.
*   **Cloud Composer for Orchestration**: Apache Airflow provides robust scheduling, dependency management, and monitoring capabilities, replacing the simple shell-based invocation.
*   **Trade-offs**:
    *   **`DWPA_UTIL_SKRIPT` Complexity**: The functionality of `DWPA_UTIL_SKRIPT` requires detailed analysis and potential re-implementation as BigQuery UDFs or procedures, which can be a significant effort.
    *   **Join Logic Interpretation**: Translating complex shell `join` commands (especially with `-o` and `-a` options) to SQL requires careful interpretation to ensure equivalent data output, potentially leading to more verbose SQL (e.g., multiple `FULL OUTER JOIN`s).
    *   **Environment Variables**: The `. $HOME/.dw_init` environment setup is replaced by explicit parameter passing or BigQuery project/dataset context, requiring careful management of configuration.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps and prerequisites must be completed:

1.  **BigQuery Project and Dataset Setup**:
    *   Ensure the target BigQuery project (`your_project_id`) and dataset (`your_dataset_id`) exist. Replace these placeholders in all generated SQL and DAG files.
2.  **IAM Permissions**:
    *   Grant the service account used by Cloud Composer (Airflow) and any users executing BigQuery jobs the necessary IAM roles:
        *   `BigQuery Data Editor` (or `BigQuery Admin`) on the target dataset for creating/updating tables and stored procedures.
        *   `BigQuery Job User` for running queries and procedures.
        *   `Storage Object Admin` if `EXPORT DATA` to GCS is used for `cibasisprodukt.csv`.
3.  **Airflow Connection**:
    *   Ensure a `google_cloud_default` connection is configured in your Airflow environment, pointing to the correct Google Cloud project.
4.  **Source Data Ingestion**:
    *   **Crucial Step**: The BigQuery tables corresponding to the legacy Oracle sources (`dwtk_meldungen`, `sof_ta_bpr_instance`, `sof_ta_apn_carmen`) must be created using the provided DDLs and populated with data from the original Oracle system. This typically involves setting up a separate data ingestion pipeline (e.g., using Datastream, Dataflow, or batch loads to GCS then BigQuery).
5.  **`DWPA_UTIL_SKRIPT` Migration**:
    *   **Crucial Step**: The functionality of `PACKAGE:DWPA_UTIL_SKRIPT` (referenced in `d_ausd_bp_ta_bpr_apn.sql`) must be analyzed and migrated to BigQuery UDFs or stored procedures. These BigQuery objects must be created and available in the target dataset before `sp_d_ausd_bp_ta_bpr_apn` can function correctly.
6.  **Post-processing Input Tables (if activated)**:
    *   If the optional post-processing logic (`cibasisprodukt_processor.sql`) is to be used, the input tables (`cibasis_data24`, `cibasis_data96`, `cibasis_fax`) must be created and populated in BigQuery.
7.  **Airflow DAG Deployment and Configuration**:
    *   Upload the `dags/k_ausd_bp_ta_bpr_apn_dag.py` file to your Cloud Composer environment's DAGs folder.
    *   Define the `schedule` for the DAG as per business requirements (currently `None`).
    *   Adjust the `job_kennung_param`, `eintrags_nr_param`, `stichtag_param`, and `wiederanlauf_wert_param` in the DAG to reflect the actual values or use Airflow's parameterization features.
8.  **Review and Adjust `v_datum` Derivation**:
    *   In `sp_d_ausd_bp_ta_bpr_apn.sql`, the `v_datum` derivation assumes `p_Stichtag_raw` is the primary date. If the original `MAX(m.timecreated)` logic from `dwtk_meldungen` is the intended source for `v_datum`, the commented-out section in the SP needs to be uncommented and potentially adjusted.

## 5. Known Gaps & Unresolved References

The following items were identified during the migration and require further attention or are considered risks:

*   **`DWPA_UTIL_SKRIPT` Functionality**: The exact functions and logic within `PACKAGE:DWPA_UTIL_SKRIPT` are not fully known from the provided context. This package needs a thorough analysis to ensure its correct translation to BigQuery UDFs or stored procedures. This is a critical `B3: Manual` or `B4: Redesign` item.
*   **Error Code Mapping**: The legacy `ErrNr` (error number) system needs a defined mapping to BigQuery's error handling mechanisms or a custom error code system within BigQuery for consistent error reporting.
*   **Commented Code Activation**: The post-processing logic (`sed`, `sort`, `join`) was commented out in the original script. While a BigQuery equivalent (`cibasisprodukt_processor.sql`) has been provided, its activation implies additional development, testing, and data ingestion for its input tables.
*   **Invoking Script Context (`r_ausd_bp_ta_bpr_apn.ksh`)**: The migration of `k_ausd_bp_ta_bpr_apn.ksh` is part of a larger job structure invoked by `r_ausd_bp_ta_bpr_apn.ksh`. The migration of this parent job will need to be updated to call the new Airflow DAG or BigQuery Stored Procedure.
*   **`p_wiederanlaufWert` Idempotency**: The "restart/recovery value" (`p_wiederanlaufWert`) needs careful consideration. The current BigQuery Stored Procedure does not explicitly implement restart logic. If the original script had complex restart capabilities, these need to be designed and implemented in BigQuery to ensure idempotency or proper state management.
*   **`v_TabName='PoolBasisprodukt'`**: The usage and significance of this table name, especially in the context of `FOSJobDeaktivate` and `FOSJobErzeugeEintrag` (which are commented or external functions), require further investigation to determine if it has any implications for the BigQuery migration.
*   **`v_datum` Derivation in `sp_d_ausd_bp_ta_bpr_apn`**: The current implementation assumes `p_Stichtag_raw` is the source for `v_datum`. If the original logic intended to derive `v_datum` from `MAX(m.timecreated)` in `dwtk_meldungen` (as suggested by the commented code in the SP), this section requires manual review and adjustment to match the exact legacy behavior.

## 6. Validation

To validate the successful migration and functionality of the new BigQuery components:

1.  **Individual Component Testing**:
    *   **BigQuery DDLs**: Verify that all DDL scripts execute successfully and create the tables with the correct schemas in the target BigQuery dataset.
    *   **`sp_d_ausd_bp_ta_bpr_apn`**: Call this stored procedure directly from the BigQuery console with sample parameters.
        ```sql
        DECLARE records_processed INT64;
        CALL `your_project_id.your_dataset_id.sp_d_ausd_bp_ta_bpr_apn`('TEST_ENTRY', 'TEST_JOB', '01012023', records_processed);
        SELECT records_processed;
        ```
        Verify that data is inserted into `sof_ta_bpr_apn` and `records_processed` matches the expected count.
    *   **`sp_k_ausd_bp_ta_bpr_apn`**: Call this main orchestration stored procedure directly.
        ```sql
        CALL `your_project_id.your_dataset_id.sp_k_ausd_bp_ta_bpr_apn`('TEST_JOB_K', 'TEST_ENTRY_K', '01012023', 0);
        ```
        Check the `job_log` and `error_log` tables for entries.
    *   **`cibasisprodukt_processor.sql` (if activated)**: Execute the SQL script directly in BigQuery. Verify that `cibasisprodukt` table is created with the correct data.
2.  **End-to-End Testing via Airflow**:
    *   Trigger the `k_ausd_bp_ta_bpr_apn_migration_dag` in Cloud Composer.
    *   Monitor the DAG run in the Airflow UI for successful task completion.
    *   Verify the `job_log` table shows a `status = 'SUCCESS'` entry for the corresponding `run_id`.
    *   Confirm the `error_log` table is empty for successful runs.
    *   Check the `sof_ta_bpr_apn` table for the expected number of records and data content.
    *   If post-processing is active, verify the `cibasisprodukt` table (or GCS export) contains the expected output.
3.  **Data Validation**:
    *   **Record Counts**: Compare the `record_count` in the BigQuery `job_log` table with the record counts produced by the legacy KornShell script.
    *   **Data Content**: Perform a detailed comparison of the data in the target BigQuery table (`sof_ta_bpr_apn`) with the data produced by the legacy Oracle process. This can involve checksums, row-by-row comparisons, or aggregate checks.
    *   **Error Scenarios**: Test the BigQuery procedures with invalid parameters (e.g., missing `p_Stichtag`, invalid date format) to ensure error handling and logging (`error_log` table) work as expected.

**"Passing" Criteria**:
*   All Airflow DAG tasks complete successfully without errors.
*   The `job_log` table contains an entry for the job run with `status = 'SUCCESS'`.
*   The `error_log` table contains no entries for the successful job run.
*   The `sof_ta_bpr_apn` table contains the exact same data (record count and content) as produced by the legacy system for the same input parameters.
*   If post-processing is active, the `cibasisprodukt` table (or exported CSV) matches the legacy output.

## 7. Rollback Procedure

In case of issues during or after go-live, the following rollback procedure can be initiated:

1.  **Immediate Action (Airflow)**:
    *   **Pause/Delete DAG**: In the Cloud Composer UI, pause or delete the `k_ausd_bp_ta_bpr_apn_migration_dag` to prevent further execution of the migrated job.
2.  **Revert to Legacy System**:
    *   **Resume Original Job**: Reactivate the original KornShell script (`k_ausd_bp_ta_bpr_apn.ksh`) and ensure it is scheduled and running as before on the legacy platform.
    *   **Verify Legacy Output**: Confirm that the legacy job is producing the expected output in the Oracle database.
3.  **Data Rollback (BigQuery)**:
    *   **Target Table (`sof_ta_bpr_apn`)**:
        *   If the `sof_ta_bpr_apn` table was truncated or overwritten, use BigQuery's Time Travel feature to restore the table to its state before the problematic migration run.
            ```sql
            CREATE OR REPLACE TABLE `your_project_id.your_dataset_id.sof_ta_bpr_apn` AS
            SELECT * FROM `your_project_id.your_dataset_id.sof_ta_bpr_apn` FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL X MINUTE);
            ```
            (Adjust `X` to the appropriate time before the migration run).
        *   Alternatively, if a full backup was taken, restore from that backup.
    *   **Post-processing Output (`cibasisprodukt`)**: If the `cibasisprodukt` table was created or modified, drop it or restore it similarly if it's critical.
    *   **Log Tables**: The `job_log` and `error_log` tables are append-only. No specific rollback is needed, but entries from failed migration runs will remain.
4.  **Cleanup (Optional, after successful rollback)**:
    *   Once the legacy system is stable and the data is restored, the newly created BigQuery Stored Procedures (`sp_d_ausd_bp_ta_bpr_apn`, `sp_k_ausd_bp_ta_bpr_apn`) and potentially the DDLs for the target tables can be dropped from BigQuery if they are not needed for further testing or future migration attempts.
    *   Remove the DAG file from the Cloud Composer environment.

**Note**: This rollback procedure assumes that the legacy system remains operational and can be reactivated. It also relies on BigQuery's time travel capabilities for data recovery. For critical data, ensure robust backup and recovery strategies are in place.