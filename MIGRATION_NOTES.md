# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the legacy KornShell script `k_ausd_v_ta_acc_ref.ksh` and its dependent Oracle SQL script `d_ausd_v_ta_acc_ref.sql`. The original job orchestrated a data transformation process, truncating and inserting data into the `sof$ta_acc_ref` table based on a cut-off date derived from `dwtk_meldungen` and data from `cds$ta_acc_ref`.

The job has been migrated to **Google BigQuery**. The orchestration logic from the KornShell script and the data transformation logic from the Oracle SQL script have been re-implemented as BigQuery Stored Procedures. Auxiliary functions for job management, logging, and error handling have been replaced by dedicated BigQuery tables.

## 2. Generated artifacts

The migration process generated the following BigQuery DDL and Stored Procedure files:

*   **`project/dataset/ddl/dwtk_meldungen.sql`**
    *   **Role**: Defines the schema for the `dwtk_meldungen` table in BigQuery. This table serves as the BigQuery equivalent of the `isbert_schema.dwtk_meldungen` Oracle table, used to determine the data cut-off date.
*   **`project/dataset/ddl/cds_ta_acc_ref.sql`**
    *   **Role**: Defines the schema for the `cds_ta_acc_ref` table in BigQuery. This table serves as the BigQuery equivalent of the `cds$ta_acc_ref` Oracle table, which is the primary source for the data transformation.
*   **`project/dataset/ddl/sof_ta_acc_ref.sql`**
    *   **Role**: Defines the schema for the `sof_ta_acc_ref` table in BigQuery. This is the target table where the transformed data will be loaded, replacing the `sof$ta_acc_ref` Oracle table.
*   **`project/dataset/ddl/job_table.sql`**
    *   **Role**: Defines a BigQuery table for managing job status. It tracks active, completed, and failed job executions, replacing the shell script's internal job management logic.
*   **`project/dataset/ddl/error_log.sql`**
    *   **Role**: Defines a BigQuery table for centralized error logging. This replaces the shell script's `DWMSG_MeldeFehler` function and provides a structured, queryable log of job failures.
*   **`project/dataset/ddl/job_log.sql`**
    *   **Role**: Defines a BigQuery table for logging successful job execution details, including the number of records processed. This replaces the shell script's temporary file output for record counts.
*   **`project/dataset/stored_procedures/sp_d_ausd_v_ta_acc_ref_transform.sql`**
    *   **Role**: A BigQuery Stored Procedure that encapsulates the core data transformation logic. It translates the SQL from `d_ausd_v_ta_acc_ref.sql`, including determining the cut-off date, truncating the target table, and inserting filtered data from `cds_ta_acc_ref` into `sof_ta_acc_ref`.
*   **`project/dataset/stored_procedures/sp_k_ausd_v_ta_acc_ref_control.sql`**
    *   **Role**: A BigQuery Stored Procedure that serves as the main orchestration script. It replaces `k_ausd_v_ta_acc_ref.ksh`, handling parameter validation, job registration/deactivation, calling the transformation procedure, logging record counts, and managing error handling.

## 3. Key design decisions

The migration to BigQuery involved several key design decisions to ensure functional parity, performance, and maintainability:

*   **Orchestration in BigQuery Stored Procedures**: The KornShell script's orchestration logic (`k_ausd_v_ta_acc_ref.ksh`) was translated into a BigQuery Stored Procedure (`sp_k_ausd_v_ta_acc_ref_control`). This centralizes the entire job execution within BigQuery, eliminating external shell dependencies and leveraging BigQuery's native capabilities for control flow, parameter handling, and error management.
*   **Data Transformation in BigQuery Stored Procedures**: The Oracle SQL logic (`d_ausd_v_ta_acc_ref.sql`) was re-implemented as a separate BigQuery Stored Procedure (`sp_d_ausd_v_ta_acc_ref_transform`). This promotes modularity, allowing the transformation logic to be called independently if needed, and ensures optimal performance by executing directly within BigQuery's analytical engine.
*   **Centralized Job Management and Logging Tables**: Instead of relying on temporary files, shell variables, and custom logging functions, dedicated BigQuery tables (`job_table`, `error_log`, `job_log`) were introduced. This provides a scalable, queryable, and robust mechanism for tracking job status, logging errors, and recording execution metrics, significantly improving observability and debugging capabilities.
*   **Native BigQuery Data Sources**: The source Oracle tables (`isbert_schema.dwtk_meldungen`, `cds$ta_acc_ref`) are assumed to be ingested into BigQuery as native tables (`project.dataset.dwtk_meldungen`, `project.dataset.cds_ta_acc_ref`). This eliminates the complexity and performance overhead of federated queries or database links, allowing the transformation to run entirely within BigQuery.
*   **BigQuery SQL for Date Conversions**: Oracle-specific date functions (`TO_CHAR`, `TO_DATE`) were replaced with their BigQuery SQL equivalents (`FORMAT_DATE`, `PARSE_DATE`).
*   **Truncate and Insert Pattern**: The `TRUNCATE TABLE` operation was translated to `DELETE FROM ... WHERE TRUE;` followed by an `INSERT INTO ... SELECT` statement. This ensures the target table (`sof_ta_acc_ref`) is cleared before new data is loaded, maintaining the original job's behavior.
*   **Robust Error Handling**: The BigQuery stored procedures utilize `EXCEPTION WHEN ERROR THEN` blocks to catch and log errors to the `error_log` table, and update the `job_table` status to 'FAILED'. This provides a structured approach to error management, replacing the ad-hoc shell script error handling.

**Notable Trade-offs**:
*   **Initial Data Ingestion**: The migration requires a robust strategy for ingesting data from the source Oracle databases into BigQuery, which is an additional setup and maintenance overhead not present in the original Oracle-native setup.
*   **Complexity of BigQuery SQL**: While powerful, BigQuery Stored Procedures can be more verbose and complex than simple shell scripts for orchestration, requiring a deeper understanding of BigQuery SQL scripting.
*   **`PRIMARY KEY NOT ENFORCED`**: BigQuery's `PRIMARY KEY` constraint is not enforced, meaning data integrity checks for uniqueness must be handled explicitly if required, unlike traditional RDBMS.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**:
    *   Ensure the target BigQuery dataset (`project.dataset`) exists. If not, create it:
        ```sql
        CREATE SCHEMA `project.dataset`;
        ```
2.  **DDL Execution**:
    *   Execute all generated DDL scripts to create the necessary tables:
        *   `project/dataset/ddl/dwtk_meldungen.sql`
        *   `project/dataset/ddl/cds_ta_acc_ref.sql`
        *   `project/dataset/ddl/sof_ta_acc_ref.sql`
        *   `project/dataset/ddl/job_table.sql`
        *   `project/dataset/ddl/error_log.sql`
        *   `project/dataset/ddl/job_log.sql`
    *   These can be run using the BigQuery console, `bq` command-line tool, or a CI/CD pipeline.
3.  **Stored Procedure Creation**:
    *   Execute the generated stored procedure scripts to create the procedures:
        *   `project/dataset/stored_procedures/sp_d_ausd_v_ta_acc_ref_transform.sql`
        *   `project/dataset/stored_procedures/sp_k_ausd_v_ta_acc_ref_control.sql`
4.  **IAM/Permissions Configuration**:
    *   Ensure the service account or user identity that will execute the BigQuery stored procedures has the necessary IAM roles:
        *   `BigQuery Data Editor` on `project.dataset` to create/write/update tables.
        *   `BigQuery Data Viewer` on `project.dataset` to read from tables.
        *   `BigQuery Job User` to run jobs and stored procedures.
5.  **Initial Data Ingestion**:
    *   Set up and execute a process to ingest historical and ongoing data from the source Oracle tables (`isbert_schema.dwtk_meldungen`, `cds$ta_acc_ref`) into their respective BigQuery tables (`project.dataset.dwtk_meldungen`, `project.dataset.cds_ta_acc_ref`). This might involve tools like DataStream, Fivetran, or custom ETL pipelines.
6.  **Scheduling Configuration**:
    *   Integrate the execution of `CALL `project.dataset.sp_k_ausd_v_ta_acc_ref_control`('YOUR_JOB_KENNUNG', 'YOUR_EINTRAGS_NR');` into the target scheduling system (e.g., Cloud Composer/Airflow, Dataform, or BigQuery Scheduled Queries). Ensure the parameters `p_JobKennung` and `p_EintragsNr` are correctly passed.

## 5. Known gaps & unresolved references

The following items were identified during the design phase and remain as potential gaps or areas for further consideration:

*   **Job Management Logic in `starteSQLSkript`**: The exact, full implementation details of the `starteSQLSkript` function from the original KornShell environment (e.g., specific handling of job IDs, concurrency, or complex state transitions) were not fully available. The migrated `job_table` and its logic assume a standard job status management pattern (active, deactivated, completed, failed). Any subtle nuances of the original system's job management might require further refinement.
*   **Environment Variable Resolution (`BERT_DIR_ROOT`, `DW_DIR_UTL`)**: The original script relied on environment variables for directory paths. In BigQuery, these are replaced by explicit dataset and table names. If these variables influenced any other logic not directly related to file paths (e.g., dynamic configuration loading), that aspect would need to be addressed.
*   **Oracle-specific SQL Features**: While common Oracle SQL constructs were translated, there's always a risk of subtle Oracle-specific functions or behaviors in the full `d_ausd_v_ta_acc_ref.sql` that might not have been fully captured or perfectly replicated in BigQuery SQL. Thorough testing is crucial.
*   **Performance of Data Ingestion**: The design assumes that `cds_ta_acc_ref` and `dwtk_meldungen` are available in BigQuery. The performance and reliability of the data ingestion pipeline from Oracle to BigQuery are critical and need to be established and monitored independently.
*   **Error Code Semantics**: The original script used specific error codes (e.g., 193 for missing arguments). While the BigQuery procedure logs a generic error code or the specific one if set, the exact mapping and propagation of these legacy error codes to external systems might need further definition if strict compatibility is required.
*   **BigQuery `PRIMARY KEY NOT ENFORCED`**: The `job_table` DDL includes `PRIMARY KEY (job_kennung, eintrags_nr) NOT ENFORCED`. This means BigQuery does not automatically prevent duplicate entries based on this key. While the `ON CONFLICT DO UPDATE` clause in the control procedure handles updates for existing job entries, it's important to be aware that BigQuery will not prevent direct `INSERT` statements that violate this logical key.

## 6. Validation

To validate the successful migration and functionality of the BigQuery job, perform the following steps:

1.  **Prerequisites**: Ensure all manual steps (DDL, SP creation, data ingestion, IAM) are completed.
2.  **Test Data**: Populate `project.dataset.dwtk_meldungen` and `project.dataset.cds_ta_acc_ref` with representative test data that covers various scenarios (e.g., different `timecreated` values, `insert_at`, `modified_at`, `valid_from`, `valid_to` combinations, `is_production` values).
3.  **Execute the Control Procedure**:
    *   Run the main orchestration stored procedure with valid parameters:
        ```sql
        CALL `project.dataset.sp_k_ausd_v_ta_acc_ref_control`('TEST_JOB_KENNUNG_1', '001');
        ```
    *   Run with parameters that should trigger an error (e.g., missing `p_JobKennung`):
        ```sql
        CALL `project.dataset.sp_k_ausd_v_ta_acc_ref_control`(NULL, '002');
        ```
4.  **Verification of "Passing" Criteria**:

    *   **Successful Execution**:
        *   Query `project.dataset.job_table`: The entry for `('TEST_JOB_KENNUNG_1', '001')` should have `status = 'COMPLETED'`.
        *   Query `project.dataset.job_log`: A new entry should exist for `('TEST_JOB_KENNUNG_1', '001')` with a `record_count` matching the expected number of rows inserted into `sof_ta_acc_ref`.
        *   Query `project.dataset.sof_ta_acc_ref`: The table should contain the expected transformed data, matching the logic from `d_ausd_v_ta_acc_ref.sql` applied to the test data. The count of rows should match `record_count` in `job_log`.
        *   Query `project.dataset.error_log`: There should be no new entries related to `('TEST_JOB_KENNUNG_1', '001')`.
    *   **Error Handling Verification**:
        *   Query `project.dataset.job_table`: The entry for `(NULL, '002')` (or the last valid `job_kennung` if the error occurs before registration) should have `status = 'FAILED'`.
        *   Query `project.dataset.error_log`: A new entry should exist for the failed execution, containing the `error_code` (e.g., 193) and `error_message` (e.g., 'FEHLER: p_JobKennung darf nicht leer sein.').
        *   The BigQuery job execution should terminate with an error, indicating the failure to the calling orchestrator.
    *   **Data Parity**: Compare the data in `project.dataset.sof_ta_acc_ref` with the output of running the original Oracle job against the same source data. This is the ultimate validation of data transformation correctness.

## 7. Rollback procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure can be initiated:

1.  **Stop New BigQuery Job Executions**:
    *   Immediately pause or disable the BigQuery scheduled query or Cloud Composer DAG that invokes `sp_k_ausd_v_ta_acc_ref_control`.
2.  **Revert Scheduling to Original System**:
    *   Re-enable the original scheduling mechanism for `k_ausd_v_ta_acc_ref.ksh` in the legacy environment.
3.  **Data Restoration (if necessary)**:
    *   If the BigQuery job caused data corruption or incorrect data in `project.dataset.sof_ta_acc_ref`, and if this data is consumed by other systems, consider restoring `project.dataset.sof_ta_acc_ref` to a previous known good state using BigQuery's time travel capabilities or a backup.
    *   Alternatively, running the original `k_ausd_v_ta_acc_ref.ksh` job will repopulate the `sof$ta_acc_ref` table in Oracle, which might then be re-ingested into BigQuery, effectively overwriting any incorrect data.
4.  **Cleanup (Optional)**:
    *   If the rollback is permanent, the generated BigQuery stored procedures and tables (`job_table`, `error_log`, `job_log`, `sof_ta_acc_ref`) can be dropped to avoid resource consumption and clutter.
    *   **Caution**: Only drop `sof_ta_acc_ref` if its data is not consumed by other BigQuery processes or if it can be safely recreated.
    ```sql
    DROP PROCEDURE IF EXISTS `project.dataset.sp_k_ausd_v_ta_acc_ref_control`;
    DROP PROCEDURE IF EXISTS `project.dataset.sp_d_ausd_v_ta_acc_ref_transform`;
    DROP TABLE IF EXISTS `project.dataset.sof_ta_acc_ref`;
    DROP TABLE IF EXISTS `project.dataset.job_table`;
    DROP TABLE IF EXISTS `project.dataset.error_log`;
    DROP TABLE IF EXISTS `project.dataset.job_log`;
    -- Do NOT drop dwtk_meldungen or cds_ta_acc_ref unless they are exclusively for this job and no other BigQuery processes depend on them.
    ```