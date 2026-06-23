# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the ETL job `k_ausd_bp_ta_rn_vertrag.ksh` from a KornShell-orchestrated Oracle SQL environment to Google BigQuery. The original job processed `PoolBasisprodukt` related data, reading from `DWTK_MELDUNGEN` and `SOF$TA_RN_EINZELN`, and writing to `SOF$TA_RN_VERTRAG`.

The migration involved:
*   **Source Platform:** KornShell script orchestrating Oracle SQL (`sqlplus`).
*   **Target Platform:** Google BigQuery, utilizing BigQuery Stored Procedures for orchestration and data transformation, and BigQuery tables for data storage and logging.

The primary goal was to achieve functional equivalence and leverage BigQuery's native capabilities for scalability, performance, and managed services.

## 2. Generated Artifacts

The migration produced the following BigQuery artifacts:

*   **`my_project.my_dataset/dwtk_meldungen_ddl.sql`**
    *   **Role:** BigQuery DDL (Data Definition Language) script to create the `dwtk_meldungen` table. This table serves as the BigQuery equivalent of the Oracle `DWTK_MELDUNGEN` table, used for tracking job metadata and determining the `v_datum_from_dwtk` value.
*   **`my_project.my_dataset/sof_ta_rn_einzeln_ddl.sql`**
    *   **Role:** BigQuery DDL script to create the `sof_ta_rn_einzeln` table. This table is the BigQuery equivalent of the Oracle `SOF$TA_RN_EINZELN` table, serving as a primary source for the data transformation. The schema is inferred based on common data patterns.
*   **`my_project.my_dataset/sof_ta_rn_vertrag_ddl.sql`**
    *   **Role:** BigQuery DDL script to create the `sof_ta_rn_vertrag` table. This table is the BigQuery equivalent of the Oracle `SOF$TA_RN_VERTRAG` table, serving as the target for the processed data. Its schema mirrors `sof_ta_rn_einzeln` as per the inferred transformation.
*   **`my_project.my_dataset/job_log_ddl.sql`**
    *   **Role:** BigQuery DDL script to create a `job_log` table. This table replaces the shell script's basic logging and temporary file usage, providing a structured and persistent record of job executions, including start/end times, status, and processed record counts.
*   **`my_project.my_dataset/error_log_ddl.sql`**
    *   **Role:** BigQuery DDL script to create an `error_log` table. This table captures detailed error information, including messages, stack traces, and run IDs, replacing the shell script's `DWMSG_MeldeFehler` and standard error output.
*   **`my_project.my_dataset/r_ausd_bp_ta_rn_vertrag_sp.sql`**
    *   **Role:** BigQuery Stored Procedure that encapsulates the entire migrated workflow. It handles parameter validation, date calculations, the core data transformation logic (derived from `d_ausd_bp_ta_rn_vertrag.sql`), and comprehensive logging to the `job_log` and `error_log` tables. This procedure replaces the `k_ausd_bp_ta_rn_vertrag.ksh` shell script and its invoked SQL.

## 3. Key Design Decisions

*   **Orchestration to BigQuery Stored Procedure:** The KornShell script's orchestration logic (parameter parsing, validation, date calculations, SQL execution) was fully translated into a BigQuery Stored Procedure (`r_ausd_bp_ta_rn_vertrag`). This centralizes the entire workflow within BigQuery, eliminating external dependencies on shell environments and `sqlplus`.
*   **Embedded Data Transformation:** The core SQL logic from `d_ausd_bp_ta_rn_vertrag.sql` was refactored and embedded directly within the BigQuery Stored Procedure. This avoids separate SQL files and allows for seamless execution and error handling within the procedural context.
*   **BigQuery Native Logging:** Shell-based logging and temporary files (`$DW_DIR_UTL/bert_k_ausd_bp_ta_rn_vertrag.tmp`) were replaced with dedicated BigQuery logging tables (`job_log`, `error_log`). This provides structured, queryable, and persistent logging, improving observability and debugging capabilities.
*   **BigQuery Date Functions:** Shell script date utilities (e.g., `gestern.ksh`, `DWDate_Datum_Check`) were replaced by BigQuery's rich set of date and time functions (`CURRENT_DATE()`, `DATE_SUB`, `FORMAT_DATE`, `SAFE.PARSE_DATE`, `REGEXP_CONTAINS`) for robust date handling and validation.
*   **Inferred Transformation Logic:** Based on the source inventory and the target table schema mirroring the source, the transformation from `SOF$TA_RN_EINZELN` to `SOF$TA_RN_VERTRAG` was inferred as a `GROUP BY cntrct_id` with `MAX` aggregations for other fields. This assumes that `SOF$TA_RN_VERTRAG` is a summarized or de-duplicated view of `SOF$TA_RN_EINZELN` based on `cntrct_id`.
*   **Handling `DWPA_UTIL_SKRIPT`:** The `TRUNCATE TABLE` operation was directly translated, and the `MAX(timecreated)` logic for `v_datum_from_dwtk` was implemented using BigQuery SQL, assuming these were the primary uses of the Oracle package. Any other complex functions from `DWPA_UTIL_SKRIPT` would require further analysis and re-implementation as BigQuery UDFs or equivalent SQL.
*   **Error Handling:** BigQuery's `EXCEPTION WHEN ERROR` block is used to catch and log errors, providing a structured way to manage failures and record them in the `error_log` table, similar to the original script's error reporting.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the BigQuery dataset `my_project.my_dataset` exists in your GCP project. If not, create it.
2.  **Schema Deployment (DDL Execution):**
    *   Execute the following DDL scripts in BigQuery to create the necessary tables:
        *   `my_project.my_dataset/dwtk_meldungen_ddl.sql`
        *   `my_project.my_dataset/sof_ta_rn_einzeln_ddl.sql`
        *   `my_project.my_dataset/sof_ta_rn_vertrag_ddl.sql`
        *   `my_project.my_dataset/job_log_ddl.sql`
        *   `my_project.my_dataset/error_log_ddl.sql`
3.  **Stored Procedure Deployment:**
    *   Execute the `my_project.my_dataset/r_ausd_bp_ta_rn_vertrag_sp.sql` script in BigQuery to create the stored procedure.
4.  **IAM Permissions:**
    *   Grant the service account or user that will execute the BigQuery Stored Procedure the following IAM roles:
        *   `BigQuery Data Editor` on `my_project.my_dataset` (for `INSERT`, `UPDATE`, `TRUNCATE` on tables).
        *   `BigQuery Job User` (to run BigQuery jobs, including stored procedures).
5.  **Data Ingestion for Source Tables:**
    *   Establish and configure a data ingestion pipeline to populate `my_project.my_dataset.dwtk_meldungen` and `my_project.my_dataset.sof_ta_rn_einzeln` with data from their respective Oracle sources. This is a critical prerequisite for the job to function correctly. Ensure the data is up-to-date and matches the expected schema.
6.  **Scheduling Configuration:**
    *   Configure a scheduling mechanism (e.g., Cloud Scheduler, Cloud Composer, or a custom orchestrator) to invoke the `my_project.my_dataset.r_ausd_bp_ta_rn_vertrag` stored procedure with the required parameters (`p_job_kennung`, `p_eintrags_nr`, `p_stichtag`, `p_wiederanlauf_wert`) at the desired frequency.

## 5. Known Gaps & Unresolved References

*   **`d_ausd_bp_ta_rn_vertrag.sql` Content:** The actual SQL code for `d_ausd_bp_ta_rn_vertrag.sql` was not available. The transformation logic implemented in `r_ausd_bp_ta_rn_vertrag_sp.sql` (specifically the `INSERT ... SELECT ... GROUP BY` statement) is an inference based on the target table schema and common ETL patterns. **This is a critical item for review and potential redesign (B4) if the original SQL had more complex logic.**
*   **`DWPA_UTIL_SKRIPT` Package Details:** The full functionality of the Oracle `DWPA_UTIL_SKRIPT` package is unknown. Only the `TRUNCATE TABLE` equivalent and the `MAX(timecreated)` logic for `v_datum_from_dwtk` were explicitly handled. If other functions from this package were used in `d_ausd_bp_ta_rn_vertrag.sql`, their logic needs to be identified and re-implemented in BigQuery (e.g., as UDFs or sub-procedures).
*   **`PoolBasisprodukt` Table:** The design document mentions `PoolBasisprodukt` as a source table, but it is not referenced in the generated BigQuery DDLs or the stored procedure logic. Its role in the original `d_ausd_bp_ta_rn_vertrag.sql` needs clarification. If it was used, its migration and integration into the BigQuery stored procedure are pending.
*   **Job Management (`FOSJobErzeugeEintrag`, `FOSJobDeaktivate`):** While commented out in the original script, these indicate a potential broader job management framework. The current BigQuery solution provides basic job logging. If a more integrated job management system is required, further development for a BigQuery-native equivalent or integration with existing GCP monitoring tools would be necessary.
*   **Data Type Mapping:** The DDLs use generic BigQuery data types (e.g., `STRING`, `DATE`, `INT64`). A precise mapping from Oracle data types to BigQuery data types should be verified against the actual Oracle schema to ensure data integrity and prevent truncation or conversion errors.

## 6. Validation

To validate the migrated job, follow these steps:

1.  **Prepare Test Data:**
    *   Ensure `my_project.my_dataset.dwtk_meldungen` and `my_project.my_dataset.sof_ta_rn_einzeln` contain representative test data that mirrors the Oracle source data. Include edge cases, nulls, and a sufficient volume of data.
    *   Optionally, populate `dwtk_meldungen` with a `BERT_DROP_TEMP_TABLE` entry to test the `v_datum_from_dwtk` logic.
2.  **Execute the Stored Procedure:**
    *   Open the BigQuery console or use the `bq` command-line tool.
    *   Execute the stored procedure with sample parameters:
        ```sql
        CALL my_project.my_dataset.r_ausd_bp_ta_rn_vertrag(
            'TEST_JOB_KENNUNG',
            '12345',
            '01012023', -- Example stichtag (DDMMYYYY)
            'RESTART_VALUE' -- Can be NULL or empty string if not applicable
        );
        ```
    *   Test with invalid parameters (e.g., missing `p_job_kennung`, malformed `p_stichtag`) to ensure validation logic works.
3.  **Check `job_log` Table:**
    *   Query `my_project.my_dataset.job_log` for the `run_id` generated during the execution.
    *   **Passing Criteria:**
        *   A record for `r_ausd_bp_ta_rn_vertrag` should exist with `status = 'SUCCESS'`.
        *   `start_time`, `end_time`, and `records_processed` should be populated correctly.
        *   The `message` field should indicate successful completion.
4.  **Check `error_log` Table (for failed runs):**
    *   If testing failure scenarios, query `my_project.my_dataset.error_log`.
    *   **Passing Criteria:**
        *   A record should exist for the failed run with `error_message` and `stack_trace` populated.
5.  **Verify Target Data (`sof_ta_rn_vertrag`):**
    *   Query `my_project.my_dataset.sof_ta_rn_vertrag`.
    *   **Passing Criteria:**
        *   The number of rows should match the expected output based on the input data and the inferred transformation logic.
        *   Sample rows should be compared against the expected output from the original Oracle job (if possible) or manually verified for correctness.
        *   Ensure data types and values are correctly preserved or transformed.
6.  **Performance Check:**
    *   Monitor the execution time of the BigQuery job in the BigQuery console. Compare it against the historical execution time of the Oracle job.

## 7. Rollback Procedure

In case of issues or critical failures after go-live, the following rollback procedure can be executed to revert to the original system:

1.  **Disable BigQuery Job Scheduling:**
    *   Immediately disable or delete the scheduler (e.g., Cloud Scheduler job, Cloud Composer DAG) that invokes the `my_project.my_dataset.r_ausd_bp_ta_rn_vertrag` stored procedure.
2.  **Re-enable Original Job Scheduling:**
    *   Re-enable the scheduling mechanism for the original `k_ausd_bp_ta_rn_vertrag.ksh` script in the legacy environment.
3.  **Verify Original Job Execution:**
    *   Monitor the execution of the original KornShell script to ensure it runs successfully and processes data as expected.
4.  **Data Reconciliation (if necessary):**
    *   If the BigQuery job partially processed data or introduced inconsistencies before rollback, a data reconciliation step might be required. This could involve:
        *   Restoring the `SOF$TA_RN_VERTRAG` table in Oracle from a recent backup taken before the BigQuery job's first execution.
        *   Manually correcting any data discrepancies in the Oracle target table.
    *   *Note: This step is highly dependent on the nature of the failure and the impact on data.*
5.  **Post-Rollback Analysis:**
    *   Analyze the `job_log` and `error_log` tables in BigQuery to understand the root cause of the failure before attempting re-migration.
6.  **Cleanup (Optional, after successful rollback):**
    *   Once the original system is stable, the BigQuery stored procedure and tables can be dropped if they are no longer needed for analysis or debugging.