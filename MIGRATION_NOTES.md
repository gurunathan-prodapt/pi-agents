# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the `k_ausd_v_ta_c_bfc.ksh` job, which is responsible for calculating and managing "bindefrist" (binding period) data. The original job consisted of a KornShell (KSH) script (`k_ausd_v_ta_c_bfc.ksh`) for orchestration and an embedded Oracle SQL script (`d_ausd_v_ta_c_bfc.sql`) for core data transformation.

The job has been migrated to **Google BigQuery**. The KSH orchestration logic has been refactored into a BigQuery Stored Procedure (BQSP), and the Oracle SQL data transformation logic, including its custom function, has been converted into a separate BigQuery Stored Procedure and a User-Defined Function (UDF). All associated Oracle tables have been migrated or are planned for migration to BigQuery tables.

## 2. Generated artifacts

The migration process has generated the following BigQuery-native artifacts:

*   **`sql/ddl/isbert_schema_tables.sql`**
    *   **Role:** This file contains the Data Definition Language (DDL) statements for creating all necessary BigQuery tables. This includes the target tables (`sof$ta_c_bfc`), intermediate tables (`sof$ta_c_bfc_akt`), source tables replicated from Oracle (`dwtk_meldungen`, `sof$ta_cntrct_crs`, `sof$ta_barrier`, `sof$ta_cntrct_valid`, `sof$ta_period`), and auxiliary logging/job control tables (`job_error_log`, `job_table`, `job_run_log`). These tables replace their Oracle counterparts and provide the necessary structure for the migrated job.

*   **`sql/udf/bfc_get_bindefrist_udf.sql`**
    *   **Role:** This file defines a BigQuery User-Defined Function (UDF) named `bfc_get_bindefrist`. It is intended to replicate the logic of the original Oracle `bfc_get_bindefrist` function, which was critical for calculating binding period values. **Note:** As detailed in Section 5, this UDF is currently a placeholder and requires further implementation of the original Oracle function's logic.

*   **`sql/stored_procedures/d_ausd_v_ta_c_bfc_sp.sql`**
    *   **Role:** This BigQuery Stored Procedure encapsulates the core data transformation logic previously found in `d_ausd_v_ta_c_bfc.sql`. It performs data aggregation, initial population, merging of temporary data into the main target table, and updates based on procedure age. It utilizes the `bfc_get_bindefrist` UDF and handles Oracle-specific SQL constructs by translating them to BigQuery equivalents.

*   **`sql/stored_procedures/k_ausd_v_ta_c_bfc_sp.sql`**
    *   **Role:** This BigQuery Stored Procedure replaces the `k_ausd_v_ta_c_bfc.ksh` KornShell script. It acts as the orchestrator for the entire job, handling input parameters, performing validation, logging job execution status, and invoking the `d_ausd_v_ta_c_bfc_sp` for data transformation. It also captures the number of processed records.

## 3. Key design decisions

*   **Orchestration Migration (KSH to BQSP):** The KornShell script's orchestration logic was migrated directly into a BigQuery Stored Procedure (`k_ausd_v_ta_c_bfc_sp`). This approach leverages BigQuery's native procedural capabilities, simplifying deployment, reducing external dependencies (like shell environments), and centralizing the job's execution within the data platform. While Cloud Composer (Airflow) was considered for more complex scheduling, a direct BQSP was chosen for its simplicity and direct integration with BigQuery's execution model for this specific job.
*   **Data Transformation Migration (Oracle SQL to BQSP):** The complex Oracle SQL logic from `d_ausd_v_ta_c_bfc.sql` was translated into a BigQuery Stored Procedure (`d_ausd_v_ta_c_bfc_sp`). This allows for direct execution within BigQuery, leveraging its optimized query engine and eliminating the need for Oracle-specific features like `SQL*Plus` or `DB_LINK` for internal processing.
*   **Function Re-implementation (Oracle Function to BQ UDF):** The critical `bfc_get_bindefrist` Oracle function was designed to be re-implemented as a BigQuery UDF (`bfc_get_bindefrist_udf`). This maintains modularity and encapsulates the specific business logic for binding period calculation. The trade-off is the need for careful re-implementation of the original function's logic, which was not fully available in the provided source.
*   **External Dependency Handling (`DB_LINK:PCRS1`):** The external Oracle system `PCRS1` (accessed via `DB_LINK`) for `spr_schema.cds$vr_Bindefrist` and `all_objects` is planned to be handled either through data replication into BigQuery or BigQuery Federated Queries. Data replication (e.g., via Datastream) is generally preferred for performance and data freshness, bringing the external data directly into BigQuery tables. Federated queries are an option if the external system remains Oracle and only occasional, read-only access is sufficient, but they introduce latency and external system dependency during job execution. The current DDL assumes replication into BigQuery tables.
*   **Logging and Job Control:** The legacy job's implicit logging and job control mechanisms (e.g., `DWMSG_MeldeFehler`, `tmpFile` for record counts) have been replaced with dedicated BigQuery tables (`job_error_log`, `job_run_log`) and BigQuery's native `RAISE` and `EXCEPTION` handling. This provides centralized, queryable logging within BigQuery.
*   **Oracle-specific Constructs Removal:** Oracle hints (`/*+ append */`, `/*+ full(c) parallel(c,4) */`), `ROWNUM`, `NVL()`, `TO_DATE()`, `TRUNC()`, and `(+)` outer join syntax have been removed or translated to their BigQuery equivalents (`COALESCE()`, `PARSE_DATE()`, `DATE_TRUNC()`, `LEFT JOIN`). This ensures compatibility and leverages BigQuery's query optimizer. The `ROWNUM`-based `v_max_update` limit was removed as BigQuery does not have a direct equivalent for limiting `UPDATE` statements in this manner; if batching is critical, a different strategy would be required.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **GCP Project and BigQuery Dataset Setup:**
    *   Ensure a Google Cloud Project is set up.
    *   Create the BigQuery dataset, e.g., `isbert_schema`, within your GCP project.
    *   **Action:** `bq mk --dataset your-gcp-project:isbert_schema`

2.  **Update Placeholders in Generated Code:**
    *   Replace all instances of ``your-gcp-project.isbert_schema`` with your actual GCP Project ID and BigQuery Dataset ID in all generated SQL files (`sql/ddl/isbert_schema_tables.sql`, `sql/udf/bfc_get_bindefrist_udf.sql`, `sql/stored_procedures/d_ausd_v_ta_c_bfc_sp.sql`, `sql/stored_procedures/k_ausd_v_ta_c_bfc_sp.sql`).

3.  **Schema/Dataset Creation:**
    *   Execute the DDL statements in `sql/ddl/isbert_schema_tables.sql` to create all necessary BigQuery tables.
    *   **Action:** `bq query --use_legacy_sql=false < sql/ddl/isbert_schema_tables.sql`

4.  **Data Ingestion for Source Tables:**
    *   Ingest historical and ongoing data from the Oracle source tables (`isbert_schema.dwtk_meldungen`, `sof$ta_cntrct_crs`, `sof$ta_barrier`, `sof$ta_cntrct_valid`, `sof$ta_period`) into their corresponding BigQuery tables. This typically involves a one-time bulk load followed by continuous replication (e.g., using Google Cloud Datastream, Fivetran, or custom ETL).
    *   **Action:** Implement data ingestion pipelines for each source table.

5.  **External Data (`PCRS1`) Setup:**
    *   **Crucial Step:** Implement the strategy for accessing data from the `PCRS1` external Oracle system. This involves:
        *   **Replication:** Replicate `spr_schema.cds$vr_Bindefrist` and `all_objects` from `PCRS1` into BigQuery tables (e.g., `your-gcp-project.isbert_schema.all_objects`).
        *   **Federated Queries:** If replication is not chosen, set up BigQuery Federated Queries to access the `PCRS1` data directly. This might require setting up a Cloud SQL instance as a proxy or using third-party connectors.
    *   **Action:** Configure and deploy data replication or federated query mechanisms for `PCRS1` data.

6.  **Implement `bfc_get_bindefrist` UDF Logic:**
    *   **Critical Step:** The `sql/udf/bfc_get_bindefrist_udf.sql` file contains a placeholder UDF. The actual business logic of the original Oracle `Cds$vr_Bindefrist.GetBindeFrist` function (from `PCRS1`) must be extracted, understood, and accurately re-implemented in BigQuery SQL or JavaScript within this UDF.
    *   **Action:** Update `sql/udf/bfc_get_bindefrist_udf.sql` with the correct logic and deploy it.
    *   **Action:** `bq query --use_legacy_sql=false < sql/udf/bfc_get_bindefrist_udf.sql`

7.  **Deploy Stored Procedures:**
    *   Deploy the `d_ausd_v_ta_c_bfc_sp` and `k_ausd_v_ta_c_bfc_sp` BigQuery Stored Procedures.
    *   **Action:**
        *   `bq query --use_legacy_sql=false < sql/stored_procedures/d_ausd_v_ta_c_bfc_sp.sql`
        *   `bq query --use_legacy_sql=false < sql/stored_procedures/k_ausd_v_ta_c_bfc_sp.sql`

8.  **IAM/Permissions:**
    *   Grant the necessary Identity and Access Management (IAM) roles to the service account or user that will execute the BigQuery stored procedures. This typically includes `BigQuery Data Editor` (for modifying tables) and `BigQuery Job User` (for running jobs).
    *   **Action:** Configure IAM roles for the execution identity.

9.  **Scheduling:**
    *   Set up a scheduling mechanism to trigger the `k_ausd_v_ta_c_bfc_sp` BigQuery Stored Procedure at the required frequency. Options include:
        *   **Cloud Scheduler:** For simple cron-based scheduling.
        *   **Cloud Composer (Airflow):** For more complex workflows, dependency management, and monitoring.
    *   **Action:** Configure a Cloud Scheduler job or a Cloud Composer DAG to call `CALL `your-gcp-project.isbert_schema.k_ausd_v_ta_c_bfc_sp`('your_job_kennung', 'your_eintrags_nr', NULL);`

## 5. Known gaps & unresolved references

*   **`bfc_get_bindefrist` UDF Logic (B4 Item):** The most significant gap is the complete re-implementation of the `bfc_get_bindefrist` function. The provided `sql/udf/bfc_get_bindefrist_udf.sql` is a placeholder. The exact logic of the original Oracle `Cds$vr_Bindefrist.GetBindeFrist` package/function from the `PCRS1` system needs to be extracted, analyzed, and accurately translated into BigQuery SQL or JavaScript. Without this, the `bindefrist` calculation will be incorrect. This is a **Blocker for Go-Live (B4)**.
*   **External `PCRS1` System Details:** While a strategy for `PCRS1` access (replication/federated queries) has been outlined, the specific details of `spr_schema.cds$vr_Bindefrist` and `all_objects` (e.g., data types, volume, update frequency, and the internal logic of `Cds$vr_Bindefrist.GetBindeFrist`) are not fully known. A thorough assessment is required to ensure correct data ingestion/access and UDF re-implementation.
*   **`v_max_update` / ROWNUM Behavior:** The Oracle `UPDATE` statement used `ROWNUM <= &v_max_update` to limit the number of records updated in a single run. BigQuery does not have a direct equivalent for `ROWNUM` in an `UPDATE` context. The migrated BigQuery SP removes this limit. If batching updates is a critical requirement (e.g., due to performance or transaction size constraints), this part of the logic needs to be redesigned, potentially using `ROW_NUMBER()` in a subquery or by processing data in chunks.
*   **`v_bfc_procedure` Determination:** The `v_bfc_procedure` variable relies on querying the `all_objects` table for the creation date of `CDS$VR_BINDEFRIST`. It is crucial that the `isbert_schema.all_objects` BigQuery table is correctly populated with this metadata from the `PCRS1` system. If this data is missing or incorrect, the `bfc_procedure` date will be `NULL` or inaccurate, affecting subsequent date comparisons and updates.
*   **Comprehensive Error Handling and Logging:** While basic error logging to `job_error_log` is implemented, a review should be conducted to ensure it meets all legacy requirements for specific error codes, detailed stack traces, and integration with broader monitoring systems (e.g., Cloud Logging, Cloud Monitoring alerts).
*   **`starteSQLSkript` Abstraction:** The original KSH script used `starteSQLSkript` as an abstraction for `SQL*Plus` execution. While the core SQL logic is migrated, any additional functionalities embedded within `starteSQLSkript` (e.g., specific connection handling, pre/post-SQL execution hooks, advanced error reporting) need to be verified as covered by the BigQuery orchestration SP or deemed unnecessary.

## 6. Validation

Validation of the migrated job involves a multi-stage approach to ensure functional equivalence and data integrity.

### How to run the tests:

1.  **Data Preparation:**
    *   Ensure all BigQuery source tables (`dwtk_meldungen`, `sof$ta_cntrct_crs`, `sof$ta_barrier`, `sof$ta_cntrct_valid`, `sof$ta_period`, `all_objects`) are populated with a representative dataset identical to a known state in the Oracle source system.
    *   Ensure the `sof$ta_c_bfc` target table in BigQuery is either empty or in a known baseline state.

2.  **Execute the Orchestration Stored Procedure:**
    *   Call the main orchestration BigQuery Stored Procedure, providing sample parameters:
        ```sql
        DECLARE records_processed INT64;
        CALL `your-gcp-project.isbert_schema.k_ausd_v_ta_c_bfc_sp`('TEST_JOB_KENNUNG', '12345', records_processed);
        SELECT records_processed;
        ```
    *   Repeat this for different parameter combinations, including edge cases if known from the original job.

3.  **Monitor Logs:**
    *   Query the `job_run_log` and `job_error_log` tables in BigQuery to monitor the execution status and check for any errors.
    *   **Action:**
        ```sql
        SELECT * FROM `your-gcp-project.isbert_schema.job_run_log` ORDER BY start_time DESC LIMIT 10;
        SELECT * FROM `your-gcp-project.isbert_schema.job_error_log` ORDER BY log_time DESC LIMIT 10;
        ```

4.  **Data Comparison:**
    *   After the BigQuery job completes, compare the data in the BigQuery target table (`your-gcp-project.isbert_schema.sof$ta_c_bfc`) with the corresponding table in the Oracle source system (`sof$ta_c_bfc`) after an equivalent run of the original Oracle job.
    *   **Action:** Use data comparison tools or write SQL queries to compare row counts, specific column values (especially `bindefrist`, `bfc_age`, `bfc_count`), and checksums between the two systems.

### What "passing" means:

*   **Successful Execution:** The `k_ausd_v_ta_c_bfc_sp` completes without raising any unhandled exceptions. The `job_run_log` table shows a `status` of 'SUCCEEDED' for the run.
*   **No Errors Logged:** The `job_error_log` table contains no entries related to the execution of this job.
*   **Correct Record Count:** The `records_processed` output parameter from `k_ausd_v_ta_c_bfc_sp` matches the number of records expected to be processed (e.g., total rows in `sof$ta_c_bfc` or the count reported by the original Oracle job).
*   **Data Equivalence:**
    *   The row count in `your-gcp-project.isbert_schema.sof$ta_c_bfc` is identical to the row count in the Oracle `sof$ta_c_bfc` table after an equivalent run.
    *   All relevant columns (`cntrct_id`, `bindefrist`, `bfc_age`, `bfc_count`, `bfc_procedure`, `commitment_reference_date`, `cntrct_validity_id`) in `your-gcp-project.isbert_schema.sof$ta_c_bfc` match their counterparts in the Oracle `sof$ta_c_bfc` table for the same `cntrct_id`. This is especially critical for the `bindefrist` column, which relies on the re-implemented UDF.
*   **Performance:** The BigQuery job completes within acceptable performance thresholds, ideally matching or exceeding the performance of the original Oracle job.

## 7. Rollback procedure

In the event that the migrated BigQuery job encounters critical issues after go-live, the following rollback procedure should be followed:

1.  **Halt BigQuery Job Execution:**
    *   Immediately disable or delete any scheduled triggers (e.g., Cloud Scheduler jobs, Cloud Composer DAGs) that invoke the `k_ausd_v_ta_c_bfc_sp` BigQuery Stored Procedure.
    *   **Action:** Stop/delete Cloud Scheduler job or pause Cloud Composer DAG.

2.  **Re-enable Original Oracle Job:**
    *   Re-enable the scheduling and execution of the original `k_ausd_v_ta_c_bfc.ksh` job in the Oracle environment.
    *   **Action:** Revert any changes made to the Oracle scheduler (e.g., cron, Oracle Scheduler jobs) that disabled the original job.

3.  **Data Restoration (Conditional):**
    *   If the BigQuery job has modified the `sof$ta_c_bfc` table in BigQuery, and if these modifications are deemed incorrect or have caused data integrity issues that cannot be easily corrected, and if a full data rollback is required for the Oracle system:
        *   Restore the Oracle `sof$ta_c_bfc` table from a backup taken immediately *before* the migration cutover. This step is only necessary if the BigQuery job's output was used to update other systems or if the Oracle system's state needs to be reverted to a known good point.
    *   **Action:** Coordinate with the Oracle DBA team to perform a point-in-time recovery or restore of the `sof$ta_c_bfc` table if necessary.

4.  **Investigation and Remediation:**
    *   Analyze the `job_error_log` and `job_run_log` in BigQuery, along with any Cloud Logging entries, to identify the root cause of the issues.
    *   Address the identified problems in the BigQuery stored procedures, UDFs, or data ingestion pipelines.

5.  **Decommission BigQuery Assets (Optional):**
    *   Once the rollback is complete and the original Oracle job is stable, the BigQuery tables, UDFs, and stored procedures related to this migration can be deleted if they are no longer needed for testing or future re-migration attempts.
    *   **Action:** `bq rm -f -r your-gcp-project:isbert_schema` (to remove the entire dataset and its contents, use with extreme caution) or selectively delete individual tables, UDFs, and procedures.