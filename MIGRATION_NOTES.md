# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the data processing job originally defined by the KornShell (KSH) script `k_ausd_v_ta_barrier_zusgf.ksh` and its invoked Oracle SQL script `d_ausd_v_ta_barrier_zusgf.sql`. The original job processed barrier-related data from `sof$ta_barrier`, aggregated attributes, and inserted results into `sof$ta_barrier_zusgf`, also interacting with `isbert_schema.dwtk_meldungen` for a timestamp.

The job has been migrated to Google Cloud Platform, specifically to **BigQuery**. The KSH orchestration and Oracle SQL transformation logic have been consolidated into a single **BigQuery Stored Procedure**.

## 2. Generated Artifacts

The migration produced the following BigQuery artifact:

*   **`project.dataset.k_ausd_v_ta_barrier_zusgf` (BigQuery Stored Procedure)**
    *   **File**: `project/dataset/k_ausd_v_ta_barrier_zusgf.sql`
    *   **Role**: This stored procedure encapsulates the entire logic of the original KSH and Oracle SQL scripts. It handles parameter validation, retrieves the `v_datum` from `dwtk_meldungen`, truncates the target table, performs the core data aggregation and transformation using BigQuery SQL functions (`ARRAY_AGG`, `STRING_AGG`, `FORMAT_DATE`), and inserts the results into `project.dataset.sof_ta_barrier_zusgf`. It also includes error handling and logging to an `execution_log` table.

Additionally, the migration implicitly relies on the existence of:

*   **`project.dataset.execution_log` (BigQuery Table)**
    *   **Role**: A new table introduced to capture execution metadata, status, and error messages for the stored procedure, replacing the temporary file output and basic logging of the original KSH script.

## 3. Key Design Decisions

The following key design decisions were made during the migration:

*   **Consolidation into BigQuery Stored Procedure**: The KSH script's orchestration (parameter handling, error management, SQL execution) and the Oracle SQL script's data transformation logic were combined into a single BigQuery Stored Procedure. This simplifies deployment, execution, and monitoring within the BigQuery ecosystem.
*   **BigQuery SQL for Transformation**: Oracle's `PIPELINED` table function logic was re-implemented using standard BigQuery SQL constructs, primarily `ARRAY_AGG` for grouping and `STRING_AGG` for concatenation. This leverages BigQuery's native capabilities for efficient data processing.
*   **Direct Table Operations**: Oracle's `DWPA_UTIL_SKRIPT.runstatement` for truncation was replaced with a direct `TRUNCATE TABLE` statement in BigQuery, which is the idiomatic way to clear a table in BigQuery.
*   **Parameter Handling**: KSH command-line arguments (`Jobkennung`, `EintragsNr`) are directly mapped to input parameters of the BigQuery Stored Procedure (`p_JobKennung`, `p_EintragsNr`).
*   **Logging and Error Handling**: The KSH script's temporary file-based record count and basic error handling were replaced with a dedicated `execution_log` BigQuery table and BigQuery's `EXCEPTION WHEN ERROR` block, providing structured and centralized logging.
*   **Removal of `v_carmen` DB-Link**: The `DEFINE v_carmen = "@pcrs1"` reference was removed. The design assumes that data from the Carmen DB (specifically `sof_ta_barrier` and `dwtk_meldungen`) is pre-ingested into BigQuery as local tables, eliminating the need for federated queries or external database links within the stored procedure itself.
*   **`v_datum` Retrieval**: The logic to retrieve `v_datum` from `dwtk_meldungen` was directly translated into a `SELECT MAX(DATE(timecreated))` query against the BigQuery `dwtk_meldungen` table.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**: Ensure the target BigQuery dataset (`project.dataset`) exists.
2.  **Source Data Ingestion**:
    *   **`project.dataset.sof_ta_barrier`**: This table must be created in BigQuery, and data from the source Oracle `sof$ta_barrier` table must be ingested into it. An ongoing data ingestion pipeline (e.g., batch ETL, CDC) should be established if the source table is continuously updated.
    *   **`project.dataset.dwtk_meldungen`**: This table must be created in BigQuery, and data from the source Oracle `isbert_schema.dwtk_meldungen` table must be ingested into it. An ongoing data ingestion pipeline should be established.
3.  **Target Table Creation**:
    *   **`project.dataset.sof_ta_barrier_zusgf`**: Create this table in BigQuery with the appropriate schema.
        *   `cntrct_id`: `INT64` (or equivalent)
        *   `sperrart_alle`: `STRING` (consider max length, e.g., 500 characters if original had limits)
        *   `sperrgrund_alle`: `STRING` (consider max length, e.g., 500 characters if original had limits)
        *   `stilllegungszeitraum_alle`: `STRING` (consider max length, e.g., 100 characters if original had limits)
        *   `sperrgrund_zusgf`: `INT64`
    *   **`project.dataset.execution_log`**: Create this table in BigQuery with the following schema:
        *   `job_kennung`: `STRING`
        *   `eintrags_nr`: `STRING`
        *   `execution_timestamp`: `TIMESTAMP`
        *   `record_count`: `INT64` (nullable)
        *   `status`: `STRING` (`RUNNING`, `SUCCESS`, `FAILED`, `WARNING`)
        *   `message`: `STRING`
4.  **IAM Permissions**:
    *   The Google Cloud service account or user identity that will execute the BigQuery Stored Procedure must have the necessary IAM roles:
        *   `BigQuery Data Editor` on `project.dataset` (or more granular `bigquery.tables.getData`, `bigquery.tables.updateData`, `bigquery.tables.create`, `bigquery.routines.call` for the specific tables/procedure).
        *   `BigQuery Job User` to run BigQuery jobs.
5.  **Scheduling**:
    *   Configure an orchestration mechanism (e.g., Cloud Composer/Airflow DAG, Cloud Workflows, or BigQuery Scheduled Query) to invoke the `project.dataset.k_ausd_v_ta_barrier_zusgf` stored procedure.
    *   Ensure the `p_JobKennung` and `p_EintragsNr` parameters are correctly passed during scheduling.

## 5. Known Gaps & Unresolved References

The following items are flagged for follow-up or represent potential areas for further refinement (B4 items):

*   **`v_datum` Filter Application**: The original KSH script retrieves `v_datum` but the design document and generated code do not explicitly define how this `v_datum` should be used to filter the `sof_ta_barrier` source table. The generated code includes a commented-out placeholder (`-- WHERE (v_datum IS NULL OR <some_date_column> <= v_datum)`), indicating this is an unresolved aspect of the legacy logic's intent. **Action**: Clarify the intended use of `v_datum` for filtering and implement it if necessary.
*   **String Length Limits**: The original Oracle logic likely had implicit or explicit length constraints on concatenated strings (`sperrart_alle`, `sperrgrund_alle`, `stilllegungszeitraum_alle`). While BigQuery `STRING_AGG` does not have an inherent length limit, the target BigQuery table schema should reflect any required maximum lengths. The generated code includes comments about adding `SUBSTR` if needed. **Action**: Confirm if specific length limits are a functional requirement and apply `SUBSTR` functions in the `STRING_AGG` clauses if necessary.
*   **`v_carmen` DB-Link Data Ingestion**: The design assumes `sof_ta_barrier` and `dwtk_meldungen` are fully ingested into BigQuery. The robustness and latency of this ingestion pipeline (especially if the original `v_carmen` pointed to a frequently updated source) are critical and need to be thoroughly validated. **Action**: Verify the data ingestion strategy for `sof_ta_barrier` and `dwtk_meldungen` to ensure it meets the job's requirements for freshness and completeness.
*   **Exact Oracle `PIPELINED` Function Behavior**: While `ARRAY_AGG` and `STRING_AGG` closely mimic the concatenation logic, subtle differences in ordering or handling of `NULL`s within the Oracle `PIPELINED` function's state machine might exist. The BigQuery solution uses `ORDER BY` within `ARRAY_AGG` and `STRING_AGG` to maintain consistency, but thorough data validation is essential.
*   **Error Handling Granularity**: The BigQuery stored procedure provides basic `EXCEPTION WHEN ERROR` handling. However, the KSH script's `WHENEVER SQLERROR` and custom error messaging might have had more granular error codes or specific recovery actions. **Action**: Review the original KSH error handling for any specific scenarios that require more detailed BigQuery error management or orchestration-level alerts.

## 6. Validation

To validate the migrated job, follow these steps:

1.  **Prepare Test Data**:
    *   Ensure `project.dataset.sof_ta_barrier` and `project.dataset.dwtk_meldungen` contain a representative set of test data that mirrors the Oracle source, including edge cases (e.g., `NULL` values, multiple barriers for a `cntrct_id`, `ist_stillegung` variations).
    *   Record the expected output for `project.dataset.sof_ta_barrier_zusgf` based on the legacy system's execution with this test data.
2.  **Execute the Stored Procedure**:
    *   Manually execute the BigQuery stored procedure:
        ```sql
        CALL `project.dataset.k_ausd_v_ta_barrier_zusgf`('TEST_JOB', '12345');
        ```
    *   Alternatively, if orchestration is set up, trigger the test DAG/Scheduled Query.
3.  **Verify Execution Log**:
    *   Query `project.dataset.execution_log` to confirm an entry for the execution.
    *   **Passing**: The `status` column should be `SUCCESS`, and the `message` should indicate successful completion. If `status` is `WARNING`, investigate the message. If `status` is `FAILED`, examine the `message` for error details.
4.  **Verify Target Table Population**:
    *   Query `project.dataset.sof_ta_barrier_zusgf` to check if data has been inserted.
    *   **Passing**: The table should contain records, and the `record_count` in `execution_log` should match the number of rows in `sof_ta_barrier_zusgf`.
5.  **Data Content Validation**:
    *   Compare the data in `project.dataset.sof_ta_barrier_zusgf` with the expected output from the legacy system for the same input data.
    *   **Passing**:
        *   `cntrct_id` values match.
        *   `sperrart_alle`, `sperrgrund_alle`, `stilllegungszeitraum_alle` values are correctly concatenated and match the legacy output (considering potential ordering differences if not explicitly handled in legacy).
        *   `sperrgrund_zusgf` derivation is correct.
        *   All transformations (e.g., `REPLACE` for `sperrart`, `FORMAT_DATE` for `stilllegungszeitraum_alle`) are accurate.
6.  **Performance Check**: Monitor the BigQuery job execution time and cost to ensure it meets performance requirements.

## 7. Rollback Procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure can be initiated:

1.  **Stop New BigQuery Executions**: Immediately disable or pause any scheduled executions (e.g., Cloud Composer DAG, BigQuery Scheduled Query) of the `project.dataset.k_ausd_v_ta_barrier_zusgf` stored procedure.
2.  **Re-enable Legacy Job**: Re-enable the original KSH script (`k_ausd_v_ta_barrier_zusgf.ksh`) and its associated Oracle SQL job in the legacy environment.
3.  **Clear BigQuery Target Table (Optional but Recommended)**: If the BigQuery job has populated `project.dataset.sof_ta_barrier_zusgf` with incorrect or incomplete data, truncate the table:
    ```sql
    TRUNCATE TABLE `project.dataset.sof_ta_barrier_zusgf`;
    ```
    This ensures that if the BigQuery job is re-enabled later, it starts with a clean slate.
4.  **Verify Legacy System Operation**: Confirm that the legacy job is running successfully and populating the Oracle `sof$ta_barrier_zusgf` table as expected.
5.  **Review and Rectify**: Analyze the cause of the rollback. Address any identified issues in the BigQuery stored procedure, data ingestion, or orchestration before attempting another migration deployment.